// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, TableReference};
use datafusion_expr::{
    AggregateUDF, LogicalTableSource, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::{ContextProvider, ParserOptions, SqlToRel};

struct CountingAllocator;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REQUESTED_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: The layout and contract are forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: The layout and contract are forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from the system allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            REQUESTED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        // SAFETY: The pointer, layout, and requested size are forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy)]
struct QueryCase {
    name: &'static str,
    sql: &'static str,
}

const WORKLOAD: &[QueryCase] = &[
    QueryCase {
        name: "point_read",
        sql: "SELECT id, amount, status FROM orders WHERE id = $1",
    },
    QueryCase {
        name: "range_read",
        sql: "SELECT id, customer_id, amount FROM orders WHERE customer_id = $1 AND created_at >= $2 ORDER BY created_at DESC LIMIT 50",
    },
    QueryCase {
        name: "insert",
        sql: "INSERT INTO orders (id, customer_id, amount, status) VALUES ($1, $2, $3, $4) RETURNING id",
    },
    QueryCase {
        name: "update",
        sql: "UPDATE orders SET amount = amount + $1, status = $2 WHERE id = $3 RETURNING id, amount",
    },
    QueryCase {
        name: "delete",
        sql: "DELETE FROM orders WHERE id = $1 AND status = $2 RETURNING id",
    },
    QueryCase {
        name: "join",
        sql: "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = $1 AND c.region_id = $2 ORDER BY o.id",
    },
    QueryCase {
        name: "wide_join",
        sql: "SELECT o.id, c.name, r.name AS region_name, p.name AS product_name, l.quantity, l.price FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id JOIN line_items l ON l.order_id = o.id JOIN products p ON p.id = l.product_id WHERE o.created_at >= $1 AND o.status IN ('open', 'paid')",
    },
    QueryCase {
        name: "aggregate",
        sql: "SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total_amount, AVG(amount) AS average_amount FROM orders WHERE created_at >= $1 GROUP BY customer_id HAVING SUM(amount) > $2 ORDER BY total_amount DESC",
    },
    QueryCase {
        name: "cte",
        sql: "WITH recent AS (SELECT id, customer_id, amount FROM orders WHERE created_at >= $1), totals AS (SELECT customer_id, SUM(amount) AS total FROM recent GROUP BY customer_id) SELECT c.id, c.name, t.total FROM customers c JOIN totals t ON t.customer_id = c.id WHERE t.total > $2",
    },
    QueryCase {
        name: "correlated",
        sql: "SELECT c.id, c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > $1) AND c.region_id IN (SELECT r.id FROM regions r WHERE r.status = 'active')",
    },
    QueryCase {
        name: "union",
        sql: "SELECT id, name, 'customer' AS kind FROM customers WHERE status = $1 UNION ALL SELECT id, name, 'product' AS kind FROM products WHERE status = $2 ORDER BY id LIMIT 100",
    },
];

struct BenchProvider {
    options: ConfigOptions,
    schema: SchemaRef,
}

impl BenchProvider {
    fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("product_id", DataType::Int64, false),
            Field::new("region_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("quantity", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        Self {
            options: ConfigOptions::default(),
            schema,
        }
    }
}

impl ContextProvider for BenchProvider {
    fn get_table_source(&self, _name: TableReference) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(LogicalTableSource::new(Arc::clone(&self.schema))))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match name {
            "avg" => Some(datafusion_functions_aggregate::average::avg_udaf()),
            "count" => Some(datafusion_functions_aggregate::count::count_udaf()),
            "max" => Some(datafusion_functions_aggregate::min_max::max_udaf()),
            "min" => Some(datafusion_functions_aggregate::min_max::min_udaf()),
            "sum" => Some(datafusion_functions_aggregate::sum::sum_udaf()),
            _ => None,
        }
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        vec![
            "avg".to_string(),
            "count".to_string(),
            "max".to_string(),
            "min".to_string(),
            "sum".to_string(),
        ]
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

fn planner<'a>(provider: &'a BenchProvider) -> SqlToRel<'a, BenchProvider> {
    SqlToRel::new_with_options(
        provider,
        ParserOptions::default()
            .with_parse_float_as_decimal(true)
            .with_enable_ident_normalization(true)
            .with_map_string_types_to_utf8view(false),
    )
}

fn parse_and_plan(provider: &BenchProvider, case: &QueryCase) {
    let mut statements =
        DFParser::parse_sql(case.sql).expect("benchmark SQL should parse");
    let statement = statements
        .pop_front()
        .expect("benchmark SQL should contain one statement");
    assert!(statements.is_empty());
    black_box(
        planner(provider)
            .statement_to_plan(statement)
            .expect("benchmark SQL should plan"),
    );
}

fn run_workload(provider: &BenchProvider) {
    for case in WORKLOAD {
        parse_and_plan(provider, case);
    }
}

fn allocation_report() {
    let provider = BenchProvider::new();
    run_workload(&provider);

    ALLOCATIONS.store(0, Ordering::Relaxed);
    REALLOCATIONS.store(0, Ordering::Relaxed);
    REQUESTED_BYTES.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::SeqCst);
    run_workload(&provider);
    COUNTING.store(false, Ordering::SeqCst);

    println!("pipeline       SQL   allocs  reallocs   bytes allocated");
    println!("-------------- --- -------- -------- ---------------");
    println!(
        "parse_plan     {:>3} {:>8} {:>8} {:>15}",
        WORKLOAD.len(),
        ALLOCATIONS.load(Ordering::Relaxed),
        REALLOCATIONS.load(Ordering::Relaxed),
        REQUESTED_BYTES.load(Ordering::Relaxed),
    );
}

fn criterion_report() {
    let provider = BenchProvider::new();
    run_workload(&provider);

    let sql_bytes = WORKLOAD.iter().map(|case| case.sql.len() as u64).sum();
    let mut criterion = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(6))
        .sample_size(40)
        .configure_from_args();
    let mut group = criterion.benchmark_group("sql_planner");
    group.throughput(Throughput::Bytes(sql_bytes));
    group.bench_function("parse_plan/workload", |bencher| {
        bencher.iter(|| run_workload(black_box(&provider)));
    });
    for case in WORKLOAD {
        group.throughput(Throughput::Bytes(case.sql.len() as u64));
        group.bench_function(format!("parse_plan/{}", case.name), |bencher| {
            bencher.iter(|| parse_and_plan(black_box(&provider), black_box(case)));
        });
    }
    group.finish();
    criterion.final_summary();
}

fn main() {
    if std::env::args().any(|arg| arg == "--allocation-report") {
        allocation_report();
    } else {
        criterion_report();
    }
}
