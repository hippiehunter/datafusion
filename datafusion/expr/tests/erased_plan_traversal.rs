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

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{
    EmptyRelation, Filter, LogicalPlan, Projection, lit, scalar_subquery,
};

fn sample_plan() -> LogicalPlan {
    let schema = Arc::new(
        DFSchema::try_from(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
            .expect("test schema"),
    );
    let branch = || {
        let mut plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::clone(&schema),
        });
        for _ in 0..4 {
            plan = LogicalPlan::Filter(
                Filter::try_new(lit(true), Arc::new(plan)).expect("test filter"),
            );
        }
        plan
    };
    let subquery = branch();
    LogicalPlan::Projection(
        Projection::try_new(
            vec![scalar_subquery(Arc::new(subquery))],
            Arc::new(branch()),
        )
        .expect("test projection"),
    )
}

// This is the formerly generic recursive implementation. Keeping the reference
// version in a test lets us prove that callback erasure changes dispatch only,
// not traversal order or TreeNodeRecursion behavior.
fn generic_apply<F>(node: &LogicalPlan, f: &mut F) -> Result<TreeNodeRecursion>
where
    F: FnMut(&LogicalPlan) -> Result<TreeNodeRecursion>,
{
    f(node)?.visit_children(|| {
        node.apply_subqueries(|child| generic_apply(child, f))?
            .visit_sibling(|| node.apply_children(|child| generic_apply(child, f)))
    })
}

fn generic_transform_up<F>(
    node: LogicalPlan,
    f: &mut F,
) -> Result<Transformed<LogicalPlan>>
where
    F: FnMut(LogicalPlan) -> Result<Transformed<LogicalPlan>>,
{
    node.map_subqueries(|child| generic_transform_up(child, f))?
        .transform_sibling(|node| {
            node.map_children(|child| generic_transform_up(child, f))
        })?
        .transform_parent(f)
}

fn node_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::EmptyRelation(_) => "empty",
        LogicalPlan::Filter(_) => "filter",
        LogicalPlan::Projection(_) => "projection",
        LogicalPlan::Subquery(_) => "subquery",
        _ => "other",
    }
}

#[test]
fn erased_apply_matches_the_generic_recursive_walk() {
    let plan = sample_plan();
    let mut generic_trace = Vec::new();
    generic_apply(&plan, &mut |node| {
        generic_trace.push(node_kind(node));
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("generic walk");

    let mut erased_trace = Vec::new();
    plan.apply_with_subqueries(|node| {
        erased_trace.push(node_kind(node));
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("erased walk");

    assert_eq!(erased_trace, generic_trace);
}

#[test]
fn erased_transform_up_matches_the_generic_recursive_walk() {
    let plan = sample_plan();
    let mut generic_trace = Vec::new();
    let generic = generic_transform_up(plan.clone(), &mut |node| {
        generic_trace.push(node_kind(&node));
        Ok(Transformed::no(node))
    })
    .expect("generic transform");

    let mut erased_trace = Vec::new();
    let erased = plan
        .transform_up_with_subqueries(|node| {
            erased_trace.push(node_kind(&node));
            Ok(Transformed::no(node))
        })
        .expect("erased transform");

    assert_eq!(erased_trace, generic_trace);
    assert_eq!(erased, generic);
}

fn time_transform(plan: &LogicalPlan, iterations: usize, erased: bool) -> Duration {
    let start = Instant::now();
    for _ in 0..iterations {
        let mut visited = 0usize;
        let transformed = if erased {
            plan.clone().transform_up_with_subqueries(|node| {
                visited += 1;
                Ok(Transformed::no(node))
            })
        } else {
            generic_transform_up(plan.clone(), &mut |node| {
                visited += 1;
                Ok(Transformed::no(node))
            })
        }
        .expect("benchmark transform");
        black_box((transformed, visited));
    }
    start.elapsed()
}

#[test]
#[ignore = "manual microbenchmark; reports rather than asserting timing"]
fn report_erased_transform_dispatch_cost() {
    let plan = sample_plan();
    let iterations = 50_000;
    black_box(time_transform(&plan, 1_000, false));
    black_box(time_transform(&plan, 1_000, true));

    // Run both orders so ambient load or frequency scaling does not
    // systematically favor either implementation.
    let generic_first = time_transform(&plan, iterations, false);
    let erased_second = time_transform(&plan, iterations, true);
    let erased_first = time_transform(&plan, iterations, true);
    let generic_second = time_transform(&plan, iterations, false);
    let generic = generic_first + generic_second;
    let erased = erased_first + erased_second;

    eprintln!(
        "logical-plan transform_up_with_subqueries: iterations={} generic={:?} erased={:?} ratio={:.4}",
        iterations * 2,
        generic,
        erased,
        erased.as_secs_f64() / generic.as_secs_f64(),
    );
}
