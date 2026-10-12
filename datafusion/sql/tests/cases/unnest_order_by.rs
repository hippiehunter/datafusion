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

//! ORDER BY over a select list holding a set-returning function binds each key
//! to the output column it names after the SRF is expanded. Two output columns
//! may share a name, `a.c` and `b.c`, or a table column and an SRF aliased
//! after it; each key must read its own.

use datafusion_expr::{Expr, LogicalPlan};
use datafusion_sql::planner::SqlToRel;
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

use crate::{MockContextProvider, MockSessionState};

fn plan(sql: &str) -> LogicalPlan {
    let dialect = PostgreSqlDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(sql)
        .expect("unable to create parser")
        .parse_statement()
        .expect("unable to parse query");
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    SqlToRel::new(&context)
        .sql_statement_to_plan(statement)
        .expect("the query plans")
}

/// The output position each ORDER BY key of `sql` reads.
fn sort_key_positions(sql: &str) -> Vec<usize> {
    let plan = plan(sql);
    let LogicalPlan::Sort(sort) = &plan else {
        panic!("expected the ORDER BY on top:\n{}", plan.display_indent());
    };
    sort.expr
        .iter()
        .map(|key| match &key.expr {
            Expr::Column(column) => sort
                .input
                .schema()
                .index_of_column(column)
                .expect("the key reads an output column"),
            other => panic!("expected an output column, got {other}"),
        })
        .collect()
}

#[test]
fn same_named_columns_of_two_relations_are_different_sort_keys() {
    assert_eq!(
        sort_key_positions(
            "SELECT a.array_col, b.array_col, unnest(a.array_col) \
             FROM unnest_table a, unnest_table b ORDER BY 2, 1, 3"
        ),
        vec![1, 0, 2]
    );
    assert_eq!(
        sort_key_positions(
            "SELECT a.array_col, b.array_col, unnest(a.array_col) \
             FROM unnest_table a, unnest_table b ORDER BY b.array_col, a.array_col"
        ),
        vec![1, 0]
    );
}

#[test]
fn an_srf_aliased_after_a_table_column_is_its_own_sort_key() {
    assert_eq!(
        sort_key_positions(
            "SELECT a.array_col, unnest(a.array_col) AS array_col \
             FROM unnest_table a ORDER BY 2, 1"
        ),
        vec![1, 0]
    );
}
