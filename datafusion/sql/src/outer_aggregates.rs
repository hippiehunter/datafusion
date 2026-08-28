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

//! Aggregates whose arguments belong to an enclosing query.
//!
//! An aggregate call inside a subquery is evaluated by the query its
//! arguments come from (PostgreSQL's `check_agg_arguments`): when every
//! column it reads is an outer reference, the innermost of the referenced
//! queries owns the aggregate. `... GROUP BY t HAVING EXISTS (SELECT 1 FROM
//! b WHERE sum(a.f) = b.f)` computes `sum(a.f)` in the outer aggregate and
//! the subquery reads the result as a correlated reference. An aggregate that
//! reads any column of the subquery itself belongs to the subquery.

use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchemaRef, Result};
use datafusion_expr::{Expr, ExprSchemable};

use crate::planner::PlannerContext;

/// Replace every aggregate of `expr` that belongs to an enclosing query with
/// an outer reference to that query's result column, handing the aggregate
/// itself to the enclosing query through `planner_context`.
pub(crate) fn hoist_outer_level_aggregates(
    expr: Expr,
    planner_context: &mut PlannerContext,
) -> Result<Expr> {
    let stack: Vec<DFSchemaRef> = planner_context.outer_query_schema_stack().to_vec();
    if stack.is_empty() {
        return Ok(expr);
    }
    expr.transform_down(|expr| {
        if !matches!(expr, Expr::AggregateFunction(_)) {
            return Ok(Transformed::no(expr));
        }
        let Some(level) = outer_aggregate_level(&expr, &stack)? else {
            return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
        };
        let hoisted = rebase_to_level(expr, level, &stack)?;
        let schema = stack[level].as_ref();
        let data_type = hoisted.get_type(schema)?;
        let nullable = hoisted.nullable(schema)?;
        let name = hoisted.schema_name().to_string();
        planner_context.push_outer_level_aggregate(level, hoisted);
        Ok(Transformed::new(
            Expr::OuterReferenceColumn(
                Arc::new(Field::new(name.clone(), data_type, nullable)),
                Column::from_name(name),
            ),
            true,
            TreeNodeRecursion::Jump,
        ))
    })
    .map(|transformed| transformed.data)
}

/// The stack index of the query that owns `aggregate`, or `None` when the
/// aggregate belongs to the query being planned: it reads one of that
/// query's own columns, reads no column at all, or nests a subquery.
fn outer_aggregate_level(
    aggregate: &Expr,
    stack: &[DFSchemaRef],
) -> Result<Option<usize>> {
    let mut level: Option<usize> = None;
    let mut local = false;
    aggregate.apply(|expr| {
        match expr {
            Expr::Column(_)
            | Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_) => {
                local = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Expr::OuterReferenceColumn(_, column) => {
                let Some(found) = column_level(column, stack) else {
                    local = true;
                    return Ok(TreeNodeRecursion::Stop);
                };
                level = Some(level.map_or(found, |current| current.max(found)));
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(if local { None } else { level })
}

/// The innermost enclosing query whose schema carries `column`.
fn column_level(column: &Column, stack: &[DFSchemaRef]) -> Option<usize> {
    stack
        .iter()
        .enumerate()
        .rev()
        .find(|(_, schema)| schema.has_column(column))
        .map(|(index, _)| index)
}

/// Rewrite the aggregate as the query at `level` sees it: references to that
/// query's own columns become plain columns, references to queries further
/// out stay outer references.
fn rebase_to_level(aggregate: Expr, level: usize, stack: &[DFSchemaRef]) -> Result<Expr> {
    aggregate
        .transform_down(|expr| match expr {
            Expr::OuterReferenceColumn(_, ref column)
                if column_level(column, stack) == Some(level) =>
            {
                Ok(Transformed::yes(Expr::Column(column.clone())))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
}
