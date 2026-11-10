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
//!
//! A subquery in the aggregate's arguments counts through the columns it
//! reads from outside itself: `(SELECT max((SELECT i.f FROM i WHERE i.k =
//! o.k))) FROM o` aggregates over `o`, since `o.k` is the only column the
//! argument reads from any query around it.

use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef, Result};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan};

use crate::planner::PlannerContext;

/// Replace every aggregate of `expr` that belongs to an enclosing query with
/// an outer reference to that query's result column, handing the aggregate
/// itself to the enclosing query through `planner_context`. `local_schema`
/// holds the columns of the query being planned.
pub(crate) fn hoist_outer_level_aggregates(
    expr: Expr,
    local_schema: &DFSchema,
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
        let Some(level) = outer_aggregate_level(&expr, local_schema, &stack)? else {
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
/// query's own columns, directly or from a subquery in its arguments, or
/// reads no column of any query around it.
fn outer_aggregate_level(
    aggregate: &Expr,
    local_schema: &DFSchema,
    stack: &[DFSchemaRef],
) -> Result<Option<usize>> {
    let mut level: Option<usize> = None;
    let mut local = false;
    aggregate.apply(|expr| {
        match expr {
            Expr::Column(_) => {
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
            expr => {
                if let Some(subquery) = expr.subquery() {
                    let mut escaping = Vec::new();
                    escaping_outer_references(
                        subquery.subquery.as_ref(),
                        &mut Vec::new(),
                        &mut escaping,
                    )?;
                    for column in &escaping {
                        // The query being planned is the nearest one around
                        // the subquery, so its columns are found first.
                        if local_schema.has_column(column) {
                            local = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        let Some(found) = column_level(column, stack) else {
                            local = true;
                            return Ok(TreeNodeRecursion::Stop);
                        };
                        level =
                            Some(level.map_or(found, |current| current.max(found)));
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(if local { None } else { level })
}

/// Collect into `escaping` the columns `plan`, a subquery, reads from outside
/// itself. `enclosing` holds, for each subquery between the outermost one
/// walked and `plan`, the schemas of that subquery's own relations: an outer
/// reference found in one of them stays inside the outermost subquery.
fn escaping_outer_references(
    plan: &LogicalPlan,
    enclosing: &mut Vec<Vec<DFSchemaRef>>,
    escaping: &mut Vec<Column>,
) -> Result<()> {
    let mut own: Vec<DFSchemaRef> = Vec::new();
    plan.apply(|node| {
        own.push(Arc::clone(node.schema()));
        Ok(TreeNodeRecursion::Continue)
    })?;
    plan.apply(|node| {
        for expr in node.expressions() {
            expr.apply(|expr| {
                match expr {
                    Expr::OuterReferenceColumn(_, column) => {
                        let inside = enclosing
                            .iter()
                            .flatten()
                            .any(|schema| schema.has_column(column));
                        if !inside && !escaping.contains(column) {
                            escaping.push(column.clone());
                        }
                    }
                    expr => {
                        if let Some(subquery) = expr.subquery() {
                            enclosing.push(own.clone());
                            escaping_outer_references(
                                subquery.subquery.as_ref(),
                                enclosing,
                                escaping,
                            )?;
                            enclosing.pop();
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
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
