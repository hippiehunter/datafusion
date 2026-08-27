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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::plan_err;
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Expr, GroupingSet};
use sqlparser::ast::Expr as SQLExpr;

/// Whether a grouping element is the empty one, `()`, which contributes no
/// column to the sets it appears in.
pub(crate) fn is_empty_grouping_element(element: &SQLExpr) -> bool {
    matches!(element, SQLExpr::Tuple(exprs) if exprs.is_empty())
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_grouping_sets_to_expr(
        &self,
        exprs: &[Vec<SQLExpr>],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args: Result<Vec<Vec<_>>> = exprs
            .iter()
            .map(|v| {
                v.iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .collect()
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::GroupingSets(args?)))
    }

    pub(super) fn sql_rollup_to_expr(
        &self,
        exprs: &[Vec<SQLExpr>],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args: Result<Vec<_>> = exprs
            .iter()
            .map(|v| {
                if v.len() != 1 {
                    plan_err!(
                        "Tuple expressions are not supported for Rollup expressions"
                    )
                } else {
                    self.sql_expr_to_logical_expr(&v[0], schema, planner_context)
                }
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::Rollup(args?)))
    }

    pub(super) fn sql_cube_to_expr(
        &self,
        exprs: &[Vec<SQLExpr>],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args: Result<Vec<_>> = exprs
            .iter()
            .map(|v| {
                if v.len() != 1 {
                    plan_err!("Tuple expressions not are supported for Cube expressions")
                } else {
                    self.sql_expr_to_logical_expr(&v[0], schema, planner_context)
                }
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::Cube(args?)))
    }

    /// `GROUPING SETS ( <grouping element>, ... )` whose elements are not all
    /// parenthesized column lists. Each element names a list of grouping sets —
    /// a bare expression names one set holding it, `ROLLUP`/`CUBE` name their
    /// own expansions, and a nested `GROUPING SETS` names its own elements —
    /// and the clause's sets are those lists concatenated.
    pub(super) fn sql_grouping_sets_elements_to_expr(
        &self,
        elements: &[SQLExpr],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut sets = Vec::new();
        for element in elements {
            sets.extend(self.grouping_element_sets(element, schema, planner_context)?);
        }
        Ok(Expr::GroupingSet(GroupingSet::GroupingSets(sets)))
    }

    /// The grouping sets one grouping element names.
    fn grouping_element_sets(
        &self,
        element: &SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Vec<Expr>>> {
        match element {
            SQLExpr::Tuple(exprs) => Ok(vec![self.plan_grouping_exprs(
                exprs,
                schema,
                planner_context,
            )?]),
            SQLExpr::Nested(inner) => Ok(vec![vec![self.sql_expr_to_logical_expr(
                inner.as_ref(),
                schema,
                planner_context,
            )?]]),
            SQLExpr::GroupingSets(sets) => sets
                .iter()
                .map(|set| self.plan_grouping_exprs(set, schema, planner_context))
                .collect(),
            SQLExpr::GroupingSetsElements(elements) => {
                let mut sets = Vec::new();
                for element in elements {
                    sets.extend(self.grouping_element_sets(
                        element,
                        schema,
                        planner_context,
                    )?);
                }
                Ok(sets)
            }
            SQLExpr::Rollup(units) => {
                let units = self.plan_grouping_units(units, schema, planner_context)?;
                Ok(rollup_sets(&units))
            }
            SQLExpr::Cube(units) => {
                let units = self.plan_grouping_units(units, schema, planner_context)?;
                Ok(cube_sets(&units))
            }
            other => Ok(vec![vec![self.sql_expr_to_logical_expr(
                other,
                schema,
                planner_context,
            )?]]),
        }
    }

    fn plan_grouping_exprs(
        &self,
        exprs: &[SQLExpr],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        exprs
            .iter()
            .map(|expr| self.sql_expr_to_logical_expr(expr, schema, planner_context))
            .collect()
    }

    /// The units a `ROLLUP`/`CUBE` is built from. A parenthesized element is one
    /// unit holding several columns, which enter and leave a set together.
    fn plan_grouping_units(
        &self,
        units: &[Vec<SQLExpr>],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Vec<Expr>>> {
        units
            .iter()
            .map(|unit| self.plan_grouping_exprs(unit, schema, planner_context))
            .collect()
    }
}

/// `ROLLUP(u1, ..., un)`: the n+1 sets that drop one trailing unit at a time.
fn rollup_sets(units: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    (0..=units.len())
        .rev()
        .map(|len| units[..len].iter().flatten().cloned().collect())
        .collect()
}

/// `CUBE(u1, ..., un)`: every subset of the units, ordered so that a set
/// holding an earlier unit precedes one that drops it.
fn cube_sets(units: &[Vec<Expr>]) -> Vec<Vec<Expr>> {
    let Some((head, rest)) = units.split_first() else {
        return vec![Vec::new()];
    };
    let rest_sets = cube_sets(rest);
    let with_head = rest_sets.iter().map(|set| {
        let mut combined = head.clone();
        combined.extend(set.iter().cloned());
        combined
    });
    with_head.chain(rest_sets.iter().cloned()).collect()
}
