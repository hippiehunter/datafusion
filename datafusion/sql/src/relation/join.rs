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

use crate::planner::{PlannerContext, SqlToRel};
use arrow::datatypes::DataType;
use datafusion_common::{
    Column, Result, TableReference, not_impl_err, plan_datafusion_err,
};
use datafusion_expr::expr::{Alias, Case, Cast};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    Join, JoinConstraint, JoinOperator, ObjectName, TableFactor, TableWithJoins,
};
use std::collections::HashSet;

impl SqlToRel<'_> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.plan_table_with_joins_ref(&t, planner_context)
    }

    pub(crate) fn plan_table_with_joins_ref(
        &self,
        t: &TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let mut left = if is_lateral(&t.relation) {
            self.create_relation_subquery_ref(&t.relation, planner_context)?
        } else {
            self.create_relation_ref(&t.relation, planner_context)?
        };
        let old_outer_from_schema = planner_context.outer_from_schema();
        for join in &t.joins {
            planner_context.extend_outer_from_schema(left.schema())?;
            left = self.parse_relation_join_ref(left, join, planner_context)?;
        }
        planner_context.set_outer_from_schema(old_outer_from_schema);
        Ok(left)
    }

    pub(crate) fn parse_relation_join_ref(
        &self,
        left: LogicalPlan,
        join: &Join,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let right = if is_lateral_join(&join)? {
            self.create_relation_subquery_ref(&join.relation, planner_context)?
        } else {
            self.create_relation_ref(&join.relation, planner_context)?
        };
        match &join.join_operator {
            JoinOperator::LeftOuter(constraint) | JoinOperator::Left(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) | JoinOperator::Right(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::LeftSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftSemi,
                planner_context,
            ),
            JoinOperator::RightSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightSemi,
                planner_context,
            ),
            JoinOperator::LeftAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftAnti,
                planner_context,
            ),
            JoinOperator::RightAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightAnti,
                planner_context,
            ),
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin(JoinConstraint::None) => {
                self.parse_cross_join(left, right)
            }
            other => not_impl_err!("Unsupported JOIN operator {other:?}"),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: &JoinConstraint,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let join_schema = left.schema().join(right.schema())?;
                // parse ON expression
                let expr = self.sql_to_expr_ref(
                    sql_expr.as_ref(),
                    &join_schema,
                    planner_context,
                )?;
                LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(expr))?
                    .build()
            }
            JoinConstraint::UsingWithAlias { columns, alias } => {
                let keys = self.using_clause_keys(columns)?;
                let alias =
                    TableReference::bare(self.ident_normalizer.normalize(alias.clone()));
                self.plan_using_join(left, right, join_type, keys, Some(alias))
            }
            JoinConstraint::Using(object_names) => {
                let keys = self.using_clause_keys(object_names)?;
                self.plan_using_join(left, right, join_type, keys, None)
            }
            JoinConstraint::Natural => {
                let left_names: HashSet<String> = visible_column_names(&left)?;
                let keys: Vec<String> = visible_column_names(&right)?
                    .into_iter()
                    .filter(|name| left_names.contains(name))
                    .collect();
                if keys.is_empty() {
                    self.parse_cross_join(left, right)
                } else {
                    // The right side's visible names come back in no particular
                    // order; PostgreSQL merges them in the left input's order.
                    let mut ordered: Vec<String> = Vec::with_capacity(keys.len());
                    for (_, field) in left.schema().iter() {
                        if keys.contains(field.name()) && !ordered.contains(field.name())
                        {
                            ordered.push(field.name().clone());
                        }
                    }
                    self.plan_using_join(left, right, join_type, ordered, None)
                }
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .join_on(right, join_type, [])?
                .build(),
        }
    }

    /// The column names a USING clause lists, normalized.
    fn using_clause_keys(&self, object_names: &[ObjectName]) -> Result<Vec<String>> {
        object_names
            .iter()
            .map(|object_name| {
                let ObjectName(parts) = object_name;
                if parts.len() != 1 {
                    return not_impl_err!(
                        "Invalid identifier in USING clause. Expected single identifier, got {object_name}"
                    );
                }
                parts[0]
                    .as_ident()
                    .ok_or_else(|| plan_datafusion_err!("Expected identifier in USING clause"))
                    .map(|ident| self.ident_normalizer.normalize(ident.clone()))
            })
            .collect()
    }

    /// A USING or NATURAL join in PostgreSQL's merged-column model.
    ///
    /// The join itself keeps both inputs' columns. The projection placed over
    /// it is the join's column list: one merged column per join name first —
    /// the left input's value for an inner or left join, the right input's for
    /// a right join, whichever is not null for a full join, in the two inputs'
    /// common type — then each input's remaining columns. After those come the
    /// inputs' own copies of the join columns and, for `USING (...) AS alias`,
    /// the merged columns again under that alias: reachable by their
    /// qualifier, but neither what an unqualified name means nor part of `*`
    /// (see `LogicalPlan::using_columns`). A copy that is itself unqualified —
    /// the merged column of an unaliased join below — has no qualifier to be
    /// reached by, so the new merged column replaces it outright.
    fn plan_using_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        keys: Vec<String>,
        alias: Option<TableReference>,
    ) -> Result<LogicalPlan> {
        let left_keys = keys
            .iter()
            .map(|key| normalize_visible_column(&left, key))
            .collect::<Result<Vec<Column>>>()?;
        let right_keys = keys
            .iter()
            .map(|key| normalize_visible_column(&right, key))
            .collect::<Result<Vec<Column>>>()?;
        let hidden_below: HashSet<Column> = hidden_columns(&left)?
            .into_iter()
            .chain(hidden_columns(&right)?)
            .collect();

        let mut merged_exprs = Vec::with_capacity(keys.len());
        let mut alias_copies = Vec::new();
        for ((name, l), r) in keys.iter().zip(&left_keys).zip(&right_keys) {
            let left_type = left
                .schema()
                .qualified_field_from_column(l)?
                .1
                .data_type()
                .clone();
            let right_type = right
                .schema()
                .qualified_field_from_column(r)?
                .1
                .data_type()
                .clone();
            let common = if left_type == right_type {
                left_type.clone()
            } else {
                comparison_coercion(&left_type, &right_type).ok_or_else(|| {
                    plan_datafusion_err!(
                        "JOIN/USING types {left_type} and {right_type} cannot be matched"
                    )
                })?
            };
            let cast_to_common = |column: &Column, data_type: &DataType| {
                let expr = Expr::Column(column.clone());
                if *data_type == common {
                    expr
                } else {
                    Expr::Cast(Cast::new(Box::new(expr), common.clone()))
                }
            };
            let left_value = cast_to_common(l, &left_type);
            let right_value = cast_to_common(r, &right_type);
            let merged = match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark => left_value,
                JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightMark => right_value,
                JoinType::Full => Expr::Case(Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(left_value.clone().is_not_null()),
                        Box::new(left_value),
                    )],
                    else_expr: Some(Box::new(right_value)),
                }),
            };
            if let Some(alias) = &alias {
                alias_copies.push(Expr::Alias(Alias::new(
                    merged.clone(),
                    Some(alias.clone()),
                    name.clone(),
                )));
            }
            merged_exprs.push(merged.alias(name.clone()));
        }

        let join = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                join_type,
                keys.iter().map(Column::from_name).collect(),
            )?
            .build()?;
        let key_columns: HashSet<&Column> = left_keys.iter().chain(&right_keys).collect();
        let mut exprs = merged_exprs;
        for (qualifier, field) in join.schema().iter() {
            let column = Column::from((qualifier, field));
            if key_columns.contains(&column) || hidden_below.contains(&column) {
                continue;
            }
            exprs.push(Expr::Column(column));
        }
        for column in left_keys.iter().chain(&right_keys) {
            if column.relation.is_some() && join.schema().has_column(column) {
                exprs.push(Expr::Column(column.clone()));
            }
        }
        for (qualifier, field) in join.schema().iter() {
            let column = Column::from((qualifier, field));
            if hidden_below.contains(&column) {
                exprs.push(Expr::Column(column));
            }
        }
        exprs.extend(alias_copies);
        LogicalPlanBuilder::from(join).project(exprs)?.build()
    }
}

/// The columns a USING join beneath `plan` hides from unqualified lookup and
/// from `*`.
fn hidden_columns(plan: &LogicalPlan) -> Result<HashSet<Column>> {
    Ok(plan
        .using_columns()?
        .into_iter()
        .flat_map(|using| using.hidden)
        .collect())
}

/// The names `plan` exposes to an unqualified reference or to `*`.
fn visible_column_names(plan: &LogicalPlan) -> Result<HashSet<String>> {
    let hidden = hidden_columns(plan)?;
    Ok(plan
        .schema()
        .iter()
        .filter(|(qualifier, field)| {
            !hidden.contains(&Column::from((*qualifier, *field)))
        })
        .map(|(_, field)| field.name().clone())
        .collect())
}

/// Resolve a USING name against one join input, as an unqualified reference
/// written in the query would be.
fn normalize_visible_column(plan: &LogicalPlan, name: &str) -> Result<Column> {
    Column::from_name(name).normalize_with_schemas_and_ambiguity_check(
        &[&[plan.schema()]],
        &plan.using_columns()?,
    )
}

/// Return `true` iff the given [`TableFactor`] is lateral.
pub(crate) fn is_lateral(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Derived { lateral, .. } => *lateral,
        TableFactor::Function { lateral, .. } => *lateral,
        TableFactor::RowsFrom { lateral, .. } => *lateral,
        TableFactor::UNNEST { .. } => true,
        _ => false,
    }
}

/// Return `true` iff the given [`Join`] is lateral.
pub(crate) fn is_lateral_join(join: &Join) -> Result<bool> {
    let is_lateral_syntax = is_lateral(&join.relation);
    let is_apply_syntax = match join.join_operator {
        JoinOperator::FullOuter(..)
        | JoinOperator::Right(..)
        | JoinOperator::RightOuter(..)
        | JoinOperator::RightAnti(..)
        | JoinOperator::RightSemi(..)
            if is_lateral_syntax =>
        {
            return not_impl_err!(
                "LATERAL syntax is not supported for \
                 FULL OUTER and RIGHT [OUTER | ANTI | SEMI] joins"
            );
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => true,
        _ => false,
    };
    Ok(is_lateral_syntax || is_apply_syntax)
}
