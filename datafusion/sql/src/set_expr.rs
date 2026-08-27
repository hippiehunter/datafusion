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

use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{
    Column, DataFusionError, Diagnostic, JoinType, NullEquality, Result, Span,
    not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion_expr::logical_plan::builder::requalify_sides_if_needed;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, WindowFrame, WindowFunctionDefinition,
    WindowUDF,
};
use sqlparser::ast::{SetExpr, SetOperator, SetQuantifier, Spanned};

impl<S: ContextProvider> SqlToRel<'_, S> {
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    pub(super) fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.set_expr_to_plan_ref(&set_expr, planner_context)
    }

    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    pub(super) fn set_expr_to_plan_ref(
        &self,
        set_expr: &SetExpr,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let set_expr_span = Span::try_from_sqlparser_span(set_expr.span());
        match set_expr {
            SetExpr::Select(s) => {
                self.select_to_plan_ref(s.as_ref(), None, planner_context)
            }
            SetExpr::Values(v) => self.sql_values_to_plan_ref(v, planner_context),
            SetExpr::SetOperation {
                op,
                left,
                right,
                set_quantifier,
            } => {
                let left_span = Span::try_from_sqlparser_span(left.span());
                let right_span = Span::try_from_sqlparser_span(right.span());
                let left_plan = self.set_expr_to_plan_ref(left.as_ref(), planner_context);
                let right_plan =
                    self.set_expr_to_plan_ref(right.as_ref(), planner_context);
                let (left_plan, right_plan) = match (left_plan, right_plan) {
                    (Ok(left_plan), Ok(right_plan)) => (left_plan, right_plan),
                    (Err(left_err), Err(right_err)) => {
                        return Err(DataFusionError::Collection(vec![
                            left_err, right_err,
                        ]));
                    }
                    (Err(err), _) | (_, Err(err)) => {
                        return Err(err);
                    }
                };
                if !(*set_quantifier == SetQuantifier::ByName
                    || *set_quantifier == SetQuantifier::AllByName)
                {
                    self.validate_set_expr_num_of_columns(
                        op.clone(),
                        left_span,
                        right_span,
                        &left_plan,
                        &right_plan,
                        set_expr_span,
                    )?;
                }
                self.set_operation_to_plan(
                    op.clone(),
                    left_plan,
                    right_plan,
                    set_quantifier.clone(),
                )
            }
            SetExpr::Query(q) => self.query_to_plan_ref(q.as_ref(), planner_context),
            SetExpr::Insert(stmt) => {
                // Handle INSERT statements within a query (e.g., from WITH clause)
                self.sql_statement_to_plan_with_context_ref(
                    stmt.as_ref(),
                    planner_context,
                )
            }
            SetExpr::Update(stmt) => {
                // Handle UPDATE statements within a query (e.g., from WITH clause)
                self.sql_statement_to_plan_with_context_ref(
                    stmt.as_ref(),
                    planner_context,
                )
            }
            SetExpr::Delete(stmt) => {
                // Handle DELETE statements within a query (e.g., from WITH clause)
                self.sql_statement_to_plan_with_context_ref(
                    stmt.as_ref(),
                    planner_context,
                )
            }
            _ => not_impl_err!("Query {set_expr} not implemented yet"),
        }
    }

    pub(super) fn is_union_all(set_quantifier: SetQuantifier) -> Result<bool> {
        match set_quantifier {
            SetQuantifier::All | SetQuantifier::AllByName => Ok(true),
            SetQuantifier::Distinct
            | SetQuantifier::ByName
            | SetQuantifier::DistinctByName
            | SetQuantifier::None => Ok(false),
        }
    }

    fn validate_set_expr_num_of_columns(
        &self,
        op: SetOperator,
        left_span: Option<Span>,
        right_span: Option<Span>,
        left_plan: &LogicalPlan,
        right_plan: &LogicalPlan,
        set_expr_span: Option<Span>,
    ) -> Result<()> {
        if left_plan.schema().fields().len() == right_plan.schema().fields().len() {
            return Ok(());
        }
        let diagnostic = Diagnostic::new_error(
            format!("{op} queries have different number of columns"),
            set_expr_span,
        )
        .with_note(
            format!("this side has {} fields", left_plan.schema().fields().len()),
            left_span,
        )
        .with_note(
            format!(
                "this side has {} fields",
                right_plan.schema().fields().len()
            ),
            right_span,
        );
        plan_err!("{} queries have different number of columns", op; diagnostic =diagnostic)
    }

    pub(super) fn set_operation_to_plan(
        &self,
        op: SetOperator,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        set_quantifier: SetQuantifier,
    ) -> Result<LogicalPlan> {
        match (op, set_quantifier) {
            (SetOperator::Union, SetQuantifier::All) => {
                LogicalPlanBuilder::from(left_plan)
                    .union(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::AllByName) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_by_name(right_plan)?
                    .build()
            }
            (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::from(left_plan)
                    .union_distinct(right_plan)?
                    .build()
            }
            (
                SetOperator::Union,
                SetQuantifier::ByName | SetQuantifier::DistinctByName,
            ) => LogicalPlanBuilder::from(left_plan)
                .union_by_name_distinct(right_plan)?
                .build(),
            (SetOperator::Intersect, SetQuantifier::All) => self
                .multiset_intersect_or_except(left_plan, right_plan, JoinType::LeftSemi),
            (SetOperator::Intersect, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::intersect(left_plan, right_plan, false)
            }
            (SetOperator::Except, SetQuantifier::All) => self
                .multiset_intersect_or_except(left_plan, right_plan, JoinType::LeftAnti),
            (SetOperator::Except, SetQuantifier::Distinct | SetQuantifier::None) => {
                LogicalPlanBuilder::except(left_plan, right_plan, false)
            }
            (op, quantifier) => {
                not_impl_err!("{op} {quantifier} not implemented")
            }
        }
    }

    /// `INTERSECT ALL` and `EXCEPT ALL`, which keep input multiplicity: a value
    /// present `n` times on the left and `m` times on the right belongs to the
    /// result `min(n, m)` times for INTERSECT ALL and `max(n - m, 0)` times for
    /// EXCEPT ALL.
    ///
    /// Numbering the duplicates of each value on both sides turns that counting
    /// into an ordinary match. The left row carrying ordinal `k` pairs with the
    /// right side exactly when the right side holds at least `k` copies, so a
    /// semi-join keeps the first `min(n, m)` and an anti-join keeps the rest.
    fn multiset_intersect_or_except(
        &self,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        join_type: JoinType,
    ) -> Result<LogicalPlan> {
        let row_number = self
            .context_provider
            .get_window_meta(DUPLICATE_ORDINAL_FUNCTION)
            .ok_or_else(|| {
                plan_datafusion_err!(
                    "INTERSECT ALL / EXCEPT ALL needs the {DUPLICATE_ORDINAL_FUNCTION} \
                     window function to preserve input multiplicity"
                )
            })?;

        let (left_builder, right_builder, _requalified) = requalify_sides_if_needed(
            LogicalPlanBuilder::from(left_plan),
            LogicalPlanBuilder::from(right_plan),
        )?;
        let left_plan = number_duplicates(left_builder.build()?, &row_number)?;
        let right_plan = number_duplicates(right_builder.build()?, &row_number)?;

        let join_keys: (Vec<Column>, Vec<Column>) = left_plan
            .schema()
            .fields()
            .iter()
            .zip(right_plan.schema().fields().iter())
            .map(|(left_field, right_field)| {
                (
                    Column::from_name(left_field.name()),
                    Column::from_name(right_field.name()),
                )
            })
            .unzip();

        // The ordinal is scaffolding for the match, not a result column.
        let result_columns = left_plan
            .schema()
            .columns()
            .into_iter()
            .take(left_plan.schema().fields().len() - 1)
            .map(Expr::Column)
            .collect::<Vec<_>>();

        LogicalPlanBuilder::from(left_plan)
            .join_detailed(
                right_plan,
                join_type,
                join_keys,
                None,
                NullEquality::NullEqualsNull,
            )?
            .project(result_columns)?
            .build()
    }
}

/// The window function that numbers the duplicates of a value within its own
/// partition, which is how the multiset set operations count copies.
const DUPLICATE_ORDINAL_FUNCTION: &str = "row_number";

/// The duplicate ordinal column, named so it cannot collide with a result
/// column of the query.
const DUPLICATE_ORDINAL_COLUMN: &str = "__multiset_ordinal";

/// Add a column numbering each row within the group of rows sharing all of its
/// values, so duplicates of one value carry 1, 2, 3, ...
fn number_duplicates(
    plan: LogicalPlan,
    row_number: &Arc<WindowUDF>,
) -> Result<LogicalPlan> {
    let partition_by = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let ordinal = Expr::WindowFunction(Box::new(WindowFunction {
        fun: WindowFunctionDefinition::WindowUDF(Arc::clone(row_number)),
        params: WindowFunctionParams {
            args: vec![],
            partition_by,
            order_by: vec![],
            window_frame: WindowFrame::new(None),
            filter: None,
            null_treatment: None,
            distinct: false,
        },
    }))
    .alias(DUPLICATE_ORDINAL_COLUMN);
    let mut projection = plan
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    projection.push(Expr::Column(Column::from_name(DUPLICATE_ORDINAL_COLUMN)));
    LogicalPlanBuilder::from(plan)
        .window(vec![ordinal])?
        .project(projection)?
        .build()
}
