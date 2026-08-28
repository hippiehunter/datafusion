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
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    Column, DFSchema, Diagnostic, Result, ScalarValue, Span, Spans, not_impl_err,
    plan_err,
};
use datafusion_expr::expr::{AllExpr, AnyExpr, Case, Exists, InSubquery, QuantifiedSource};
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Subquery, lit};
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, FunctionArg, FunctionArgExpr, FunctionArguments,
    Query, SelectItem, SetExpr,
};
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum RowQuantifier {
    Any,
    All,
}

/// The expression compared against a one-column subquery: a row constructor
/// is compared element-wise, so `ROW(x)` compares `x` itself, at any nesting
/// depth (`ROW(ROW(ROW(1))) = ANY (SELECT ROW(ROW(1)))` compares `ROW(ROW(1))`
/// to the column). Anything else is compared as written.
fn single_row_element<'a>(expr: &'a SQLExpr, row: Option<&[&'a SQLExpr]>) -> &'a SQLExpr {
    match row {
        Some([element]) => element,
        _ => expr,
    }
}

/// The elements of a syntactic row constructor: `(a, b)` or `ROW(a, b)`.
fn row_constructor_elements(expr: &SQLExpr) -> Option<Vec<&SQLExpr>> {
    match expr {
        SQLExpr::Tuple(elements) => Some(elements.iter().collect()),
        SQLExpr::Nested(inner) => row_constructor_elements(inner),
        SQLExpr::Function(function)
            if function.name.to_string().eq_ignore_ascii_case("row") =>
        {
            let FunctionArguments::List(list) = &function.args else {
                return None;
            };
            list.args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                    _ => None,
                })
                .collect()
        }
        _ => None,
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn parse_exists_subquery(
        &self,
        subquery: &Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Push current schema onto stack to enable multi-level correlation
        let prev_stack_len =
            planner_context.push_outer_query_schema(input_schema.clone().into());
        let sub_plan = self.query_to_plan_ref(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);
        Ok(Expr::Exists(Exists {
            subquery: Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
                spans: Spans::new(),
            },
            negated,
        }))
    }

    pub(super) fn parse_in_subquery(
        &self,
        expr: &SQLExpr,
        subquery: &Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Push current schema onto stack to enable multi-level correlation
        let prev_stack_len =
            planner_context.push_outer_query_schema(input_schema.clone().into());

        let mut spans = Spans::new();
        if let SetExpr::Select(select) = &subquery.body.as_ref() {
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(SQLExpr::Identifier(ident)) = item
                    && let Some(span) = Span::try_from_sqlparser_span(ident.span)
                {
                    spans.add_span(span);
                }
            }
        }

        let sub_plan = self.query_to_plan_ref(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        let row = row_constructor_elements(expr);
        if let Some(row) = &row
            && row.len() > 1
        {
            // `row IN (subquery)` is `row = ANY (subquery)`.
            let any = self.row_quantified_subquery(
                row,
                &BinaryOperator::Eq,
                RowQuantifier::Any,
                sub_plan,
                input_schema,
                planner_context,
            )?;
            return Ok(if negated { Expr::Not(Box::new(any)) } else { any });
        }

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        let expr = single_row_element(expr, row.as_deref());
        let expr_obj = self.sql_to_expr_ref(expr, input_schema, planner_context)?;

        Ok(Expr::InSubquery(InSubquery::new(
            Box::new(expr_obj),
            Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
                spans,
            },
            negated,
        )))
    }

    pub(super) fn parse_scalar_subquery(
        &self,
        subquery: &Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Push current schema onto stack to enable multi-level correlation
        let prev_stack_len =
            planner_context.push_outer_query_schema(input_schema.clone().into());
        let mut spans = Spans::new();
        if let SetExpr::Select(select) = subquery.body.as_ref() {
            for item in &select.projection {
                if let SelectItem::ExprWithAlias { alias, .. } = item
                    && let Some(span) = Span::try_from_sqlparser_span(alias.span)
                {
                    spans.add_span(span);
                }
            }
        }
        let sub_plan = self.query_to_plan_ref(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        // Collapse trivial wrappers: (SELECT (SELECT agg(...) FROM ...))
        // where the outer query is Projection([ScalarSubquery], EmptyRelation).
        // This arises from ARRAY(SELECT ...) inside (SELECT ...) wrappers.
        if let LogicalPlan::Projection(proj) = &sub_plan
            && proj.expr.len() == 1
            && matches!(proj.input.as_ref(), LogicalPlan::EmptyRelation(_))
        {
            let expr = &proj.expr[0];
            let inner = match expr {
                Expr::ScalarSubquery(sq) => Some(sq),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::ScalarSubquery(sq) => Some(sq),
                    _ => None,
                },
                _ => None,
            };
            if let Some(inner_sq) = inner {
                return Ok(Expr::ScalarSubquery(inner_sq.clone()));
            }
        }

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(sub_plan),
            outer_ref_columns,
            spans,
        }))
    }

    /// Parse an ANY subquery expression like `x > ANY(SELECT ...)`
    pub(super) fn parse_any_subquery(
        &self,
        expr: &SQLExpr,
        compare_op: &BinaryOperator,
        subquery: &Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Push current schema onto stack to enable multi-level correlation
        let prev_stack_len =
            planner_context.push_outer_query_schema(input_schema.clone().into());

        let mut spans = Spans::new();
        if let SetExpr::Select(select) = &subquery.body.as_ref() {
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(SQLExpr::Identifier(ident)) = item
                    && let Some(span) = Span::try_from_sqlparser_span(ident.span)
                {
                    spans.add_span(span);
                }
            }
        }

        let sub_plan = self.query_to_plan_ref(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        let row = row_constructor_elements(expr);
        if let Some(row) = &row
            && row.len() > 1
        {
            return self.row_quantified_subquery(
                row,
                compare_op,
                RowQuantifier::Any,
                sub_plan,
                input_schema,
                planner_context,
            );
        }

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        let expr = single_row_element(expr, row.as_deref());
        let expr_obj = self.sql_to_expr_ref(expr, input_schema, planner_context)?;
        let op = self.parse_sql_binary_op(compare_op)?;

        Ok(Expr::AnyExpr(AnyExpr::new(
            Box::new(expr_obj),
            op,
            QuantifiedSource::Subquery(Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
                spans,
            }),
        )))
    }

    /// Parse an ALL subquery expression like `x > ALL(SELECT ...)`
    pub(super) fn parse_all_subquery(
        &self,
        expr: &SQLExpr,
        compare_op: &BinaryOperator,
        subquery: &Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Push current schema onto stack to enable multi-level correlation
        let prev_stack_len =
            planner_context.push_outer_query_schema(input_schema.clone().into());

        let mut spans = Spans::new();
        if let SetExpr::Select(select) = &subquery.body.as_ref() {
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(SQLExpr::Identifier(ident)) = item
                    && let Some(span) = Span::try_from_sqlparser_span(ident.span)
                {
                    spans.add_span(span);
                }
            }
        }

        let sub_plan = self.query_to_plan_ref(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        let row = row_constructor_elements(expr);
        if let Some(row) = &row
            && row.len() > 1
        {
            return self.row_quantified_subquery(
                row,
                compare_op,
                RowQuantifier::All,
                sub_plan,
                input_schema,
                planner_context,
            );
        }

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        let expr = single_row_element(expr, row.as_deref());
        let expr_obj = self.sql_to_expr_ref(expr, input_schema, planner_context)?;
        let op = self.parse_sql_binary_op(compare_op)?;

        Ok(Expr::AllExpr(AllExpr::new(
            Box::new(expr_obj),
            op,
            QuantifiedSource::Subquery(Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
                spans,
            }),
        )))
    }

    /// `row op ANY (subquery)` / `row op ALL (subquery)` over a subquery with
    /// as many columns as the row has elements.
    ///
    /// The quantified result is built from two correlated EXISTS tests over
    /// the subquery — one for rows where the comparison holds and one for
    /// rows where it is unknown — which gives the SQL three-valued result:
    /// ANY is true if some comparison is true, else unknown if some is
    /// unknown, else false; ALL is false if some comparison is false, else
    /// unknown if some is unknown, else true.
    fn row_quantified_subquery(
        &self,
        row: &[&SQLExpr],
        compare_op: &BinaryOperator,
        quantifier: RowQuantifier,
        sub_plan: LogicalPlan,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let sub_schema = Arc::clone(sub_plan.schema());
        if row.len() != sub_schema.fields().len() {
            return plan_err!(
                "subquery has {} columns but the row being compared has {}",
                sub_schema.fields().len(),
                row.len()
            );
        }
        // The row's elements are outer expressions: plan them against the
        // outer schema, then turn their columns into outer references for
        // use inside the subquery.
        let mut left_elements = Vec::with_capacity(row.len());
        for element in row {
            let planned = self.sql_to_expr_ref(element, input_schema, planner_context)?;
            left_elements.push(planned.transform(|expr| match expr {
                Expr::Column(column) => {
                    let (_, field) = input_schema.qualified_field_from_column(&column)?;
                    Ok(Transformed::yes(Expr::OuterReferenceColumn(
                        Arc::clone(field),
                        column,
                    )))
                }
                other => Ok(Transformed::no(other)),
            })?.data);
        }
        let right_elements = sub_schema
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect::<Vec<_>>();
        let left_row = self.plan_row_constructor(left_elements)?;
        let right_row = self.plan_row_constructor(right_elements)?;
        let comparison = self.build_logical_expr(
            compare_op.clone(),
            left_row,
            right_row,
            sub_schema.as_ref(),
        )?;

        let exists_where = |predicate: Expr| -> Result<Expr> {
            let plan = LogicalPlanBuilder::from(sub_plan.clone())
                .filter(predicate)?
                .build()?;
            let outer_ref_columns = plan.all_out_ref_exprs();
            Ok(Expr::Exists(Exists {
                subquery: Subquery {
                    subquery: Arc::new(plan),
                    outer_ref_columns,
                    spans: Spans::new(),
                },
                negated: false,
            }))
        };
        let unknown = exists_where(comparison.clone().is_null())?;
        let null_bool = Expr::Literal(ScalarValue::Boolean(None), None);
        Ok(match quantifier {
            RowQuantifier::Any => Expr::Case(Case {
                expr: None,
                when_then_expr: vec![
                    (Box::new(exists_where(comparison)?), Box::new(lit(true))),
                    (Box::new(unknown), Box::new(null_bool)),
                ],
                else_expr: Some(Box::new(lit(false))),
            }),
            RowQuantifier::All => Expr::Case(Case {
                expr: None,
                when_then_expr: vec![
                    (
                        Box::new(exists_where(Expr::Not(Box::new(comparison)))?),
                        Box::new(lit(false)),
                    ),
                    (Box::new(unknown), Box::new(null_bool)),
                ],
                else_expr: Some(Box::new(lit(true))),
            }),
        })
    }

    /// A row constructor over already-planned elements, through the
    /// expression planners' struct-literal hook as `ROW(...)` itself is.
    fn plan_row_constructor(&self, elements: Vec<Expr>) -> Result<Expr> {
        let mut args = elements;
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_struct_literal(args, false)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(original) => args = original,
            }
        }
        not_impl_err!("Row constructor not supported by ExprPlanner: {args:?}")
    }

    fn validate_single_column(
        &self,
        sub_plan: &LogicalPlan,
        spans: &Spans,
        error_message: &str,
        help_message: &str,
    ) -> Result<()> {
        if sub_plan.schema().fields().len() > 1 {
            let sub_schema = sub_plan.schema();
            let field_names = sub_schema.field_names();
            let diagnostic =
                self.build_multi_column_diagnostic(spans, error_message, help_message);
            plan_err!("{}: {}", error_message, field_names.join(", "); diagnostic=diagnostic)
        } else {
            Ok(())
        }
    }

    fn build_multi_column_diagnostic(
        &self,
        spans: &Spans,
        error_message: &str,
        help_message: &str,
    ) -> Diagnostic {
        let full_span = Span::union_iter(spans.0.iter().cloned());
        let mut diagnostic = Diagnostic::new_error(error_message, full_span);

        for (i, span) in spans.iter().skip(1).enumerate() {
            diagnostic.add_note(format!("Extra column {}", i + 1), Some(*span));
        }

        diagnostic.add_help(help_message, None);
        diagnostic
    }
}
