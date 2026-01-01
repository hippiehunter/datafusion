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
use datafusion_common::{DFSchema, Diagnostic, Result, Span, Spans, plan_err};
use datafusion_expr::expr::{AllExpr, AnyExpr, Exists, InSubquery, QuantifiedSource};
use datafusion_expr::{Expr, LogicalPlan, Operator, Subquery};
use sqlparser::ast::Expr as SQLExpr;
use sqlparser::ast::{Query, SelectItem, SetExpr};
use std::sync::Arc;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn parse_exists_subquery(
        &self,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Push current schema onto stack to enable multi-level correlation
        let prev_stack_len =
            planner_context.push_outer_query_schema(input_schema.clone().into());
        let sub_plan = self.query_to_plan(subquery, planner_context)?;
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
        expr: SQLExpr,
        subquery: Query,
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

        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        let expr_obj = self.sql_to_expr(expr, input_schema, planner_context)?;

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
        subquery: Query,
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
        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

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
        expr: SQLExpr,
        op: Operator,
        subquery: Query,
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

        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        let expr_obj = self.sql_to_expr(expr, input_schema, planner_context)?;

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
        expr: SQLExpr,
        op: Operator,
        subquery: Query,
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

        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        // Restore the stack to its previous state
        planner_context.pop_outer_query_schema(prev_stack_len);

        self.validate_single_column(
            &sub_plan,
            &spans,
            "Too many columns! The subquery should only return one column",
            "Select only one column in the subquery",
        )?;

        let expr_obj = self.sql_to_expr(expr, input_schema, planner_context)?;

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
