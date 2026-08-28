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

//! Function calls as FROM items: `f(args)`, `ROWS FROM (f(), g())`, `WITH
//! ORDINALITY`, and the `AS (name type, ...)` column definition list of a
//! record-returning function.
//!
//! A function in FROM reads the FROM items written before it without an
//! explicit `LATERAL`, so its arguments are planned with those items in
//! scope, and a call that read one of them becomes a correlated subquery for
//! the join to resolve.
//!
//! A function the provider expands as lists (`SetReturningColumns`) plans
//! as `Project → Unnest → Project`. The functions of one `ROWS FROM` share
//! that `Unnest`, which walks their lists in lockstep and pads the ones that
//! end early with NULL. A function with no list expansion is a table source
//! the provider plans itself.

use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{
    Column, DFSchema, Result, Spans, UnnestOptions, not_impl_err, plan_err,
};
use datafusion_common::metadata::FieldMetadata;
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{
    Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Subquery,
    WindowFunctionDefinition,
};
use sqlparser::ast::{
    DataType as SQLDataType, Expr as SQLExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, TableAlias, TableFunctionColumnDef,
};

/// One function call of a FROM item, as written.
pub(super) struct SetReturningCall {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
    pub column_defs: Vec<TableFunctionColumnDef>,
}

impl SetReturningCall {
    /// The call a `ROWS FROM` item or a `TABLE(...)` operand spells as a
    /// function expression.
    pub fn from_function_expr(
        expr: &SQLExpr,
        column_defs: &[TableFunctionColumnDef],
    ) -> Result<Self> {
        let SQLExpr::Function(function) = expr else {
            return not_impl_err!("TableFunction with non-function expression: {expr:?}");
        };
        let args = match &function.args {
            FunctionArguments::List(list) => list.args.clone(),
            FunctionArguments::None => Vec::new(),
            other => {
                return not_impl_err!("Unsupported table function arguments: {other:?}");
            }
        };
        Ok(Self {
            name: function.name.clone(),
            args,
            column_defs: column_defs.to_vec(),
        })
    }

    /// `UNNEST(array, ...)` spelled as the `unnest` call it is.
    pub fn unnest(array_exprs: &[SQLExpr]) -> Self {
        Self {
            name: ObjectName::from(vec![Ident::new("unnest")]),
            args: array_exprs
                .iter()
                .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr.clone())))
                .collect(),
            column_defs: Vec::new(),
        }
    }
}

const LIST_COLUMN_PREFIX: &str = "__srf_";
const ORDINALITY_COLUMN: &str = "ordinality";

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Plan the function calls of one FROM item.
    pub(super) fn plan_function_relations(
        &self,
        calls: Vec<SetReturningCall>,
        with_ordinality: bool,
        alias: Option<TableAlias>,
        planner_context: &mut PlannerContext,
    ) -> Result<(LogicalPlan, Option<TableAlias>)> {
        // The FROM items before this one are in scope for the arguments,
        // nearer than any enclosing query.
        let siblings = planner_context.outer_from_schema();
        let empty_argument_schema = DFSchema::empty();
        let argument_schema = siblings.as_deref().unwrap_or(&empty_argument_schema);
        let pushed = siblings
            .as_ref()
            .map(|schema| planner_context.push_outer_query_schema(Arc::clone(schema)));
        let planned = self.plan_function_relations_in_scope(
            calls,
            with_ordinality,
            alias,
            argument_schema,
            planner_context,
        );
        if let Some(depth) = pushed {
            planner_context.pop_outer_query_schema(depth);
        }
        let (plan, alias) = planned?;
        let Some(siblings) = siblings else {
            return Ok((plan, alias));
        };
        // A call that read a sibling item is evaluated once per row of that
        // item: the join it sits in resolves the correlation.
        let outer_ref_columns = plan.all_out_ref_exprs();
        let reads_sibling = outer_ref_columns.iter().any(|expr| {
            matches!(expr, Expr::OuterReferenceColumn(_, column) if siblings.has_column(column))
        });
        if !reads_sibling {
            return Ok((plan, alias));
        }
        Ok((
            LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns,
                spans: Spans::new(),
            }),
            alias,
        ))
    }

    fn plan_function_relations_in_scope(
        &self,
        calls: Vec<SetReturningCall>,
        with_ordinality: bool,
        alias: Option<TableAlias>,
        argument_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(LogicalPlan, Option<TableAlias>)> {
        let single_call = calls.len() == 1;
        // `AS t(a int, b text)` types the alias columns: that is the column
        // definition list of the one function the item calls.
        let typed_alias_columns: Vec<(Ident, SQLDataType)> = alias
            .as_ref()
            .filter(|_| single_call)
            .map(|table_alias| {
                table_alias
                    .columns
                    .iter()
                    .filter_map(|column| {
                        column
                            .data_type
                            .clone()
                            .map(|data_type| (column.name.clone(), data_type))
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|typed| {
                !typed.is_empty()
                    && alias.as_ref().is_some_and(|table_alias| {
                        typed.len() == table_alias.columns.len()
                    })
            })
            .unwrap_or_default();
        let mut columns: Vec<(String, Expr)> = Vec::new();
        let mut relation_name = None;
        // Earlier FROM items are correlated inputs, not columns of the
        // function relation's own one-row input. They are on the outer-query
        // stack above, so expression binding must use an empty local schema;
        // `argument_schema` remains available to the provider for declared
        // type checks and output planning.
        let local_argument_schema = DFSchema::empty();
        for call in calls {
            let reference = self.object_name_to_table_reference(call.name)?;
            let name = reference.table();
            relation_name.get_or_insert_with(|| name.to_string());
            let args = self.plan_table_function_args(
                name,
                call.args,
                &local_argument_schema,
                planner_context,
            )?;
            let column_definitions = call
                .column_defs
                .iter()
                .map(|def| (&def.name, &def.data_type))
                .chain(
                    typed_alias_columns
                        .iter()
                        .map(|(name, data_type)| (name, data_type)),
                )
                .map(|(name, data_type)| {
                    let field = self.convert_data_type_to_field(data_type)?;
                    let name = self.ident_normalizer.normalize(name.clone());
                    Ok(Arc::new(field.as_ref().clone().with_name(name)))
                })
                .collect::<Result<Vec<FieldRef>>>()?;
            match self.context_provider.plan_set_returning_function(
                name,
                &args,
                argument_schema,
                Some(&column_definitions),
            )? {
                Some(expansion) => columns.extend(expansion.columns),
                None if name == "unnest" => {
                    for arg in args {
                        Self::check_unnest_arg(&arg, argument_schema)?;
                        columns.push((name.to_string(), arg));
                    }
                }
                None if single_call => {
                    return self.plan_table_source_relation(
                        name,
                        args,
                        &column_definitions,
                        with_ordinality,
                        alias,
                    );
                }
                None => {
                    return not_impl_err!(
                        "ROWS FROM over {name}, which does not expand as lists"
                    );
                }
            }
        }
        let Some(relation_name) = relation_name else {
            return plan_err!("ROWS FROM needs at least one function");
        };
        if columns.is_empty() {
            return plan_err!("{relation_name} produces no columns");
        }

        let internal: Vec<String> = (0..columns.len())
            .map(|index| format!("{LIST_COLUMN_PREFIX}{index}"))
            .collect();
        let lists = columns
            .iter()
            .zip(&internal)
            .map(|((_, expr), name)| expr.clone().alias(name))
            .collect::<Vec<_>>();
        let unnested = LogicalPlanBuilder::empty(true)
            .project(lists)?
            .unnest_columns_with_options(
                internal.iter().map(Column::from_name).collect(),
                UnnestOptions::new()
                    .with_preserve_nulls(false)
                    .with_ordinality(with_ordinality),
            )?;

        let mut names: Vec<String> = columns.into_iter().map(|(name, _)| name).collect();
        let mut alias = alias;
        // A function returning a base type takes a bare table alias as the
        // name of its one column.
        if single_call
            && names.len() == 1
            && let Some(table_alias) = &alias
            && table_alias.columns.is_empty()
        {
            names[0] = self.ident_normalizer.normalize(table_alias.name.clone());
        }
        let mut output: Vec<Expr> = internal
            .iter()
            .zip(&names)
            .map(|(internal, name)| Expr::Column(Column::from_name(internal)).alias(name))
            .collect();
        if with_ordinality {
            output.push(Expr::Column(Column::from_name(ORDINALITY_COLUMN)));
        }
        // The alias column list names positions, the ordinality column
        // included. Applying it here lets `ROWS FROM` columns that share a
        // default name be told apart before the schema is formed.
        if let Some(table_alias) = alias.as_mut()
            && !table_alias.columns.is_empty()
        {
            if table_alias.columns.len() > output.len() {
                return plan_err!(
                    "Source table contains {} columns but {} names given as column alias",
                    output.len(),
                    table_alias.columns.len()
                );
            }
            for (expr, column) in output.iter_mut().zip(table_alias.columns.drain(..)) {
                let name = self.ident_normalizer.normalize(column.name);
                *expr = expr.clone().unalias().alias(name);
            }
        }
        let plan = unnested.project(output)?.build()?;
        Ok(self.qualify_function_relation(plan, alias, &relation_name)?)
    }

    /// A function the provider plans as a table source of its own.
    fn plan_table_source_relation(
        &self,
        name: &str,
        args: Vec<Expr>,
        column_definitions: &[FieldRef],
        with_ordinality: bool,
        alias: Option<TableAlias>,
    ) -> Result<(LogicalPlan, Option<TableAlias>)> {
        let provider = self
            .context_provider
            .get_table_function_source_with_columns(name, args, column_definitions)?;
        let mut plan = if let Some(inline_plan) = provider.get_logical_plan() {
            let inline_plan = inline_plan.into_owned();
            if inline_plan.all_out_ref_exprs().is_empty() {
                LogicalPlanBuilder::scan(name, provider, None)?.build()?
            } else {
                inline_plan
            }
        } else {
            LogicalPlanBuilder::scan(name, provider, None)?.build()?
        };
        if !column_definitions.is_empty() {
            plan = Self::apply_column_definitions(plan, column_definitions, name)?;
        }
        if with_ordinality {
            plan = self.number_rows(plan, name)?;
        }
        let base_columns = plan.schema().fields().len() - usize::from(with_ordinality);
        if base_columns == 1
            && let Some(table_alias) = &alias
            && table_alias.columns.is_empty()
        {
            let alias_name = self.ident_normalizer.normalize(table_alias.name.clone());
            let mut output: Vec<Expr> = plan
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            output[0] = output[0].clone().alias(alias_name);
            plan = LogicalPlanBuilder::from(plan).project(output)?.build()?;
        }
        Ok(self.qualify_function_relation(plan, alias, name)?)
    }

    /// A FROM item is addressed by its alias, or by the function's name when
    /// it has none.
    fn qualify_function_relation(
        &self,
        plan: LogicalPlan,
        alias: Option<TableAlias>,
        name: &str,
    ) -> Result<(LogicalPlan, Option<TableAlias>)> {
        if alias.is_some() {
            return Ok((plan, alias));
        }
        Ok((LogicalPlanBuilder::from(plan).alias(name)?.build()?, None))
    }

    /// `AS (name type, ...)` over a table source: the list names and types
    /// the function's columns, so it has to describe exactly those columns.
    fn apply_column_definitions(
        plan: LogicalPlan,
        column_definitions: &[FieldRef],
        name: &str,
    ) -> Result<LogicalPlan> {
        let fields = plan.schema().fields().len();
        if column_definitions.len() != fields {
            return plan_err!(
                "the column definition list names {} columns but {name} returns {fields}",
                column_definitions.len()
            );
        }
        let output = plan
            .schema()
            .columns()
            .into_iter()
            .zip(column_definitions)
            .map(|(column, definition)| {
                let expr = Expr::Column(column);
                let expr = if expr.get_type(plan.schema())? == *definition.data_type() {
                    expr
                } else {
                    Expr::Cast(Cast::new(Box::new(expr), definition.data_type().clone()))
                };
                let mut metadata = definition.metadata().clone();
                metadata.insert(
                    "pg_column_definition_list".to_string(),
                    "true".to_string(),
                );
                Ok(expr.alias_with_metadata(
                    definition.name(),
                    Some(FieldMetadata::from(metadata)),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::from(plan).project(output)?.build()
    }

    /// `WITH ORDINALITY` over a table source: number its rows in the order
    /// they come out, as a trailing bigint column.
    fn number_rows(&self, plan: LogicalPlan, name: &str) -> Result<LogicalPlan> {
        let Some(row_number) = self.context_provider.get_window_meta("row_number") else {
            return not_impl_err!(
                "WITH ORDINALITY over {name} needs the row_number window function"
            );
        };
        let ordinal = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(row_number),
            Vec::new(),
        ))
        .alias(ORDINALITY_COLUMN);
        let numbered = LogicalPlanBuilder::from(plan)
            .window(vec![ordinal])?
            .build()?;
        let mut output: Vec<Expr> = numbered
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let Some(ordinal) = output.pop() else {
            return plan_err!("WITH ORDINALITY over {name} produced no columns");
        };
        output.push(
            Expr::Cast(Cast::new(Box::new(ordinal.unalias()), DataType::Int64))
                .alias(ORDINALITY_COLUMN),
        );
        LogicalPlanBuilder::from(numbered).project(output)?.build()
    }
}
