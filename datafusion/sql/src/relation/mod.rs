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

use std::collections::HashSet;
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use arrow::datatypes::{DataType, Field};
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    Column, DFSchema, DataFusionError, Diagnostic, Result, ScalarValue, Span, Spans,
    TableReference, UnnestOptions, not_impl_err, plan_err,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlannerContext, RelationPlanning,
    TableSampleMethod as LogicalTableSampleMethod,
};
use datafusion_expr::{
    EdgeDirection, EdgePattern, GraphColumn, GraphPattern, GraphPatternElement,
    GraphPatternExpr, GraphTable, JsonTable, JsonTableColumnDef, JsonTableErrorHandling,
    LabelExpression, NodePattern, PathFinding, PathMode, RepetitionQuantifier,
    RowLimiting, Subquery, SubqueryAlias,
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, expr::Unnest};
use sqlparser::ast::{
    AstBox as SQLBox, Expr as SQLExpr, FunctionArg, FunctionArgExpr, Ident,
    JsonOnBehavior, JsonQueryWrapper, JsonQuotesBehavior, Spanned, SqlJsonTable,
    SqlJsonTableColumn, SqlJsonTableExistsColumn, SqlJsonTableNestedColumn,
    SqlJsonTableRegularColumn, TableAliasColumnDef, TableFactor,
};

mod join;
mod set_returning;

use set_returning::SetReturningCall;

fn normalized_json_table_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// PostgreSQL puts JSON path names and output column names in one namespace.
/// Validate it before discarding the optional `AS path_name` labels during
/// relational lowering.
fn validate_sql_json_table_names(
    root_path_name: Option<&Ident>,
    columns: &[SqlJsonTableColumn],
) -> Result<()> {
    fn insert_name(seen: &mut HashSet<String>, ident: &Ident) -> Result<()> {
        let name = normalized_json_table_ident(ident);
        if !seen.insert(name.clone()) {
            return Err(datafusion_common::sqlstate_datafusion_err(
                "42712",
                format!("duplicate JSON_TABLE name {name}"),
            ));
        }
        Ok(())
    }

    fn visit(columns: &[SqlJsonTableColumn], seen: &mut HashSet<String>) -> Result<()> {
        for column in columns {
            match column {
                SqlJsonTableColumn::ForOrdinality(name) => insert_name(seen, name)?,
                SqlJsonTableColumn::Regular(column) => insert_name(seen, &column.name)?,
                SqlJsonTableColumn::Exists(column) => insert_name(seen, &column.name)?,
                SqlJsonTableColumn::Nested(column) => {
                    if let Some(name) = &column.path_name {
                        insert_name(seen, name)?;
                    }
                    visit(&column.columns, seen)?;
                }
            }
        }
        Ok(())
    }

    let mut seen = HashSet::new();
    if let Some(name) = root_path_name {
        insert_name(&mut seen, name)?;
    }
    visit(columns, &mut seen)
}

struct SqlToRelRelationContext<'a, 'b, S: ContextProvider> {
    planner: &'a SqlToRel<'b, S>,
    planner_context: &'a mut PlannerContext,
}

// Implement RelationPlannerContext
impl<'a, 'b, S: ContextProvider> RelationPlannerContext
    for SqlToRelRelationContext<'a, 'b, S>
{
    fn context_provider(&self) -> &dyn ContextProvider {
        self.planner.context_provider
    }

    fn plan(&mut self, relation: TableFactor) -> Result<LogicalPlan> {
        self.planner.create_relation(relation, self.planner_context)
    }

    fn sql_to_expr(
        &mut self,
        expr: sqlparser::ast::Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        self.planner.sql_to_expr(expr, schema, self.planner_context)
    }

    fn sql_expr_to_logical_expr(
        &mut self,
        expr: sqlparser::ast::Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        self.planner
            .sql_expr_to_logical_expr(expr, schema, self.planner_context)
    }

    fn normalize_ident(&self, ident: Ident) -> String {
        self.planner.ident_normalizer.normalize(ident)
    }

    fn object_name_to_table_reference(
        &self,
        name: sqlparser::ast::ObjectName,
    ) -> Result<TableReference> {
        self.planner.object_name_to_table_reference(name)
    }
}

/// Extracts all named pattern variable symbols from a MATCH_RECOGNIZE pattern.
///
/// This function recursively traverses the pattern AST and collects all Named symbols
/// (excluding Start and End anchors). The symbols are normalized and deduplicated
/// to prevent case-sensitivity issues (e.g., 'A' and 'a' are treated as the same).
fn extract_pattern_symbols(
    pattern: &sqlparser::ast::MatchRecognizePattern,
    normalizer: &impl Fn(Ident) -> String,
) -> Vec<String> {
    let mut symbols = Vec::new();
    extract_pattern_symbols_recursive(pattern, &mut symbols, normalizer);
    symbols
}

/// Recursive helper to extract pattern symbols from nested pattern structures.
///
/// Normalizes identifiers during extraction to ensure deduplication works correctly
/// with case-insensitive identifiers (e.g., PATTERN (A B a) should only have [a, b]).
fn extract_pattern_symbols_recursive(
    pattern: &sqlparser::ast::MatchRecognizePattern,
    symbols: &mut Vec<String>,
    normalizer: &impl Fn(Ident) -> String,
) {
    use sqlparser::ast::MatchRecognizePattern;

    match pattern {
        MatchRecognizePattern::Symbol(sqlparser::ast::MatchRecognizeSymbol::Named(
            ident,
        )) => {
            let name = normalizer(ident.clone());
            if !symbols.contains(&name) {
                symbols.push(name);
            }
        }
        MatchRecognizePattern::Symbol(_) => {
            // Skip Start and End anchors
        }
        MatchRecognizePattern::Exclude(sqlparser::ast::MatchRecognizeSymbol::Named(
            ident,
        )) => {
            let name = normalizer(ident.clone());
            if !symbols.contains(&name) {
                symbols.push(name);
            }
        }
        MatchRecognizePattern::Exclude(_) => {
            // Skip Start and End anchors
        }
        MatchRecognizePattern::Permute(syms) => {
            for sym in syms {
                if let sqlparser::ast::MatchRecognizeSymbol::Named(ident) = sym {
                    let name = normalizer(ident.clone());
                    if !symbols.contains(&name) {
                        symbols.push(name);
                    }
                }
            }
        }
        MatchRecognizePattern::Concat(patterns) => {
            for pat in patterns {
                extract_pattern_symbols_recursive(pat, symbols, normalizer);
            }
        }
        MatchRecognizePattern::Group(pat) => {
            extract_pattern_symbols_recursive(pat, symbols, normalizer);
        }
        MatchRecognizePattern::Alternation(patterns) => {
            for pat in patterns {
                extract_pattern_symbols_recursive(pat, symbols, normalizer);
            }
        }
        MatchRecognizePattern::Repetition(pat, _quantifier) => {
            extract_pattern_symbols_recursive(pat, symbols, normalizer);
        }
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Plan the arguments of a table function.
    ///
    /// A set-returning function takes the same named parameters as the scalar
    /// function of that name, so `srf(a, b, opt => v)` resolves against that
    /// signature before the arguments reach the table-function provider.
    fn plan_table_function_args(
        &self,
        function_name: &str,
        args: impl IntoIterator<Item = FunctionArg>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        let mut arg_names: Vec<Option<String>> = Vec::new();
        let mut planned: Vec<Expr> = Vec::new();
        for arg in args {
            arg_names.push(match &arg {
                FunctionArg::Named { name, .. } => Some(name.value.clone()),
                FunctionArg::ExprNamed {
                    name: SQLExpr::Identifier(name),
                    ..
                } => Some(name.value.clone()),
                _ => None,
            });
            let expr = match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                | FunctionArg::Variadic(FunctionArgExpr::Expr(expr))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                }
                | FunctionArg::ExprNamed {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                } => self.sql_expr_to_logical_expr(expr, schema, planner_context)?,
                other => {
                    return plan_err!("Unsupported function argument: {other:?}");
                }
            };
            // Every column in a function argument refers to the preceding
            // (implicit-lateral) FROM items. Convert nested references too:
            // ARRAY[r, r + 1] has a constructor at its root, so matching only
            // a top-level Column leaves its children local to the function's
            // empty one-row input.
            let expr = expr
                .transform(|expr| match expr {
                    Expr::Column(column) => {
                        match schema.qualified_field_from_column(&column) {
                            Ok((_, field)) => Ok(Transformed::yes(
                                Expr::OuterReferenceColumn(Arc::clone(field), column),
                            )),
                            Err(_) => Ok(Transformed::no(Expr::Column(column))),
                        }
                    }
                    other => Ok(Transformed::no(other)),
                })
                .data()?;
            planned.push(expr);
        }
        if !arg_names.iter().any(Option::is_some) {
            return Ok(planned);
        }
        let signature = self
            .context_provider
            .get_function_meta(function_name)
            .map(|func| func.signature().clone());
        match signature.as_ref().and_then(|s| s.parameter_names.as_ref()) {
            Some(param_names) => datafusion_expr::arguments::resolve_function_arguments(
                param_names,
                signature
                    .as_ref()
                    .and_then(|s| s.parameter_defaults.as_deref()),
                planned,
                arg_names,
            ),
            None => {
                plan_err!("Function '{function_name}' does not support named arguments")
            }
        }
    }

    /// Creates an augmented schema for MATCH_RECOGNIZE that includes pattern variables
    /// as valid table qualifiers.
    ///
    /// For each pattern variable (e.g., "A", "B"), adds all input columns qualified
    /// with that pattern variable name. This allows expressions like `A.value` to
    /// resolve correctly in MEASURES and DEFINE clauses.
    fn create_match_recognize_schema(
        &self,
        input_schema: &Arc<DFSchema>,
        pattern_var_names: &[String],
    ) -> Result<Arc<DFSchema>> {
        let mut qualified_fields: Vec<(Option<TableReference>, Arc<Field>)> = Vec::new();

        // For each pattern variable, add all input columns with that qualifier
        for pattern_var in pattern_var_names {
            let pattern_var_ref = TableReference::Bare {
                table: pattern_var.clone().into(),
            };

            for field in input_schema.fields() {
                // Create a qualified field with the pattern variable as the qualifier
                qualified_fields.push((Some(pattern_var_ref.clone()), field.clone()));
            }
        }

        // Also include the original input schema fields (with their original qualifiers)
        for (qualifier, field) in input_schema.iter() {
            qualified_fields.push((qualifier.cloned(), field.clone()));
        }

        // Create new schema with all qualified fields
        DFSchema::new_with_metadata(qualified_fields, input_schema.metadata().clone())
            .map(Arc::new)
    }

    /// Strips pattern variable qualifiers from expressions in MATCH_RECOGNIZE.
    ///
    /// Converts references like `A.value` to unqualified `value`, making the expression
    /// compatible with the input schema validation while preserving the semantic meaning
    /// for later pattern matching execution.
    fn strip_pattern_var_qualifiers(
        &self,
        expr: Expr,
        pattern_var_names: &[String],
    ) -> Expr {
        expr.transform(|e| {
            if let Expr::Column(col) = &e {
                if let Some(qualifier) = &col.relation {
                    // Check if this qualifier is a pattern variable
                    if pattern_var_names
                        .iter()
                        .any(|pv| qualifier.table() == pv.as_str())
                    {
                        // Strip the pattern variable qualifier, making it unqualified
                        return Ok(Transformed::yes(Expr::Column(
                            Column::new_unqualified(&col.name),
                        )));
                    }
                }
            }
            Ok(Transformed::no(e))
        })
        .data()
        .expect("transform should not fail")
    }

    /// Create a `LogicalPlan` that scans the named relation.
    ///
    /// First tries any registered extension planners. If no extension handles
    /// the relation, falls back to the default planner.
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let planned_relation =
            match self.create_extension_relation(relation, planner_context)? {
                RelationPlanning::Planned(planned) => planned,
                RelationPlanning::Original(original) => {
                    self.create_default_relation(original, planner_context)?
                }
            };

        let optimized_plan = optimize_subquery_sort(planned_relation.plan)?.data;
        if let Some(alias) = planned_relation.alias {
            self.apply_table_alias(optimized_plan, alias)
        } else {
            Ok(optimized_plan)
        }
    }

    fn create_relation_ref(
        &self,
        relation: &TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Extension planners currently own their input. Preserve that contract
        // at the explicit extension boundary; the built-in planner remains
        // borrowed for the normal Gantry path.
        if !self.context_provider.get_relation_planners().is_empty() {
            return self.create_relation(relation.clone(), planner_context);
        }

        let planned_relation =
            self.create_default_relation_ref(relation, planner_context)?;
        let optimized_plan = optimize_subquery_sort(planned_relation.plan)?.data;
        if let Some(alias) = planned_relation.alias {
            self.apply_table_alias(optimized_plan, alias)
        } else {
            Ok(optimized_plan)
        }
    }

    fn create_default_relation_ref(
        &self,
        relation: &TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<PlannedRelation> {
        let relation_span = relation.span();
        let (plan, alias) = match relation {
            TableFactor::Table {
                name,
                alias,
                args: Some(func_args),
                with_ordinality,
                ..
            } => self.plan_function_relations(
                vec![SetReturningCall {
                    name: name.clone(),
                    args: func_args.args.clone(),
                    column_defs: Vec::new(),
                }],
                *with_ordinality,
                alias.clone(),
                planner_context,
            )?,
            TableFactor::Table {
                name,
                alias,
                args: None,
                only,
                sample,
                ..
            } => {
                let table_ref = self.object_name_to_table_reference(name.clone())?;
                let table_name = table_ref.to_string();
                let cte = planner_context.get_cte(&table_name);
                let is_cte = cte.is_some();
                let resolved_table_ref = if is_cte {
                    table_ref.clone()
                } else {
                    self.context_provider
                        .resolve_table_reference(table_ref.clone())?
                };
                let mut plan = match (
                    cte,
                    self.context_provider
                        .get_table_source(resolved_table_ref.clone()),
                ) {
                    (Some(cte_plan), _) => Ok(cte_plan.clone()),
                    (_, Ok(provider)) => {
                        let plan = LogicalPlanBuilder::scan(
                            resolved_table_ref.clone(),
                            provider,
                            None,
                        )?
                        .build()?;
                        if *only {
                            if let LogicalPlan::TableScan(mut scan) = plan {
                                scan.only = true;
                                Ok(LogicalPlan::TableScan(scan))
                            } else {
                                Ok(plan)
                            }
                        } else {
                            Ok(plan)
                        }
                    }
                    (None, Err(error)) => {
                        Err(error.with_diagnostic(Diagnostic::new_error(
                            format!("table '{table_ref}' not found"),
                            Span::try_from_sqlparser_span(relation_span),
                        )))
                    }
                }?;
                if let Some(sample) = sample {
                    plan = self.apply_table_sample(
                        plan,
                        &resolved_table_ref,
                        sample,
                        is_cte,
                        planner_context,
                    )?;
                }
                (plan, alias.clone())
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => (
                self.query_to_plan_ref(subquery.as_ref(), planner_context)?,
                alias.clone(),
            ),
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let plan = self.plan_table_with_joins_ref(
                    table_with_joins.as_ref(),
                    planner_context,
                )?;
                let plan = match alias {
                    Some(_) => project_visible_columns(plan)?,
                    None => plan,
                };
                (plan, alias.clone())
            }
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
            } if self.context_provider.is_set_returning_function("unnest") => self
                .plan_function_relations(
                    vec![SetReturningCall::unnest(array_exprs)],
                    *with_ordinality,
                    alias.clone(),
                    planner_context,
                )?,
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
            } => {
                let schema = DFSchema::empty();
                let input = LogicalPlanBuilder::empty(true).build()?;
                let unnest_exprs = array_exprs
                    .iter()
                    .map(|sql_expr| {
                        let expr = self.sql_expr_to_logical_expr(
                            sql_expr,
                            &schema,
                            planner_context,
                        )?;
                        Self::check_unnest_arg(&expr, &schema)?;
                        Ok(Expr::Unnest(Unnest::new(expr)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if unnest_exprs.is_empty() {
                    return plan_err!("UNNEST must have at least one argument");
                }
                let options = if *with_ordinality {
                    Some(
                        UnnestOptions::new()
                            .with_preserve_nulls(false)
                            .with_ordinality(true),
                    )
                } else {
                    None
                };
                let single_unnest_output = !*with_ordinality && unnest_exprs.len() == 1;
                let logical_plan =
                    self.try_process_unnest_with_options(input, unnest_exprs, options)?;
                let mut alias = alias.clone();
                if single_unnest_output
                    && let Some(table_alias) = alias.as_mut()
                    && table_alias.columns.is_empty()
                {
                    table_alias.columns.push(TableAliasColumnDef {
                        name: table_alias.name.clone(),
                        data_type: None,
                        collation: None,
                    });
                }
                (logical_plan, alias)
            }
            TableFactor::Function {
                name, args, alias, ..
            } => self.plan_function_relations(
                vec![SetReturningCall {
                    name: name.clone(),
                    args: args.clone(),
                    column_defs: Vec::new(),
                }],
                false,
                alias.clone(),
                planner_context,
            )?,
            TableFactor::TableFunction { expr, alias } => self.plan_function_relations(
                vec![SetReturningCall::from_function_expr(expr, &[])?],
                false,
                alias.clone(),
                planner_context,
            )?,
            TableFactor::RowsFrom {
                functions,
                with_ordinality,
                alias,
                ..
            } => {
                let calls = functions
                    .iter()
                    .map(|item| {
                        SetReturningCall::from_function_expr(
                            &item.function,
                            &item.column_defs,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.plan_function_relations(
                    calls,
                    *with_ordinality,
                    alias.clone(),
                    planner_context,
                )?
            }
            // Complex relation extensions retain the established owned
            // implementation and only detach this individual factor.
            _ => return self.create_default_relation(relation.clone(), planner_context),
        };
        Ok(PlannedRelation::new(plan, alias))
    }

    /// Lower a parsed TABLESAMPLE clause directly into the provider's typed
    /// sampling predicate. No SQL rendering or reparsing occurs here.
    fn apply_table_sample(
        &self,
        plan: LogicalPlan,
        table_ref: &TableReference,
        kind: &sqlparser::ast::TableSampleKind,
        is_cte: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let sample = match kind {
            sqlparser::ast::TableSampleKind::BeforeTableAlias(sample)
            | sqlparser::ast::TableSampleKind::AfterTableAlias(sample) => sample.as_ref(),
        };
        if is_cte {
            return Err(self.context_provider.table_sample_source_error(table_ref));
        }
        if !matches!(
            sample.modifier,
            sqlparser::ast::TableSampleModifier::TableSample
        ) || sample.bucket.is_some()
            || sample.offset.is_some()
        {
            return not_impl_err!("unsupported sampling clause on {table_ref}");
        }
        let method = match &sample.name {
            Some(sqlparser::ast::TableSampleMethod::Bernoulli) => {
                LogicalTableSampleMethod::Bernoulli
            }
            Some(sqlparser::ast::TableSampleMethod::System) => {
                LogicalTableSampleMethod::System
            }
            Some(method) => return plan_err!("unsupported TABLESAMPLE method {method}"),
            None => return plan_err!("TABLESAMPLE method is required"),
        };
        let quantity = sample.quantity.as_ref().ok_or_else(|| {
            DataFusionError::Plan("TABLESAMPLE percentage is required".to_string())
        })?;
        if matches!(quantity.unit, Some(sqlparser::ast::TableSampleUnit::Rows)) {
            return not_impl_err!("TABLESAMPLE row counts are not supported");
        }
        let empty_schema = DFSchema::empty();
        let percentage = self.sql_expr_to_logical_expr(
            &quantity.value,
            &empty_schema,
            planner_context,
        )?;
        let repeatable = sample
            .seed
            .as_ref()
            .map(|seed| {
                let seed_expr = seed.expr.clone().unwrap_or_else(|| {
                    SQLExpr::Value(sqlparser::ast::ValueWithSpan::from(
                        seed.value.clone(),
                    ))
                });
                self.sql_expr_to_logical_expr(seed_expr, &empty_schema, planner_context)
            })
            .transpose()?;
        let predicate = self
            .context_provider
            .plan_table_sample(table_ref, method, percentage, repeatable)?;
        LogicalPlanBuilder::from(plan).filter(predicate)?.build()
    }

    fn create_extension_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<RelationPlanning> {
        let planners = self.context_provider.get_relation_planners();
        if planners.is_empty() {
            return Ok(RelationPlanning::Original(relation));
        }

        let mut current_relation = relation;
        for planner in planners.iter() {
            let mut context = SqlToRelRelationContext {
                planner: self,
                planner_context,
            };

            match planner.plan_relation(current_relation, &mut context)? {
                RelationPlanning::Planned(planned) => {
                    return Ok(RelationPlanning::Planned(planned));
                }
                RelationPlanning::Original(original) => {
                    current_relation = original;
                }
            }
        }

        Ok(RelationPlanning::Original(current_relation))
    }

    fn create_default_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<PlannedRelation> {
        if Self::is_function_relation(&relation, self.context_provider) {
            return self.create_default_relation_ref(&relation, planner_context);
        }
        let relation_span = relation.span();
        let (plan, alias) = match relation {
            TableFactor::Table {
                name,
                alias,
                only,
                sample,
                ..
            } => {
                // Normalize name and alias
                let table_ref = self.object_name_to_table_reference(name)?;
                let table_name = table_ref.to_string();
                let cte = planner_context.get_cte(&table_name);
                let is_cte = cte.is_some();
                let resolved_table_ref = if is_cte {
                    table_ref.clone()
                } else {
                    self.context_provider
                        .resolve_table_reference(table_ref.clone())?
                };
                let mut plan = match (
                    cte,
                    self.context_provider
                        .get_table_source(resolved_table_ref.clone()),
                ) {
                    (Some(cte_plan), _) => Ok(cte_plan.clone()),
                    (_, Ok(provider)) => {
                        let plan = LogicalPlanBuilder::scan(
                            resolved_table_ref.clone(),
                            provider,
                            None,
                        )?
                        .build()?;
                        // Preserve the PostgreSQL `FROM ONLY t` modifier on
                        // the scan so the engine can exclude inheriting
                        // descendant tables.
                        if only {
                            if let LogicalPlan::TableScan(mut scan) = plan {
                                scan.only = true;
                                Ok(LogicalPlan::TableScan(scan))
                            } else {
                                Ok(plan)
                            }
                        } else {
                            Ok(plan)
                        }
                    }
                    (None, Err(e)) => {
                        let e = e.with_diagnostic(Diagnostic::new_error(
                            format!("table '{table_ref}' not found"),
                            Span::try_from_sqlparser_span(relation_span),
                        ));
                        Err(e)
                    }
                }?;
                if let Some(sample) = sample.as_ref() {
                    plan = self.apply_table_sample(
                        plan,
                        &resolved_table_ref,
                        sample,
                        is_cte,
                        planner_context,
                    )?;
                }
                (plan, alias)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan =
                    self.query_to_plan(SQLBox::into_owned(subquery), planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let plan = self.plan_table_with_joins(
                    SQLBox::into_owned(table_with_joins),
                    planner_context,
                )?;
                let plan = match alias {
                    Some(_) => project_visible_columns(plan)?,
                    None => plan,
                };
                (plan, alias)
            }
            TableFactor::UNNEST {
                mut alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
            } => {
                // Unnest table factor has empty input
                let schema = DFSchema::empty();
                let input = LogicalPlanBuilder::empty(true).build()?;
                // Unnest table factor can have multiple arguments.
                // We treat each argument as a separate unnest expression.
                let unnest_exprs = array_exprs
                    .into_iter()
                    .map(|sql_expr| {
                        let expr = self.sql_expr_to_logical_expr(
                            sql_expr,
                            &schema,
                            planner_context,
                        )?;
                        Self::check_unnest_arg(&expr, &schema)?;
                        Ok(Expr::Unnest(Unnest::new(expr)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if unnest_exprs.is_empty() {
                    return plan_err!("UNNEST must have at least one argument");
                }

                // Create options with ordinality if requested
                let options = if with_ordinality {
                    Some(
                        UnnestOptions::new()
                            .with_preserve_nulls(false)
                            .with_ordinality(true),
                    )
                } else {
                    None
                };

                let single_unnest_output = !with_ordinality && unnest_exprs.len() == 1;
                let logical_plan =
                    self.try_process_unnest_with_options(input, unnest_exprs, options)?;

                // PostgreSQL compatibility: for a single-argument UNNEST with an alias but no
                // explicit column alias list, treat the relation alias as the output column name.
                // Example: `UNNEST(arr) AS x` exposes column `x`.
                if single_unnest_output {
                    if let Some(table_alias) = alias.as_mut()
                        && table_alias.columns.is_empty()
                    {
                        table_alias.columns.push(TableAliasColumnDef {
                            name: table_alias.name.clone(),
                            data_type: None,
                            collation: None,
                        });
                    }
                }

                (logical_plan, alias)
            }
            TableFactor::UNNEST { .. } => {
                return not_impl_err!(
                    "UNNEST table factor with offset is not supported yet"
                );
            }
            TableFactor::MatchRecognize {
                table,
                partition_by,
                order_by,
                measures,
                rows_per_match,
                after_match_skip,
                pattern,
                subsets,
                symbols,
                alias,
            } => {
                use datafusion_expr::{
                    AfterMatchSkipOption, EmptyMatchesMode, MatchRecognize, MeasureExpr,
                    Pattern, PatternSymbol, RepetitionQuantifier, RowsPerMatchOption,
                    SubsetDef, SymbolDef,
                };
                use sqlparser::ast::{
                    AfterMatchSkip, EmptyMatchesMode as SqlEmptyMatchesMode,
                    MatchRecognizePattern, MatchRecognizeSymbol, Measure,
                    RepetitionQuantifier as SqlRepetitionQuantifier, RowsPerMatch,
                    SubsetDefinition, SymbolDefinition,
                };

                // Plan the input table
                let input_plan =
                    self.create_relation(SQLBox::into_owned(table), planner_context)?;
                let input_schema = input_plan.schema();

                // Convert partition by expressions
                let partition_by_exprs: Vec<Expr> = partition_by
                    .into_iter()
                    .map(|e| self.sql_to_expr(e, input_schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;

                // Convert order by expressions
                let order_by_exprs = self.order_by_to_sort_expr(
                    order_by,
                    input_schema,
                    planner_context,
                    true,
                    None,
                )?;

                // Collect all pattern variable names from both PATTERN and DEFINE clauses
                // for schema augmentation. Pattern variables can appear in PATTERN without
                // being defined in DEFINE (e.g., PATTERN (STRT DOWN+ UP+) where STRT has no DEFINE).
                // Normalize during extraction to ensure proper deduplication.
                let mut pattern_var_names = extract_pattern_symbols(&pattern, &|ident| {
                    self.ident_normalizer.normalize(ident)
                });

                // Also add symbols from DEFINE clause
                for SymbolDefinition { symbol, .. } in &symbols {
                    let name = self.ident_normalizer.normalize(symbol.clone());
                    if !pattern_var_names.contains(&name) {
                        pattern_var_names.push(name);
                    }
                }

                // Create an augmented schema that includes pattern variables as qualifiers
                // This allows expressions like A.value to resolve correctly in MEASURES and DEFINE
                let augmented_schema =
                    self.create_match_recognize_schema(input_schema, &pattern_var_names)?;

                // Convert measures using the augmented schema, then strip pattern variable qualifiers
                let measure_exprs: Vec<MeasureExpr> = measures
                    .into_iter()
                    .map(|Measure { expr, alias }| {
                        let mut converted_expr =
                            self.sql_to_expr(expr, &augmented_schema, planner_context)?;
                        // Strip pattern variable qualifiers from the expression
                        // to make it compatible with input schema validation
                        converted_expr = self.strip_pattern_var_qualifiers(
                            converted_expr,
                            &pattern_var_names,
                        );
                        Ok(MeasureExpr {
                            expr: converted_expr,
                            alias: self.ident_normalizer.normalize(alias),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Convert rows per match
                let rows_per_match_opt = rows_per_match.map(|rpm| match rpm {
                    RowsPerMatch::OneRow => RowsPerMatchOption::OneRow,
                    RowsPerMatch::AllRows(mode) => {
                        let empty_mode = mode.map(|m| match m {
                            SqlEmptyMatchesMode::Show => EmptyMatchesMode::Show,
                            SqlEmptyMatchesMode::Omit => EmptyMatchesMode::Omit,
                            SqlEmptyMatchesMode::WithUnmatched => {
                                EmptyMatchesMode::WithUnmatched
                            }
                        });
                        RowsPerMatchOption::AllRows(empty_mode)
                    }
                });

                // Convert after match skip
                let after_match_skip_opt = after_match_skip.map(|ams| match ams {
                    AfterMatchSkip::PastLastRow => AfterMatchSkipOption::PastLastRow,
                    AfterMatchSkip::ToNextRow => AfterMatchSkipOption::ToNextRow,
                    AfterMatchSkip::ToFirst(ident) => AfterMatchSkipOption::ToFirst(
                        self.ident_normalizer.normalize(ident),
                    ),
                    AfterMatchSkip::ToLast(ident) => AfterMatchSkipOption::ToLast(
                        self.ident_normalizer.normalize(ident),
                    ),
                });

                // Helper function to convert pattern symbols
                let convert_symbol = |sym: MatchRecognizeSymbol| match sym {
                    MatchRecognizeSymbol::Named(ident) => {
                        PatternSymbol::Named(self.ident_normalizer.normalize(ident))
                    }
                    MatchRecognizeSymbol::Start => PatternSymbol::Start,
                    MatchRecognizeSymbol::End => PatternSymbol::End,
                };

                // Helper function to convert repetition quantifiers
                let convert_quantifier =
                    |q: SqlRepetitionQuantifier| -> Result<RepetitionQuantifier> {
                        match q {
                            SqlRepetitionQuantifier::ZeroOrMore => {
                                Ok(RepetitionQuantifier::ZeroOrMore)
                            }
                            SqlRepetitionQuantifier::OneOrMore => {
                                Ok(RepetitionQuantifier::OneOrMore)
                            }
                            SqlRepetitionQuantifier::AtMostOne => {
                                Ok(RepetitionQuantifier::AtMostOne)
                            }
                            SqlRepetitionQuantifier::Exactly(n) => {
                                if n == 0 {
                                    plan_err!(
                                        "Invalid pattern quantifier: {{0}} can never match"
                                    )
                                } else {
                                    Ok(RepetitionQuantifier::Exactly(n))
                                }
                            }
                            SqlRepetitionQuantifier::AtLeast(n) => {
                                Ok(RepetitionQuantifier::AtLeast(n))
                            }
                            SqlRepetitionQuantifier::AtMost(n) => {
                                if n == 0 {
                                    plan_err!(
                                        "Invalid pattern quantifier: {{,0}} can never match"
                                    )
                                } else {
                                    Ok(RepetitionQuantifier::AtMost(n))
                                }
                            }
                            SqlRepetitionQuantifier::Range(min, max) => {
                                if min > max {
                                    plan_err!(
                                        "Invalid pattern quantifier: minimum {} exceeds maximum {}",
                                        min,
                                        max
                                    )
                                } else if min == 0 && max == 0 {
                                    plan_err!(
                                        "Invalid pattern quantifier: {{0,0}} can never match"
                                    )
                                } else {
                                    Ok(RepetitionQuantifier::Range(min, max))
                                }
                            }
                        }
                    };

                // Recursive function to convert pattern
                fn convert_pattern(
                    pat: MatchRecognizePattern,
                    convert_symbol: &impl Fn(MatchRecognizeSymbol) -> PatternSymbol,
                    convert_quantifier: &impl Fn(
                        SqlRepetitionQuantifier,
                    )
                        -> Result<RepetitionQuantifier>,
                ) -> Result<Pattern> {
                    match pat {
                        MatchRecognizePattern::Symbol(sym) => {
                            Ok(Pattern::Symbol(convert_symbol(sym)))
                        }
                        MatchRecognizePattern::Exclude(sym) => {
                            Ok(Pattern::Exclude(convert_symbol(sym)))
                        }
                        MatchRecognizePattern::Permute(syms) => Ok(Pattern::Permute(
                            syms.into_iter().map(convert_symbol).collect(),
                        )),
                        MatchRecognizePattern::Concat(pats) => {
                            let converted_pats = pats
                                .into_iter()
                                .map(|p| {
                                    convert_pattern(p, convert_symbol, convert_quantifier)
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Ok(Pattern::Concat(converted_pats))
                        }
                        MatchRecognizePattern::Group(pat) => {
                            let converted_pat = convert_pattern(
                                SQLBox::into_owned(pat),
                                convert_symbol,
                                convert_quantifier,
                            )?;
                            Ok(Pattern::Group(Box::new(converted_pat)))
                        }
                        MatchRecognizePattern::Alternation(pats) => {
                            let converted_pats = pats
                                .into_iter()
                                .map(|p| {
                                    convert_pattern(p, convert_symbol, convert_quantifier)
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Ok(Pattern::Alternation(converted_pats))
                        }
                        MatchRecognizePattern::Repetition(pat, quant) => {
                            let converted_pat = convert_pattern(
                                SQLBox::into_owned(pat),
                                convert_symbol,
                                convert_quantifier,
                            )?;
                            let converted_quant = convert_quantifier(quant)?;
                            Ok(Pattern::Repetition(
                                Box::new(converted_pat),
                                converted_quant,
                            ))
                        }
                    }
                }

                let pattern_expr =
                    convert_pattern(pattern, &convert_symbol, &convert_quantifier)?;

                // Convert subsets
                let subset_defs: Vec<SubsetDef> = subsets
                    .into_iter()
                    .map(|SubsetDefinition { name, symbols }| SubsetDef {
                        name: self.ident_normalizer.normalize(name),
                        symbols: symbols
                            .into_iter()
                            .map(|s| self.ident_normalizer.normalize(s))
                            .collect(),
                    })
                    .collect();

                // Convert symbol definitions using the augmented schema, then strip pattern var qualifiers
                let symbol_defs: Vec<SymbolDef> = symbols
                    .into_iter()
                    .map(|SymbolDefinition { symbol, definition }| {
                        let mut converted_expr = self.sql_to_expr(
                            definition,
                            &augmented_schema,
                            planner_context,
                        )?;
                        // Strip pattern variable qualifiers from the expression
                        converted_expr = self.strip_pattern_var_qualifiers(
                            converted_expr,
                            &pattern_var_names,
                        );
                        Ok(SymbolDef {
                            symbol: self.ident_normalizer.normalize(symbol),
                            definition: converted_expr,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Create the MatchRecognize plan
                let match_recognize_plan = MatchRecognize::try_new(
                    Arc::new(input_plan),
                    partition_by_exprs,
                    order_by_exprs,
                    measure_exprs,
                    rows_per_match_opt,
                    after_match_skip_opt,
                    pattern_expr,
                    subset_defs,
                    symbol_defs,
                )?;

                (LogicalPlan::MatchRecognize(match_recognize_plan), alias)
            }
            TableFactor::JsonTable {
                json_expr,
                json_path,
                columns,
                alias,
            } => {
                // Plan JSON_TABLE function
                let plan = self.plan_json_table(
                    json_expr,
                    Self::json_table_path_string(json_path)?,
                    columns,
                    planner_context,
                )?;
                (plan, alias)
            }
            TableFactor::SqlJsonTable(json_table) => {
                let alias = json_table.alias.clone();
                let plan = self.plan_sql_json_table(json_table, planner_context)?;
                (plan, alias)
            }
            TableFactor::GraphTable {
                graph_name,
                match_clause,
                alias,
            } => {
                // Plan GRAPH_TABLE function
                let plan = self.plan_graph_table(
                    graph_name,
                    SQLBox::into_owned(match_clause),
                    planner_context,
                )?;
                (plan, alias)
            }
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };
        Ok(PlannedRelation::new(plan, alias))
    }

    /// The FROM items that are function calls, planned by
    /// [`Self::plan_function_relations`] whichever way the factor is spelled.
    fn is_function_relation(relation: &TableFactor, provider: &S) -> bool {
        match relation {
            TableFactor::Table { args: Some(_), .. }
            | TableFactor::Function { .. }
            | TableFactor::TableFunction { .. }
            | TableFactor::RowsFrom { .. } => true,
            TableFactor::UNNEST {
                with_offset: false,
                with_offset_alias: None,
                ..
            } => provider.is_set_returning_function("unnest"),
            _ => false,
        }
    }

    pub(crate) fn create_relation_subquery_ref(
        &self,
        subquery: &TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // At this point for a syntactically valid query the outer_from_schema is
        // guaranteed to be set, so the `.unwrap()` call will never panic. This
        // is the case because we only call this method for lateral table
        // factors, and those can never be the first factor in a FROM list. This
        // means we arrived here through the `for` loop in `plan_from_tables` or
        // the `for` loop in `plan_table_with_joins`.
        let old_from_schema = planner_context
            .set_outer_from_schema(None)
            .unwrap_or_else(|| Arc::new(DFSchema::empty()));
        let new_query_schema = match planner_context.outer_query_schema() {
            Some(old_query_schema) => {
                let mut new_query_schema = old_from_schema.as_ref().clone();
                new_query_schema.merge(old_query_schema);
                Some(Arc::new(new_query_schema))
            }
            None => Some(Arc::clone(&old_from_schema)),
        };
        let old_query_schema = planner_context.set_outer_query_schema(new_query_schema);

        let plan = self.create_relation_ref(subquery, planner_context)?;
        let outer_ref_columns = plan.all_out_ref_exprs();

        planner_context.set_outer_query_schema(old_query_schema);
        planner_context.set_outer_from_schema(Some(old_from_schema));

        // We can omit the subquery wrapper if there are no columns
        // referencing the outer scope.
        if outer_ref_columns.is_empty() {
            return Ok(plan);
        }

        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                subquery_alias(
                    LogicalPlan::Subquery(Subquery {
                        subquery: input,
                        outer_ref_columns,
                        spans: Spans::new(),
                    }),
                    alias,
                )
            }
            plan => Ok(LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns,
                spans: Spans::new(),
            })),
        }
    }
}

/// Narrow a join subtree that is about to be given an alias to its visible
/// column list. The alias renames every column of the subtree, and the copies
/// a USING join hides behind its merged columns have no name to be reached by
/// under it.
fn project_visible_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    let hidden: HashSet<Column> = plan
        .using_columns()?
        .into_iter()
        .flat_map(|using| using.hidden)
        .collect();
    if hidden.is_empty() {
        return Ok(plan);
    }
    let exprs: Vec<Expr> = plan
        .schema()
        .iter()
        .map(Column::from)
        .filter(|column| !hidden.contains(column))
        .map(Expr::Column)
        .collect();
    LogicalPlanBuilder::from(plan).project(exprs)?.build()
}

fn optimize_subquery_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // When initializing subqueries, we examine sort options since they might be unnecessary.
    // They are only important if the subquery result is affected by the ORDER BY statement,
    // which can happen when we have:
    // 1. DISTINCT ON / ARRAY_AGG ... => Handled by an `Aggregate` and its requirements.
    // 2. RANK / ROW_NUMBER ... => Handled by a `WindowAggr` and its requirements.
    // 3. LIMIT => Handled by a `Sort`, so we need to search for it.
    let mut has_limit = false;

    plan.transform_down(|c| {
        if let LogicalPlan::Limit(_) = c {
            has_limit = true;
            return Ok(Transformed::no(c));
        }
        match c {
            LogicalPlan::Sort(s) => {
                if !has_limit {
                    has_limit = false;
                    return Ok(Transformed::yes(s.input.as_ref().clone()));
                }
                Ok(Transformed::no(LogicalPlan::Sort(s)))
            }
            _ => Ok(Transformed::no(c)),
        }
    })
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Plan JSON_TABLE table factor.
    ///
    /// JSON_TABLE transforms JSON data into a relational table format.
    /// Syntax: JSON_TABLE(json_expr, path COLUMNS(...))
    fn plan_json_table(
        &self,
        json_expr: sqlparser::ast::Expr,
        json_path: String,
        columns: Vec<sqlparser::ast::JsonTableColumn>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Convert the JSON expression to a DataFusion Expr
        // Use an empty schema since we're creating a table from JSON
        let empty_schema = DFSchema::empty();
        let df_json_expr =
            self.sql_expr_to_logical_expr(json_expr, &empty_schema, planner_context)?;

        // Convert sqlparser column definitions to DataFusion column definitions
        let df_columns = self.convert_json_table_columns(columns)?;

        // Create the JsonTable logical plan node
        let json_table = JsonTable::try_new(df_json_expr, json_path, df_columns)?;

        Ok(LogicalPlan::JsonTable(json_table))
    }

    /// Convert sqlparser JsonTableColumn to DataFusion JsonTableColumnDef
    fn convert_json_table_columns(
        &self,
        columns: Vec<sqlparser::ast::JsonTableColumn>,
    ) -> Result<Vec<JsonTableColumnDef>> {
        columns
            .into_iter()
            .map(|col| self.convert_json_table_column(col))
            .collect()
    }

    /// Convert a single JsonTableColumn to JsonTableColumnDef
    fn convert_json_table_column(
        &self,
        column: sqlparser::ast::JsonTableColumn,
    ) -> Result<JsonTableColumnDef> {
        match column {
            sqlparser::ast::JsonTableColumn::Named(named) => {
                let name = named.name.value;
                let field = self.convert_data_type_to_field(&named.r#type)?;
                let data_type = field.data_type().clone();

                // Extract path - it should be a string literal
                let path = match named.path {
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s) => s,
                    _ => {
                        return plan_err!("JSON_TABLE path must be a string literal");
                    }
                };

                let on_empty = named
                    .on_empty
                    .map(|h| self.convert_error_handling(h))
                    .transpose()?;
                let on_error = named
                    .on_error
                    .map(|h| self.convert_error_handling(h))
                    .transpose()?;

                Ok(JsonTableColumnDef::Path {
                    name,
                    data_type,
                    metadata: FieldMetadata::from(field.as_ref()),
                    path,
                    exists: named.exists,
                    format_json: false,
                    wrapper: None,
                    omit_quotes: false,
                    on_empty,
                    on_error,
                })
            }
            sqlparser::ast::JsonTableColumn::ForOrdinality(ident) => {
                Ok(JsonTableColumnDef::Ordinality { name: ident.value })
            }
            sqlparser::ast::JsonTableColumn::Nested(nested) => {
                // Extract nested path - it should be a string literal
                let path = match nested.path {
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s) => s,
                    _ => {
                        return plan_err!(
                            "JSON_TABLE nested path must be a string literal"
                        );
                    }
                };

                let nested_columns = self.convert_json_table_columns(nested.columns)?;

                Ok(JsonTableColumnDef::Nested {
                    path,
                    columns: nested_columns,
                })
            }
        }
    }

    /// Convert sqlparser JsonTableColumnErrorHandling to DataFusion JsonTableErrorHandling
    fn convert_error_handling(
        &self,
        handling: sqlparser::ast::JsonTableColumnErrorHandling,
    ) -> Result<JsonTableErrorHandling> {
        match handling {
            sqlparser::ast::JsonTableColumnErrorHandling::Null => {
                Ok(JsonTableErrorHandling::Null)
            }
            sqlparser::ast::JsonTableColumnErrorHandling::Error => {
                Ok(JsonTableErrorHandling::Error)
            }
            sqlparser::ast::JsonTableColumnErrorHandling::Default(value) => Ok(
                JsonTableErrorHandling::Default(Self::json_table_default_scalar(value)?),
            ),
        }
    }

    /// The literal of a JSON_TABLE `DEFAULT <value> ON EMPTY/ERROR` clause.
    fn json_table_default_scalar(value: sqlparser::ast::Value) -> Result<ScalarValue> {
        match value {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(ScalarValue::Int64(Some(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(ScalarValue::Float64(Some(f)))
                } else {
                    plan_err!("Invalid numeric value in DEFAULT clause")
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => {
                Ok(ScalarValue::Utf8(Some(s)))
            }
            sqlparser::ast::Value::Boolean(b) => Ok(ScalarValue::Boolean(Some(b))),
            sqlparser::ast::Value::Null => Ok(ScalarValue::Null),
            _ => plan_err!("Unsupported default value type in JSON_TABLE"),
        }
    }

    /// Plan the PostgreSQL spelling of `JSON_TABLE` onto the same logical
    /// plan node as the generic `JSON_TABLE` table factor.
    fn plan_sql_json_table(
        &self,
        json_table: SqlJsonTable,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let SqlJsonTable {
            context_item,
            path,
            path_name,
            passing,
            columns,
            on_error,
            alias: _,
        } = json_table;
        validate_sql_json_table_names(path_name.as_ref(), &columns)?;
        let json_path = match path {
            SQLExpr::Value(value) => Self::json_table_path_string(value.value)?,
            other => {
                return plan_err!(
                    "JSON_TABLE path must be a string literal, got {other}"
                );
            }
        };
        let empty_schema = DFSchema::empty();
        let df_json_expr =
            self.sql_expr_to_logical_expr(context_item, &empty_schema, planner_context)?;
        let passing =
            self.plan_passing_variables(&passing, &empty_schema, planner_context)?;
        let df_columns = self.convert_sql_json_table_columns(columns)?;
        let on_error = match on_error {
            None | Some(JsonOnBehavior::EmptyArray) => JsonTableErrorHandling::Null,
            Some(JsonOnBehavior::Error) => JsonTableErrorHandling::Error,
            Some(other) => {
                return Err(datafusion_common::sqlstate_datafusion_err(
                    "42601",
                    format!("JSON_TABLE table-level {other} ON ERROR is not permitted"),
                ));
            }
        };
        Ok(LogicalPlan::JsonTable(JsonTable::try_new_with_options(
            df_json_expr,
            passing,
            json_path,
            df_columns,
            on_error,
        )?))
    }

    fn json_table_path_string(value: sqlparser::ast::Value) -> Result<String> {
        match value {
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(s),
            other => plan_err!("JSON_TABLE path must be a string literal, got {other}"),
        }
    }

    /// A column without `PATH` reads the member named after the column.
    fn sql_json_table_column_path(
        name: &Ident,
        path: Option<sqlparser::ast::Value>,
    ) -> Result<String> {
        match path {
            Some(value) => Self::json_table_path_string(value),
            None => Ok(format!("$.{}", name.value)),
        }
    }

    fn convert_sql_json_table_columns(
        &self,
        columns: Vec<SqlJsonTableColumn>,
    ) -> Result<Vec<JsonTableColumnDef>> {
        columns
            .into_iter()
            .map(|column| self.convert_sql_json_table_column(column))
            .collect()
    }

    fn convert_sql_json_table_column(
        &self,
        column: SqlJsonTableColumn,
    ) -> Result<JsonTableColumnDef> {
        match column {
            SqlJsonTableColumn::ForOrdinality(ident) => {
                Ok(JsonTableColumnDef::Ordinality { name: ident.value })
            }
            SqlJsonTableColumn::Regular(SqlJsonTableRegularColumn {
                name,
                data_type,
                format,
                path,
                wrapper,
                quotes,
                on_empty,
                on_error,
            }) => {
                let format_json = format.is_some();
                let wrapper = wrapper.as_ref().map(|wrapper| match wrapper {
                    JsonQueryWrapper::Without | JsonQueryWrapper::WithoutArray => {
                        "without"
                    }
                    JsonQueryWrapper::WithConditional
                    | JsonQueryWrapper::WithConditionalArray => "conditional",
                    JsonQueryWrapper::With
                    | JsonQueryWrapper::WithArray
                    | JsonQueryWrapper::WithUnconditional
                    | JsonQueryWrapper::WithUnconditionalArray => "unconditional",
                });
                let omit_quotes = quotes
                    .as_ref()
                    .is_some_and(|quotes| quotes.behavior == JsonQuotesBehavior::Omit);
                let path = Self::sql_json_table_column_path(&name, path)?;
                let field = self.convert_data_type_to_field(&data_type)?;
                Ok(JsonTableColumnDef::Path {
                    name: name.value,
                    data_type: field.data_type().clone(),
                    metadata: FieldMetadata::from(field.as_ref()),
                    path,
                    exists: false,
                    format_json,
                    wrapper: wrapper.map(str::to_string),
                    omit_quotes,
                    on_empty: on_empty
                        .map(Self::convert_sql_json_behavior)
                        .transpose()?,
                    on_error: on_error
                        .map(Self::convert_sql_json_behavior)
                        .transpose()?,
                })
            }
            SqlJsonTableColumn::Exists(SqlJsonTableExistsColumn {
                name,
                data_type,
                path,
                on_error,
            }) => {
                let path = Self::sql_json_table_column_path(&name, path)?;
                let field = self.convert_data_type_to_field(&data_type)?;
                if !matches!(
                    field.data_type(),
                    DataType::Boolean
                        | DataType::Int32
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                ) {
                    return Err(datafusion_common::sqlstate_datafusion_err(
                        "42804",
                        format!(
                            "JSON_TABLE EXISTS column {name} must accept assignment from boolean"
                        ),
                    ));
                }
                Ok(JsonTableColumnDef::Path {
                    name: name.value,
                    data_type: field.data_type().clone(),
                    metadata: FieldMetadata::from(field.as_ref()),
                    path,
                    exists: true,
                    format_json: false,
                    wrapper: None,
                    omit_quotes: false,
                    on_empty: None,
                    on_error: on_error
                        .map(Self::convert_sql_json_behavior)
                        .transpose()?,
                })
            }
            SqlJsonTableColumn::Nested(SqlJsonTableNestedColumn {
                path,
                path_name,
                columns,
            }) => {
                let _ = path_name;
                Ok(JsonTableColumnDef::Nested {
                    path: Self::json_table_path_string(path)?,
                    columns: self.convert_sql_json_table_columns(columns)?,
                })
            }
        }
    }

    /// The behaviors a JSON_TABLE column clause can carry into the plan node:
    /// `NULL`, `ERROR`, a literal `DEFAULT`, and the boolean outcomes of an
    /// `EXISTS` column.
    fn convert_sql_json_behavior(
        behavior: JsonOnBehavior,
    ) -> Result<JsonTableErrorHandling> {
        match behavior {
            JsonOnBehavior::Null | JsonOnBehavior::Unknown => {
                Ok(JsonTableErrorHandling::Null)
            }
            JsonOnBehavior::Error => Ok(JsonTableErrorHandling::Error),
            JsonOnBehavior::True => Ok(JsonTableErrorHandling::Default(
                ScalarValue::Boolean(Some(true)),
            )),
            JsonOnBehavior::False => Ok(JsonTableErrorHandling::Default(
                ScalarValue::Boolean(Some(false)),
            )),
            JsonOnBehavior::Default(expr) => match expr.as_ref() {
                SQLExpr::Value(value) => Ok(JsonTableErrorHandling::Default(
                    Self::json_table_default_scalar(value.value.clone())?,
                )),
                other => not_impl_err!("JSON_TABLE DEFAULT {other} must be a literal"),
            },
            JsonOnBehavior::EmptyArray | JsonOnBehavior::EmptyObject => {
                not_impl_err!("JSON_TABLE {behavior} ON EMPTY/ERROR is not supported")
            }
        }
    }

    /// Plan GRAPH_TABLE table factor (SQL/PGQ).
    ///
    /// GRAPH_TABLE performs pattern matching on property graphs and returns
    /// results as a relational table.
    /// Syntax: GRAPH_TABLE(graph_name MATCH pattern WHERE ... COLUMNS(...))
    fn plan_graph_table(
        &self,
        graph_name: sqlparser::ast::ObjectName,
        match_clause: sqlparser::ast::GraphMatchClause,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Convert graph name to table reference
        let graph_ref = self.object_name_to_table_reference(graph_name)?;

        // Convert path finding algorithm
        let path_finding = match_clause
            .path_finding
            .map(|pf| self.convert_path_finding(pf))
            .transpose()?;

        // Convert path mode
        let path_mode = match_clause.path_mode.map(|pm| self.convert_path_mode(pm));

        // Convert row limiting
        let row_limiting = match_clause
            .row_limiting
            .map(|rl| self.convert_row_limiting(rl));

        // Convert graph patterns
        let patterns = match_clause
            .patterns
            .into_iter()
            .map(|p| self.convert_graph_pattern(p))
            .collect::<Result<Vec<_>>>()?;

        // Convert WHERE clause if present
        // Use an empty schema initially - schema will be computed based on patterns
        let empty_schema = DFSchema::empty();
        let where_clause = match_clause
            .where_clause
            .map(|expr| {
                self.sql_expr_to_logical_expr(expr, &empty_schema, planner_context)
            })
            .transpose()?;

        // Convert COLUMNS clause
        let columns = match_clause
            .columns
            .map(|cols| {
                cols.columns
                    .into_iter()
                    .map(|col| {
                        let expr = self.sql_expr_to_logical_expr(
                            col.expr,
                            &empty_schema,
                            planner_context,
                        )?;
                        Ok(GraphColumn {
                            expr,
                            alias: col.alias.map(|a| self.ident_normalizer.normalize(a)),
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        // Build schema from columns
        let schema = self.build_graph_table_schema(&columns, &empty_schema)?;

        // Create the GraphTable logical plan node
        let graph_table = GraphTable::try_new(
            graph_ref,
            path_finding,
            path_mode,
            row_limiting,
            patterns,
            where_clause,
            columns,
            Arc::new(schema),
        )?;

        Ok(LogicalPlan::GraphTable(graph_table))
    }

    /// Convert sqlparser path finding to DataFusion PathFinding
    fn convert_path_finding(
        &self,
        pf: sqlparser::ast::PathFinding,
    ) -> Result<PathFinding> {
        use sqlparser::ast::PathFinding as SqlPF;
        use sqlparser::ast::PathVariant;
        Ok(match pf {
            SqlPF::Any => PathFinding::Any,
            SqlPF::AnyShortest => PathFinding::AnyShortest,
            SqlPF::AllShortest => PathFinding::AllShortest,
            SqlPF::Shortest { k, variant } => PathFinding::Shortest {
                k,
                groups: matches!(variant, Some(PathVariant::PathGroups)),
            },
            SqlPF::AnyCheapest => PathFinding::AnyCheapest,
            SqlPF::AllCheapest => PathFinding::AllCheapest,
            SqlPF::Cheapest { k, variant } => PathFinding::Cheapest {
                k,
                groups: matches!(variant, Some(PathVariant::PathGroups)),
            },
            SqlPF::All => PathFinding::All,
        })
    }

    /// Convert sqlparser path mode to DataFusion PathMode
    fn convert_path_mode(&self, pm: sqlparser::ast::PathMode) -> PathMode {
        use sqlparser::ast::PathMode as SqlPM;
        match pm {
            SqlPM::Walk => PathMode::Walk,
            SqlPM::Trail => PathMode::Trail,
            SqlPM::Acyclic => PathMode::Acyclic,
            SqlPM::Simple => PathMode::Simple,
        }
    }

    /// Convert sqlparser row limiting to DataFusion RowLimiting
    fn convert_row_limiting(&self, rl: sqlparser::ast::RowLimiting) -> RowLimiting {
        use sqlparser::ast::RowLimiting as SqlRL;
        match rl {
            SqlRL::OneRowPerMatch => RowLimiting::OneRowPerMatch,
            SqlRL::OneRowPerVertex => RowLimiting::OneRowPerVertex,
            SqlRL::OneRowPerStep => RowLimiting::OneRowPerStep,
        }
    }

    /// Convert sqlparser GraphPattern to DataFusion GraphPattern
    fn convert_graph_pattern(
        &self,
        pattern: sqlparser::ast::GraphPattern,
    ) -> Result<GraphPattern> {
        Ok(GraphPattern {
            path_variable: pattern
                .path_variable
                .map(|v| self.ident_normalizer.normalize(v)),
            expr: self.convert_graph_pattern_expr(pattern.expr)?,
        })
    }

    /// Convert sqlparser GraphPatternExpr to DataFusion GraphPatternExpr
    fn convert_graph_pattern_expr(
        &self,
        expr: sqlparser::ast::GraphPatternExpr,
    ) -> Result<GraphPatternExpr> {
        use sqlparser::ast::GraphPatternExpr as SqlGPE;
        Ok(match expr {
            SqlGPE::Chain(elements) => GraphPatternExpr::Chain(
                elements
                    .into_iter()
                    .map(|e| self.convert_graph_pattern_element(e))
                    .collect::<Result<Vec<_>>>()?,
            ),
            SqlGPE::Alternation(patterns) => GraphPatternExpr::Alternation(
                patterns
                    .into_iter()
                    .map(|p| self.convert_graph_pattern_expr(p))
                    .collect::<Result<Vec<_>>>()?,
            ),
            SqlGPE::Group {
                pattern,
                quantifier,
            } => GraphPatternExpr::Group {
                pattern: Box::new(
                    self.convert_graph_pattern_expr(SQLBox::into_owned(pattern))?,
                ),
                quantifier: quantifier.map(|q| self.convert_quantifier(q)),
            },
        })
    }

    /// Convert sqlparser GraphPatternElement to DataFusion GraphPatternElement
    fn convert_graph_pattern_element(
        &self,
        element: sqlparser::ast::GraphPatternElement,
    ) -> Result<GraphPatternElement> {
        use sqlparser::ast::GraphPatternElement as SqlGPE;
        Ok(match element {
            SqlGPE::Node(node) => {
                GraphPatternElement::Node(self.convert_node_pattern(node)?)
            }
            SqlGPE::Edge(edge) => {
                GraphPatternElement::Edge(self.convert_edge_pattern(edge)?)
            }
            SqlGPE::Subpattern(expr) => GraphPatternElement::Subpattern(Box::new(
                self.convert_graph_pattern_expr(expr)?,
            )),
        })
    }

    /// Convert sqlparser NodePattern to DataFusion NodePattern
    fn convert_node_pattern(
        &self,
        node: sqlparser::ast::NodePattern,
    ) -> Result<NodePattern> {
        let empty_schema = DFSchema::empty();
        let mut planner_context = PlannerContext::new();

        Ok(NodePattern {
            variable: node.variable.map(|v| self.ident_normalizer.normalize(v)),
            labels: node
                .labels
                .into_iter()
                .map(|l| self.convert_label_expression(l))
                .collect(),
            properties: node
                .properties
                .into_iter()
                .map(|p| {
                    let key = self.ident_normalizer.normalize(p.key);
                    let value = self.sql_expr_to_logical_expr(
                        p.value,
                        &empty_schema,
                        &mut planner_context,
                    )?;
                    Ok((key, value))
                })
                .collect::<Result<Vec<_>>>()?,
            where_clause: node
                .where_clause
                .map(|e| {
                    self.sql_expr_to_logical_expr(e, &empty_schema, &mut planner_context)
                })
                .transpose()?,
        })
    }

    /// Convert sqlparser EdgePattern to DataFusion EdgePattern
    fn convert_edge_pattern(
        &self,
        edge: sqlparser::ast::EdgePattern,
    ) -> Result<EdgePattern> {
        let empty_schema = DFSchema::empty();
        let mut planner_context = PlannerContext::new();

        Ok(EdgePattern {
            variable: edge.variable.map(|v| self.ident_normalizer.normalize(v)),
            labels: edge
                .labels
                .into_iter()
                .map(|l| self.convert_label_expression(l))
                .collect(),
            properties: edge
                .properties
                .into_iter()
                .map(|p| {
                    let key = self.ident_normalizer.normalize(p.key);
                    let value = self.sql_expr_to_logical_expr(
                        p.value,
                        &empty_schema,
                        &mut planner_context,
                    )?;
                    Ok((key, value))
                })
                .collect::<Result<Vec<_>>>()?,
            where_clause: edge
                .where_clause
                .map(|e| {
                    self.sql_expr_to_logical_expr(e, &empty_schema, &mut planner_context)
                })
                .transpose()?,
            direction: self.convert_edge_direction(edge.direction),
            quantifier: edge.quantifier.map(|q| self.convert_quantifier(q)),
        })
    }

    /// Convert sqlparser EdgeDirection to DataFusion EdgeDirection
    fn convert_edge_direction(
        &self,
        dir: sqlparser::ast::EdgeDirection,
    ) -> EdgeDirection {
        use sqlparser::ast::EdgeDirection as SqlED;
        match dir {
            SqlED::Right => EdgeDirection::Right,
            SqlED::Left => EdgeDirection::Left,
            SqlED::Undirected => EdgeDirection::Undirected,
            SqlED::Any => EdgeDirection::Any,
        }
    }

    /// Convert sqlparser LabelExpression to DataFusion LabelExpression
    fn convert_label_expression(
        &self,
        label: sqlparser::ast::LabelExpression,
    ) -> LabelExpression {
        use sqlparser::ast::LabelExpression as SqlLE;
        match label {
            SqlLE::Label(ident) => {
                LabelExpression::Label(self.ident_normalizer.normalize(ident))
            }
            SqlLE::Wildcard => LabelExpression::Wildcard,
            SqlLE::Not(expr) => LabelExpression::Not(Box::new(
                self.convert_label_expression(SQLBox::into_owned(expr)),
            )),
            SqlLE::And(left, right) => LabelExpression::And(
                Box::new(self.convert_label_expression(SQLBox::into_owned(left))),
                Box::new(self.convert_label_expression(SQLBox::into_owned(right))),
            ),
            SqlLE::Or(left, right) => LabelExpression::Or(
                Box::new(self.convert_label_expression(SQLBox::into_owned(left))),
                Box::new(self.convert_label_expression(SQLBox::into_owned(right))),
            ),
            SqlLE::Group(expr) => self.convert_label_expression(SQLBox::into_owned(expr)),
        }
    }

    /// Convert sqlparser RepetitionQuantifier to DataFusion RepetitionQuantifier
    fn convert_quantifier(
        &self,
        q: sqlparser::ast::RepetitionQuantifier,
    ) -> RepetitionQuantifier {
        use sqlparser::ast::RepetitionQuantifier as SqlRQ;
        match q {
            SqlRQ::ZeroOrMore => RepetitionQuantifier::ZeroOrMore,
            SqlRQ::OneOrMore => RepetitionQuantifier::OneOrMore,
            SqlRQ::AtMostOne => RepetitionQuantifier::AtMostOne,
            SqlRQ::Exactly(n) => RepetitionQuantifier::Exactly(n),
            SqlRQ::AtLeast(n) => RepetitionQuantifier::AtLeast(n),
            SqlRQ::AtMost(n) => RepetitionQuantifier::AtMost(n),
            SqlRQ::Range(min, max) => RepetitionQuantifier::Range(min, max),
        }
    }

    /// Build schema from GRAPH_TABLE COLUMNS clause
    fn build_graph_table_schema(
        &self,
        columns: &[GraphColumn],
        _input_schema: &DFSchema,
    ) -> Result<DFSchema> {
        use arrow::datatypes::DataType;
        use std::collections::HashMap;

        // For now, create a schema with Utf8 columns (in practice, this would
        // need type inference from the property graph schema)
        let fields: Vec<Arc<Field>> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let name = col.alias.clone().unwrap_or_else(|| format!("col{}", idx));
                Arc::new(Field::new(name, DataType::Utf8, true))
            })
            .collect();

        DFSchema::from_unqualified_fields(fields.into(), HashMap::new())
    }
}
