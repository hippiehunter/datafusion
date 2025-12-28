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

use arrow::datatypes::Field;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    Column, DFSchema, Diagnostic, Result, Span, Spans, TableReference, not_impl_err, plan_err,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, expr::Unnest};
use datafusion_expr::{JsonTable, JsonTableColumnDef, JsonTableErrorHandling, Subquery, SubqueryAlias};
use sqlparser::ast::{FunctionArg, FunctionArgExpr, Ident, Spanned, TableFactor};

mod join;

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
        MatchRecognizePattern::Symbol(sqlparser::ast::MatchRecognizeSymbol::Named(ident)) => {
            let name = normalizer(ident.clone());
            if !symbols.contains(&name) {
                symbols.push(name);
            }
        }
        MatchRecognizePattern::Symbol(_) => {
            // Skip Start and End anchors
        }
        MatchRecognizePattern::Exclude(sqlparser::ast::MatchRecognizeSymbol::Named(ident)) => {
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
    fn strip_pattern_var_qualifiers(&self, expr: Expr, pattern_var_names: &[String]) -> Expr {
        expr.transform(|e| {
            if let Expr::Column(col) = &e {
                if let Some(qualifier) = &col.relation {
                    // Check if this qualifier is a pattern variable
                    if pattern_var_names.iter().any(|pv| qualifier.table() == pv.as_str()) {
                        // Strip the pattern variable qualifier, making it unqualified
                        return Ok(Transformed::yes(Expr::Column(Column::new_unqualified(&col.name))));
                    }
                }
            }
            Ok(Transformed::no(e))
        }).data().expect("transform should not fail")
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
        let relation_span = relation.span();
        let (plan, alias) = match relation {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if let Some(func_args) = args {
                    let tbl_func_name =
                        name.0.first().unwrap().as_ident().unwrap().to_string();
                    let args = func_args
                        .args
                        .into_iter()
                        .flat_map(|arg| {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            {
                                self.sql_expr_to_logical_expr(
                                    expr,
                                    &DFSchema::empty(),
                                    planner_context,
                                )
                            } else {
                                plan_err!("Unsupported function argument type: {}", arg)
                            }
                        })
                        .collect::<Vec<_>>();
                    let provider = self
                        .context_provider
                        .get_table_function_source(&tbl_func_name, args)?;
                    let plan = LogicalPlanBuilder::scan(
                        TableReference::Bare {
                            table: format!("{tbl_func_name}()").into(),
                        },
                        provider,
                        None,
                    )?
                    .build()?;
                    (plan, alias)
                } else {
                    // Normalize name and alias
                    let table_ref = self.object_name_to_table_reference(name)?;
                    let table_name = table_ref.to_string();
                    let cte = planner_context.get_cte(&table_name);
                    (
                        match (
                            cte,
                            self.context_provider.get_table_source(table_ref.clone()),
                        ) {
                            (Some(cte_plan), _) => Ok(cte_plan.clone()),
                            (_, Ok(provider)) => LogicalPlanBuilder::scan(
                                table_ref.clone(),
                                provider,
                                None,
                            )?
                            .build(),
                            (None, Err(e)) => {
                                let e = e.with_diagnostic(Diagnostic::new_error(
                                    format!("table '{table_ref}' not found"),
                                    Span::try_from_sqlparser_span(relation_span),
                                ));
                                Err(e)
                            }
                        }?,
                        alias,
                    )
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self.query_to_plan(*subquery, planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(*table_with_joins, planner_context)?,
                alias,
            ),
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
            } => {
                if with_ordinality {
                    return not_impl_err!("UNNEST with ordinality is not supported yet");
                }

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
                let logical_plan = self.try_process_unnest(input, unnest_exprs)?;
                (logical_plan, alias)
            }
            TableFactor::UNNEST { .. } => {
                return not_impl_err!(
                    "UNNEST table factor with offset is not supported yet"
                );
            }
            TableFactor::Function {
                name, args, alias, ..
            } => {
                let tbl_func_ref = self.object_name_to_table_reference(name)?;
                let schema = planner_context
                    .outer_query_schema()
                    .cloned()
                    .unwrap_or_else(DFSchema::empty);
                let func_args = args
                    .into_iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => {
                            self.sql_expr_to_logical_expr(expr, &schema, planner_context)
                        }
                        _ => plan_err!("Unsupported function argument: {arg:?}"),
                    })
                    .collect::<Result<Vec<Expr>>>()?;
                let provider = self
                    .context_provider
                    .get_table_function_source(tbl_func_ref.table(), func_args)?;
                let plan =
                    LogicalPlanBuilder::scan(tbl_func_ref.table(), provider, None)?
                        .build()?;
                (plan, alias)
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
                    AfterMatchSkip, EmptyMatchesMode as SqlEmptyMatchesMode, MatchRecognizePattern,
                    MatchRecognizeSymbol, Measure, RepetitionQuantifier as SqlRepetitionQuantifier,
                    RowsPerMatch, SubsetDefinition, SymbolDefinition,
                };

                // Plan the input table
                let input_plan = self.create_relation(*table, planner_context)?;
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
                let augmented_schema = self.create_match_recognize_schema(
                    input_schema,
                    &pattern_var_names,
                )?;

                // Convert measures using the augmented schema, then strip pattern variable qualifiers
                let measure_exprs: Vec<MeasureExpr> = measures
                    .into_iter()
                    .map(|Measure { expr, alias }| {
                        let mut converted_expr = self.sql_to_expr(expr, &augmented_schema, planner_context)?;
                        // Strip pattern variable qualifiers from the expression
                        // to make it compatible with input schema validation
                        converted_expr = self.strip_pattern_var_qualifiers(converted_expr, &pattern_var_names);
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
                    AfterMatchSkip::ToFirst(ident) => {
                        AfterMatchSkipOption::ToFirst(self.ident_normalizer.normalize(ident))
                    }
                    AfterMatchSkip::ToLast(ident) => {
                        AfterMatchSkipOption::ToLast(self.ident_normalizer.normalize(ident))
                    }
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
                let convert_quantifier = |q: SqlRepetitionQuantifier| -> Result<RepetitionQuantifier> {
                    match q {
                        SqlRepetitionQuantifier::ZeroOrMore => Ok(RepetitionQuantifier::ZeroOrMore),
                        SqlRepetitionQuantifier::OneOrMore => Ok(RepetitionQuantifier::OneOrMore),
                        SqlRepetitionQuantifier::AtMostOne => Ok(RepetitionQuantifier::AtMostOne),
                        SqlRepetitionQuantifier::Exactly(n) => {
                            if n == 0 {
                                plan_err!("Invalid pattern quantifier: {{0}} can never match")
                            } else {
                                Ok(RepetitionQuantifier::Exactly(n))
                            }
                        }
                        SqlRepetitionQuantifier::AtLeast(n) => Ok(RepetitionQuantifier::AtLeast(n)),
                        SqlRepetitionQuantifier::AtMost(n) => {
                            if n == 0 {
                                plan_err!("Invalid pattern quantifier: {{,0}} can never match")
                            } else {
                                Ok(RepetitionQuantifier::AtMost(n))
                            }
                        }
                        SqlRepetitionQuantifier::Range(min, max) => {
                            if min > max {
                                plan_err!("Invalid pattern quantifier: minimum {} exceeds maximum {}", min, max)
                            } else if min == 0 && max == 0 {
                                plan_err!("Invalid pattern quantifier: {{0,0}} can never match")
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
                    convert_quantifier: &impl Fn(SqlRepetitionQuantifier) -> Result<RepetitionQuantifier>,
                ) -> Result<Pattern> {
                    match pat {
                        MatchRecognizePattern::Symbol(sym) => {
                            Ok(Pattern::Symbol(convert_symbol(sym)))
                        }
                        MatchRecognizePattern::Exclude(sym) => {
                            Ok(Pattern::Exclude(convert_symbol(sym)))
                        }
                        MatchRecognizePattern::Permute(syms) => {
                            Ok(Pattern::Permute(syms.into_iter().map(convert_symbol).collect()))
                        }
                        MatchRecognizePattern::Concat(pats) => {
                            let converted_pats = pats.into_iter()
                                .map(|p| convert_pattern(p, convert_symbol, convert_quantifier))
                                .collect::<Result<Vec<_>>>()?;
                            Ok(Pattern::Concat(converted_pats))
                        }
                        MatchRecognizePattern::Group(pat) => {
                            let converted_pat = convert_pattern(*pat, convert_symbol, convert_quantifier)?;
                            Ok(Pattern::Group(Box::new(converted_pat)))
                        }
                        MatchRecognizePattern::Alternation(pats) => {
                            let converted_pats = pats.into_iter()
                                .map(|p| convert_pattern(p, convert_symbol, convert_quantifier))
                                .collect::<Result<Vec<_>>>()?;
                            Ok(Pattern::Alternation(converted_pats))
                        }
                        MatchRecognizePattern::Repetition(pat, quant) => {
                            let converted_pat = convert_pattern(
                                *pat,
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
                        converted_expr = self.strip_pattern_var_qualifiers(converted_expr, &pattern_var_names);
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
                    json_path.to_string(),
                    columns,
                    planner_context,
                )?;
                (plan, alias)
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };
        Ok(PlannedRelation::new(plan, alias))
    }

    pub(crate) fn create_relation_subquery(
        &self,
        subquery: TableFactor,
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

        let plan = self.create_relation(subquery, planner_context)?;
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
        let df_json_expr = self.sql_expr_to_logical_expr(json_expr, &empty_schema, planner_context)?;

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
                    path,
                    exists: named.exists,
                    on_empty,
                    on_error,
                })
            }
            sqlparser::ast::JsonTableColumn::ForOrdinality(ident) => {
                Ok(JsonTableColumnDef::Ordinality {
                    name: ident.value,
                })
            }
            sqlparser::ast::JsonTableColumn::Nested(nested) => {
                // Extract nested path - it should be a string literal
                let path = match nested.path {
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s) => s,
                    _ => {
                        return plan_err!("JSON_TABLE nested path must be a string literal");
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
        use datafusion_common::ScalarValue;

        match handling {
            sqlparser::ast::JsonTableColumnErrorHandling::Null => {
                Ok(JsonTableErrorHandling::Null)
            }
            sqlparser::ast::JsonTableColumnErrorHandling::Error => {
                Ok(JsonTableErrorHandling::Error)
            }
            sqlparser::ast::JsonTableColumnErrorHandling::Default(value) => {
                // Convert the default value to a ScalarValue
                let scalar = match value {
                    sqlparser::ast::Value::Number(n, _) => {
                        // Try parsing as different numeric types
                        if let Ok(i) = n.parse::<i64>() {
                            ScalarValue::Int64(Some(i))
                        } else if let Ok(f) = n.parse::<f64>() {
                            ScalarValue::Float64(Some(f))
                        } else {
                            return plan_err!("Invalid numeric value in DEFAULT clause");
                        }
                    }
                    sqlparser::ast::Value::SingleQuotedString(s)
                    | sqlparser::ast::Value::DoubleQuotedString(s) => {
                        ScalarValue::Utf8(Some(s))
                    }
                    sqlparser::ast::Value::Boolean(b) => ScalarValue::Boolean(Some(b)),
                    sqlparser::ast::Value::Null => ScalarValue::Null,
                    _ => {
                        return plan_err!("Unsupported default value type in JSON_TABLE");
                    }
                };
                Ok(JsonTableErrorHandling::Default(scalar))
            }
        }
    }
}
