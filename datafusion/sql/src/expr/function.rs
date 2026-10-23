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

use crate::definition_plan::{WithQuery, as_definition_node};
use crate::planner::{
    ContextProvider, PlannerContext, PlannerResult, RawAggregateExpr, RawCallForm,
    RawWindowExpr, RoutineCallKind, SqlToRel,
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::graph_functions::GraphFunction;
use datafusion_expr::{Distinct, DistinctOn, LogicalPlan, Projection};

use arrow::datatypes::DataType;
use datafusion_common::{
    Column, DFSchema, DataFusionError, Dependency, Diagnostic, Result, ScalarValue,
    Spans, internal_datafusion_err, internal_err, not_impl_err, plan_datafusion_err,
    plan_err, sqlstate_datafusion_err,
};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlanBuilder, Operator, SortExpr, Subquery, WindowFrame,
    WindowFunctionDefinition, expr,
    expr::{
        BinaryExpr, Case, NullTreatment, ScalarFunction, Unnest, WildcardOptions,
        WindowFunction,
    },
};
use sqlparser::ast::{
    AstBox as SQLBox, DuplicateTreatment, Expr as SQLExpr, Function as SQLFunction,
    FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArgumentList,
    FunctionArguments, ObjectName, OrderByExpr, Spanned, Value, WindowType,
};

/// Suggest a valid function based on an invalid input function name
///
/// Returns `None` if no valid matches are found. This happens when there are no
/// functions registered with the context.
pub fn suggest_valid_function(
    input_function_name: &str,
    is_window_func: bool,
    ctx: &dyn ContextProvider,
) -> Option<String> {
    let valid_funcs = if is_window_func {
        // All aggregate functions and builtin window functions
        let mut funcs = Vec::new();

        funcs.extend(ctx.udaf_names());
        funcs.extend(ctx.udwf_names());

        funcs
    } else {
        // All scalar functions and aggregate functions
        let mut funcs = Vec::new();

        funcs.extend(ctx.udf_names());
        funcs.extend(ctx.udaf_names());

        funcs
    };
    find_closest_match(valid_funcs, input_function_name)
}

/// Find the closest matching string to the target string in the candidates list, using edit distance(case insensitive)
/// Input `candidates` must not be empty otherwise an error is returned.
fn find_closest_match(candidates: Vec<String>, target: &str) -> Option<String> {
    let target = target.to_lowercase();
    candidates.into_iter().min_by_key(|candidate| {
        datafusion_common::utils::datafusion_strsim::levenshtein(
            &candidate.to_lowercase(),
            &target,
        )
    })
}

fn convert_null_treatment(value: sqlparser::ast::NullTreatment) -> NullTreatment {
    match value {
        sqlparser::ast::NullTreatment::IgnoreNulls => NullTreatment::IgnoreNulls,
        sqlparser::ast::NullTreatment::RespectNulls => NullTreatment::RespectNulls,
    }
}

/// Arguments for a function call extracted from the SQL AST
#[derive(Debug)]
struct FunctionArgs<'a> {
    /// Function name
    name: &'a ObjectName,
    /// Argument expressions
    args: &'a [FunctionArg],
    /// ORDER BY clause, if any
    order_by: &'a [OrderByExpr],
    /// OVER clause, if any
    over: Option<&'a WindowType>,
    /// FILTER clause, if any
    filter: Option<&'a SQLBox<SQLExpr>>,
    /// NULL treatment clause, if any
    null_treatment: Option<NullTreatment>,
    /// DISTINCT
    distinct: bool,
    /// WITHIN GROUP clause, if any
    within_group: &'a [OrderByExpr],
    /// Was the function called without parenthesis, i.e. could this also be a column reference?
    function_without_parentheses: bool,
}

impl<'a> FunctionArgs<'a> {
    fn try_new(function: &'a SQLFunction) -> Result<Self> {
        let name = &function.name;
        let args = &function.args;
        let over = function.over.as_ref();
        let filter = function.filter.as_ref();
        let mut null_treatment = function.null_treatment.as_ref();
        let within_group = function.within_group.as_slice();

        // Handle no argument form (aka `current_time`  as opposed to `current_time()`)
        let FunctionArguments::List(args) = args else {
            return Ok(Self {
                name,
                args: &[],
                order_by: &[],
                over,
                filter,
                null_treatment: null_treatment.cloned().map(convert_null_treatment),
                distinct: false,
                within_group,
                function_without_parentheses: matches!(args, FunctionArguments::None),
            });
        };

        let FunctionArgumentList {
            duplicate_treatment,
            args,
            clauses,
            ..
        } = args;

        let distinct = match duplicate_treatment {
            Some(DuplicateTreatment::Distinct) => true,
            Some(DuplicateTreatment::All) => false,
            None => false,
        };

        // Pull out argument handling
        let mut order_by = None;
        for clause in clauses {
            match clause {
                FunctionArgumentClause::IgnoreOrRespectNulls(nt) => {
                    if null_treatment.is_some() {
                        return not_impl_err!(
                            "Calling {name}: Duplicated null treatment clause"
                        );
                    }
                    null_treatment = Some(nt);
                }
                FunctionArgumentClause::OrderBy(oby) => {
                    if order_by.is_some() {
                        if !within_group.is_empty() {
                            return plan_err!(
                                "ORDER BY clause is only permitted in WITHIN GROUP clause when a WITHIN GROUP is used"
                            );
                        }
                        return not_impl_err!(
                            "Calling {name}: Duplicated ORDER BY clause in function arguments"
                        );
                    }
                    order_by = Some(oby.as_slice());
                }
                FunctionArgumentClause::Limit(limit) => {
                    return not_impl_err!(
                        "Calling {name}: LIMIT not supported in function arguments: {limit}"
                    );
                }
                FunctionArgumentClause::OnOverflow(overflow) => {
                    return not_impl_err!(
                        "Calling {name}: ON OVERFLOW not supported in function arguments: {overflow}"
                    );
                }
                FunctionArgumentClause::Having(having) => {
                    return not_impl_err!(
                        "Calling {name}: HAVING not supported in function arguments: {having}"
                    );
                }
                FunctionArgumentClause::Separator(sep) => {
                    return not_impl_err!(
                        "Calling {name}: SEPARATOR not supported in function arguments: {sep}"
                    );
                }
                FunctionArgumentClause::JsonNullClause(_) => {
                    // JSON NULL clause is accepted but ignored for now
                    // SQL:2016 T8xx JSON support
                }
                FunctionArgumentClause::JsonReturningClause(_) => {
                    // JSON RETURNING clause is accepted but ignored for now
                    // SQL:2016 T8xx JSON support
                }
                FunctionArgumentClause::JsonOnEmpty(_) => {
                    // JSON ON EMPTY clause is accepted but ignored for now
                    // SQL:2016 T8xx JSON support
                }
                FunctionArgumentClause::JsonOnError(_) => {
                    // JSON ON ERROR clause is accepted but ignored for now
                    // SQL:2016 T8xx JSON support
                }
                FunctionArgumentClause::JsonPassing(_) => {
                    return not_impl_err!(
                        "Calling {name}: PASSING not supported in function arguments"
                    );
                }
                FunctionArgumentClause::JsonQuotes(quotes) => {
                    return not_impl_err!(
                        "Calling {name}: QUOTES clause not supported in function arguments: {quotes}"
                    );
                }
                FunctionArgumentClause::JsonFormat(format) => {
                    return not_impl_err!(
                        "Calling {name}: FORMAT clause not supported in function arguments: {format}"
                    );
                }
                FunctionArgumentClause::JsonQueryWrapper(jw) => {
                    return not_impl_err!(
                        "Calling {name}: JSON query wrapper not supported in function arguments: {jw}"
                    );
                }
                FunctionArgumentClause::JsonUniqueKeys(uk) => {
                    return not_impl_err!(
                        "Calling {name}: JSON unique keys not supported in function arguments: {uk}"
                    );
                }
                FunctionArgumentClause::OracleJsonPassing(passing) => {
                    return not_impl_err!(
                        "Calling {name}: Oracle JSON PASSING not supported in function arguments: {passing:?}"
                    );
                }
            }
        }

        let order_by = order_by.unwrap_or_default();

        Ok(Self {
            name,
            args: args.as_slice(),
            order_by,
            over,
            filter,
            null_treatment: null_treatment.cloned().map(convert_null_treatment),
            distinct,
            within_group,
            function_without_parentheses: false,
        })
    }
}

// Helper type for extracting WITHIN GROUP ordering and prepended args
type WithinGroupExtraction = (Vec<SortExpr>, Vec<Expr>, Vec<Option<String>>);

/// Expose the query's already-bound ordering to a scalar aggregate. Generated
/// keys are evaluated once at the original Sort input and stay in scope through
/// the final projection and row limit. Neither row selection nor deduplication
/// moves across the aggregate.
fn scalar_subquery_ordering(plan: LogicalPlan) -> Result<(LogicalPlan, Vec<SortExpr>)> {
    let mut used_names = std::collections::HashSet::new();
    plan.apply(|node| {
        used_names.extend(
            node.schema()
                .fields()
                .iter()
                .map(|field| field.name().clone()),
        );
        Ok(TreeNodeRecursion::Continue)
    })?;
    expose_scalar_subquery_ordering(plan, &mut used_names)
}

fn scalar_subquery_order_key(
    used_names: &mut std::collections::HashSet<String>,
) -> String {
    let mut suffix = used_names.len();
    loop {
        let name = format!("__scalar_subquery_order_{suffix}");
        if used_names.insert(name.clone()) {
            return name;
        }
        suffix += 1;
    }
}

fn materialize_scalar_subquery_ordering(
    input: LogicalPlan,
    ordering: &[SortExpr],
    used_names: &mut std::collections::HashSet<String>,
) -> Result<(LogicalPlan, Vec<SortExpr>)> {
    let mut expressions = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let mut exposed_ordering = Vec::with_capacity(ordering.len());
    for sort in ordering {
        let name = scalar_subquery_order_key(used_names);
        expressions.push(sort.expr.clone().alias(name.clone()));
        exposed_ordering
            .push(sort.with_expr(Expr::Column(Column::new_unqualified(name))));
    }
    let projected = LogicalPlanBuilder::from(input)
        .project(expressions)?
        .build()?;
    Ok((projected, exposed_ordering))
}

fn expose_scalar_subquery_ordering(
    plan: LogicalPlan,
    used_names: &mut std::collections::HashSet<String>,
) -> Result<(LogicalPlan, Vec<SortExpr>)> {
    if let Some(with) = as_definition_node::<WithQuery>(&plan) {
        let (query, ordering) =
            expose_scalar_subquery_ordering(with.query().clone(), used_names)?;
        let items = with
            .items_with_plans()
            .map(|(item, input)| (item.clone(), input.clone()))
            .collect();
        return Ok((
            WithQuery::new(with.recursive, items, query).into_plan(),
            ordering,
        ));
    }

    match plan {
        LogicalPlan::Sort(mut sort) => {
            let (input, ordering) = materialize_scalar_subquery_ordering(
                std::sync::Arc::unwrap_or_clone(sort.input),
                &sort.expr,
                used_names,
            )?;
            sort.input = std::sync::Arc::new(input);
            sort.expr = ordering.clone();
            Ok((LogicalPlan::Sort(sort), ordering))
        }
        LogicalPlan::Projection(projection) => {
            let (input, ordering) = expose_scalar_subquery_ordering(
                std::sync::Arc::unwrap_or_clone(projection.input),
                used_names,
            )?;
            let mut expressions = projection.expr;
            expressions.extend(ordering.iter().map(|sort| sort.expr.clone()));
            // Sort's final trim projection contains column references only.
            // Keep Sort directly below LIMIT so correlated row-limit lowering
            // can use the same keys for its per-domain ranking.
            let columns_only = expressions
                .iter()
                .all(|expr| matches!(expr.clone().unalias(), Expr::Column(_)));
            let projected = match input {
                LogicalPlan::Sort(mut sort) if columns_only => {
                    sort.input = std::sync::Arc::new(LogicalPlan::Projection(
                        Projection::try_new(expressions, sort.input)?,
                    ));
                    LogicalPlan::Sort(sort)
                }
                input => LogicalPlanBuilder::from(input)
                    .project(expressions)?
                    .build()?,
            };
            Ok((projected, ordering))
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, ordering) = expose_scalar_subquery_ordering(
                std::sync::Arc::unwrap_or_clone(limit.input),
                used_names,
            )?;
            limit.input = std::sync::Arc::new(input);
            Ok((LogicalPlan::Limit(limit), ordering))
        }
        LogicalPlan::Distinct(Distinct::On(distinct)) => {
            let Some(ordering) = &distinct.sort_expr else {
                return Ok((LogicalPlan::Distinct(Distinct::On(distinct)), vec![]));
            };
            let (input, exposed_ordering) = materialize_scalar_subquery_ordering(
                distinct.input.as_ref().clone(),
                ordering,
                used_names,
            )?;
            let mut expressions = input.expressions();
            let mut on_expr = Vec::with_capacity(distinct.on_expr.len());
            for expr in &distinct.on_expr {
                if let Some(position) =
                    ordering.iter().position(|sort| sort.expr == *expr)
                {
                    on_expr.push(exposed_ordering[position].expr.clone());
                } else {
                    let name = scalar_subquery_order_key(used_names);
                    expressions.push(expr.clone().alias(name.clone()));
                    on_expr.push(Expr::Column(Column::new_unqualified(name)));
                }
            }
            let input = match input {
                LogicalPlan::Projection(projection) => LogicalPlanBuilder::from(
                    std::sync::Arc::unwrap_or_clone(projection.input),
                )
                .project(expressions)?
                .build()?,
                _ => return internal_err!("ordering materialization did not project"),
            };
            let mut select_expr = distinct.select_expr;
            select_expr.extend(exposed_ordering.iter().map(|sort| sort.expr.clone()));
            let distinct = DistinctOn::try_new(
                on_expr,
                select_expr,
                Some(exposed_ordering.clone()),
                std::sync::Arc::new(input),
            )?;
            Ok((
                LogicalPlan::Distinct(Distinct::On(distinct)),
                exposed_ordering,
            ))
        }
        // DISTINCT ALL retains its exact one-column comparison domain. Its
        // query ORDER BY is a Sort above this node; do not reach into nested
        // queries and widen the rows used for duplicate elimination.
        other => Ok((other, vec![])),
    }
}

impl SqlToRel<'_> {
    /// Catalog keys contain identifier values, not their SQL quoting syntax.
    /// Apply the same configured folding to every component of a qualified name.
    fn normalized_function_name(&self, name: &ObjectName) -> Result<String> {
        name.0
            .iter()
            .map(|part| {
                let Some(ident) = part.as_ident() else {
                    return plan_err!(
                        "Expected an identifier in function name, but found {part:?}"
                    );
                };
                Ok(self.ident_normalizer.normalize(ident.clone()))
            })
            .collect::<Result<Vec<_>>>()
            .map(|parts| parts.join("."))
    }

    pub(super) fn sql_function_to_expr(
        &self,
        function: &SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if let Some(arguments) = function
            .oracle_decode_arguments()
            .map_err(|error| plan_datafusion_err!("{error}"))?
        {
            let expression = self.sql_expr_to_logical_expr(
                arguments.expression,
                schema,
                planner_context,
            )?;
            let when_then_expr = arguments
                .pairs
                .into_iter()
                .map(|(search, result)| {
                    let search =
                        self.sql_expr_to_logical_expr(search, schema, planner_context)?;
                    let result =
                        self.sql_expr_to_logical_expr(result, schema, planner_context)?;
                    let condition = Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(expression.clone()),
                        Operator::IsNotDistinctFrom,
                        Box::new(search),
                    ));
                    Ok((Box::new(condition), Box::new(result)))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_expr = arguments
                .default
                .map(|default| {
                    self.sql_expr_to_logical_expr(default, schema, planner_context)
                        .map(Box::new)
                })
                .transpose()?;
            return Ok(Expr::Case(Case::new(None, when_then_expr, else_expr)));
        }

        // Handle ARRAY subquery constructor (SQL:2016 S095)
        // Transform ARRAY(SELECT ...) into (SELECT ARRAY_AGG(...) FROM ...)
        let name = self.normalized_function_name(&function.name)?;

        if name.eq_ignore_ascii_case("array")
            && let FunctionArguments::Subquery(query) = &function.args
        {
            return self.plan_array_subquery_constructor(
                query.as_ref(),
                schema,
                planner_context,
            );
        }

        // PostgreSQL typecast-as-function syntax: bpchar(expr) → CAST(expr AS CHARACTER)
        if name.eq_ignore_ascii_case("bpchar")
            && let FunctionArguments::List(list) = &function.args
            && list.args.len() == 1
            && let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = &list.args[0]
        {
            let inner_expr =
                self.sql_expr_to_logical_expr(inner, schema, planner_context)?;
            return Ok(Expr::Cast(datafusion_expr::Cast::new(
                Box::new(inner_expr),
                DataType::Utf8,
            )));
        }

        // The SQL/JSON forms carry their behaviour in clauses rather than in
        // ordinary arguments, so they are resolved before the generic binder
        // sees the call.
        if let Some(planned) =
            self.plan_sql_json_function(&name, function, schema, planner_context)?
        {
            return Ok(planned);
        }

        let function_args = FunctionArgs::try_new(function)?;
        let FunctionArgs {
            name: object_name,
            args,
            order_by,
            over,
            filter,
            null_treatment,
            distinct,
            within_group,
            function_without_parentheses,
        } = function_args;

        if !order_by.is_empty() && !within_group.is_empty() {
            return plan_err!(
                "ORDER BY and WITHIN GROUP clauses cannot be used together in the same aggregate function"
            );
        }

        // If function is a window function (it has an OVER clause),
        // it shouldn't have ordering requirement as function argument
        // required ordering should be defined in OVER clause.
        let is_function_window = over.is_some();
        let sql_parser_span = object_name.0[0].span();
        let name = self.normalized_function_name(object_name)?;
        let call_form = |resolved| RawCallForm {
            name: name.clone(),
            name_span: crate::utils::convert_parser_span(sql_parser_span),
            args: Vec::new(),
            arg_spans: Vec::new(),
            within_group: 0,
            star: false,
            distinct,
            order_by: !order_by.is_empty(),
            filter: filter.is_some(),
            over: over.is_some(),
            resolved,
        };

        // A call that writes WITHIN GROUP is checked against the routine its
        // arguments and sort keys select before any family plans it.
        if !within_group.is_empty() {
            self.check_call_form(
                call_form(None),
                args,
                within_group,
                schema,
                planner_context,
            )?;
            if over.is_some() {
                return plan_err!(
                    "OVER and WITHIN GROUP clause cannot be used together. \
                    OVER is for window functions, whereas WITHIN GROUP is for ordered set aggregate functions"
                );
            }
        }

        // handle make_map and map functions
        // make_map always uses plan_make_map: make_map(k1, v1, k2, v2, ...)
        // map has 2 syntaxes:
        //     1. map([keys], [values]) - two arrays that get zipped
        //     2. map(k1, v1, k2, v2, ...) - variadic pairs (uses plan_make_map)
        let use_plan_make_map = match name.as_str() {
            "make_map" => true,
            "map" => {
                // for map, check if this is the first syntax variant (two-array)
                let args = self.function_args_to_expr(args, schema, planner_context)?;

                let is_two_array_syntax = args.len() == 2
                    && args.iter().all(|arg| {
                        matches!(
                            arg.get_type(schema),
                            Ok(DataType::List(_))
                                | Ok(DataType::LargeList(_))
                                | Ok(DataType::FixedSizeList(_, _))
                        )
                    });

                // map function with variadic syntax requires non-empty list of arguments
                if !is_two_array_syntax && args.is_empty() {
                    return plan_err!(
                        "Function 'map' expected at least one argument but received 0"
                    );
                }

                !is_two_array_syntax
            }
            _ => false,
        };

        if use_plan_make_map {
            let mut fn_args =
                self.function_args_to_expr(args, schema, planner_context)?;

            for planner in self.context_provider.get_expr_planners().iter() {
                match planner.plan_make_map(fn_args)? {
                    PlannerResult::Planned(expr) => return Ok(expr),
                    PlannerResult::Original(args) => fn_args = args,
                }
            }
        }

        if planner_context.in_graph_table()
            && let Some(function) = GraphFunction::from_name(&name)
        {
            // ELEMENT_NUMBER names an element variable, not a value: its
            // argument stays the bare name the compiler resolves.
            let args = if function == GraphFunction::ElementNumber {
                args.iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(SQLExpr::Identifier(ident))) => {
                            Ok(Expr::Column(Column::from_name(
                                self.ident_normalizer.normalize(ident.clone()),
                            )))
                        }
                        other => plan_err!("ELEMENT_NUMBER takes an element variable, not {other}"),
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                self.function_args_to_expr(args, schema, planner_context)?
            };
            return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                function.udf(),
                args,
            )));
        }

        // A set-returning function multiplies the row it is written in: each
        // of its rows becomes a row of the query, so the call plans as the
        // unnest of the lists it expands to. In the SELECT list that unnest
        // happens after grouping, windows and HAVING, where the planner turns
        // `Expr::Unnest` into the plan's `Unnest`.
        if over.is_none() && self.context_provider.is_set_returning_function(&name) {
            let (srf_args, arg_names) =
                self.function_args_to_expr_with_names(args, schema, planner_context)?;
            let srf_args = self.resolve_named_arguments(&name, srf_args, arg_names)?;
            return match self
                .context_provider
                .plan_set_returning_function(&name, &srf_args, schema, None)?
            {
                Some(crate::planner::SetReturningColumns::Lists(columns)) => {
                    self.set_returning_expr(&name, columns)
                }
                Some(crate::planner::SetReturningColumns::Rows(rows)) => {
                    Ok(Expr::Unnest(Unnest::new(rows)))
                }
                None => self.set_returning_source_expr(&name, srf_args),
            };
        }

        // User-defined function (UDF) should have precedence. PostgreSQL
        // resolves routines by kind and signature together: when an
        // aggregate family shares this name and the scalar family cannot
        // accept the written argument types, the call belongs to aggregate
        // resolution below rather than failing inside the scalar family's
        // coercion.
        let scalar_udf = match self.context_provider.get_function_meta(&name) {
            Some(fm) if self.context_provider.get_aggregate_meta(&name).is_some() => {
                let (probe_args, _) =
                    self.function_args_to_expr_with_names(args, schema, planner_context)?;
                let accepts = match probe_args
                    .iter()
                    .map(|arg| arg.get_type(schema))
                    .collect::<Result<Vec<_>>>()
                {
                    Ok(types) => {
                        datafusion_expr::type_coercion::functions::data_types_with_scalar_udf(
                            &types, &fm,
                        )
                        .is_ok()
                    }
                    Err(_) => true,
                };
                accepts.then_some(fm)
            }
            other => other,
        };
        if let Some(fm) = scalar_udf {
            if !within_group.is_empty() {
                return plan_err!(
                    "WITHIN GROUP is only supported for ordered-set aggregate functions"
                );
            }
            let normal_form_args = unicode_normal_form_args(&name, args);
            let args = normal_form_args.as_deref().unwrap_or(args);
            // The clauses of an aggregate or window call, and the `*` of
            // `count(*)`, are the routine's to accept, and a scalar function
            // takes none of them.
            if over.is_some()
                || filter.is_some()
                || distinct
                || !order_by.is_empty()
                || args.iter().any(is_star_argument)
            {
                self.check_call_form(
                    call_form(Some(RoutineCallKind::Scalar)),
                    args,
                    &[],
                    schema,
                    planner_context,
                )?;
            }
            let (args, arg_names) =
                self.function_args_to_expr_with_names(args, schema, planner_context)?;

            // Special handling for JSON_OBJECT: convert named args (key: value) to positional args
            let (args, arg_names) = if fm.name().eq_ignore_ascii_case("json_object")
                && arg_names.iter().any(|name| name.is_some())
            {
                // For JSON_OBJECT, named arguments represent key-value pairs
                // Convert "key: value" to positional args [key, value]
                let mut positional_args = Vec::new();
                for (arg, name) in args.into_iter().zip(arg_names.iter()) {
                    if let Some(key_name) = name {
                        // Add the key as a string literal
                        positional_args.push(Expr::Literal(
                            ScalarValue::Utf8(Some(key_name.clone())),
                            None,
                        ));
                        // Add the value
                        positional_args.push(arg);
                    } else {
                        // If no name, just add the arg
                        positional_args.push(arg);
                    }
                }
                let len = positional_args.len();
                (positional_args, vec![None; len])
            } else {
                (args, arg_names)
            };

            let mut written_arguments = None;
            let (fm, resolved_args) = if arg_names.iter().any(|name| name.is_some()) {
                if let Some((chosen, resolved)) = self
                    .context_provider
                    .plan_named_scalar_function(&fm, &args, &arg_names, schema)?
                {
                    written_arguments =
                        chosen.signature().parameter_names.as_deref().and_then(
                            |param_names| {
                                written_argument_positions(param_names, &arg_names)
                            },
                        );
                    (chosen, resolved)
                } else if let Some(param_names) = &fm.signature().parameter_names {
                    let written = written_argument_positions(param_names, &arg_names);
                    let resolved =
                        datafusion_expr::arguments::resolve_function_arguments(
                            param_names,
                            fm.signature().parameter_defaults.as_deref(),
                            args,
                            arg_names,
                        )?;
                    written_arguments = written;
                    (fm, resolved)
                } else {
                    return plan_err!(
                        "Function '{}' does not support named arguments",
                        fm.name()
                    );
                }
            } else {
                (fm, args)
            };

            // After resolution, all arguments are positional
            let mut resolved_args = resolved_args;
            for planner in self.context_provider.get_expr_planners() {
                match planner.plan_function_arguments(&fm, resolved_args, schema)? {
                    PlannerResult::Planned(expr) => return Ok(expr),
                    PlannerResult::Original(args) => resolved_args = args,
                }
            }
            let inner = ScalarFunction::new_udf(fm, resolved_args);

            if let Some(written) = written_arguments {
                let mut call = Expr::ScalarFunction(inner);
                for planner in self.context_provider.get_expr_planners() {
                    match planner.plan_named_call(call, &written)? {
                        PlannerResult::Planned(planned) => return Ok(planned),
                        PlannerResult::Original(original) => call = original,
                    }
                }
                return Ok(call);
            }
            if name.eq_ignore_ascii_case(inner.name()) {
                return Ok(Expr::ScalarFunction(inner));
            } else {
                // If the function is called by an alias, a verbose string representation is created
                // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                // to ensure the output column name matches the user's query.
                let arg_names = inner
                    .args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let verbose_alias = format!("{name}({arg_names})");

                return Ok(Expr::ScalarFunction(inner).alias(verbose_alias));
            }
        }

        // Build Unnest expression
        if name.eq("unnest") {
            let mut exprs = self.function_args_to_expr(args, schema, planner_context)?;
            if exprs.len() != 1 {
                return plan_err!("unnest() requires exactly one argument");
            }
            let expr = exprs.swap_remove(0);
            Self::check_unnest_arg(&expr, schema)?;
            return Ok(Expr::Unnest(Unnest::new(expr)));
        }

        if !order_by.is_empty() && is_function_window {
            let message = "aggregate ORDER BY is not implemented for window functions";
            return Err(sqlstate_datafusion_err("0A000", message).with_diagnostic(
                Diagnostic::new_error(
                    message,
                    crate::utils::convert_parser_span(sql_parser_span),
                ),
            ));
        }
        // Then, window function
        if let Some(WindowType::WindowSpec(window)) = over {
            let partition_by = window
                .partition_by
                .iter()
                // Ignore window spec PARTITION BY for scalar values
                // as they do not change and thus do not generate new partitions
                .filter(|e| !matches!(e, sqlparser::ast::Expr::Value { .. },))
                .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                .collect::<Result<Vec<_>>>()?;
            let mut order_by = self.order_by_to_sort_expr(
                &window.order_by,
                schema,
                planner_context,
                // Numeric literals in window function ORDER BY are treated as constants
                false,
                None,
            )?;

            let func_deps = schema.functional_dependencies();
            // Find whether ties are possible in the given ordering
            let is_ordering_strict = order_by.iter().find_map(|orderby_expr| {
                if let Expr::Column(col) = &orderby_expr.expr {
                    let idx = schema.index_of_column(col).ok()?;
                    return if func_deps.iter().any(|dep| {
                        dep.source_indices == vec![idx] && dep.mode == Dependency::Single
                    }) {
                        Some(true)
                    } else {
                        Some(false)
                    };
                }
                Some(false)
            });

            let window_frame = window
                .window_frame
                .as_ref()
                .map(|window_frame| {
                    let window_frame = super::window_frame::convert_window_frame(
                        window_frame.clone(),
                        &mut |interval| {
                            self.frame_offset_interval(interval, planner_context)
                        },
                    )?;
                    window_frame
                        .regularize_order_bys(&mut order_by)
                        .map(|_| window_frame)
                })
                .transpose()?;

            let window_frame = if let Some(window_frame) = window_frame {
                window_frame
            } else if let Some(is_ordering_strict) = is_ordering_strict {
                WindowFrame::new(Some(is_ordering_strict))
            } else {
                WindowFrame::new((!order_by.is_empty()).then_some(false))
            };

            if let Ok(fun) = self.find_window_func(&name) {
                let (args, arg_names) =
                    self.function_args_to_expr_with_names(args, schema, planner_context)?;

                let resolved_args = if arg_names.iter().any(|name| name.is_some()) {
                    let signature = match &fun {
                        WindowFunctionDefinition::AggregateUDF(udaf) => udaf.signature(),
                        WindowFunctionDefinition::WindowUDF(udwf) => udwf.signature(),
                    };

                    if let Some(param_names) = &signature.parameter_names {
                        datafusion_expr::arguments::resolve_function_arguments(
                            param_names,
                            signature.parameter_defaults.as_deref(),
                            args,
                            arg_names,
                        )?
                    } else {
                        return plan_err!(
                            "Window function '{}' does not support named arguments",
                            name
                        );
                    }
                } else {
                    args
                };

                // Plan FILTER clause if present
                let filter = filter
                    .map(|e| {
                        self.sql_expr_to_logical_expr(e.as_ref(), schema, planner_context)
                    })
                    .transpose()?
                    .map(Box::new);

                let mut window_expr = RawWindowExpr {
                    name: name.clone(),
                    name_span: crate::utils::convert_parser_span(sql_parser_span),
                    func_def: fun,
                    args: resolved_args,
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct: function_args.distinct,
                };

                for planner in self.context_provider.get_expr_planners().iter() {
                    match planner.plan_window(window_expr, schema)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => window_expr = expr,
                    }
                }

                let RawWindowExpr {
                    name: _,
                    name_span: _,
                    func_def,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                    filter,
                    null_treatment,
                    distinct,
                } = window_expr;

                let inner = WindowFunction {
                    fun: func_def,
                    params: expr::WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        filter,
                        null_treatment,
                        distinct,
                    },
                };

                if name.eq_ignore_ascii_case(inner.fun.name()) {
                    return Ok(Expr::WindowFunction(Box::new(inner)));
                } else {
                    // If the function is called by an alias, a verbose string representation is created
                    // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                    // to ensure the output column name matches the user's query.
                    let arg_names = inner
                        .params
                        .args
                        .iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    let verbose_alias = format!("{name}({arg_names})");

                    return Ok(Expr::WindowFunction(Box::new(inner)).alias(verbose_alias));
                }
            }
        } else {
            // User defined aggregate functions (UDAF) have precedence in case it has the same name as a scalar built-in function
            if let Some(fm) = self.context_provider.get_aggregate_meta(&name) {
                if null_treatment.is_some() && !fm.supports_null_handling_clause() {
                    return plan_err!(
                        "[IGNORE | RESPECT] NULLS are not permitted for {}",
                        fm.name()
                    );
                }

                // UDAFs must opt-in via `supports_within_group_clause()` to
                // accept a WITHIN GROUP clause.
                let supports_within_group = fm.supports_within_group_clause();

                // `f(*)` passes an ordered-set aggregate no argument.
                let (mut args, mut arg_names): (Vec<Expr>, Vec<Option<String>>) = args
                    .iter()
                    .filter(|arg| !(supports_within_group && is_star_argument(arg)))
                    .map(|arg| {
                        self.sql_fn_arg_to_logical_expr_with_name(
                            arg,
                            schema,
                            planner_context,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .unzip();

                if !within_group.is_empty() && !supports_within_group {
                    return plan_err!(
                        "WITHIN GROUP is only supported for ordered-set aggregate functions"
                    );
                }

                // The written sort items, one per entry of `order_by` below.
                // ORDER BY and WITHIN GROUP are exclusive, so at most one of
                // the two lists is non-empty.
                let written_order = if supports_within_group || order_by.is_empty() {
                    within_group
                } else {
                    order_by
                };
                // If the UDAF supports WITHIN GROUP, convert the ordering into
                // sort expressions and prepend them as unnamed function args.
                let order_by = if supports_within_group {
                    let (within_group_sorts, new_args, new_arg_names) = self
                        .extract_and_prepend_within_group_args(
                            written_order,
                            args,
                            arg_names,
                            schema,
                            planner_context,
                        )?;
                    args = new_args;
                    arg_names = new_arg_names;
                    within_group_sorts
                } else {
                    // An aggregate's ORDER BY sorts its input rows by
                    // expressions; an integer there is a constant, not an
                    // output column position.
                    self.order_by_to_sort_expr(
                        written_order,
                        schema,
                        planner_context,
                        false,
                        None,
                    )?
                };

                let filter: Option<Box<Expr>> = filter
                    .map(|e| {
                        self.sql_expr_to_logical_expr(e.as_ref(), schema, planner_context)
                    })
                    .transpose()?
                    .map(Box::new);

                // Special handling for JSON_OBJECTAGG: convert named args (key: value) to positional args
                let (args, arg_names) =
                    if fm.name().eq_ignore_ascii_case("json_objectagg")
                        && arg_names.iter().any(|name| name.is_some())
                    {
                        // For JSON_OBJECTAGG, named arguments represent key-value pairs
                        // Convert "key: value" to positional args [key, value]
                        let mut positional_args = Vec::new();
                        for (arg, name) in args.into_iter().zip(arg_names.iter()) {
                            if let Some(key_name) = name {
                                // Add the key as a string literal
                                positional_args.push(Expr::Literal(
                                    ScalarValue::Utf8(Some(key_name.clone())),
                                    None,
                                ));
                                // Add the value
                                positional_args.push(arg);
                            } else {
                                // If no name, just add the arg (shouldn't happen for JSON_OBJECTAGG)
                                positional_args.push(arg);
                            }
                        }
                        let len = positional_args.len();
                        (positional_args, vec![None; len])
                    } else {
                        (args, arg_names)
                    };

                let resolved_args = if arg_names.iter().any(|name| name.is_some()) {
                    if let Some(param_names) = &fm.signature().parameter_names {
                        datafusion_expr::arguments::resolve_function_arguments(
                            param_names,
                            fm.signature().parameter_defaults.as_deref(),
                            args,
                            arg_names,
                        )?
                    } else {
                        return plan_err!(
                            "Aggregate function '{}' does not support named arguments",
                            fm.name()
                        );
                    }
                } else {
                    args
                };

                if distinct {
                    // DISTINCT reduces the input to its distinct argument
                    // values before the aggregate sees them, so a sort key
                    // outside the argument list no longer names anything.
                    if let Some(foreign) = order_by
                        .iter()
                        .position(|sort| !resolved_args.contains(&sort.expr))
                    {
                        let message = "in an aggregate with DISTINCT, ORDER BY \
                                       expressions must appear in argument list";
                        let span = crate::utils::convert_parser_span(
                            written_order[foreign].expr.span(),
                        );
                        return Err(sqlstate_datafusion_err("42P10", message)
                            .with_diagnostic(Diagnostic::new_error(message, span)));
                    }
                }

                let mut aggregate_expr = RawAggregateExpr {
                    name: name.clone(),
                    name_span: crate::utils::convert_parser_span(sql_parser_span),
                    func: fm,
                    args: resolved_args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                };
                for planner in self.context_provider.get_expr_planners().iter() {
                    match planner.plan_aggregate(aggregate_expr, schema)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => aggregate_expr = expr,
                    }
                }

                let RawAggregateExpr {
                    name: _,
                    name_span: _,
                    func,
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                } = aggregate_expr;

                let inner = expr::AggregateFunction::new_udf(
                    func,
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                );

                if name.eq_ignore_ascii_case(inner.func.name()) {
                    return Ok(Expr::AggregateFunction(inner));
                } else {
                    // If the function is called by an alias, a verbose string representation is created
                    // (e.g., "my_alias(arg1, arg2)") and the expression is wrapped in an `Alias`
                    // to ensure the output column name matches the user's query.
                    let arg_names = inner
                        .params
                        .args
                        .iter()
                        .map(|arg| arg.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    let verbose_alias = format!("{name}({arg_names})");

                    return Ok(Expr::AggregateFunction(inner).alias(verbose_alias));
                }
            }
        }

        // workaround for https://github.com/apache/datafusion-sqlparser-rs/issues/1909
        if function_without_parentheses {
            let maybe_ids = object_name
                .0
                .iter()
                .map(|part| part.as_ident().cloned().ok_or(()))
                .collect::<Result<Vec<_>, ()>>();
            if let Ok(ids) = maybe_ids {
                if ids.len() == 1 {
                    return self.sql_identifier_to_expr(&ids[0], schema, planner_context);
                } else {
                    return self.sql_compound_identifier_to_expr(
                        &ids,
                        schema,
                        planner_context,
                    );
                }
            }
        }

        // A window function called without OVER plans as nothing. A call
        // with WITHIN GROUP has had its check.
        if over.is_none()
            && within_group.is_empty()
            && self.context_provider.get_window_meta(&name).is_some()
        {
            self.check_call_form(
                call_form(Some(RoutineCallKind::Window)),
                args,
                &[],
                schema,
                planner_context,
            )?;
        }

        // All function families have declined the call. Let its owner report
        // a dialect-specific error with the original argument identities.
        let mut unresolved_args = || {
            self.function_args_to_expr_with_names(args, schema, planner_context)
                .map(|(arguments, _)| arguments)
        };
        if let Some(error) = self.context_provider.undefined_function_error(
            &name,
            &mut unresolved_args,
            schema,
        )? {
            let span = crate::utils::convert_parser_span(sql_parser_span);
            let message = error.to_string();
            return Err(error.with_diagnostic(Diagnostic::new_error(message, span)));
        }

        // Could not find the relevant function, so return an error
        if let Some(suggested_func_name) =
            suggest_valid_function(&name, is_function_window, self.context_provider)
        {
            let span = crate::utils::convert_parser_span(sql_parser_span);
            let mut diagnostic =
                Diagnostic::new_error(format!("Invalid function '{name}'"), span);
            diagnostic
                .add_note(format!("Possible function '{suggested_func_name}'"), None);
            plan_err!("Invalid function '{name}'.\nDid you mean '{suggested_func_name}'?"; diagnostic=diagnostic)
        } else {
            internal_err!("No functions registered with this context.")
        }
    }

    pub(super) fn sql_fn_name_to_expr(
        &self,
        expr: &SQLExpr,
        fn_name: &str,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let fun = self
            .context_provider
            .get_function_meta(fn_name)
            .ok_or_else(|| {
                internal_datafusion_err!("Unable to find expected '{fn_name}' function")
            })?;
        let args = vec![self.sql_expr_to_logical_expr(expr, schema, planner_context)?];
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, args)))
    }

    pub(super) fn find_window_func(
        &self,
        name: &str,
    ) -> Result<WindowFunctionDefinition> {
        let window_udf = self.context_provider.get_window_meta(name);
        // An aggregate of the same name wins over the window function, except
        // for the positional functions and for an ordered-set aggregate: a
        // hypothetical-set aggregate such as `rank` shares its name with a
        // window function, and `rank(...) OVER (...)` is the window function.
        let udaf = self
            .context_provider
            .get_aggregate_meta(name)
            .filter(|udaf| {
                udaf.name() != "first_value"
                    && udaf.name() != "last_value"
                    && udaf.name() != "nth_value"
                    && !(udaf.supports_within_group_clause() && window_udf.is_some())
            });
        if let Some(udaf) = udaf {
            Ok(WindowFunctionDefinition::AggregateUDF(udaf))
        } else {
            window_udf
                .map(WindowFunctionDefinition::WindowUDF)
                .ok_or_else(|| {
                    plan_datafusion_err!("There is no window function named {name}")
                })
        }
    }

    fn sql_fn_arg_to_logical_expr(
        &self,
        sql: &FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let (expr, _) =
            self.sql_fn_arg_to_logical_expr_with_name(sql, schema, planner_context)?;
        Ok(expr)
    }

    fn sql_fn_arg_to_logical_expr_with_name(
        &self,
        sql: &FunctionArg,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Expr, Option<String>)> {
        match sql {
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(arg),
                operator: _,
            } => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                let arg_name = self.ident_normalizer.normalize(name.clone());
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Wildcard,
                operator: _,
            } => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                let arg_name = self.ident_normalizer.normalize(name.clone());
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                Ok((expr, None))
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                Ok((expr, None))
            }
            FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(object_name)) => {
                let qualifier =
                    self.object_name_to_table_reference(object_name.clone())?;
                // Sanity check on qualifier with schema
                let qualified_indices = schema.fields_indices_with_qualified(&qualifier);
                if qualified_indices.is_empty() {
                    return plan_err!("Invalid qualifier {qualifier}");
                }

                // In a function argument PostgreSQL's `relation.*` is one
                // composite whole-row value, not a projection wildcard. This
                // is what lets calls such as `row_to_json(t.*)` preserve the
                // relation's field names. A bare `*` remains the special
                // aggregate wildcard used by count(*).
                if let Some(record) = self.try_plan_whole_row_reference(
                    &qualifier,
                    schema,
                    planner_context,
                )? {
                    return Ok((record, None));
                }

                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: qualifier.into(),
                    options: Box::new(WildcardOptions::default()),
                };
                Ok((expr, None))
            }
            // PostgreSQL dialect uses ExprNamed variant with expression for name
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(name),
                arg: FunctionArgExpr::Expr(arg),
                operator: _,
            } => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                let arg_name = self.ident_normalizer.normalize(name.clone());
                Ok((expr, Some(arg_name)))
            }
            FunctionArg::ExprNamed {
                name: SQLExpr::Identifier(name),
                arg: FunctionArgExpr::Wildcard,
                operator: _,
            } => {
                #[expect(deprecated)]
                let expr = Expr::Wildcard {
                    qualifier: None,
                    options: Box::new(WildcardOptions::default()),
                };
                let arg_name = self.ident_normalizer.normalize(name.clone());
                Ok((expr, Some(arg_name)))
            }
            // JSON_OBJECT uses string literal as key name: JSON_OBJECT('key': value)
            // Extract the key from the string literal and return it as the argument name
            FunctionArg::ExprNamed {
                name: SQLExpr::Value(sqlparser::ast::ValueWithSpan { value, .. }),
                arg: FunctionArgExpr::Expr(arg),
                operator: _,
            } => {
                let value_expr =
                    self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                // Extract the string value from the literal to use as the key name
                let key_name = match value {
                    Value::SingleQuotedString(s) => Some(s.clone()),
                    Value::DoubleQuotedString(s) => Some(s.clone()),
                    _ => None,
                };
                Ok((value_expr, key_name))
            }
            // PostgreSQL `VARIADIC expr`: the array is spread over the
            // function's variadic parameter. The planner cannot spread it
            // here (the array may be a column), so the argument is wrapped in
            // the marker function the server registers under this name and
            // resolves per callee before execution.
            FunctionArg::Variadic(FunctionArgExpr::Expr(arg)) => {
                let expr = self.sql_expr_to_logical_expr(arg, schema, planner_context)?;
                let marker = self
                    .context_provider
                    .get_function_meta(VARIADIC_MARKER_FUNCTION)
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "VARIADIC function arguments are not supported by this session"
                                .to_string(),
                        )
                    })?;
                Ok((
                    Expr::ScalarFunction(ScalarFunction::new_udf(marker, vec![expr])),
                    None,
                ))
            }
            _ => not_impl_err!("Unsupported qualified wildcard argument: {sql:?}"),
        }
    }

    /// Run [`crate::planner::ExprPlanner::check_call_form`] over `call`
    /// with the written arguments `args` and `WITHIN GROUP` sort keys
    /// `within_group` lowered against `schema`.
    fn check_call_form(
        &self,
        mut call: RawCallForm,
        args: &[FunctionArg],
        within_group: &[OrderByExpr],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<()> {
        let planners = self.context_provider.get_expr_planners();
        if planners.is_empty() {
            return Ok(());
        }
        for arg in args {
            if is_star_argument(arg) {
                call.star = true;
                continue;
            }
            let (expr, _) =
                self.sql_fn_arg_to_logical_expr_with_name(arg, schema, planner_context)?;
            call.args.push(expr);
            call.arg_spans
                .push(crate::utils::convert_parser_span(arg.span()));
        }
        for sort in within_group {
            call.args.push(self.sql_expr_to_logical_expr(
                &sort.expr,
                schema,
                planner_context,
            )?);
            call.arg_spans
                .push(crate::utils::convert_parser_span(sort.expr.span()));
        }
        call.within_group = within_group.len();
        for planner in planners {
            planner.check_call_form(&call, schema)?;
        }
        Ok(())
    }

    pub(super) fn function_args_to_expr(
        &self,
        args: &[FunctionArg],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        args.iter()
            .map(|a| self.sql_fn_arg_to_logical_expr(a, schema, planner_context))
            .collect::<Result<Vec<Expr>>>()
    }

    pub(super) fn function_args_to_expr_with_names(
        &self,
        args: &[FunctionArg],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Vec<Expr>, Vec<Option<String>>)> {
        let results: Result<Vec<(Expr, Option<String>)>> = args
            .iter()
            .map(|a| {
                self.sql_fn_arg_to_logical_expr_with_name(a, schema, planner_context)
            })
            .collect();

        let pairs = results?;
        let (exprs, names): (Vec<Expr>, Vec<Option<String>>) = pairs.into_iter().unzip();
        Ok((exprs, names))
    }

    fn extract_and_prepend_within_group_args(
        &self,
        within_group: &[OrderByExpr],
        mut args: Vec<Expr>,
        mut arg_names: Vec<Option<String>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<WithinGroupExtraction> {
        let within_group = self.order_by_to_sort_expr(
            within_group,
            schema,
            planner_context,
            false,
            None,
        )?;

        if !within_group.is_empty() {
            let within_group_count = within_group.len();
            arg_names = std::iter::repeat_n(None, within_group_count)
                .chain(arg_names)
                .collect();

            args = within_group
                .iter()
                .map(|sort| sort.expr.clone())
                .chain(args)
                .collect::<Vec<_>>();
        }

        Ok((within_group, args, arg_names))
    }

    /// Plan an ARRAY subquery constructor (SQL:2016 S095)
    ///
    /// Transforms `ARRAY(SELECT col FROM t ORDER BY x)` into:
    /// `(SELECT ARRAY_AGG(col ORDER BY x) FROM t)` as a ScalarSubquery
    pub(super) fn plan_array_subquery_constructor(
        &self,
        query: &sqlparser::ast::Query,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let array_agg_fn = self
            .context_provider
            .get_aggregate_meta("array_agg")
            .ok_or_else(|| internal_datafusion_err!("ARRAY_AGG function not found"))?;
        self.plan_scalar_subquery_aggregate(
            query,
            array_agg_fn,
            Vec::new(),
            None,
            schema,
            planner_context,
        )
    }

    /// Aggregate a one-column subquery into a single scalar value, which is
    /// how `ARRAY(SELECT ...)` and `JSON_ARRAY(SELECT ...)` are defined.
    ///
    /// `extra_args` follow the aggregated column in the aggregate's argument
    /// list, for aggregates that take options after their value.
    ///
    /// `value_wrapper` names a scalar function the projected column passes
    /// through first, which is how an aggregate whose members are
    /// type-dependent text gets its arguments rendered before they reach the
    /// accumulator.
    pub(super) fn plan_scalar_subquery_aggregate(
        &self,
        query: &sqlparser::ast::Query,
        aggregate: std::sync::Arc<datafusion_expr::AggregateUDF>,
        extra_args: Vec<Expr>,
        value_wrapper: Option<&str>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // A trivial scalar wrapper has an empty schema and must retain the
        // outer scopes it inherited. A nonempty query adds a scope rather
        // than replacing the previous one, so nested correlation still sees
        // every enclosing query. Restore the stack even when planning fails.
        let previous_scope = (!schema.fields().is_empty())
            .then(|| planner_context.push_outer_query_schema(schema.clone().into()));
        let sub_plan = self.query_to_plan_ref(query, planner_context);
        if let Some(previous_scope) = previous_scope {
            planner_context.pop_outer_query_schema(previous_scope);
        }
        let sub_plan = sub_plan?;

        if sub_plan.schema().fields().len() != 1 {
            return plan_err!(
                "aggregated subquery must return exactly one column, but got {}",
                sub_plan.schema().fields().len()
            );
        }

        // SELECT has already resolved ORDER BY against its input, output
        // aliases, and ordinals. Rebinding its SQL against the final single
        // projected field loses unselected input columns. Keep that typed
        // ordering and carry its keys through the query's final projection
        // and row limit instead. The complete query remains below the
        // aggregate, preserving DISTINCT and LIMIT/OFFSET semantics.
        let (sub_plan, order_by_sort_exprs) = if query.order_by.is_some() {
            scalar_subquery_ordering(sub_plan)?
        } else {
            (sub_plan, vec![])
        };
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        let (qualifier, field) = sub_plan.schema().qualified_field(0);
        let agg_arg = Expr::Column(Column::new(qualifier.cloned(), field.name()));

        let agg_arg = match value_wrapper {
            Some(name) => {
                let Some(func) = self.context_provider.get_function_meta(name) else {
                    return not_impl_err!("the '{name}' function is not registered");
                };
                Expr::ScalarFunction(ScalarFunction::new_udf(func, vec![agg_arg]))
            }
            None => agg_arg,
        };
        let mut aggregate_args = vec![agg_arg];
        aggregate_args.extend(extra_args);
        let aggregate_expr = expr::AggregateFunction::new_udf(
            aggregate,
            aggregate_args,
            false, // not distinct
            None,  // no filter
            order_by_sort_exprs,
            None, // no null treatment
        );

        // Build the new subquery: SELECT <agg>(col) FROM (original_query)
        let group_expr: Vec<Expr> = vec![];
        let aggregate_plan = LogicalPlanBuilder::from(sub_plan)
            .aggregate(group_expr, vec![Expr::AggregateFunction(aggregate_expr)])?
            .build()?;

        // Return as a scalar subquery
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: std::sync::Arc::new(aggregate_plan),
            outer_ref_columns,
            spans: Spans::new(),
        }))
    }

    /// Positional arguments for a call that named some of them, resolved
    /// against the function's declared parameter names.
    fn resolve_named_arguments(
        &self,
        name: &str,
        args: Vec<Expr>,
        arg_names: Vec<Option<String>>,
    ) -> Result<Vec<Expr>> {
        if !arg_names.iter().any(Option::is_some) {
            return Ok(args);
        }
        let signature = self
            .context_provider
            .get_function_meta(name)
            .map(|function| function.signature().clone());
        match signature.as_ref().and_then(|s| s.parameter_names.as_ref()) {
            Some(parameter_names) => {
                datafusion_expr::arguments::resolve_function_arguments(
                    parameter_names,
                    signature
                        .as_ref()
                        .and_then(|s| s.parameter_defaults.as_deref()),
                    args,
                    arg_names,
                )
            }
            None => plan_err!("Function '{name}' does not support named arguments"),
        }
    }

    /// The expression a set-returning call stands for in a SELECT list: the
    /// unnest of its one column, or of each column gathered into a record
    /// when it returns several. The columns unnest together, so the record's
    /// fields come from the same row of the call.
    fn set_returning_expr(
        &self,
        name: &str,
        mut columns: Vec<(String, Expr)>,
    ) -> Result<Expr> {
        if columns.is_empty() {
            return plan_err!("{name} produces no columns");
        }
        if columns.len() == 1 {
            let (_, expr) = columns.swap_remove(0);
            return Ok(Expr::Unnest(Unnest::new(expr)));
        }
        let unnested = columns
            .into_iter()
            .map(|(column, expr)| (column, Expr::Unnest(Unnest::new(expr))))
            .collect();
        self.record_of_columns(name, unnested)
    }

    /// A set-returning function the provider plans as a table source, in
    /// expression position: its rows gathered into one array per evaluation
    /// and unnested back, so it multiplies the row the way a list expansion
    /// does. A multi-column source is gathered as records.
    fn set_returning_source_expr(&self, name: &str, args: Vec<Expr>) -> Result<Expr> {
        let source = self
            .context_provider
            .get_table_function_source(name, args)?;
        let plan = match source.get_logical_plan() {
            Some(plan) => plan.into_owned(),
            None => LogicalPlanBuilder::scan(name, source, None)?.build()?,
        };
        let outer_ref_columns = plan.all_out_ref_exprs();
        let mut columns: Vec<(String, Expr)> = plan
            .schema()
            .columns()
            .into_iter()
            .map(|column| (column.name.clone(), Expr::Column(column)))
            .collect();
        let element = if columns.len() == 1 {
            columns.swap_remove(0).1
        } else {
            self.record_of_columns(name, columns)?
        };
        let Some(array_agg) = self.context_provider.get_aggregate_meta("array_agg")
        else {
            return plan_err!(
                "{name} in expression position needs the array_agg aggregate"
            );
        };
        let gathered = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            array_agg,
            vec![element],
            false,
            None,
            vec![],
            None,
        ));
        let rows = LogicalPlanBuilder::from(plan)
            .aggregate(Vec::<Expr>::new(), vec![gathered])?
            .build()?;
        Ok(Expr::Unnest(Unnest::new(Expr::ScalarSubquery(Subquery {
            subquery: std::sync::Arc::new(rows),
            outer_ref_columns,
            spans: Spans::new(),
        }))))
    }

    /// A record whose fields are the named columns, built the way the dialect
    /// builds `named_struct`.
    fn record_of_columns(
        &self,
        name: &str,
        columns: Vec<(String, Expr)>,
    ) -> Result<Expr> {
        let mut args = Vec::with_capacity(columns.len() * 2);
        for (column, expr) in columns {
            args.push(Expr::Literal(ScalarValue::Utf8(Some(column)), None));
            args.push(expr);
        }
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_struct_literal(args, true)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(original) => args = original,
            }
        }
        if let Some(named_struct) =
            self.context_provider.get_function_meta("named_struct")
        {
            return Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                named_struct,
                args,
            )));
        }
        plan_err!(
            "{name} returns several columns and no record constructor is registered"
        )
    }

    pub(crate) fn check_unnest_arg(arg: &Expr, schema: &DFSchema) -> Result<()> {
        // Check argument type, array types are supported
        match arg.get_type(schema)? {
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Null => Ok(()),
            _ => {
                plan_err!("unnest() can only be applied to array, struct and null")
            }
        }
    }
}

/// Name of the marker scalar function a `VARIADIC expr` argument is wrapped
/// in. The context provider registers it; the server rewrites each call site
/// according to the callee's variadic parameter before execution.
pub const VARIADIC_MARKER_FUNCTION: &str = "__dbl_variadic";

/// Whether `arg` is the bare `*` of `count(*)`.
fn is_star_argument(arg: &FunctionArg) -> bool {
    matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
}

/// PostgreSQL spells the Unicode normal form of `normalize(text, NFC)` as a
/// bare keyword. The parser hands it over as an identifier; lower it to the
/// string literal the function takes.
fn unicode_normal_form_args(
    name: &str,
    args: &[FunctionArg],
) -> Option<Vec<FunctionArg>> {
    if !name.eq_ignore_ascii_case("normalize") || args.len() != 2 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(SQLExpr::Identifier(ident))) =
        &args[1]
    else {
        return None;
    };
    let form = ident.value.to_ascii_uppercase();
    if ident.quote_style.is_some()
        || !matches!(form.as_str(), "NFC" | "NFD" | "NFKC" | "NFKD")
    {
        return None;
    }
    let mut lowered = args.to_vec();
    lowered[1] = FunctionArg::Unnamed(FunctionArgExpr::Expr(SQLExpr::Value(
        Value::SingleQuotedString(form).with_empty_span(),
    )));
    Some(lowered)
}

/// The arguments of a call in the order the SQL wrote them: the parameter
/// position each binds to in `param_names` and the name it was written with
/// (`None` for a positional argument). `None` when a written name names no
/// parameter.
fn written_argument_positions(
    param_names: &[String],
    arg_names: &[Option<String>],
) -> Option<Vec<(usize, Option<String>)>> {
    arg_names
        .iter()
        .enumerate()
        .map(|(position, name)| match name {
            Some(name) => param_names
                .iter()
                .position(|param| param == name)
                .map(|position| (position, Some(name.clone()))),
            None => Some((position, None)),
        })
        .collect()
}

#[cfg(test)]
mod scalar_subquery_aggregate_tests {
    use super::*;
    use crate::parser::{DFParser, Statement};
    use crate::planner::ExprPlanner;
    use arrow::datatypes::{Field, Schema};
    use datafusion_common::TableReference;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::error::{DataFusionSqlStateError, sqlstate_datafusion_err};
    use datafusion_common::metadata::FieldMetadata;
    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
    use datafusion_functions_aggregate::array_agg::array_agg_udaf;
    use sqlparser::dialect::PostgreSqlDialect;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Default)]
    struct ArrayContext {
        options: ConfigOptions,
        owns_missing_functions: bool,
        owns_national_literals: bool,
        planners: Vec<Arc<dyn ExprPlanner>>,
    }

    /// Records every call form it is asked to check, refusing each with
    /// `refusal` when one is set.
    #[derive(Debug, Default)]
    struct CallFormRecorder {
        calls: std::sync::Mutex<Vec<RawCallForm>>,
        refusal: Option<&'static str>,
    }

    impl ExprPlanner for CallFormRecorder {
        fn check_call_form(&self, call: &RawCallForm, _: &DFSchema) -> Result<()> {
            self.calls.lock().unwrap().push(call.clone());
            match self.refusal {
                Some(message) => Err(sqlstate_datafusion_err("42809", message)),
                None => Ok(()),
            }
        }
    }

    impl ContextProvider for ArrayContext {
        fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
            &self.planners
        }
        fn get_table_source(&self, _: TableReference) -> Result<Arc<dyn TableSource>> {
            Ok(Arc::new(LogicalTableSource::new(Arc::new(Schema::new(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Int32, true),
                ],
            )))))
        }
        fn plan_literal_value(&self, value: &Value) -> Result<Option<Expr>> {
            if self.owns_national_literals
                && let Value::NationalStringLiteral(value) = value
            {
                return Ok(Some(Expr::Literal(
                    ScalarValue::Binary(Some(value.as_bytes().to_vec())),
                    Some(FieldMetadata::from(HashMap::from([(
                        "owner_type".to_owned(),
                        "owned_text".to_owned(),
                    )]))),
                )));
            }
            Ok(None)
        }
        fn undefined_function_error(
            &self,
            name: &str,
            args: &mut dyn FnMut() -> Result<Vec<Expr>>,
            schema: &DFSchema,
        ) -> Result<Option<DataFusionError>> {
            if !self.owns_missing_functions {
                return Ok(None);
            }
            let types = args()?
                .iter()
                .map(|argument| {
                    let (_, field) = argument.to_field(schema)?;
                    Ok(field
                        .metadata()
                        .get("owner_type")
                        .cloned()
                        .unwrap_or_else(|| field.data_type().to_string()))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            Ok(Some(sqlstate_datafusion_err(
                "42883",
                format!("function {name}({types}) does not exist"),
            )))
        }
        fn get_function_meta(&self, _: &str) -> Option<Arc<ScalarUDF>> {
            None
        }
        fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
            (name == "array_agg").then(array_agg_udaf)
        }
        fn get_window_meta(&self, _: &str) -> Option<Arc<WindowUDF>> {
            None
        }
        fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
            None
        }
        fn options(&self) -> &ConfigOptions {
            &self.options
        }
        fn udf_names(&self) -> Vec<String> {
            vec![]
        }
        fn udaf_names(&self) -> Vec<String> {
            vec!["array_agg".into()]
        }
        fn udwf_names(&self) -> Vec<String> {
            vec![]
        }
    }

    fn plan_with_context(sql: &str, context: &ArrayContext) -> Result<LogicalPlan> {
        let statement = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})?
            .pop_front()
            .expect("one statement");
        SqlToRel::new(context).statement_to_plan(statement)
    }

    #[test]
    fn literal_owner_can_preserve_its_type_and_binary_carrier() -> Result<()> {
        let context = ArrayContext {
            owns_national_literals: true,
            ..Default::default()
        };
        let plan = plan_with_context("SELECT N'é'", &context)?;
        let LogicalPlan::Projection(projection) = plan else {
            panic!("literal query projects its result")
        };
        let expression = projection.expr[0].clone().unalias();
        assert!(
            matches!(&expression, Expr::Literal(ScalarValue::Binary(Some(value)), _) if value == &[0xc3, 0xa9])
        );
        let (_, field) = expression.to_field(projection.input.schema())?;
        assert_eq!(field.data_type(), &DataType::Binary);
        assert_eq!(
            field.metadata().get("owner_type").map(String::as_str),
            Some("owned_text")
        );
        Ok(())
    }

    #[test]
    fn declined_literal_preserves_default_types_and_unsupported_values() -> Result<()> {
        let context = ArrayContext::default();
        let plan = plan_with_context("SELECT 'ordinary', 3", &context)?;
        assert_eq!(plan.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(plan.schema().field(1).data_type(), &DataType::Int32);
        let error = plan_with_context("SELECT N'ordinary'", &context).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unsupported Value 'NationalStringLiteral")
        );
        Ok(())
    }

    #[test]
    fn missing_function_owner_gets_original_fields_and_source_location() {
        let context = ArrayContext {
            owns_missing_functions: true,
            owns_national_literals: true,
            ..Default::default()
        };
        let error =
            plan_with_context("SELECT absent(N'word', a) FROM t", &context).unwrap_err();
        let DataFusionError::External(source) = error.find_root() else {
            panic!("owner SQLSTATE must survive diagnostic attachment")
        };
        let state = source.downcast_ref::<DataFusionSqlStateError>().unwrap();
        assert_eq!(state.sqlstate, "42883");
        assert_eq!(
            state.message,
            "function absent(owned_text, Int32) does not exist"
        );
        assert_eq!(error.diagnostic().unwrap().span.unwrap().start.column, 8);
    }

    fn recording_context(refusal: Option<&'static str>) -> (ArrayContext, Arc<CallFormRecorder>) {
        let recorder = Arc::new(CallFormRecorder {
            refusal,
            ..Default::default()
        });
        let context = ArrayContext {
            planners: vec![Arc::clone(&recorder) as Arc<dyn ExprPlanner>],
            ..Default::default()
        };
        (context, recorder)
    }

    #[test]
    fn within_group_calls_are_checked_with_their_sort_keys_as_trailing_arguments() {
        let (context, recorder) = recording_context(None);
        let error = plan_with_context(
            "SELECT array_agg(a) WITHIN GROUP (ORDER BY b DESC) FROM t",
            &context,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("WITHIN GROUP is only supported for ordered-set aggregate functions"),
            "{error}"
        );
        let calls = recorder.calls.lock().unwrap();
        let [call] = calls.as_slice() else {
            panic!("one call form is checked: {calls:?}")
        };
        assert_eq!(call.name, "array_agg");
        assert_eq!(call.args.len(), 2);
        assert_eq!(call.within_group, 1);
        assert!(matches!(&call.args[1], Expr::Column(column) if column.name == "b"));
        assert_eq!(
            call.arg_spans
                .iter()
                .map(|span| span.map(|span| span.start.column))
                .collect::<Vec<_>>(),
            vec![Some(18), Some(44)]
        );
        assert_eq!(call.name_span.unwrap().start.column, 8);
        assert!(!call.over && !call.star && call.resolved.is_none());
    }

    #[test]
    fn a_star_argument_is_no_argument_of_the_checked_call() {
        let (context, recorder) = recording_context(None);
        let error = plan_with_context(
            "SELECT array_agg(*) WITHIN GROUP (ORDER BY a) OVER () FROM t",
            &context,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("OVER and WITHIN GROUP"),
            "{error}"
        );
        let calls = recorder.calls.lock().unwrap();
        let [call] = calls.as_slice() else {
            panic!("one call form is checked: {calls:?}")
        };
        assert!(call.star && call.over);
        assert_eq!((call.args.len(), call.within_group), (1, 1));
    }

    #[test]
    fn a_refused_call_form_reports_the_planner_error() {
        let (context, _) = recording_context(Some("refused"));
        let error = plan_with_context(
            "SELECT array_agg(a) WITHIN GROUP (ORDER BY b) FROM t",
            &context,
        )
        .unwrap_err();
        let DataFusionError::External(source) = error.find_root() else {
            panic!("the refusal keeps its SQLSTATE: {error}")
        };
        let state = source.downcast_ref::<DataFusionSqlStateError>().unwrap();
        assert_eq!((state.sqlstate.as_str(), state.message.as_str()), ("42809", "refused"));
    }

    #[test]
    fn declined_missing_function_does_not_plan_arguments() {
        let context = ArrayContext::default();
        let error = plan_with_context("SELECT absent(no_such_column) FROM t", &context)
            .unwrap_err();
        assert!(error.to_string().contains("Invalid function 'absent'"));
        assert!(!error.to_string().contains("No field named"));
    }

    #[test]
    fn missing_function_owner_preserves_argument_errors_and_registered_calls()
    -> Result<()> {
        let context = ArrayContext {
            owns_missing_functions: true,
            ..Default::default()
        };
        let error = plan_with_context("SELECT absent(no_such_column) FROM t", &context)
            .unwrap_err();
        assert!(error.to_string().contains("no_such_column"));
        assert!(!error.to_string().contains("function absent("));
        assert!(plan_with_context("SELECT array_agg(a) FROM t", &context).is_ok());
        Ok(())
    }

    fn planned_array(sql: &str) -> Result<datafusion_expr::Aggregate> {
        let context = ArrayContext::default();
        let statement = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})?
            .pop_front()
            .expect("one statement");
        let plan = SqlToRel::new(&context).statement_to_plan(statement)?;
        let LogicalPlan::Projection(projection) = plan else {
            panic!("ARRAY query must project its result")
        };
        let Expr::ScalarSubquery(subquery) = projection.expr[0].clone().unalias() else {
            panic!("ARRAY result must be a scalar subquery")
        };
        let LogicalPlan::Aggregate(aggregate) = subquery.subquery.as_ref() else {
            panic!("ARRAY body must aggregate the complete query")
        };
        Ok(aggregate.clone())
    }

    fn aggregate_ordering(aggregate: &datafusion_expr::Aggregate) -> &[SortExpr] {
        let Expr::AggregateFunction(function) = &aggregate.aggr_expr[0] else {
            panic!("expected ARRAY_AGG")
        };
        for argument in &function.params.args {
            for column in argument.column_refs() {
                assert!(aggregate.input.schema().has_column(column), "{column}");
            }
        }
        for sort in &function.params.order_by {
            for column in sort.expr.column_refs() {
                assert!(aggregate.input.schema().has_column(column), "{column}");
            }
        }
        &function.params.order_by
    }

    #[test]
    fn scalar_subquery_aggregate_orders_computed_projection_by_unselected_input()
    -> Result<()> {
        let aggregate = planned_array(
            "SELECT ARRAY(SELECT a + 1 FROM t ORDER BY b DESC NULLS LAST)",
        )?;
        let ordering = aggregate_ordering(&aggregate);
        assert_eq!(ordering.len(), 1);
        assert!(!ordering[0].asc);
        assert!(!ordering[0].nulls_first);
        let mut materialized = 0;
        aggregate.input.apply(|plan| {
            if let LogicalPlan::Projection(projection) = plan {
                materialized += projection.expr.iter().filter(|expr| {
                    matches!(expr, Expr::Alias(alias) if alias.name.starts_with("__scalar_subquery_order_")
                        && matches!(alias.expr.as_ref(), Expr::Column(column) if column.name == "b"))
                }).count();
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(
            materialized, 1,
            "ordering is evaluated once, before row selection"
        );
        Ok(())
    }

    #[test]
    fn scalar_subquery_aggregate_resolves_output_aliases_and_ordinals() -> Result<()> {
        for sql in [
            "SELECT ARRAY(SELECT a + 1 AS value FROM t ORDER BY value DESC)",
            "SELECT ARRAY(SELECT a + 1 FROM t ORDER BY 1 DESC)",
            "SELECT ARRAY(SELECT a + 1 AS b FROM t ORDER BY b DESC)",
        ] {
            let aggregate = planned_array(sql)?;
            let ordering = aggregate_ordering(&aggregate);
            assert_eq!(ordering.len(), 1, "{sql}");
            assert!(!ordering[0].asc, "{sql}");
        }
        Ok(())
    }

    #[test]
    fn scalar_subquery_aggregate_preserves_limit_and_distinct() -> Result<()> {
        for (sql, limits, distincts) in [
            (
                "SELECT ARRAY(SELECT a + 1 FROM t ORDER BY b LIMIT 2 OFFSET 1)",
                1,
                0,
            ),
            (
                "SELECT ARRAY(SELECT DISTINCT a FROM t ORDER BY a DESC)",
                0,
                1,
            ),
            (
                "SELECT ARRAY(SELECT DISTINCT a FROM t ORDER BY a DESC LIMIT 2)",
                1,
                1,
            ),
            (
                "SELECT ARRAY(SELECT DISTINCT ON (a) a + 1 FROM t ORDER BY a, b DESC LIMIT 2)",
                1,
                1,
            ),
        ] {
            let aggregate = planned_array(sql)?;
            assert!(!aggregate_ordering(&aggregate).is_empty(), "{sql}");
            let (mut found_limits, mut found_distincts) = (0, 0);
            aggregate.input.apply(|plan| {
                match plan {
                    LogicalPlan::Limit(_) => found_limits += 1,
                    LogicalPlan::Distinct(_) => found_distincts += 1,
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(
                (found_limits, found_distincts),
                (limits, distincts),
                "{sql}"
            );
        }
        Ok(())
    }

    fn query(sql: &str) -> sqlparser::ast::AstBox<sqlparser::ast::Query> {
        let Statement::Statement(statement) =
            DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})
                .expect("valid SQL")
                .pop_front()
                .expect("one statement")
        else {
            panic!("ordinary SQL statement")
        };
        let sqlparser::ast::Statement::Query(query) = *statement else {
            panic!("query statement")
        };
        query
    }

    fn outer_schema(qualifier: &str) -> DFSchema {
        DFSchema::try_from_qualified_schema(
            qualifier,
            &Schema::new(vec![Field::new("a", DataType::Int32, false)]),
        )
        .expect("qualified schema")
    }

    #[test]
    fn scalar_subquery_aggregate_keeps_all_outer_correlation_scopes() -> Result<()> {
        let context = ArrayContext::default();
        let planner = SqlToRel::new(&context);
        let mut scopes = PlannerContext::new();
        let outer: Arc<DFSchema> = Arc::new(outer_schema("outer_row"));
        scopes.push_outer_query_schema(Arc::clone(&outer));
        let expr = planner.plan_array_subquery_constructor(
            &query("SELECT i.a + middle_row.a FROM t i WHERE i.b = outer_row.a ORDER BY i.b LIMIT 1"),
            &outer_schema("middle_row"), &mut scopes,
        )?;
        let Expr::ScalarSubquery(subquery) = expr else {
            panic!("ARRAY subquery")
        };
        assert_eq!(subquery.outer_ref_columns.len(), 2);
        assert_eq!(scopes.outer_query_schema_stack().len(), 1);
        assert!(Arc::ptr_eq(&scopes.outer_query_schema_stack()[0], &outer));
        Ok(())
    }

    #[test]
    fn scalar_subquery_aggregate_restores_scopes_after_planning_error() {
        let context = ArrayContext::default();
        let planner = SqlToRel::new(&context);
        let mut scopes = PlannerContext::new();
        let outer: Arc<DFSchema> = Arc::new(outer_schema("outer_row"));
        scopes.push_outer_query_schema(Arc::clone(&outer));
        assert!(
            planner
                .plan_array_subquery_constructor(
                    &query("SELECT missing_column FROM t ORDER BY b"),
                    &outer_schema("middle_row"),
                    &mut scopes,
                )
                .is_err()
        );
        assert_eq!(scopes.outer_query_schema_stack().len(), 1);
        assert!(Arc::ptr_eq(&scopes.outer_query_schema_stack()[0], &outer));
    }
}
