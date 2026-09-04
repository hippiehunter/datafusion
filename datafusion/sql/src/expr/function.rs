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

use crate::planner::{
    ContextProvider, PlannerContext, PlannerResult, RawAggregateExpr, RawWindowExpr,
    SqlToRel,
};

use arrow::datatypes::DataType;
use datafusion_common::{
    Column, DFSchema, DataFusionError, Dependency, Diagnostic, Result, ScalarValue,
    Spans, internal_datafusion_err, internal_err, not_impl_err, plan_datafusion_err,
    plan_err,
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

impl SqlToRel<'_> {
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
        let name = if function.name.0.len() > 1 {
            function.name.to_string()
        } else {
            match function.name.0[0].as_ident() {
                Some(ident) => self.ident_normalizer.normalize(ident.clone()),
                None => function.name.to_string(),
            }
        };

        if name.eq_ignore_ascii_case("array") {
            if let FunctionArguments::Subquery(query) = &function.args {
                return self.plan_array_subquery_constructor(
                    query.as_ref(),
                    schema,
                    planner_context,
                );
            }
        }

        // PostgreSQL typecast-as-function syntax: bpchar(expr) → CAST(expr AS CHARACTER)
        if name.eq_ignore_ascii_case("bpchar") {
            if let FunctionArguments::List(list) = &function.args {
                if list.args.len() == 1 {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) =
                        &list.args[0]
                    {
                        let inner_expr = self.sql_expr_to_logical_expr(
                            inner,
                            schema,
                            planner_context,
                        )?;
                        return Ok(Expr::Cast(datafusion_expr::Cast::new(
                            Box::new(inner_expr),
                            DataType::Utf8,
                        )));
                    }
                }
            }
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

        if over.is_some() && !within_group.is_empty() {
            return plan_err!(
                "OVER and WITHIN GROUP clause cannot be used together. \
                OVER is for window functions, whereas WITHIN GROUP is for ordered set aggregate functions"
            );
        }

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
        let name = if object_name.0.len() > 1 {
            // DF doesn't handle compound identifiers
            // (e.g. "foo.bar") for function names yet
            object_name.to_string()
        } else {
            match object_name.0[0].as_ident() {
                Some(ident) => self.ident_normalizer.normalize(ident.clone()),
                None => {
                    return plan_err!(
                        "Expected an identifier in function name, but found {:?}",
                        object_name.0[0]
                    );
                }
            }
        };

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
                Some(expansion) => self.set_returning_expr(&name, expansion.columns),
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
            let normal_form_args = unicode_normal_form_args(&name, args);
            let args = normal_form_args.as_deref().unwrap_or(args);
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
                        "Function '{}' does not support named arguments",
                        fm.name()
                    );
                }
            } else {
                args
            };

            // After resolution, all arguments are positional
            let inner = ScalarFunction::new_udf(fm, resolved_args);

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
            return plan_err!(
                "Aggregate ORDER BY is not implemented for window functions"
            );
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
                    let window_frame =
                        super::window_frame::convert_window_frame(window_frame.clone())?;
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
                    match planner.plan_window(window_expr)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => window_expr = expr,
                    }
                }

                let RawWindowExpr {
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

                let (mut args, mut arg_names) =
                    self.function_args_to_expr_with_names(args, schema, planner_context)?;

                // UDAFs must opt-in via `supports_within_group_clause()` to
                // accept a WITHIN GROUP clause.
                let supports_within_group = fm.supports_within_group_clause();

                if !within_group.is_empty() && !supports_within_group {
                    return plan_err!(
                        "WITHIN GROUP is only supported for ordered-set aggregate functions"
                    );
                }

                // If the UDAF supports WITHIN GROUP, convert the ordering into
                // sort expressions and prepend them as unnamed function args.
                let order_by = if supports_within_group {
                    let (within_group_sorts, new_args, new_arg_names) = self
                        .extract_and_prepend_within_group_args(
                            within_group,
                            args,
                            arg_names,
                            schema,
                            planner_context,
                        )?;
                    args = new_args;
                    arg_names = new_arg_names;
                    within_group_sorts
                } else {
                    let order_by = if !order_by.is_empty() {
                        order_by
                    } else {
                        within_group
                    };
                    self.order_by_to_sort_expr(
                        order_by,
                        schema,
                        planner_context,
                        true,
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
                        .find(|sort| !resolved_args.contains(&sort.expr))
                    {
                        return plan_err!(
                            "in an aggregate with DISTINCT, ORDER BY expressions must \
                             appear in argument list, but '{}' does not",
                            foreign.expr
                        );
                    }
                }

                let mut aggregate_expr = RawAggregateExpr {
                    func: fm,
                    args: resolved_args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                };
                for planner in self.context_provider.get_expr_planners().iter() {
                    match planner.plan_aggregate(aggregate_expr)? {
                        PlannerResult::Planned(expr) => return Ok(expr),
                        PlannerResult::Original(expr) => aggregate_expr = expr,
                    }
                }

                let RawAggregateExpr {
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
                let arg_name = crate::utils::normalize_ident(name.clone());
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
                let arg_name = crate::utils::normalize_ident(name.clone());
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
                if let Some(record) =
                    self.try_plan_whole_row_reference(qualifier.table(), schema)
                {
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
                let arg_name = crate::utils::normalize_ident(name.clone());
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
                let arg_name = crate::utils::normalize_ident(name.clone());
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
        use crate::query::to_order_by_exprs_with_select;

        // Extract ORDER BY from the original query if present
        let order_by_exprs =
            to_order_by_exprs_with_select(query.order_by.as_ref(), None)?;

        // Plan the subquery to get the logical plan.
        // When ARRAY(SELECT ...) appears inside a trivial scalar subquery
        // wrapper like (SELECT ARRAY(SELECT ... WHERE x = outer.col)),
        // the wrapper's schema is empty. Preserve the existing outer
        // query schema so correlation references resolve transitively.
        let override_schema = !schema.fields().is_empty();
        let old_outer_query_schema = if override_schema {
            planner_context.set_outer_query_schema(Some(schema.clone().into()))
        } else {
            None
        };
        let sub_plan = self.query_to_plan_ref(query, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        if override_schema {
            planner_context.set_outer_query_schema(old_outer_query_schema);
        }

        // Validate that the subquery returns exactly one column
        if sub_plan.schema().fields().len() != 1 {
            return plan_err!(
                "aggregated subquery must return exactly one column, but got {}",
                sub_plan.schema().fields().len()
            );
        }

        // Convert ORDER BY to sort expressions if present. ORDER BY resolves
        // against the subquery's projected output schema, so capture it before
        // stripping the projection below.
        let order_by_sort_exprs = if !order_by_exprs.is_empty() {
            self.order_by_to_sort_expr(
                order_by_exprs,
                sub_plan.schema(),
                planner_context,
                false,
                None,
            )?
        } else {
            vec![]
        };

        // Strip Sort and Projection from sub_plan so the Aggregate
        // operates on the full schema. The Projection only selects the
        // output column but hides correlation columns (like the join key)
        // that the subquery decorrelator needs. The Sort is redundant since
        // ORDER BY is embedded in the array_agg expression.
        //
        // When the projection is a single computed expression
        // (e.g. ARRAY(SELECT f(x) FROM t)), the projection's output field is
        // named after the expression ("f(x)"), but that name does not exist in
        // the stripped input's schema. Aggregating a plain column reference to
        // it would fail to resolve, so feed the projection's actual expression
        // to ARRAY_AGG instead.
        let mut sub_plan = sub_plan;
        if let datafusion_expr::LogicalPlan::Sort(sort) = sub_plan {
            sub_plan = std::sync::Arc::unwrap_or_clone(sort.input);
        }
        let agg_arg = if let datafusion_expr::LogicalPlan::Projection(proj) = &sub_plan {
            if proj.expr.len() == 1 {
                let projected = proj.expr[0].clone();
                sub_plan = std::sync::Arc::unwrap_or_clone(proj.input.clone());
                match projected {
                    Expr::Column(_) => projected,
                    Expr::Alias(alias) => *alias.expr,
                    other => other,
                }
            } else {
                let (qualifier, field) = sub_plan.schema().qualified_field(0);
                Expr::Column(Column::new(qualifier.cloned(), field.name()))
            }
        } else {
            let (qualifier, field) = sub_plan.schema().qualified_field(0);
            Expr::Column(Column::new(qualifier.cloned(), field.name()))
        };

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
