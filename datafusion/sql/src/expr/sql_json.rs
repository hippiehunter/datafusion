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

//! Planning of the SQL/JSON function forms.
//!
//! `JSON_VALUE`, `JSON_QUERY`, `JSON_EXISTS` and the SQL/JSON constructors
//! carry their behaviour in clauses (`PASSING`, `RETURNING`, the array
//! wrapper, the quotes mode, `ON EMPTY` / `ON ERROR`, `ABSENT ON NULL`,
//! `WITH UNIQUE KEYS`) rather than in ordinary arguments. Those clauses are
//! resolved here into the positional arguments of the function the context
//! provider registers for each form, and the declared `RETURNING` type becomes
//! the cast around the call — so the document is built once and typed after.

use datafusion_common::{DFSchema, Result, ScalarValue, not_impl_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, lit};
use sqlparser::ast::{
    CastKind, DataType as SQLDataType, Expr as SQLExpr, ExprWithAlias,
    Function as SQLFunction, FunctionArg, FunctionArgExpr, FunctionArgumentClause,
    FunctionArguments, JsonNullClause, JsonOnBehavior, JsonPredicateUniqueKeyConstraint,
    JsonQueryWrapper, JsonQuotesBehavior, JsonQuotesClause, OrderByExpr,
};

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

/// The clauses a SQL/JSON call carries.
#[derive(Default)]
struct SqlJsonClauses {
    returning: Option<SQLDataType>,
    returning_encoding: bool,
    wrapper: Option<JsonQueryWrapper>,
    quotes: Option<JsonQuotesClause>,
    on_empty: Option<JsonOnBehavior>,
    on_error: Option<JsonOnBehavior>,
    passing: Vec<ExprWithAlias>,
    absent_on_null: Option<bool>,
    unique_keys: bool,
    order_by: Vec<OrderByExpr>,
}

impl SqlJsonClauses {
    fn collect(clauses: &[FunctionArgumentClause]) -> Self {
        let mut collected = Self::default();
        for clause in clauses {
            match clause {
                FunctionArgumentClause::JsonReturningClause(returning) => {
                    collected.returning = Some(returning.data_type.clone());
                    collected.returning_encoding = returning.format.is_some();
                }
                FunctionArgumentClause::JsonQueryWrapper(wrapper) => {
                    collected.wrapper = Some(wrapper.clone());
                }
                FunctionArgumentClause::JsonQuotes(quotes) => {
                    collected.quotes = Some(quotes.clone());
                }
                FunctionArgumentClause::JsonOnEmpty(behavior) => {
                    collected.on_empty = Some(behavior.clone());
                }
                FunctionArgumentClause::JsonOnError(behavior) => {
                    collected.on_error = Some(behavior.clone());
                }
                FunctionArgumentClause::JsonPassing(bindings)
                | FunctionArgumentClause::OracleJsonPassing(bindings) => {
                    collected.passing = bindings.clone();
                }
                FunctionArgumentClause::JsonNullClause(JsonNullClause::AbsentOnNull) => {
                    collected.absent_on_null = Some(true);
                }
                FunctionArgumentClause::JsonNullClause(JsonNullClause::NullOnNull) => {
                    collected.absent_on_null = Some(false);
                }
                FunctionArgumentClause::JsonUniqueKeys(constraint) => {
                    collected.unique_keys = matches!(
                        constraint,
                        JsonPredicateUniqueKeyConstraint::WithUniqueKeys
                    );
                }
                FunctionArgumentClause::OrderBy(order_by) => {
                    collected.order_by = order_by.clone();
                }
                _ => {}
            }
        }
        collected
    }
}

/// The spelling each `ON EMPTY` / `ON ERROR` behaviour reaches the function as.
fn behavior_name(behavior: &JsonOnBehavior) -> &'static str {
    match behavior {
        JsonOnBehavior::Null => "null",
        JsonOnBehavior::Error => "error",
        JsonOnBehavior::Default(_) => "default",
        JsonOnBehavior::EmptyArray => "empty_array",
        JsonOnBehavior::EmptyObject => "empty_object",
        JsonOnBehavior::True => "true",
        JsonOnBehavior::False => "false",
        JsonOnBehavior::Unknown => "unknown",
    }
}

fn wrapper_name(wrapper: &JsonQueryWrapper) -> &'static str {
    match wrapper {
        JsonQueryWrapper::Without | JsonQueryWrapper::WithoutArray => "without",
        JsonQueryWrapper::WithConditional | JsonQueryWrapper::WithConditionalArray => {
            "conditional"
        }
        JsonQueryWrapper::With
        | JsonQueryWrapper::WithArray
        | JsonQueryWrapper::WithUnconditional
        | JsonQueryWrapper::WithUnconditionalArray => "unconditional",
    }
}

fn null_text() -> Expr {
    lit(ScalarValue::Utf8(None))
}

/// One SQL/JSON call's arguments, split by the shape the form takes.
struct SqlJsonArguments {
    values: Vec<SQLExpr>,
    pairs: Vec<(SQLExpr, SQLExpr)>,
    subquery: Option<SQLExpr>,
    /// Whether an argument carried `FORMAT JSON ENCODING ...`, which no
    /// constructor accepts.
    encoding: bool,
}

impl SqlJsonArguments {
    fn collect(args: &[FunctionArg]) -> Self {
        let mut collected = Self {
            values: Vec::new(),
            pairs: Vec::new(),
            subquery: None,
            encoding: false,
        };
        for arg in args {
            match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) => {
                    collected
                        .values
                        .push(strip_format(value, &mut collected.encoding));
                }
                FunctionArg::ExprNamed {
                    name,
                    arg: FunctionArgExpr::Expr(value),
                    ..
                } => collected
                    .pairs
                    .push((name.clone(), strip_format(value, &mut collected.encoding))),
                FunctionArg::Named {
                    name,
                    arg: FunctionArgExpr::Expr(value),
                    ..
                } => collected.pairs.push((
                    SQLExpr::Value(
                        sqlparser::ast::Value::SingleQuotedString(name.value.clone())
                            .into(),
                    ),
                    strip_format(value, &mut collected.encoding),
                )),
                FunctionArg::Unnamed(FunctionArgExpr::Query(query)) => {
                    collected.subquery = Some(SQLExpr::Subquery(query.clone()));
                }
                _ => {}
            }
        }
        collected
    }
}

/// `expr FORMAT JSON [ENCODING ...]` carries no value of its own; the encoding
/// is recorded so the constructor can reject it.
fn strip_format(expr: &SQLExpr, encoding: &mut bool) -> SQLExpr {
    match expr {
        SQLExpr::JsonFormatted(formatted) => {
            if formatted.format.encoding.is_some() {
                *encoding = true;
            }
            formatted.expr.as_ref().clone()
        }
        other => other.clone(),
    }
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Plan a SQL/JSON function form, or return `None` when the call is not
    /// one — including the PostgreSQL `json_object(text[])` function, which
    /// only shares the constructor's name.
    pub(super) fn plan_sql_json_function(
        &self,
        name: &str,
        function: &SQLFunction,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<Expr>> {
        let name = name.to_ascii_lowercase();
        if !matches!(
            name.as_str(),
            "json"
                | "json_scalar"
                | "json_serialize"
                | "json_object"
                | "json_array"
                | "json_arrayagg"
                | "json_objectagg"
                | "json_value"
                | "json_query"
                | "json_exists"
        ) {
            return Ok(None);
        }
        let FunctionArguments::List(list) = &function.args else {
            return Ok(None);
        };
        let clauses = SqlJsonClauses::collect(&list.clauses);
        // `json_object(text[])` is a distinct PostgreSQL function sharing the
        // constructor's name; only the SQL/JSON spelling is planned here.
        if name == "json_object"
            && !list.args.is_empty()
            && !list.args.iter().any(|arg| {
                matches!(
                    arg,
                    FunctionArg::Named { .. } | FunctionArg::ExprNamed { .. }
                )
            })
            && clauses.returning.is_none()
            && clauses.absent_on_null.is_none()
            && !clauses.unique_keys
        {
            return Ok(None);
        }
        let arguments = SqlJsonArguments::collect(&list.args);

        let planned = match name.as_str() {
            "json_value" | "json_query" | "json_exists" => self
                .plan_sql_json_query_function(
                    &name,
                    &arguments,
                    &clauses,
                    schema,
                    planner_context,
                )?,
            "json_arrayagg" | "json_objectagg" => self.plan_sql_json_aggregate(
                &name,
                &arguments,
                &clauses,
                schema,
                planner_context,
            )?,
            _ => self.plan_sql_json_constructor(
                &name,
                &arguments,
                &clauses,
                schema,
                planner_context,
            )?,
        };
        Ok(Some(
            self.cast_sql_json_result(planned, &name, &clauses, schema)?,
        ))
    }

    /// The declared result type is applied outside the call: the function
    /// renders the document as text and the cast gives it the requested type.
    /// A coercion failure the `ON ERROR` clause must catch is decided inside
    /// the function, which is why it also receives the type name.
    fn cast_sql_json_result(
        &self,
        expr: Expr,
        name: &str,
        clauses: &SqlJsonClauses,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let target = match (&clauses.returning, name) {
            (Some(data_type), _) => data_type.clone(),
            (None, "json_value" | "json_exists" | "json_serialize") => return Ok(expr),
            (None, "json_query") => SQLDataType::JSONB,
            (None, _) => SQLDataType::JSON,
        };
        self.finish_cast_expr(expr, &target, CastKind::Cast, None, schema)
    }

    /// `PASSING v AS n, ...` becomes the `vars` object the jsonpath evaluator
    /// takes, built so each binding keeps the JSON type its SQL type maps to —
    /// a text binding is a JSON string, never a number.
    fn plan_passing_variables(
        &self,
        bindings: &[ExprWithAlias],
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if bindings.is_empty() {
            return Ok(null_text());
        }
        let mut args = Vec::with_capacity(bindings.len() * 2);
        for binding in bindings {
            let name = match &binding.alias {
                Some(alias) => alias.value.clone(),
                None => binding.expr.to_string(),
            };
            args.push(lit(name));
            args.push(self.sql_expr_to_logical_expr(
                &binding.expr,
                schema,
                planner_context,
            )?);
        }
        self.sql_json_function_call("jsonb_build_object", args)
    }

    fn sql_json_function_call(&self, name: &str, args: Vec<Expr>) -> Result<Expr> {
        let Some(func) = self.context_provider.get_function_meta(name) else {
            return not_impl_err!(
                "SQL/JSON requires the '{name}' function to be registered"
            );
        };
        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(func, args)))
    }

    /// The `ON EMPTY` / `ON ERROR` pair each query function receives: the
    /// behaviour's name, then the value a `DEFAULT` behaviour substitutes.
    fn plan_behavior(
        &self,
        behavior: &Option<JsonOnBehavior>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<(Expr, Expr)> {
        let name = match behavior {
            Some(behavior) => lit(behavior_name(behavior)),
            None => null_text(),
        };
        let default = match behavior {
            Some(JsonOnBehavior::Default(expr)) => {
                self.sql_expr_to_logical_expr(expr.as_ref(), schema, planner_context)?
            }
            _ => null_text(),
        };
        Ok((name, default))
    }

    fn plan_sql_json_query_function(
        &self,
        name: &str,
        arguments: &SqlJsonArguments,
        clauses: &SqlJsonClauses,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let [context_item, path] = arguments.values.as_slice() else {
            return plan_err!("{name} takes a context item and a path expression");
        };
        let mut args = vec![
            self.sql_expr_to_logical_expr(context_item, schema, planner_context)?,
            self.sql_expr_to_logical_expr(path, schema, planner_context)?,
            self.plan_passing_variables(&clauses.passing, schema, planner_context)?,
        ];
        let (on_error, on_error_default) =
            self.plan_behavior(&clauses.on_error, schema, planner_context)?;
        if name == "json_exists" {
            args.push(on_error);
            return self.sql_json_function_call(name, args);
        }
        let returning = clauses
            .returning
            .as_ref()
            .map_or_else(null_text, |data_type| lit(data_type.to_string()));
        args.push(returning);
        if name == "json_query" {
            args.push(
                clauses
                    .wrapper
                    .as_ref()
                    .map_or_else(null_text, |wrapper| lit(wrapper_name(wrapper))),
            );
            args.push(clauses.quotes.as_ref().map_or_else(null_text, |quotes| {
                lit(match quotes.behavior {
                    JsonQuotesBehavior::Keep => "keep",
                    JsonQuotesBehavior::Omit => "omit",
                })
            }));
        }
        let (on_empty, on_empty_default) =
            self.plan_behavior(&clauses.on_empty, schema, planner_context)?;
        args.extend([on_empty, on_empty_default, on_error, on_error_default]);
        self.sql_json_function_call(name, args)
    }

    fn plan_sql_json_constructor(
        &self,
        name: &str,
        arguments: &SqlJsonArguments,
        clauses: &SqlJsonClauses,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let first_value =
            |this: &Self, planner_context: &mut PlannerContext| match arguments
                .values
                .first()
            {
                Some(value) => {
                    this.sql_expr_to_logical_expr(value, schema, planner_context)
                }
                None => Ok(null_text()),
            };
        // `JSON_ARRAY` omits nulls unless told otherwise; `JSON_OBJECT` keeps
        // them unless told otherwise.
        let absent_on_null = clauses.absent_on_null.unwrap_or(name == "json_array");
        match name {
            "json" => {
                let value = first_value(self, planner_context)?;
                self.sql_json_function_call(
                    "json_constructor",
                    vec![value, lit(clauses.unique_keys), lit(arguments.encoding)],
                )
            }
            "json_scalar" => {
                let value = first_value(self, planner_context)?;
                self.sql_json_function_call("json_scalar_constructor", vec![value])
            }
            "json_serialize" => {
                let value = first_value(self, planner_context)?;
                let returning = clauses
                    .returning
                    .as_ref()
                    .map_or_else(null_text, |data_type| lit(data_type.to_string()));
                self.sql_json_function_call(
                    "json_serialize_constructor",
                    vec![value, returning],
                )
            }
            "json_object" => {
                let mut args = vec![
                    lit(absent_on_null),
                    lit(clauses.unique_keys),
                    lit(clauses.returning_encoding),
                ];
                for (key, value) in &arguments.pairs {
                    args.push(self.sql_expr_to_logical_expr(
                        key,
                        schema,
                        planner_context,
                    )?);
                    args.push(self.sql_expr_to_logical_expr(
                        value,
                        schema,
                        planner_context,
                    )?);
                }
                self.sql_json_function_call("json_object_constructor", args)
            }
            _ => {
                // `JSON_ARRAY(<query>)` aggregates the query's rows.
                if let Some(subquery) = &arguments.subquery {
                    return self.plan_sql_json_array_subquery(
                        subquery,
                        absent_on_null,
                        schema,
                        planner_context,
                    );
                }
                let mut args = vec![lit(absent_on_null)];
                for value in &arguments.values {
                    args.push(self.sql_expr_to_logical_expr(
                        value,
                        schema,
                        planner_context,
                    )?);
                }
                self.sql_json_function_call("json_array_constructor", args)
            }
        }
    }

    /// `JSON_ARRAY(<query>)`: the query's single output column feeds the array
    /// aggregate, so the whole result set builds one document.
    fn plan_sql_json_array_subquery(
        &self,
        subquery: &SQLExpr,
        absent_on_null: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let SQLExpr::Subquery(query) = subquery else {
            return plan_err!("JSON_ARRAY expects a query or a value list");
        };
        let Some(aggregate) = self.context_provider.get_aggregate_meta("json_arrayagg")
        else {
            return not_impl_err!(
                "SQL/JSON requires the 'json_arrayagg' aggregate to be registered"
            );
        };
        self.plan_scalar_subquery_aggregate(
            query.as_ref(),
            aggregate,
            vec![lit(absent_on_null)],
            schema,
            planner_context,
        )
    }

    fn plan_sql_json_aggregate(
        &self,
        name: &str,
        arguments: &SqlJsonArguments,
        clauses: &SqlJsonClauses,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let Some(aggregate) = self.context_provider.get_aggregate_meta(name) else {
            return not_impl_err!(
                "SQL/JSON requires the '{name}' aggregate to be registered"
            );
        };
        // `JSON_ARRAYAGG` and `JSON_OBJECTAGG` both omit nulls unless told
        // otherwise, which is the opposite of their `json_agg` counterparts.
        let absent_on_null = clauses.absent_on_null.unwrap_or(true);
        // The member text an argument contributes depends on its declared
        // type, which only a scalar function sees. Rendering each argument
        // before it reaches the aggregate keeps the accumulator a plain
        // concatenation of `json` fragments.
        let args = if name == "json_arrayagg" {
            let Some(value) = arguments.values.first() else {
                return plan_err!("JSON_ARRAYAGG takes one value expression");
            };
            let value = self.sql_expr_to_logical_expr(value, schema, planner_context)?;
            vec![
                self.sql_json_function_call("json_member_value", vec![value])?,
                lit(absent_on_null),
            ]
        } else {
            let Some((key, value)) = arguments.pairs.first() else {
                return plan_err!("JSON_OBJECTAGG takes a key and a value expression");
            };
            let key = self.sql_expr_to_logical_expr(key, schema, planner_context)?;
            let value = self.sql_expr_to_logical_expr(value, schema, planner_context)?;
            vec![
                self.sql_json_function_call("json_member_key", vec![key])?,
                self.sql_json_function_call("json_member_value", vec![value])?,
                lit(clauses.unique_keys),
            ]
        };
        let order_by = if clauses.order_by.is_empty() {
            Vec::new()
        } else {
            self.order_by_to_sort_expr(
                clauses.order_by.clone(),
                schema,
                planner_context,
                true,
                None,
            )?
        };
        Ok(Expr::AggregateFunction(
            datafusion_expr::expr::AggregateFunction::new_udf(
                aggregate, args, false, None, order_by, None,
            ),
        ))
    }
}
