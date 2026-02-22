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
use arrow::datatypes::DataType;
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    Array, Expr as SQLExpr, Ident, Value, ValueWithSpan, Values as SQLValues,
};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_values_to_plan(
        &self,
        values: SQLValues,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let SQLValues {
            explicit_row: _,
            rows,
            ..
        } = values;

        let empty_schema = Arc::new(DFSchema::empty());
        let defaults = planner_context.take_values_defaults();
        let table_schema = planner_context.table_schema();
        let values = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        if let (Some(defaults), SQLExpr::Identifier(ident)) =
                            (defaults.as_ref(), &v)
                        {
                            if is_default_identifier(ident) {
                                let default_expr = defaults
                                    .get(idx)
                                    .and_then(|expr| expr.clone())
                                    .unwrap_or(Expr::Literal(ScalarValue::Null, None));
                                return Ok(default_expr);
                            }
                        }
                        let target_type = table_schema
                            .as_ref()
                            .and_then(|s| s.fields().get(idx))
                            .map(|f| f.data_type());
                        let v = maybe_rewrite_pg_array_literal(v, target_type);
                        self.sql_to_expr(v, &empty_schema, planner_context)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = planner_context
            .table_schema()
            .unwrap_or(empty_schema);
        if schema.fields().is_empty() {
            LogicalPlanBuilder::values(values)?.build()
        } else {
            LogicalPlanBuilder::values_with_schema(values, &schema)?.build()
        }
    }
}

fn is_default_identifier(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("default")
}

/// Rewrite a PostgreSQL-style array literal (`'{1,2,3}'`) to `SQLExpr::Array`
/// when the target column is a List type. This allows the downstream planner
/// to handle array construction with proper type information.
pub(crate) fn maybe_rewrite_pg_array_literal(
    expr: SQLExpr,
    target_type: Option<&DataType>,
) -> SQLExpr {
    let is_list = matches!(
        target_type,
        Some(DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _))
    );
    if !is_list {
        return expr;
    }
    if let SQLExpr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(ref s),
        ..
    }) = expr
    {
        let trimmed = s.trim();
        if trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.len() >= 2 {
            if let Some(array_expr) = parse_pg_array_literal_to_sql_array(trimmed) {
                return array_expr;
            }
        }
    }
    expr
}

/// Parse a PostgreSQL array literal string like `{1,2,3}` or `{"tag1","tag2"}`
/// into an `SQLExpr::Array`. Supports nested arrays like `{{1,2},{3,4}}`.
fn parse_pg_array_literal_to_sql_array(s: &str) -> Option<SQLExpr> {
    let inner = s[1..s.len() - 1].trim();

    if inner.is_empty() {
        return Some(SQLExpr::Array(Array {
            elem: vec![],
            named: true,
        }));
    }

    let elements = split_pg_array_elements(inner);
    let mut exprs = Vec::with_capacity(elements.len());

    for elem in &elements {
        let elem = elem.trim();
        if elem.starts_with('{') && elem.ends_with('}') {
            // Nested array
            match parse_pg_array_literal_to_sql_array(elem) {
                Some(nested) => exprs.push(nested),
                None => return None,
            }
        } else if elem.eq_ignore_ascii_case("NULL") {
            exprs.push(sql_val(Value::Null));
        } else if elem.starts_with('"') && elem.ends_with('"') && elem.len() >= 2 {
            let unquoted = elem[1..elem.len() - 1]
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
            exprs.push(sql_val(Value::SingleQuotedString(unquoted)));
        } else if elem.parse::<i64>().is_ok() || elem.parse::<f64>().is_ok() {
            exprs.push(sql_val(Value::Number(elem.to_string(), false)));
        } else {
            exprs.push(sql_val(Value::SingleQuotedString(elem.to_string())));
        }
    }

    Some(SQLExpr::Array(Array {
        elem: exprs,
        named: true,
    }))
}

fn sql_val(v: Value) -> SQLExpr {
    SQLExpr::Value(v.into())
}

/// Split PG array elements by comma, respecting double-quoted strings and brace depth.
fn split_pg_array_elements(input: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut brace_depth = 0u32;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            current.push(ch);
            if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    current.push(next);
                    chars.next();
                }
            } else if ch == '"' {
                in_quotes = false;
            }
        } else {
            match ch {
                '"' => {
                    current.push(ch);
                    in_quotes = true;
                }
                '{' => {
                    current.push(ch);
                    brace_depth += 1;
                }
                '}' => {
                    current.push(ch);
                    brace_depth = brace_depth.saturating_sub(1);
                }
                ',' if brace_depth == 0 => {
                    elements.push(current.clone());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }
    }
    if !current.is_empty() {
        elements.push(current);
    }
    elements
}
