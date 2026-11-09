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

use crate::planner::{PlannerContext, SqlToRel};
use datafusion_common::error::sqlstate_datafusion_err_with_hint;
use datafusion_common::{
    Column, DFSchema, Result, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, SortExpr};
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, OrderByExpr, OrderByOptions, Value, ValueWithSpan,
};

impl SqlToRel<'_> {
    /// Convert sql [OrderByExpr] to `Vec<Expr>`.
    ///
    /// `input_schema` is the schema of the input logical plan, typically derived from the SELECT list.
    /// A number with `literal_to_column` set is a position in it.
    ///
    /// `from_schema` is the schema of a SELECT's `FROM` clause. When it is given, an item that is
    /// not a position is an expression over the `FROM` clause's columns, as PostgreSQL reads it:
    /// an output column is reached only by its position or by a bare output name, which the caller
    /// rewrites to that position. `x + 0` over `SELECT x::text AS x` orders by the input `x`.
    ///
    /// If `literal_to_column` is true, treat any numeric literals (e.g. `2`) as a 1 based index into the
    /// SELECT list (e.g. `SELECT a, b FROM table ORDER BY 2`). Literals only reference the `input_schema`.
    ///
    /// If false, interpret numeric literals as constant values.
    pub(crate) fn order_by_to_sort_expr(
        &self,
        order_by_exprs: impl AsRef<[OrderByExpr]>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
        literal_to_column: bool,
        from_schema: Option<&DFSchema>,
    ) -> Result<Vec<SortExpr>> {
        let order_by_exprs = order_by_exprs.as_ref();
        if order_by_exprs.is_empty() {
            return Ok(vec![]);
        }

        let order_by_schema = from_schema.unwrap_or(input_schema);

        let mut sort_expr_vec = Vec::with_capacity(order_by_exprs.len());

        for order_by_expr in order_by_exprs {
            let OrderByExpr {
                expr,
                options: OrderByOptions { asc, nulls_first },
                with_fill,
                using,
            } = order_by_expr;

            if let Some(with_fill) = with_fill {
                return not_impl_err!("ORDER BY WITH FILL is not supported: {with_fill}");
            }
            if using.is_some() && asc.is_some() {
                return plan_err!(
                    "ORDER BY ... USING may not be combined with ASC or DESC"
                );
            }

            let key_schema = match expr {
                SQLExpr::Value(ValueWithSpan {
                    value: Value::Number(_, _),
                    ..
                }) if literal_to_column => input_schema,
                _ => order_by_schema,
            };
            let expr = match expr {
                SQLExpr::Value(ValueWithSpan {
                    value: Value::Number(v, _),
                    span: _,
                }) if literal_to_column => {
                    let field_index = v
                        .parse::<usize>()
                        .map_err(|err| plan_datafusion_err!("{}", err))?;

                    if field_index == 0 {
                        return plan_err!(
                            "Order by index starts at 1 for column indexes"
                        );
                    } else if input_schema.fields().len() < field_index {
                        return plan_err!(
                            "Order by column out of bounds, specified: {}, max: {}",
                            field_index,
                            input_schema.fields().len()
                        );
                    }

                    Expr::Column(Column::from(
                        input_schema.qualified_field(field_index - 1),
                    ))
                }
                e => {
                    self.sql_expr_to_logical_expr(e, order_by_schema, planner_context)?
                }
            };
            // `USING <operator>` names the ordering operator instead of
            // spelling ASC/DESC: its btree strategy in a family over the
            // key's type gives the direction, and with it the NULLS default.
            let (asc, using) = match using {
                None => (asc.unwrap_or(true), None),
                Some(operator) => {
                    let name = ordering_operator_name(operator);
                    let Some(ordering) = self
                        .context_provider
                        .ordering_operator(&name, &expr, key_schema)?
                    else {
                        return Err(sqlstate_datafusion_err_with_hint(
                            "42809",
                            format!("operator {name} is not a valid ordering operator"),
                            "Ordering operators must be \"<\" or \">\" members of btree operator families.",
                        ));
                    };
                    (
                        ordering.ascending,
                        (!ordering.default_for_type).then_some(name),
                    )
                }
            };
            let nulls_first = nulls_first
                .unwrap_or_else(|| self.options.default_null_ordering.nulls_first(asc));
            sort_expr_vec.push(Sort {
                expr,
                asc,
                nulls_first,
                using,
            });
        }

        Ok(sort_expr_vec)
    }
}

/// The name of the operator a `USING` clause wrote: `OPERATOR(schema.op)`
/// names it qualified, every other spelling is the operator itself.
fn ordering_operator_name(operator: &BinaryOperator) -> String {
    match operator {
        BinaryOperator::PGCustomBinaryOperator(parts) => parts.join("."),
        BinaryOperator::Custom(name) => name.clone(),
        other => other.to_string(),
    }
}
