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

//! SQL window-frame syntax lowering.

use arrow::datatypes::DataType;
use datafusion_common::error::sqlstate_datafusion_err;
use datafusion_common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    Expr, WindowFrame, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits,
};
use sqlparser::ast::{self, ValueWithSpan};

use super::value::sql_number_literal;
use crate::planner::{PlannerContext, SqlToRel};

impl SqlToRel<'_> {
    /// A RANGE offset written as an interval, as the interval the planner's
    /// interval rules read: the registered expression planners first, then
    /// the built-in grammar.
    pub(super) fn frame_offset_interval(
        &self,
        interval: &ast::Interval,
        planner_context: &mut PlannerContext,
    ) -> Result<ScalarValue> {
        match self.sql_interval_to_expr(
            false,
            interval,
            &DFSchema::empty(),
            planner_context,
        )? {
            Expr::Literal(value, _) => Ok(value),
            other => plan_err!(
                "Invalid window frame: a RANGE frame offset must be a constant, not {other}"
            ),
        }
    }
}

/// Lower a window frame clause. `plan_interval` reads a RANGE offset written
/// as an interval.
pub(super) fn convert_window_frame(
    value: ast::WindowFrame,
    plan_interval: &mut dyn FnMut(&ast::Interval) -> Result<ScalarValue>,
) -> Result<WindowFrame> {
    let start_bound = convert_window_frame_bound(
        value.start_bound,
        &value.units,
        "starting",
        plan_interval,
    )?;
    let end_bound = match value.end_bound {
        Some(bound) => {
            convert_window_frame_bound(bound, &value.units, "ending", plan_interval)?
        }
        None => WindowFrameBound::CurrentRow,
    };
    let exclude = value
        .exclude
        .map(convert_window_frame_exclusion)
        .unwrap_or(WindowFrameExclusion::NoOthers);

    if let WindowFrameBound::Following(val) = &start_bound {
        if val.is_null() {
            plan_err!("Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING")?
        }
    } else if let WindowFrameBound::Preceding(val) = &end_bound
        && val.is_null()
    {
        plan_err!("Invalid window frame: end bound cannot be UNBOUNDED PRECEDING")?
    }

    Ok(WindowFrame::new_bounds_with_exclusion(
        convert_window_frame_units(value.units),
        start_bound,
        end_bound,
        exclude,
    ))
}

/// One bound of a frame. `end` names the bound, `starting` or `ending`, for
/// the error a null offset raises: a frame bound of no value would read as
/// unbounded, so a null offset is rejected rather than lowered.
fn convert_window_frame_bound(
    value: ast::WindowFrameBound,
    units: &ast::WindowFrameUnits,
    end: &str,
    plan_interval: &mut dyn FnMut(&ast::Interval) -> Result<ScalarValue>,
) -> Result<WindowFrameBound> {
    Ok(match value {
        ast::WindowFrameBound::Preceding(Some(value)) => {
            WindowFrameBound::Preceding(frame_bound_offset(
                sqlparser::arena::AstBox::into_owned(value),
                units,
                end,
                plan_interval,
            )?)
        }
        ast::WindowFrameBound::Preceding(None) => {
            WindowFrameBound::Preceding(ScalarValue::UInt64(None))
        }
        ast::WindowFrameBound::Following(Some(value)) => {
            WindowFrameBound::Following(frame_bound_offset(
                sqlparser::arena::AstBox::into_owned(value),
                units,
                end,
                plan_interval,
            )?)
        }
        ast::WindowFrameBound::Following(None) => {
            WindowFrameBound::Following(ScalarValue::UInt64(None))
        }
        ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
    })
}

fn frame_bound_offset(
    value: ast::Expr,
    units: &ast::WindowFrameUnits,
    end: &str,
    plan_interval: &mut dyn FnMut(&ast::Interval) -> Result<ScalarValue>,
) -> Result<ScalarValue> {
    if is_null_offset(&value) {
        return Err(sqlstate_datafusion_err(
            "22004",
            format!("frame {end} offset must not be null"),
        ));
    }
    convert_frame_bound_to_scalar_value(value, units, plan_interval)
}

fn is_null_offset(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(ValueWithSpan {
            value: ast::Value::Null,
            span: _,
        }) => true,
        ast::Expr::Nested(inner) | ast::Expr::Cast { expr: inner, .. } => {
            is_null_offset(inner)
        }
        _ => false,
    }
}

fn fold_integer_frame_offset(expr: &ast::Expr) -> Option<i128> {
    match expr {
        ast::Expr::Nested(inner) => fold_integer_frame_offset(inner),
        ast::Expr::Value(ValueWithSpan {
            value: ast::Value::Number(text, false),
            span: _,
        }) => text.parse::<i128>().ok(),
        ast::Expr::UnaryOp { op, expr } => {
            let value = fold_integer_frame_offset(expr)?;
            match op {
                ast::UnaryOperator::Plus => Some(value),
                ast::UnaryOperator::Minus => value.checked_neg(),
                _ => None,
            }
        }
        ast::Expr::BinaryOp { left, op, right } => {
            let left = fold_integer_frame_offset(left)?;
            let right = fold_integer_frame_offset(right)?;
            match op {
                ast::BinaryOperator::Plus => left.checked_add(right),
                ast::BinaryOperator::Minus => left.checked_sub(right),
                ast::BinaryOperator::Multiply => left.checked_mul(right),
                ast::BinaryOperator::Divide => left.checked_div(right),
                ast::BinaryOperator::Modulo => left.checked_rem(right),
                _ => None,
            }
        }
        _ => None,
    }
}

fn convert_frame_bound_to_scalar_value(
    value: ast::Expr,
    units: &ast::WindowFrameUnits,
    plan_interval: &mut dyn FnMut(&ast::Interval) -> Result<ScalarValue>,
) -> Result<ScalarValue> {
    match units {
        ast::WindowFrameUnits::Rows | ast::WindowFrameUnits::Groups => row_offset(value),
        ast::WindowFrameUnits::Range => range_offset(value, plan_interval),
    }
}

fn row_offset(value: ast::Expr) -> Result<ScalarValue> {
    if let Some(offset) = fold_integer_frame_offset(&value) {
        if offset < 0 {
            return plan_err!(
                "Invalid window frame: frame offsets for ROWS / GROUPS must be non negative integers"
            );
        }
        return ScalarValue::try_from_string(offset.to_string(), &DataType::UInt64);
    }
    match value {
        ast::Expr::Value(ValueWithSpan {
            value: ast::Value::Number(value, false),
            span: _,
        }) => ScalarValue::try_from_string(value, &DataType::UInt64),
        ast::Expr::Interval(ast::Interval {
            value,
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }) => {
            let value = match sqlparser::arena::AstBox::into_owned(value) {
                ast::Expr::Value(ValueWithSpan {
                    value: ast::Value::SingleQuotedString(item),
                    span: _,
                }) => item,
                expr => return exec_err!("INTERVAL expression cannot be {expr:?}"),
            };
            ScalarValue::try_from_string(value, &DataType::UInt64)
        }
        _ => plan_err!(
            "Invalid window frame: frame offsets for ROWS / GROUPS must be non negative integers"
        ),
    }
}

/// A RANGE offset as a value of its literal's own type, which decides the
/// ORDER BY keys it can measure: a number as the exact numeric literal the
/// expression planner makes of it, a quoted string as unknown-typed text
/// the key's offset type reads, and an interval as `plan_interval` reads
/// it. The parser reads a quoted frame offset as an unqualified interval, so
/// an unqualified `INTERVAL '...'` is that quoted text; a qualified interval,
/// or a cast to `interval`, is an interval. A cast of a number or a string
/// otherwise keeps its operand's type.
fn range_offset(
    value: ast::Expr,
    plan_interval: &mut dyn FnMut(&ast::Interval) -> Result<ScalarValue>,
) -> Result<ScalarValue> {
    if let Some(offset) = fold_integer_frame_offset(&value) {
        return sql_number_literal(&offset.unsigned_abs().to_string(), offset < 0, true);
    }
    let invalid = || {
        plan_err!(
            "Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"
        )
    };
    match value {
        ast::Expr::Nested(inner) => {
            range_offset(sqlparser::arena::AstBox::into_owned(inner), plan_interval)
        }
        ast::Expr::Value(ValueWithSpan {
            value: ast::Value::Number(number, _),
            span: _,
        }) => sql_number_literal(&number, false, true),
        ast::Expr::UnaryOp {
            op: op @ (ast::UnaryOperator::Minus | ast::UnaryOperator::Plus),
            expr,
        } => match sqlparser::arena::AstBox::into_owned(expr) {
            ast::Expr::Value(ValueWithSpan {
                value: ast::Value::Number(number, _),
                span: _,
            }) => sql_number_literal(&number, op == ast::UnaryOperator::Minus, true),
            _ => invalid(),
        },
        ast::Expr::Value(ValueWithSpan {
            value: ast::Value::SingleQuotedString(text),
            span: _,
        }) => Ok(ScalarValue::Utf8(Some(text))),
        ast::Expr::Interval(ast::Interval {
            value,
            leading_field: None,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
        }) if matches!(
            value.as_ref(),
            ast::Expr::Value(ValueWithSpan {
                value: ast::Value::SingleQuotedString(_),
                span: _,
            })
        ) =>
        {
            match sqlparser::arena::AstBox::into_owned(value) {
                ast::Expr::Value(ValueWithSpan {
                    value: ast::Value::SingleQuotedString(text),
                    span: _,
                }) => Ok(ScalarValue::Utf8(Some(text))),
                _ => invalid(),
            }
        }
        ast::Expr::Interval(interval) => plan_interval(&interval),
        ast::Expr::Cast {
            expr, data_type, ..
        } => match (sqlparser::arena::AstBox::into_owned(expr), data_type) {
            (
                ast::Expr::Value(ValueWithSpan {
                    value: ast::Value::SingleQuotedString(text),
                    span,
                }),
                ast::DataType::Interval { .. },
            ) => plan_interval(&ast::Interval {
                value: sqlparser::arena::AstBox::new(ast::Expr::Value(ValueWithSpan {
                    value: ast::Value::SingleQuotedString(text),
                    span,
                })),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            }),
            (
                ast::Expr::Value(ValueWithSpan {
                    value: ast::Value::SingleQuotedString(text),
                    span: _,
                }),
                _,
            ) => Ok(ScalarValue::Utf8(Some(text))),
            (operand, _) => range_offset(operand, plan_interval),
        },
        _ => invalid(),
    }
}

fn convert_window_frame_exclusion(
    value: ast::WindowFrameExclude,
) -> WindowFrameExclusion {
    match value {
        ast::WindowFrameExclude::CurrentRow => WindowFrameExclusion::CurrentRow,
        ast::WindowFrameExclude::Group => WindowFrameExclusion::Group,
        ast::WindowFrameExclude::Ties => WindowFrameExclusion::Ties,
        ast::WindowFrameExclude::NoOthers => WindowFrameExclusion::NoOthers,
    }
}

fn convert_window_frame_units(value: ast::WindowFrameUnits) -> WindowFrameUnits {
    match value {
        ast::WindowFrameUnits::Range => WindowFrameUnits::Range,
        ast::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
        ast::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{DataFusionError, DataFusionSqlStateError};

    fn no_intervals(interval: &ast::Interval) -> Result<ScalarValue> {
        plan_err!("unexpected interval {interval}")
    }

    fn range_frame(start: ast::Expr) -> ast::WindowFrame {
        ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(Some(ast::AstBox::new(start))),
            end_bound: None,
            exclude: None,
        }
    }

    #[test]
    fn rejects_invalid_unbounded_bounds() {
        let start = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Following(None),
            end_bound: None,
            exclude: None,
        };
        assert_eq!(
            convert_window_frame(start, &mut no_intervals)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING"
        );

        let end = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(None),
            end_bound: Some(ast::WindowFrameBound::Preceding(None)),
            exclude: None,
        };
        assert_eq!(
            convert_window_frame(end, &mut no_intervals)
                .unwrap_err()
                .strip_backtrace(),
            "Error during planning: Invalid window frame: end bound cannot be UNBOUNDED PRECEDING"
        );
    }

    #[test]
    fn lowers_row_offsets() -> Result<()> {
        let input = ast::WindowFrame {
            units: ast::WindowFrameUnits::Rows,
            start_bound: ast::WindowFrameBound::Preceding(Some(ast::AstBox::new(
                ast::Expr::value(ast::Value::Number("2".to_string(), false)),
            ))),
            end_bound: Some(ast::WindowFrameBound::Preceding(Some(ast::AstBox::new(
                ast::Expr::value(ast::Value::Number("1".to_string(), false)),
            )))),
            exclude: None,
        };
        let frame = convert_window_frame(input, &mut no_intervals)?;
        assert_eq!(frame.units, WindowFrameUnits::Rows);
        assert_eq!(
            frame.start_bound,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2)))
        );
        assert_eq!(
            frame.end_bound,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1)))
        );
        Ok(())
    }

    /// A null offset is rejected with PostgreSQL's SQLSTATE, not read as the
    /// unbounded bound a frame bound of no value means.
    #[test]
    fn a_null_offset_is_rejected_rather_than_read_as_unbounded() {
        for units in [ast::WindowFrameUnits::Rows, ast::WindowFrameUnits::Range] {
            let frame = ast::WindowFrame {
                units,
                start_bound: ast::WindowFrameBound::Preceding(Some(ast::AstBox::new(
                    ast::Expr::value(ast::Value::Null),
                ))),
                end_bound: None,
                exclude: None,
            };
            let sqlstate = match convert_window_frame(frame, &mut no_intervals) {
                Err(DataFusionError::External(error)) => error
                    .downcast_ref::<DataFusionSqlStateError>()
                    .map(|error| error.sqlstate.clone()),
                _ => None,
            };
            assert_eq!(sqlstate.as_deref(), Some("22004"));
        }
    }

    /// A RANGE offset keeps the type of the literal it was written as: an
    /// exact number, unknown-typed text (which is also what the parser makes
    /// of a quoted offset, an unqualified interval), or the interval the
    /// interval planner reads of a qualified interval or a cast.
    #[test]
    fn range_offsets_keep_their_literal_types() -> Result<()> {
        let number =
            |text: &str| ast::Expr::value(ast::Value::Number(text.to_string(), false));
        let string = |text: &str| {
            ast::Expr::value(ast::Value::SingleQuotedString(text.to_string()))
        };
        let interval = ScalarValue::IntervalMonthDayNano(Some(
            arrow::datatypes::IntervalMonthDayNano::new(0, 1, 0),
        ));
        let cases = [
            (number("2"), ScalarValue::Int32(Some(2))),
            (number("1.50"), ScalarValue::Decimal128(Some(150), 3, 2)),
            (
                ast::Expr::UnaryOp {
                    op: ast::UnaryOperator::Minus,
                    expr: ast::AstBox::new(number("1.5")),
                },
                ScalarValue::Decimal128(Some(-15), 2, 1),
            ),
            (
                string("1 day"),
                ScalarValue::Utf8(Some("1 day".to_string())),
            ),
            (
                ast::Expr::Interval(ast::Interval {
                    value: ast::AstBox::new(string("1 day")),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                }),
                ScalarValue::Utf8(Some("1 day".to_string())),
            ),
            (
                ast::Expr::Interval(ast::Interval {
                    value: ast::AstBox::new(string("1")),
                    leading_field: Some(ast::DateTimeField::Day),
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                }),
                interval.clone(),
            ),
            (
                ast::Expr::Cast {
                    kind: ast::CastKind::DoubleColon,
                    expr: ast::AstBox::new(string("1 day")),
                    data_type: ast::DataType::Interval {
                        fields: None,
                        precision: None,
                    },
                    format: None,
                },
                interval.clone(),
            ),
        ];
        for (offset, expected) in cases {
            let frame =
                convert_window_frame(range_frame(offset), &mut |_| Ok(interval.clone()))?;
            assert_eq!(frame.start_bound, WindowFrameBound::Preceding(expected));
        }
        Ok(())
    }
}
