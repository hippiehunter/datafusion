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
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    WindowFrame, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits,
};
use sqlparser::ast::{self, ValueWithSpan};

pub(super) fn convert_window_frame(value: ast::WindowFrame) -> Result<WindowFrame> {
    let start_bound = convert_window_frame_bound(value.start_bound, &value.units)?;
    let end_bound = match value.end_bound {
        Some(bound) => convert_window_frame_bound(bound, &value.units)?,
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

fn convert_window_frame_bound(
    value: ast::WindowFrameBound,
    units: &ast::WindowFrameUnits,
) -> Result<WindowFrameBound> {
    Ok(match value {
        ast::WindowFrameBound::Preceding(Some(value)) => {
            WindowFrameBound::Preceding(convert_frame_bound_to_scalar_value(
                sqlparser::arena::AstBox::into_owned(value),
                units,
            )?)
        }
        ast::WindowFrameBound::Preceding(None) => {
            WindowFrameBound::Preceding(ScalarValue::UInt64(None))
        }
        ast::WindowFrameBound::Following(Some(value)) => {
            WindowFrameBound::Following(convert_frame_bound_to_scalar_value(
                sqlparser::arena::AstBox::into_owned(value),
                units,
            )?)
        }
        ast::WindowFrameBound::Following(None) => {
            WindowFrameBound::Following(ScalarValue::UInt64(None))
        }
        ast::WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
    })
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
) -> Result<ScalarValue> {
    if let Some(offset) = fold_integer_frame_offset(&value) {
        return match units {
            ast::WindowFrameUnits::Rows | ast::WindowFrameUnits::Groups => {
                if offset < 0 {
                    return plan_err!(
                        "Invalid window frame: frame offsets for ROWS / GROUPS must be non negative integers"
                    );
                }
                ScalarValue::try_from_string(offset.to_string(), &DataType::UInt64)
            }
            ast::WindowFrameUnits::Range => {
                Ok(ScalarValue::Utf8(Some(offset.to_string())))
            }
        };
    }

    match units {
        ast::WindowFrameUnits::Rows | ast::WindowFrameUnits::Groups => match value {
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
        },
        ast::WindowFrameUnits::Range => Ok(ScalarValue::Utf8(Some(match value {
            ast::Expr::Value(ValueWithSpan {
                value: ast::Value::Number(value, false),
                span: _,
            }) => value,
            ast::Expr::Interval(ast::Interval {
                value,
                leading_field,
                ..
            }) => {
                let result = match sqlparser::arena::AstBox::into_owned(value) {
                    ast::Expr::Value(ValueWithSpan {
                        value: ast::Value::SingleQuotedString(item),
                        span: _,
                    }) => item,
                    expr => return exec_err!("INTERVAL expression cannot be {expr:?}"),
                };
                leading_field
                    .map(|field| format!("{result} {field}"))
                    .unwrap_or(result)
            }
            ast::Expr::Cast {
                expr,
                data_type: ast::DataType::Interval { .. },
                ..
            } => match sqlparser::arena::AstBox::into_owned(expr) {
                ast::Expr::Value(ValueWithSpan {
                    value: ast::Value::SingleQuotedString(item),
                    span: _,
                }) => item,
                expr => return exec_err!("INTERVAL expression cannot be {expr:?}"),
            },
            ast::Expr::Cast { expr, .. } => {
                match sqlparser::arena::AstBox::into_owned(expr) {
                    ast::Expr::Value(ValueWithSpan {
                        value: ast::Value::Number(item, _),
                        span: _,
                    })
                    | ast::Expr::Value(ValueWithSpan {
                        value: ast::Value::SingleQuotedString(item),
                        span: _,
                    }) => item,
                    expr => {
                        return exec_err!(
                            "frame offset cast expression cannot be {expr:?}"
                        );
                    }
                }
            }
            _ => plan_err!(
                "Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value or an interval"
            )?,
        }))),
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

    #[test]
    fn rejects_invalid_unbounded_bounds() {
        let start = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Following(None),
            end_bound: None,
            exclude: None,
        };
        assert_eq!(
            convert_window_frame(start).unwrap_err().strip_backtrace(),
            "Error during planning: Invalid window frame: start bound cannot be UNBOUNDED FOLLOWING"
        );

        let end = ast::WindowFrame {
            units: ast::WindowFrameUnits::Range,
            start_bound: ast::WindowFrameBound::Preceding(None),
            end_bound: Some(ast::WindowFrameBound::Preceding(None)),
            exclude: None,
        };
        assert_eq!(
            convert_window_frame(end).unwrap_err().strip_backtrace(),
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
        let frame = convert_window_frame(input)?;
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
}
