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

//! JSON/JSONB operator kernels for physical execution.
//!
//! Handles all 12 PostgreSQL-compatible JSON operators:
//! - `->` (Arrow): extract field by key or element by index
//! - `->>` (LongArrow): extract field/element as text
//! - `#>` (HashArrow): extract by path
//! - `#>>` (HashLongArrow): extract by path as text
//! - `@>` (AtArrow): JSON containment
//! - `<@` (ArrowAt): JSON contained-by
//! - `?` (Question): key exists
//! - `?|` (QuestionPipe): any key exists
//! - `?&` (QuestionAnd): all keys exist
//! - `#-` (HashMinus): delete key/path/element
//! - `@?` (AtQuestion): jsonpath exists
//! - `@@` (AtAt): jsonpath predicate match

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion_expr::{ColumnarValue, Operator};
use jsonb::keypath::KeyPath;
use jsonb::{OwnedJsonb, RawJsonb};

/// Main entry point: evaluate a JSON operator on two ColumnarValues.
///
/// This is called from `BinaryExpr::evaluate()` for all JSON operators,
/// before the values are expanded to arrays.
pub fn evaluate_json_op(
    lhs: &ColumnarValue,
    op: &Operator,
    rhs: &ColumnarValue,
) -> Result<ColumnarValue> {
    match (lhs, rhs) {
        (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
            evaluate_scalar_scalar(left, op, right)
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Scalar(right)) => {
            evaluate_array_scalar(left_arr, op, right)
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Array(right_arr)) => {
            // Expand scalar to array
            let left_arr = left.to_array_of_size(right_arr.len())?;
            evaluate_array_array(&left_arr, op, right_arr)
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Array(right_arr)) => {
            evaluate_array_array(left_arr, op, right_arr)
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar x Scalar
// ---------------------------------------------------------------------------

fn evaluate_scalar_scalar(
    left: &ScalarValue,
    op: &Operator,
    right: &ScalarValue,
) -> Result<ColumnarValue> {
    // Handle nulls — all JSON operators return null when either input is null
    if left.is_null() || right.is_null() {
        return Ok(ColumnarValue::Scalar(null_result_for_op(
            op,
            left.data_type(),
        )));
    }

    let left_bytes = scalar_to_jsonb(left)?;
    let right_str = scalar_to_str(right);
    let right_i64 = scalar_to_i64(right);

    let raw = RawJsonb::new(left_bytes.as_ref());

    match op {
        Operator::Arrow => {
            let result = json_get(&raw, right_str.as_deref(), right_i64)?;
            Ok(ColumnarValue::Scalar(owned_jsonb_to_scalar(
                result,
                &left.data_type(),
            )))
        }
        Operator::LongArrow => {
            let result = json_get(&raw, right_str.as_deref(), right_i64)?;
            Ok(ColumnarValue::Scalar(owned_jsonb_to_text_scalar(result)))
        }
        Operator::HashArrow => {
            let path_str = right_str.as_deref().unwrap_or("");
            let result = json_get_by_path(&raw, path_str)?;
            Ok(ColumnarValue::Scalar(owned_jsonb_to_scalar(
                result,
                &left.data_type(),
            )))
        }
        Operator::HashLongArrow => {
            let path_str = right_str.as_deref().unwrap_or("");
            let result = json_get_by_path(&raw, path_str)?;
            Ok(ColumnarValue::Scalar(owned_jsonb_to_text_scalar(result)))
        }
        Operator::AtArrow => {
            // lhs @> rhs (lhs contains rhs)
            let right_bytes = scalar_to_jsonb(right)?;
            let right_raw = RawJsonb::new(right_bytes.as_ref());
            let result = raw.contains(&right_raw).map_err(jsonb_err)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
        }
        Operator::ArrowAt => {
            // lhs <@ rhs (lhs is contained by rhs) → rhs.contains(lhs)
            let right_bytes = scalar_to_jsonb(right)?;
            let right_raw = RawJsonb::new(right_bytes.as_ref());
            let result = right_raw.contains(&raw).map_err(jsonb_err)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
        }
        Operator::Question => {
            let key = right_str.as_deref().unwrap_or("");
            let result = json_key_exists(&raw, key)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
        }
        Operator::QuestionAnd => {
            let key = right_str.as_deref().unwrap_or("");
            let result = raw
                .exists_all_keys(std::iter::once(key))
                .map_err(jsonb_err)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
        }
        Operator::QuestionPipe => {
            let key = right_str.as_deref().unwrap_or("");
            let result = raw
                .exists_any_keys(std::iter::once(key))
                .map_err(jsonb_err)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
        }
        Operator::HashMinus => {
            let result = json_delete(&raw, right_str.as_deref(), right_i64)?;
            Ok(ColumnarValue::Scalar(owned_jsonb_to_scalar(
                Some(result),
                &left.data_type(),
            )))
        }
        Operator::AtQuestion => {
            let path_str = right_str.as_deref().unwrap_or("$");
            let result = json_path_exists(&raw, path_str)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
        }
        Operator::AtAt => {
            let path_str = right_str.as_deref().unwrap_or("$");
            let result = json_path_match(&raw, path_str)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                result.unwrap_or(false),
            ))))
        }
        other => not_impl_err!("JSON operator {other} is not supported"),
    }
}

// ---------------------------------------------------------------------------
// Array x Scalar (most common case — parse scalar once)
// ---------------------------------------------------------------------------

fn evaluate_array_scalar(
    left_arr: &ArrayRef,
    op: &Operator,
    right: &ScalarValue,
) -> Result<ColumnarValue> {
    let len = left_arr.len();
    let lhs_type = left_arr.data_type().clone();
    let is_text = is_text_type(&lhs_type);

    // Pre-parse the scalar RHS once
    let right_str = if !right.is_null() {
        scalar_to_str(right)
    } else {
        None
    };
    let right_i64 = scalar_to_i64(right);

    // For containment operators, pre-parse RHS as jsonb once
    let right_jsonb_bytes = if matches!(op, Operator::AtArrow | Operator::ArrowAt)
        && !right.is_null()
    {
        Some(scalar_to_jsonb(right)?)
    } else {
        None
    };

    // Determine result type
    match op {
        // Operators that return json/jsonb (same type as LHS)
        Operator::Arrow | Operator::HashArrow | Operator::HashMinus => {
            if is_text {
                let mut builder = StringBuilder::new();
                for i in 0..len {
                    if left_arr.is_null(i) || right.is_null() {
                        builder.append_null();
                        continue;
                    }
                    let left_bytes = text_array_value_to_jsonb(left_arr, i)?;
                    let raw = RawJsonb::new(left_bytes.as_ref());
                    match op {
                        Operator::Arrow => {
                            let result =
                                json_get(&raw, right_str.as_deref(), right_i64)?;
                            append_owned_jsonb_as_text(&mut builder, result);
                        }
                        Operator::HashArrow => {
                            let path_str = right_str.as_deref().unwrap_or("");
                            let result = json_get_by_path(&raw, path_str)?;
                            append_owned_jsonb_as_text(&mut builder, result);
                        }
                        Operator::HashMinus => {
                            let result =
                                json_delete(&raw, right_str.as_deref(), right_i64)?;
                            builder.append_value(result.to_string());
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            } else {
                let mut builder = BinaryBuilder::new();
                for i in 0..len {
                    if left_arr.is_null(i) || right.is_null() {
                        builder.append_null();
                        continue;
                    }
                    let left_bytes = binary_array_value(left_arr, i);
                    let raw = RawJsonb::new(&left_bytes);
                    match op {
                        Operator::Arrow => {
                            let result =
                                json_get(&raw, right_str.as_deref(), right_i64)?;
                            append_owned_jsonb_as_binary(&mut builder, result);
                        }
                        Operator::HashArrow => {
                            let path_str = right_str.as_deref().unwrap_or("");
                            let result = json_get_by_path(&raw, path_str)?;
                            append_owned_jsonb_as_binary(&mut builder, result);
                        }
                        Operator::HashMinus => {
                            let result =
                                json_delete(&raw, right_str.as_deref(), right_i64)?;
                            builder.append_value(result.as_ref());
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }

        // Operators that always return text
        Operator::LongArrow | Operator::HashLongArrow => {
            let mut builder = StringBuilder::new();
            for i in 0..len {
                if left_arr.is_null(i) || right.is_null() {
                    builder.append_null();
                    continue;
                }
                let left_bytes = get_jsonb_bytes(left_arr, i, is_text)?;
                let raw = RawJsonb::new(left_bytes.as_ref());
                let result = match op {
                    Operator::LongArrow => {
                        json_get(&raw, right_str.as_deref(), right_i64)?
                    }
                    Operator::HashLongArrow => {
                        let path_str = right_str.as_deref().unwrap_or("");
                        json_get_by_path(&raw, path_str)?
                    }
                    _ => unreachable!(),
                };
                match result {
                    Some(owned) => {
                        // ->> returns the text representation; for strings, strip quotes
                        let raw_result = owned.as_raw();
                        if let Ok(Some(s)) = raw_result.as_str() {
                            builder.append_value(s.as_ref());
                        } else {
                            builder.append_value(owned.to_string());
                        }
                    }
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        // Operators that return boolean
        Operator::AtArrow
        | Operator::ArrowAt
        | Operator::Question
        | Operator::QuestionAnd
        | Operator::QuestionPipe
        | Operator::AtQuestion
        | Operator::AtAt => {
            let mut builder = BooleanBuilder::new();
            for i in 0..len {
                if left_arr.is_null(i) || right.is_null() {
                    builder.append_null();
                    continue;
                }
                let left_bytes = get_jsonb_bytes(left_arr, i, is_text)?;
                let raw = RawJsonb::new(left_bytes.as_ref());
                let result = match op {
                    Operator::AtArrow => {
                        let right_bytes = right_jsonb_bytes.as_ref().unwrap();
                        let right_raw = RawJsonb::new(right_bytes.as_ref());
                        raw.contains(&right_raw).map_err(jsonb_err)?
                    }
                    Operator::ArrowAt => {
                        let right_bytes = right_jsonb_bytes.as_ref().unwrap();
                        let right_raw = RawJsonb::new(right_bytes.as_ref());
                        right_raw.contains(&raw).map_err(jsonb_err)?
                    }
                    Operator::Question => {
                        let key = right_str.as_deref().unwrap_or("");
                        json_key_exists(&raw, key)?
                    }
                    Operator::QuestionAnd => {
                        let key = right_str.as_deref().unwrap_or("");
                        raw.exists_all_keys(std::iter::once(key))
                            .map_err(jsonb_err)?
                    }
                    Operator::QuestionPipe => {
                        let key = right_str.as_deref().unwrap_or("");
                        raw.exists_any_keys(std::iter::once(key))
                            .map_err(jsonb_err)?
                    }
                    Operator::AtQuestion => {
                        let path_str = right_str.as_deref().unwrap_or("$");
                        json_path_exists(&raw, path_str)?
                    }
                    Operator::AtAt => {
                        let path_str = right_str.as_deref().unwrap_or("$");
                        json_path_match(&raw, path_str)?.unwrap_or(false)
                    }
                    _ => unreachable!(),
                };
                builder.append_value(result);
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        other => not_impl_err!("JSON operator {other} is not supported"),
    }
}

// ---------------------------------------------------------------------------
// Array x Array (element-wise)
// ---------------------------------------------------------------------------

fn evaluate_array_array(
    left_arr: &ArrayRef,
    op: &Operator,
    right_arr: &ArrayRef,
) -> Result<ColumnarValue> {
    let len = left_arr.len();
    if len != right_arr.len() {
        return exec_err!(
            "JSON operator requires arrays of equal length, got {} and {}",
            len,
            right_arr.len()
        );
    }

    let lhs_type = left_arr.data_type().clone();
    let is_text_lhs = is_text_type(&lhs_type);
    let is_text_rhs = is_text_type(right_arr.data_type());

    match op {
        Operator::Arrow | Operator::HashArrow | Operator::HashMinus => {
            if is_text_lhs {
                let mut builder = StringBuilder::new();
                for i in 0..len {
                    if left_arr.is_null(i) || right_arr.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let left_bytes = text_array_value_to_jsonb(left_arr, i)?;
                    let raw = RawJsonb::new(left_bytes.as_ref());
                    let right_str = text_array_value_as_str(right_arr, i);
                    let right_i64 = int_array_value(right_arr, i);
                    match op {
                        Operator::Arrow => {
                            let result =
                                json_get(&raw, right_str.as_deref(), right_i64)?;
                            append_owned_jsonb_as_text(&mut builder, result);
                        }
                        Operator::HashArrow => {
                            let path_str = right_str.as_deref().unwrap_or("");
                            let result = json_get_by_path(&raw, path_str)?;
                            append_owned_jsonb_as_text(&mut builder, result);
                        }
                        Operator::HashMinus => {
                            let result =
                                json_delete(&raw, right_str.as_deref(), right_i64)?;
                            builder.append_value(result.to_string());
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            } else {
                let mut builder = BinaryBuilder::new();
                for i in 0..len {
                    if left_arr.is_null(i) || right_arr.is_null(i) {
                        builder.append_null();
                        continue;
                    }
                    let left_bytes = binary_array_value(left_arr, i);
                    let raw = RawJsonb::new(&left_bytes);
                    let right_str = text_array_value_as_str(right_arr, i);
                    let right_i64 = int_array_value(right_arr, i);
                    match op {
                        Operator::Arrow => {
                            let result =
                                json_get(&raw, right_str.as_deref(), right_i64)?;
                            append_owned_jsonb_as_binary(&mut builder, result);
                        }
                        Operator::HashArrow => {
                            let path_str = right_str.as_deref().unwrap_or("");
                            let result = json_get_by_path(&raw, path_str)?;
                            append_owned_jsonb_as_binary(&mut builder, result);
                        }
                        Operator::HashMinus => {
                            let result =
                                json_delete(&raw, right_str.as_deref(), right_i64)?;
                            builder.append_value(result.as_ref());
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }

        Operator::LongArrow | Operator::HashLongArrow => {
            let mut builder = StringBuilder::new();
            for i in 0..len {
                if left_arr.is_null(i) || right_arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let left_bytes = get_jsonb_bytes(left_arr, i, is_text_lhs)?;
                let raw = RawJsonb::new(left_bytes.as_ref());
                let right_str = text_array_value_as_str(right_arr, i);
                let right_i64 = int_array_value(right_arr, i);
                let result = match op {
                    Operator::LongArrow => {
                        json_get(&raw, right_str.as_deref(), right_i64)?
                    }
                    Operator::HashLongArrow => {
                        let path_str = right_str.as_deref().unwrap_or("");
                        json_get_by_path(&raw, path_str)?
                    }
                    _ => unreachable!(),
                };
                match result {
                    Some(owned) => {
                        let raw_result = owned.as_raw();
                        if let Ok(Some(s)) = raw_result.as_str() {
                            builder.append_value(s.as_ref());
                        } else {
                            builder.append_value(owned.to_string());
                        }
                    }
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        Operator::AtArrow
        | Operator::ArrowAt
        | Operator::Question
        | Operator::QuestionAnd
        | Operator::QuestionPipe
        | Operator::AtQuestion
        | Operator::AtAt => {
            let mut builder = BooleanBuilder::new();
            for i in 0..len {
                if left_arr.is_null(i) || right_arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let left_bytes = get_jsonb_bytes(left_arr, i, is_text_lhs)?;
                let raw = RawJsonb::new(left_bytes.as_ref());
                let result = match op {
                    Operator::AtArrow => {
                        let right_bytes =
                            get_jsonb_bytes(right_arr, i, is_text_rhs)?;
                        let right_raw = RawJsonb::new(right_bytes.as_ref());
                        raw.contains(&right_raw).map_err(jsonb_err)?
                    }
                    Operator::ArrowAt => {
                        let right_bytes =
                            get_jsonb_bytes(right_arr, i, is_text_rhs)?;
                        let right_raw = RawJsonb::new(right_bytes.as_ref());
                        right_raw.contains(&raw).map_err(jsonb_err)?
                    }
                    Operator::Question => {
                        let key_str = text_array_value_as_str(right_arr, i);
                        let key = key_str.as_deref().unwrap_or("");
                        json_key_exists(&raw, key)?
                    }
                    Operator::QuestionAnd => {
                        let key_str = text_array_value_as_str(right_arr, i);
                        let key = key_str.as_deref().unwrap_or("");
                        raw.exists_all_keys(std::iter::once(key))
                            .map_err(jsonb_err)?
                    }
                    Operator::QuestionPipe => {
                        let key_str = text_array_value_as_str(right_arr, i);
                        let key = key_str.as_deref().unwrap_or("");
                        raw.exists_any_keys(std::iter::once(key))
                            .map_err(jsonb_err)?
                    }
                    Operator::AtQuestion => {
                        let path_str = text_array_value_as_str(right_arr, i);
                        let path = path_str.as_deref().unwrap_or("$");
                        json_path_exists(&raw, path)?
                    }
                    Operator::AtAt => {
                        let path_str = text_array_value_as_str(right_arr, i);
                        let path = path_str.as_deref().unwrap_or("$");
                        json_path_match(&raw, path)?.unwrap_or(false)
                    }
                    _ => unreachable!(),
                };
                builder.append_value(result);
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        other => not_impl_err!("JSON operator {other} is not supported"),
    }
}

// ===========================================================================
// Core JSON operation helpers
// ===========================================================================

/// Extract a field by name or element by index (-> operator).
fn json_get(
    raw: &RawJsonb,
    key: Option<&str>,
    index: Option<i64>,
) -> Result<Option<OwnedJsonb>> {
    if let Some(key) = key {
        raw.get_by_name(key, false).map_err(jsonb_err)
    } else if let Some(idx) = index {
        if idx < 0 {
            // PostgreSQL supports negative indexing
            if let Ok(Some(len)) = raw.array_length() {
                let actual = len as i64 + idx;
                if actual >= 0 {
                    raw.get_by_index(actual as usize).map_err(jsonb_err)
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            raw.get_by_index(idx as usize).map_err(jsonb_err)
        }
    } else {
        Ok(None)
    }
}

/// Extract by path (#> operator). Path is a PostgreSQL text array literal like '{a,b,c}'.
fn json_get_by_path(raw: &RawJsonb, path_str: &str) -> Result<Option<OwnedJsonb>> {
    let keypaths = parse_pg_path(path_str);
    raw.get_by_keypath(keypaths.iter()).map_err(jsonb_err)
}

/// Check if a key exists (? operator).
fn json_key_exists(raw: &RawJsonb, key: &str) -> Result<bool> {
    // Use get_by_name and check if result is Some
    match raw.get_by_name(key, false) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(e) => Err(jsonb_err(e)),
    }
}

/// Delete by key, path, or index (#- operator).
fn json_delete(
    raw: &RawJsonb,
    key: Option<&str>,
    index: Option<i64>,
) -> Result<OwnedJsonb> {
    if let Some(key) = key {
        // Check if it looks like a PostgreSQL path literal '{a,b}'
        let trimmed = key.trim();
        if trimmed.starts_with('{') && trimmed.ends_with('}') {
            let keypaths = parse_pg_path(key);
            raw.delete_by_keypath(keypaths.iter()).map_err(jsonb_err)
        } else {
            raw.delete_by_name(key).map_err(jsonb_err)
        }
    } else if let Some(idx) = index {
        raw.delete_by_index(idx as i32).map_err(jsonb_err)
    } else {
        // No key or index — return original
        Ok(raw.to_owned())
    }
}

/// Check jsonpath existence (@? operator).
fn json_path_exists(raw: &RawJsonb, path_str: &str) -> Result<bool> {
    let json_path = jsonb::jsonpath::parse_json_path(path_str.as_bytes())
        .map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Invalid JSON path: {e}"
            ))
        })?;
    raw.path_exists(&json_path).map_err(jsonb_err)
}

/// Check jsonpath predicate match (@@ operator).
fn json_path_match(raw: &RawJsonb, path_str: &str) -> Result<Option<bool>> {
    let json_path = jsonb::jsonpath::parse_json_path(path_str.as_bytes())
        .map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Invalid JSON path: {e}"
            ))
        })?;
    raw.path_match(&json_path).map_err(jsonb_err)
}

// ===========================================================================
// Type conversion helpers
// ===========================================================================

/// Returns true if the data type is a text/string type (JSON text representation).
fn is_text_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Convert a ScalarValue to JSONB binary bytes.
/// For text types, parses the JSON text. For binary types, uses raw bytes.
fn scalar_to_jsonb(scalar: &ScalarValue) -> Result<Vec<u8>> {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => {
            let owned = jsonb::parse_owned_jsonb(s.as_bytes()).map_err(jsonb_err)?;
            Ok(owned.to_vec())
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::LargeBinary(Some(b)) => Ok(b.clone()),
        _ => exec_err!("Cannot convert {:?} to JSONB", scalar.data_type()),
    }
}

/// Extract a string value from a ScalarValue, if it's a text type.
fn scalar_to_str(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Extract an i64 value from a ScalarValue, if it's an integer type.
fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        _ => None,
    }
}

/// Get JSONB bytes from an array element. For text arrays, parses JSON text.
/// For binary arrays, returns raw bytes.
fn get_jsonb_bytes(arr: &ArrayRef, i: usize, is_text: bool) -> Result<Vec<u8>> {
    if is_text {
        text_array_value_to_jsonb(arr, i)
    } else {
        Ok(binary_array_value(arr, i))
    }
}

/// Parse a text array value at index `i` into JSONB binary bytes.
fn text_array_value_to_jsonb(arr: &ArrayRef, i: usize) -> Result<Vec<u8>> {
    let s = text_array_value_as_str(arr, i).unwrap_or_default();
    let owned = jsonb::parse_owned_jsonb(s.as_bytes()).map_err(jsonb_err)?;
    Ok(owned.to_vec())
}

/// Get the string value from a text array at index `i`.
fn text_array_value_as_str(arr: &ArrayRef, i: usize) -> Option<String> {
    match arr.data_type() {
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref::<StringArray>()
            .and_then(|a| a.value(i).into()),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .and_then(|a| a.value(i).into()),
        DataType::Utf8View => arr
            .as_any()
            .downcast_ref::<StringViewArray>()
            .and_then(|a| a.value(i).into()),
        _ => None,
    }
    .map(|s: &str| s.to_string())
}

/// Get the integer value from an integer array at index `i`.
fn int_array_value(arr: &ArrayRef, i: usize) -> Option<i64> {
    match arr.data_type() {
        DataType::Int8 => arr
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(i) as i64),
        DataType::Int16 => arr
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(i) as i64),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(i) as i64),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(i)),
        _ => None,
    }
}

/// Get raw bytes from a binary array at index `i`.
fn binary_array_value(arr: &ArrayRef, i: usize) -> Vec<u8> {
    match arr.data_type() {
        DataType::Binary => arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|a| a.value(i).to_vec())
            .unwrap_or_default(),
        DataType::LargeBinary => arr
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|a| a.value(i).to_vec())
            .unwrap_or_default(),
        DataType::BinaryView => arr
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .map(|a| a.value(i).to_vec())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Convert an OwnedJsonb result to a ScalarValue matching the LHS type.
fn owned_jsonb_to_scalar(
    result: Option<OwnedJsonb>,
    lhs_type: &DataType,
) -> ScalarValue {
    match result {
        Some(owned) => {
            if is_text_type(lhs_type) {
                ScalarValue::Utf8View(Some(owned.to_string()))
            } else {
                ScalarValue::BinaryView(Some(owned.to_vec()))
            }
        }
        None => {
            if is_text_type(lhs_type) {
                ScalarValue::Utf8View(None)
            } else {
                ScalarValue::BinaryView(None)
            }
        }
    }
}

/// Convert an OwnedJsonb result to a text ScalarValue (for ->> and #>> operators).
fn owned_jsonb_to_text_scalar(result: Option<OwnedJsonb>) -> ScalarValue {
    match result {
        Some(owned) => {
            let raw = owned.as_raw();
            // For ->> : if the value is a string, return unquoted; otherwise return JSON text
            if let Ok(Some(s)) = raw.as_str() {
                ScalarValue::Utf8View(Some(s.to_string()))
            } else {
                ScalarValue::Utf8View(Some(owned.to_string()))
            }
        }
        None => ScalarValue::Utf8View(None),
    }
}

/// Append an OwnedJsonb to a StringBuilder (JSON text representation).
fn append_owned_jsonb_as_text(builder: &mut StringBuilder, result: Option<OwnedJsonb>) {
    match result {
        Some(owned) => builder.append_value(owned.to_string()),
        None => builder.append_null(),
    }
}

/// Append an OwnedJsonb to a BinaryBuilder (JSONB binary representation).
fn append_owned_jsonb_as_binary(
    builder: &mut BinaryBuilder,
    result: Option<OwnedJsonb>,
) {
    match result {
        Some(owned) => builder.append_value(owned.as_ref()),
        None => builder.append_null(),
    }
}

/// Return a null ScalarValue appropriate for the operator's return type.
fn null_result_for_op(op: &Operator, lhs_type: DataType) -> ScalarValue {
    match op {
        // Boolean-returning operators
        Operator::AtArrow
        | Operator::ArrowAt
        | Operator::Question
        | Operator::QuestionAnd
        | Operator::QuestionPipe
        | Operator::AtQuestion
        | Operator::AtAt => ScalarValue::Boolean(None),
        // Text-returning operators
        Operator::LongArrow | Operator::HashLongArrow => ScalarValue::Utf8View(None),
        // Same-type-as-LHS operators
        _ => {
            if is_text_type(&lhs_type) {
                ScalarValue::Utf8View(None)
            } else {
                ScalarValue::BinaryView(None)
            }
        }
    }
}

/// Parse a PostgreSQL text array path literal like '{a,b,c}' into KeyPath items.
fn parse_pg_path(path_str: &str) -> Vec<KeyPath<'static>> {
    let trimmed = path_str.trim();
    let inner = if trimmed.starts_with('{') && trimmed.ends_with('}') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    if inner.is_empty() {
        return Vec::new();
    }

    inner
        .split(',')
        .map(|part| {
            let part = part.trim();
            if let Ok(idx) = part.parse::<i32>() {
                KeyPath::Index(idx)
            } else {
                KeyPath::Name(std::borrow::Cow::Owned(part.to_string()))
            }
        })
        .collect()
}

/// Convert a jsonb crate error to a DataFusion error.
fn jsonb_err(e: jsonb::Error) -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Execution(format!("JSONB error: {e}"))
}
