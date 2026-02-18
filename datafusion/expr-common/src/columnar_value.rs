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

//! [`ColumnarValue`] represents the result of evaluating an expression.

use arrow::{
    array::{
        Array, ArrayRef, BinaryViewArray, BinaryViewBuilder, Date32Array, Date64Array,
        Decimal128Array, Decimal128Builder, LargeStringArray, NullArray, StringArray,
        StringViewArray,
    },
    compute::{CastOptions, kernels, max, min},
    datatypes::{DataType, TimeUnit},
    util::pretty::pretty_format_columns,
};
use datafusion_common::internal_datafusion_err;
use datafusion_common::{
    DataFusionError, Result, ScalarValue,
    format::DEFAULT_CAST_OPTIONS,
    internal_err,
    scalar::{date_to_timestamp_multiplier, ensure_timestamp_in_bounds},
};
use std::fmt;
use std::sync::Arc;

// PostgreSQL NUMERIC supports NaN, while Arrow Decimal128 does not.
// Use a coefficient that cannot occur for valid precision<=38 decimals.
const PG_NUMERIC_NAN_DECIMAL128_SENTINEL: i128 = i128::MAX;
const PG_DATE_POS_INFINITY_SENTINEL: i32 = i32::MAX;
const PG_DATE_NEG_INFINITY_SENTINEL: i32 = i32::MIN;
const PG_TS_POS_INFINITY_MICROS: i64 = i64::MAX;
const PG_TS_NEG_INFINITY_MICROS: i64 = i64::MIN;

/// The result of evaluating an expression.
///
/// [`ColumnarValue::Scalar`] represents a single value repeated any number of
/// times. This is an important performance optimization for handling values
/// that do not change across rows.
///
/// [`ColumnarValue::Array`] represents a column of data, stored as an  Arrow
/// [`ArrayRef`]
///
/// A slice of `ColumnarValue`s logically represents a table, with each column
/// having the same number of rows. This means that all `Array`s are the same
/// length.
///
/// # Example
///
/// A `ColumnarValue::Array` with an array of 5 elements and a
/// `ColumnarValue::Scalar` with the value 100
///
/// ```text
/// ┌──────────────┐
/// │ ┌──────────┐ │
/// │ │   "A"    │ │
/// │ ├──────────┤ │
/// │ │   "B"    │ │
/// │ ├──────────┤ │
/// │ │   "C"    │ │
/// │ ├──────────┤ │
/// │ │   "D"    │ │        ┌──────────────┐
/// │ ├──────────┤ │        │ ┌──────────┐ │
/// │ │   "E"    │ │        │ │   100    │ │
/// │ └──────────┘ │        │ └──────────┘ │
/// └──────────────┘        └──────────────┘
///
///  ColumnarValue::        ColumnarValue::
///       Array                 Scalar
/// ```
///
/// Logically represents the following table:
///
/// | Column 1| Column 2 |
/// | ------- | -------- |
/// | A | 100 |
/// | B | 100 |
/// | C | 100 |
/// | D | 100 |
/// | E | 100 |
///
/// # Performance Notes
///
/// When implementing functions or operators, it is important to consider the
/// performance implications of handling scalar values.
///
/// Because all functions must handle [`ArrayRef`], it is
/// convenient to convert [`ColumnarValue::Scalar`]s using
/// [`Self::into_array`]. For example,  [`ColumnarValue::values_to_arrays`]
/// converts multiple columnar values into arrays of the same length.
///
/// However, it is often much more performant to provide a different,
/// implementation that handles scalar values differently
#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(ScalarValue),
}

impl From<ArrayRef> for ColumnarValue {
    fn from(value: ArrayRef) -> Self {
        ColumnarValue::Array(value)
    }
}

impl From<ScalarValue> for ColumnarValue {
    fn from(value: ScalarValue) -> Self {
        ColumnarValue::Scalar(value)
    }
}

impl ColumnarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Array(array_value) => array_value.data_type().clone(),
            ColumnarValue::Scalar(scalar_value) => scalar_value.data_type(),
        }
    }

    /// Convert any [`Self::Scalar`] into an Arrow [`ArrayRef`] with the specified
    /// number of rows  by repeating the same scalar multiple times,
    /// which is not as efficient as handling the scalar directly.
    /// [`Self::Array`] will just be returned as is.
    ///
    /// See [`Self::into_array_of_size`] if you need to validate the length of the output array.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }

    /// Convert a columnar value into an Arrow [`ArrayRef`] with the specified
    /// number of rows. [`Self::Scalar`] is converted by repeating the same
    /// scalar multiple times which is not as efficient as handling the scalar
    /// directly.
    /// This validates that if this is [`Self::Array`], it has the expected length.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size or
    /// if the array length does not match the expected length
    pub fn into_array_of_size(self, num_rows: usize) -> Result<ArrayRef> {
        match self {
            ColumnarValue::Array(array) => {
                if array.len() == num_rows {
                    Ok(array)
                } else {
                    internal_err!(
                        "Array length {} does not match expected length {}",
                        array.len(),
                        num_rows
                    )
                }
            }
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
        }
    }

    /// Convert any [`Self::Scalar`] into an Arrow [`ArrayRef`] with the specified
    /// number of rows  by repeating the same scalar multiple times,
    /// which is not as efficient as handling the scalar directly.
    /// [`Self::Array`] will just be returned as is.
    ///
    /// See [`Self::to_array_of_size`] if you need to validate the length of the output array.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size
    pub fn to_array(&self, num_rows: usize) -> Result<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }

    /// Convert a columnar value into an Arrow [`ArrayRef`] with the specified
    /// number of rows. [`Self::Scalar`] is converted by repeating the same
    /// scalar multiple times which is not as efficient as handling the scalar
    /// directly.
    /// This validates that if this is [`Self::Array`], it has the expected length.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size or
    /// if the array length does not match the expected length
    pub fn to_array_of_size(&self, num_rows: usize) -> Result<ArrayRef> {
        match self {
            ColumnarValue::Array(array) => {
                if array.len() == num_rows {
                    Ok(Arc::clone(array))
                } else {
                    internal_err!(
                        "Array length {} does not match expected length {}",
                        array.len(),
                        num_rows
                    )
                }
            }
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows),
        }
    }

    /// Null columnar values are implemented as a null array in order to pass batch
    /// num_rows
    pub fn create_null_array(num_rows: usize) -> Self {
        ColumnarValue::Array(Arc::new(NullArray::new(num_rows)))
    }

    /// Converts  [`ColumnarValue`]s to [`ArrayRef`]s with the same length.
    ///
    /// # Performance Note
    ///
    /// This function expands any [`ScalarValue`] to an array. This expansion
    /// permits using a single function in terms of arrays, but it can be
    /// inefficient compared to handling the scalar value directly.
    ///
    /// Thus, it is recommended to provide specialized implementations for
    /// scalar values if performance is a concern.
    ///
    /// # Errors
    ///
    /// If there are multiple array arguments that have different lengths
    pub fn values_to_arrays(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
        if args.is_empty() {
            return Ok(vec![]);
        }

        let mut array_len = None;
        for arg in args {
            array_len = match (arg, array_len) {
                (ColumnarValue::Array(a), None) => Some(a.len()),
                (ColumnarValue::Array(a), Some(array_len)) => {
                    if array_len == a.len() {
                        Some(array_len)
                    } else {
                        return internal_err!(
                            "Arguments has mixed length. Expected length: {array_len}, found length: {}",
                            a.len()
                        );
                    }
                }
                (ColumnarValue::Scalar(_), array_len) => array_len,
            }
        }

        // If array_len is none, it means there are only scalars, so make a 1 element array
        let inferred_length = array_len.unwrap_or(1);

        let args = args
            .iter()
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        Ok(args)
    }

    /// Cast's this [ColumnarValue] to the specified `DataType`
    pub fn cast_to(
        &self,
        cast_type: &DataType,
        cast_options: Option<&CastOptions<'static>>,
    ) -> Result<ColumnarValue> {
        let cast_options = cast_options.cloned().unwrap_or(DEFAULT_CAST_OPTIONS);
        match self {
            ColumnarValue::Array(array) => {
                if is_disallowed_date_to_integer_cast(array.data_type(), cast_type) {
                    return Err(DataFusionError::Execution(format!(
                        "Cannot cast {} to {cast_type}",
                        array.data_type()
                    )));
                }
                ensure_date_array_timestamp_bounds(array, cast_type)?;
                if let Some(casted_array) = cast_string_array_to_decimal_with_pg_nan(
                    array,
                    cast_type,
                    &cast_options,
                )? {
                    return Ok(ColumnarValue::Array(casted_array));
                }
                if let Some(casted_array) =
                    cast_string_array_to_jsonb_binaryview(array, cast_type)?
                {
                    return Ok(ColumnarValue::Array(casted_array));
                }
                if let Some(casted_array) =
                    cast_jsonb_binaryview_array_to_text(array, cast_type, &cast_options)?
                {
                    return Ok(ColumnarValue::Array(casted_array));
                }
                let trimmed_integer_array =
                    normalize_string_array_for_integer_cast(array, cast_type)?;
                let array_for_decimal_normalization =
                    trimmed_integer_array.as_ref().unwrap_or(array);
                let normalized_array = normalize_scientific_notation_array_for_decimal(
                    array_for_decimal_normalization,
                    cast_type,
                )?;
                let array_to_cast = normalized_array
                    .as_ref()
                    .unwrap_or(array_for_decimal_normalization);
                Ok(ColumnarValue::Array(kernels::cast::cast_with_options(
                    array_to_cast,
                    cast_type,
                    &cast_options,
                )?))
            }
            ColumnarValue::Scalar(scalar) => {
                if is_disallowed_date_to_integer_cast(&scalar.data_type(), cast_type) {
                    return Err(DataFusionError::Execution(format!(
                        "Cannot cast {} to {cast_type}",
                        scalar.data_type()
                    )));
                }
                if let Some(casted_scalar) =
                    cast_string_scalar_to_temporal_with_pg_infinity(scalar, cast_type)
                {
                    return Ok(ColumnarValue::Scalar(casted_scalar));
                }
                if let Some(casted_scalar) =
                    cast_string_scalar_to_decimal_with_pg_nan(scalar, cast_type)
                {
                    return Ok(ColumnarValue::Scalar(casted_scalar));
                }
                if let Some(casted_scalar) =
                    cast_string_scalar_to_jsonb_binaryview(scalar, cast_type)?
                {
                    return Ok(ColumnarValue::Scalar(casted_scalar));
                }
                if let Some(casted_scalar) =
                    cast_jsonb_binaryview_scalar_to_text(scalar, cast_type)?
                {
                    return Ok(ColumnarValue::Scalar(casted_scalar));
                }
                let trimmed_integer_scalar =
                    normalize_string_scalar_for_integer_cast(scalar, cast_type);
                let scalar_for_decimal_normalization =
                    trimmed_integer_scalar.as_ref().unwrap_or(scalar);
                let normalized_scalar = normalize_scientific_notation_scalar_for_decimal(
                    scalar_for_decimal_normalization,
                    cast_type,
                );
                let scalar_to_cast = normalized_scalar
                    .as_ref()
                    .unwrap_or(scalar_for_decimal_normalization);
                Ok(ColumnarValue::Scalar(
                    scalar_to_cast.cast_to_with_options(cast_type, &cast_options)?,
                ))
            }
        }
    }
}

fn pg_timestamp_infinity_value(unit: &TimeUnit, positive: bool) -> i64 {
    let micros = if positive {
        PG_TS_POS_INFINITY_MICROS
    } else {
        PG_TS_NEG_INFINITY_MICROS
    };
    match unit {
        TimeUnit::Second => micros / 1_000_000,
        TimeUnit::Millisecond => micros / 1_000,
        TimeUnit::Microsecond => micros,
        TimeUnit::Nanosecond => micros,
    }
}

fn cast_string_scalar_to_temporal_with_pg_infinity(
    scalar: &ScalarValue,
    cast_type: &DataType,
) -> Option<ScalarValue> {
    let value = match scalar {
        ScalarValue::Utf8(Some(v)) => v,
        ScalarValue::LargeUtf8(Some(v)) => v,
        ScalarValue::Utf8View(Some(v)) => v,
        _ => return None,
    };

    let trimmed = value.trim();
    let positive = if trimmed.eq_ignore_ascii_case("infinity") {
        true
    } else if trimmed.eq_ignore_ascii_case("-infinity") {
        false
    } else {
        return None;
    };

    match cast_type {
        DataType::Date32 => Some(ScalarValue::Date32(Some(if positive {
            PG_DATE_POS_INFINITY_SENTINEL
        } else {
            PG_DATE_NEG_INFINITY_SENTINEL
        }))),
        DataType::Timestamp(unit, tz) => {
            let v = pg_timestamp_infinity_value(unit, positive);
            Some(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(Some(v), tz.clone()),
                TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(v), tz.clone())
                }
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(v), tz.clone())
                }
                TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(v), tz.clone())
                }
            })
        }
        _ => None,
    }
}

fn is_integer_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_date_data_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn is_disallowed_date_to_integer_cast(
    source_type: &DataType,
    cast_type: &DataType,
) -> bool {
    is_date_data_type(source_type) && is_integer_data_type(cast_type)
}

fn normalize_string_array_for_integer_cast(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<Option<ArrayRef>> {
    if !is_integer_data_type(cast_type) {
        return Ok(None);
    }

    let mut changed = false;
    let trimmed_values: Option<Vec<Option<String>>> = match array.data_type() {
        DataType::Utf8 => {
            let string_array =
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Expected StringArray but found {}",
                            array.data_type()
                        )
                    })?;
            Some(
                string_array
                    .iter()
                    .map(|value| {
                        value.map(|text| {
                            let trimmed = text.trim();
                            if trimmed.len() != text.len() {
                                changed = true;
                            }
                            trimmed.to_string()
                        })
                    })
                    .collect(),
            )
        }
        DataType::LargeUtf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected LargeStringArray but found {}",
                        array.data_type()
                    )
                })?;
            Some(
                string_array
                    .iter()
                    .map(|value| {
                        value.map(|text| {
                            let trimmed = text.trim();
                            if trimmed.len() != text.len() {
                                changed = true;
                            }
                            trimmed.to_string()
                        })
                    })
                    .collect(),
            )
        }
        DataType::Utf8View => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected StringViewArray but found {}",
                        array.data_type()
                    )
                })?;
            Some(
                string_array
                    .iter()
                    .map(|value| {
                        value.map(|text| {
                            let trimmed = text.trim();
                            if trimmed.len() != text.len() {
                                changed = true;
                            }
                            trimmed.to_string()
                        })
                    })
                    .collect(),
            )
        }
        _ => None,
    };

    if !changed {
        return Ok(None);
    }

    Ok(trimmed_values
        .map(StringArray::from)
        .map(Arc::new)
        .map(|array| array as ArrayRef))
}

fn normalize_string_scalar_for_integer_cast(
    scalar: &ScalarValue,
    cast_type: &DataType,
) -> Option<ScalarValue> {
    if !is_integer_data_type(cast_type) {
        return None;
    }

    match scalar {
        ScalarValue::Utf8(Some(value)) => {
            let trimmed = value.trim();
            if trimmed.len() == value.len() {
                None
            } else {
                Some(ScalarValue::Utf8(Some(trimmed.to_string())))
            }
        }
        ScalarValue::LargeUtf8(Some(value)) => {
            let trimmed = value.trim();
            if trimmed.len() == value.len() {
                None
            } else {
                Some(ScalarValue::LargeUtf8(Some(trimmed.to_string())))
            }
        }
        ScalarValue::Utf8View(Some(value)) => {
            let trimmed = value.trim();
            if trimmed.len() == value.len() {
                None
            } else {
                Some(ScalarValue::Utf8View(Some(trimmed.to_string())))
            }
        }
        _ => None,
    }
}

fn cast_string_array_to_jsonb_binaryview(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<Option<ArrayRef>> {
    if !matches!(cast_type, DataType::BinaryView) {
        return Ok(None);
    }

    let mut builder = BinaryViewBuilder::new();

    macro_rules! parse_json_values {
        ($arr:expr) => {{
            for value in $arr.iter() {
                match value {
                    None => builder.append_null(),
                    Some(text) => {
                        let parsed =
                            jsonb::parse_owned_jsonb(text.as_bytes()).map_err(|e| {
                                DataFusionError::Execution(format!("invalid JSON: {e}"))
                            })?;
                        builder.append_value(parsed.as_ref());
                    }
                }
            }
        }};
    }

    match array.data_type() {
        DataType::Utf8 => {
            let string_array =
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Expected StringArray but found {}",
                            array.data_type()
                        )
                    })?;
            parse_json_values!(string_array);
        }
        DataType::LargeUtf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected LargeStringArray but found {}",
                        array.data_type()
                    )
                })?;
            parse_json_values!(string_array);
        }
        DataType::Utf8View => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected StringViewArray but found {}",
                        array.data_type()
                    )
                })?;
            parse_json_values!(string_array);
        }
        _ => return Ok(None),
    }

    Ok(Some(Arc::new(builder.finish())))
}

fn cast_string_scalar_to_jsonb_binaryview(
    scalar: &ScalarValue,
    cast_type: &DataType,
) -> Result<Option<ScalarValue>> {
    if !matches!(cast_type, DataType::BinaryView) {
        return Ok(None);
    }

    let value = match scalar {
        ScalarValue::Utf8(Some(v)) => Some(v.as_str()),
        ScalarValue::LargeUtf8(Some(v)) => Some(v.as_str()),
        ScalarValue::Utf8View(Some(v)) => Some(v.as_str()),
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Null => return Ok(Some(ScalarValue::BinaryView(None))),
        _ => None,
    };

    let Some(value) = value else {
        return Ok(None);
    };

    let parsed = jsonb::parse_owned_jsonb(value.as_bytes())
        .map_err(|e| DataFusionError::Execution(format!("invalid JSON: {e}")))?;
    Ok(Some(ScalarValue::BinaryView(Some(
        parsed.as_ref().to_vec(),
    ))))
}

fn is_text_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
    )
}

fn cast_jsonb_binaryview_array_to_text(
    array: &ArrayRef,
    cast_type: &DataType,
    cast_options: &CastOptions<'static>,
) -> Result<Option<ArrayRef>> {
    if !is_text_data_type(cast_type) || !matches!(array.data_type(), DataType::BinaryView)
    {
        return Ok(None);
    }

    let binary_array = array
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .ok_or_else(|| {
            internal_datafusion_err!(
                "Expected BinaryViewArray but found {}",
                array.data_type()
            )
        })?;

    let mut text_values = Vec::with_capacity(binary_array.len());
    for value in binary_array.iter() {
        match value {
            None => text_values.push(None),
            Some(bytes) => {
                let owned = jsonb::OwnedJsonb::new(bytes.to_vec());
                owned.as_raw().type_of().map_err(|e| {
                    DataFusionError::Execution(format!("invalid JSONB value: {e}"))
                })?;
                text_values.push(Some(owned.as_raw().to_string()));
            }
        }
    }

    let text_array: ArrayRef = Arc::new(StringArray::from(text_values));
    if matches!(cast_type, DataType::Utf8) {
        return Ok(Some(text_array));
    }

    Ok(Some(kernels::cast::cast_with_options(
        text_array.as_ref(),
        cast_type,
        cast_options,
    )?))
}

fn cast_jsonb_binaryview_scalar_to_text(
    scalar: &ScalarValue,
    cast_type: &DataType,
) -> Result<Option<ScalarValue>> {
    if !is_text_data_type(cast_type) {
        return Ok(None);
    }

    let value = match scalar {
        ScalarValue::BinaryView(Some(bytes)) => {
            let owned = jsonb::OwnedJsonb::new(bytes.clone());
            owned.as_raw().type_of().map_err(|e| {
                DataFusionError::Execution(format!("invalid JSONB value: {e}"))
            })?;
            Some(owned.as_raw().to_string())
        }
        ScalarValue::BinaryView(None) | ScalarValue::Null => None,
        _ => return Ok(None),
    };

    let casted = match cast_type {
        DataType::Utf8 => ScalarValue::Utf8(value),
        DataType::Utf8View => ScalarValue::Utf8View(value),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(value),
        _ => return Ok(None),
    };
    Ok(Some(casted))
}

fn cast_string_array_to_decimal_with_pg_nan(
    array: &ArrayRef,
    cast_type: &DataType,
    cast_options: &CastOptions<'static>,
) -> Result<Option<ArrayRef>> {
    let DataType::Decimal128(precision, scale) = cast_type else {
        return Ok(None);
    };

    let len = array.len();
    let mut has_nan = false;
    let mut nan_positions = vec![false; len];
    let mut normalized = Vec::with_capacity(len);

    macro_rules! normalize_values {
        ($arr:expr) => {{
            for (idx, value) in $arr.iter().enumerate() {
                match value {
                    None => normalized.push(None),
                    Some(text) => {
                        if is_pg_numeric_nan_literal(text) {
                            has_nan = true;
                            nan_positions[idx] = true;
                            normalized.push(Some("0".to_string()));
                        } else if let Some(scientific) =
                            normalize_scientific_decimal_string(text)
                        {
                            normalized.push(Some(scientific));
                        } else {
                            normalized.push(Some(text.to_string()));
                        }
                    }
                }
            }
        }};
    }

    match array.data_type() {
        DataType::Utf8 => {
            let string_array =
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Expected StringArray but found {}",
                            array.data_type()
                        )
                    })?;
            normalize_values!(string_array);
        }
        DataType::LargeUtf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected LargeStringArray but found {}",
                        array.data_type()
                    )
                })?;
            normalize_values!(string_array);
        }
        DataType::Utf8View => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected StringViewArray but found {}",
                        array.data_type()
                    )
                })?;
            normalize_values!(string_array);
        }
        _ => return Ok(None),
    }

    if !has_nan {
        return Ok(None);
    }

    let normalized_array: ArrayRef = Arc::new(StringArray::from(normalized));
    let casted =
        kernels::cast::cast_with_options(&normalized_array, cast_type, cast_options)?;
    let decimal_array = casted
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected Decimal128Array after decimal cast")
        })?;

    let mut builder = Decimal128Builder::with_capacity(len)
        .with_precision_and_scale(*precision, *scale)
        .map_err(|e| {
            internal_datafusion_err!(
                "Failed to build Decimal128 with precision={}, scale={}: {e}",
                precision,
                scale
            )
        })?;

    for (idx, is_nan) in nan_positions.iter().enumerate() {
        if decimal_array.is_null(idx) {
            builder.append_null();
        } else if *is_nan {
            builder.append_value(PG_NUMERIC_NAN_DECIMAL128_SENTINEL);
        } else {
            builder.append_value(decimal_array.value(idx));
        }
    }

    Ok(Some(Arc::new(builder.finish())))
}

fn cast_string_scalar_to_decimal_with_pg_nan(
    scalar: &ScalarValue,
    cast_type: &DataType,
) -> Option<ScalarValue> {
    let DataType::Decimal128(precision, scale) = cast_type else {
        return None;
    };

    let value = match scalar {
        ScalarValue::Utf8(Some(v)) => v,
        ScalarValue::LargeUtf8(Some(v)) => v,
        ScalarValue::Utf8View(Some(v)) => v,
        _ => return None,
    };

    if is_pg_numeric_nan_literal(value) {
        Some(ScalarValue::Decimal128(
            Some(PG_NUMERIC_NAN_DECIMAL128_SENTINEL),
            *precision,
            *scale,
        ))
    } else {
        None
    }
}

fn normalize_scientific_notation_array_for_decimal(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<Option<ArrayRef>> {
    if !matches!(cast_type, DataType::Decimal128(_, _)) {
        return Ok(None);
    }

    let mut changed = false;
    let normalized_values: Option<Vec<Option<String>>> = match array.data_type() {
        DataType::Utf8 => {
            let string_array =
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Expected StringArray but found {}",
                            array.data_type()
                        )
                    })?;
            Some(
                string_array
                    .iter()
                    .map(|v| {
                        v.map(|s| match normalize_scientific_decimal_string(s) {
                            Some(normalized) => {
                                changed = true;
                                normalized
                            }
                            None => s.to_string(),
                        })
                    })
                    .collect(),
            )
        }
        DataType::LargeUtf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected LargeStringArray but found {}",
                        array.data_type()
                    )
                })?;
            Some(
                string_array
                    .iter()
                    .map(|v| {
                        v.map(|s| match normalize_scientific_decimal_string(s) {
                            Some(normalized) => {
                                changed = true;
                                normalized
                            }
                            None => s.to_string(),
                        })
                    })
                    .collect(),
            )
        }
        DataType::Utf8View => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected StringViewArray but found {}",
                        array.data_type()
                    )
                })?;
            Some(
                string_array
                    .iter()
                    .map(|v| {
                        v.map(|s| match normalize_scientific_decimal_string(s) {
                            Some(normalized) => {
                                changed = true;
                                normalized
                            }
                            None => s.to_string(),
                        })
                    })
                    .collect(),
            )
        }
        _ => None,
    };

    if !changed {
        return Ok(None);
    }

    let normalized = normalized_values
        .map(StringArray::from)
        .map(Arc::new)
        .map(|a| a as ArrayRef);
    Ok(normalized)
}

fn normalize_scientific_notation_scalar_for_decimal(
    scalar: &ScalarValue,
    cast_type: &DataType,
) -> Option<ScalarValue> {
    if !matches!(cast_type, DataType::Decimal128(_, _)) {
        return None;
    }

    match scalar {
        ScalarValue::Utf8(Some(v)) => normalize_scientific_decimal_string(v)
            .map(|normalized| ScalarValue::Utf8(Some(normalized))),
        ScalarValue::LargeUtf8(Some(v)) => normalize_scientific_decimal_string(v)
            .map(|normalized| ScalarValue::LargeUtf8(Some(normalized))),
        ScalarValue::Utf8View(Some(v)) => normalize_scientific_decimal_string(v)
            .map(|normalized| ScalarValue::Utf8View(Some(normalized))),
        _ => None,
    }
}

fn normalize_scientific_decimal_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let exponent_idx = trimmed.find(|c: char| c == 'e' || c == 'E')?;

    // Reject multiple exponent markers and let Arrow report the cast error.
    if trimmed[exponent_idx + 1..]
        .find(|c: char| c == 'e' || c == 'E')
        .is_some()
    {
        return None;
    }

    let (mantissa, exponent_part) = trimmed.split_at(exponent_idx);
    let exponent: i32 = exponent_part[1..].parse().ok()?;

    let (sign, unsigned_mantissa) = if let Some(rest) = mantissa.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = mantissa.strip_prefix('+') {
        ("", rest)
    } else {
        ("", mantissa)
    };

    let mut parts = unsigned_mantissa.split('.');
    let int_part = parts.next().unwrap_or_default();
    let frac_part = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return None;
    }
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);

    let decimal_pos = i32::try_from(int_part.len()).ok()? + exponent;
    if decimal_pos.unsigned_abs() > 1_024 {
        // Avoid building very large intermediates for values that will overflow anyway.
        return None;
    }

    let mut plain = String::new();
    if decimal_pos <= 0 {
        plain.push_str("0.");
        plain.push_str(&"0".repeat((-decimal_pos) as usize));
        plain.push_str(&digits);
    } else if decimal_pos as usize >= digits.len() {
        plain.push_str(&digits);
        plain.push_str(&"0".repeat(decimal_pos as usize - digits.len()));
    } else {
        plain.push_str(&digits[..decimal_pos as usize]);
        plain.push('.');
        plain.push_str(&digits[decimal_pos as usize..]);
    }

    let normalized = normalize_plain_decimal_string(&plain);
    if normalized == "0" {
        Some(normalized)
    } else {
        Some(format!("{sign}{normalized}"))
    }
}

fn normalize_plain_decimal_string(value: &str) -> String {
    let (int_part, frac_part) = match value.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (value, ""),
    };

    let normalized_int = int_part.trim_start_matches('0');
    let normalized_int = if normalized_int.is_empty() {
        "0"
    } else {
        normalized_int
    };

    let normalized_frac = frac_part.trim_end_matches('0');
    if normalized_frac.is_empty() {
        normalized_int.to_string()
    } else {
        format!("{normalized_int}.{normalized_frac}")
    }
}

fn is_pg_numeric_nan_literal(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("nan")
}

fn ensure_date_array_timestamp_bounds(
    array: &ArrayRef,
    cast_type: &DataType,
) -> Result<()> {
    let source_type = array.data_type().clone();
    let Some(multiplier) = date_to_timestamp_multiplier(&source_type, cast_type) else {
        return Ok(());
    };

    if multiplier <= 1 {
        return Ok(());
    }

    // Use compute kernels to find min/max instead of iterating all elements
    let (min_val, max_val): (Option<i64>, Option<i64>) = match &source_type {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected Date32Array but found {}",
                        array.data_type()
                    )
                })?;
            (min(arr).map(|v| v as i64), max(arr).map(|v| v as i64))
        }
        DataType::Date64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Expected Date64Array but found {}",
                        array.data_type()
                    )
                })?;
            (min(arr), max(arr))
        }
        _ => return Ok(()), // Not a date type, nothing to do
    };

    // Only validate the min and max values instead of all elements
    if let Some(min) = min_val {
        ensure_timestamp_in_bounds(min, multiplier, &source_type, cast_type)?;
    }
    if let Some(max) = max_val {
        ensure_timestamp_in_bounds(max, multiplier, &source_type, cast_type)?;
    }

    Ok(())
}

// Implement Display trait for ColumnarValue
impl fmt::Display for ColumnarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let formatted = match self {
            ColumnarValue::Array(array) => {
                pretty_format_columns("ColumnarValue(ArrayRef)", &[Arc::clone(array)])
            }
            ColumnarValue::Scalar(_) => {
                if let Ok(array) = self.to_array(1) {
                    pretty_format_columns("ColumnarValue(ScalarValue)", &[array])
                } else {
                    return write!(f, "Error formatting columnar value");
                }
            }
        };

        if let Ok(formatted) = formatted {
            write!(f, "{formatted}")
        } else {
            write!(f, "Error formatting columnar value")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Date64Array, Decimal128Array, Int32Array, StringArray},
        datatypes::TimeUnit,
    };

    #[test]
    fn into_array_of_size() {
        // Array case
        let arr = make_array(1, 3);
        let arr_columnar_value = ColumnarValue::Array(Arc::clone(&arr));
        assert_eq!(&arr_columnar_value.into_array_of_size(3).unwrap(), &arr);

        // Scalar case
        let scalar_columnar_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));
        let expected_array = make_array(42, 100);
        assert_eq!(
            &scalar_columnar_value.into_array_of_size(100).unwrap(),
            &expected_array
        );

        // Array case with wrong size
        let arr = make_array(1, 3);
        let arr_columnar_value = ColumnarValue::Array(Arc::clone(&arr));
        let result = arr_columnar_value.into_array_of_size(5);
        let err = result.unwrap_err();
        assert!(
            err.to_string().starts_with(
                "Internal error: Array length 3 does not match expected length 5"
            ),
            "Found: {err}"
        );
    }

    #[test]
    fn values_to_arrays() {
        // (input, expected)
        let cases = vec![
            // empty
            TestCase {
                input: vec![],
                expected: vec![],
            },
            // one array of length 3
            TestCase {
                input: vec![ColumnarValue::Array(make_array(1, 3))],
                expected: vec![make_array(1, 3)],
            },
            // two arrays length 3
            TestCase {
                input: vec![
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Array(make_array(2, 3)),
                ],
                expected: vec![make_array(1, 3), make_array(2, 3)],
            },
            // array and scalar
            TestCase {
                input: vec![
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                ],
                expected: vec![
                    make_array(1, 3),
                    make_array(100, 3), // scalar is expanded
                ],
            },
            // scalar and array
            TestCase {
                input: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Array(make_array(1, 3)),
                ],
                expected: vec![
                    make_array(100, 3), // scalar is expanded
                    make_array(1, 3),
                ],
            },
            // multiple scalars and array
            TestCase {
                input: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(200))),
                ],
                expected: vec![
                    make_array(100, 3), // scalar is expanded
                    make_array(1, 3),
                    make_array(200, 3), // scalar is expanded
                ],
            },
        ];
        for case in cases {
            case.run();
        }
    }

    #[test]
    #[should_panic(
        expected = "Arguments has mixed length. Expected length: 3, found length: 4"
    )]
    fn values_to_arrays_mixed_length() {
        ColumnarValue::values_to_arrays(&[
            ColumnarValue::Array(make_array(1, 3)),
            ColumnarValue::Array(make_array(2, 4)),
        ])
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Arguments has mixed length. Expected length: 3, found length: 7"
    )]
    fn values_to_arrays_mixed_length_and_scalar() {
        ColumnarValue::values_to_arrays(&[
            ColumnarValue::Array(make_array(1, 3)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
            ColumnarValue::Array(make_array(2, 7)),
        ])
        .unwrap();
    }

    struct TestCase {
        input: Vec<ColumnarValue>,
        expected: Vec<ArrayRef>,
    }

    impl TestCase {
        fn run(self) {
            let Self { input, expected } = self;

            assert_eq!(
                ColumnarValue::values_to_arrays(&input).unwrap(),
                expected,
                "\ninput: {input:?}\nexpected: {expected:?}"
            );
        }
    }

    /// Makes an array of length `len` with all elements set to `val`
    fn make_array(val: i32, len: usize) -> ArrayRef {
        Arc::new(Int32Array::from(vec![val; len]))
    }

    #[test]
    fn test_display_scalar() {
        let column = ColumnarValue::from(ScalarValue::from("foo"));
        assert_eq!(
            column.to_string(),
            concat!(
                "+----------------------------+\n",
                "| ColumnarValue(ScalarValue) |\n",
                "+----------------------------+\n",
                "| foo                        |\n",
                "+----------------------------+"
            )
        );
    }

    #[test]
    fn test_display_array() {
        let array: ArrayRef = Arc::new(Int32Array::from_iter_values(vec![1, 2, 3]));
        let column = ColumnarValue::from(array);
        assert_eq!(
            column.to_string(),
            concat!(
                "+-------------------------+\n",
                "| ColumnarValue(ArrayRef) |\n",
                "+-------------------------+\n",
                "| 1                       |\n",
                "| 2                       |\n",
                "| 3                       |\n",
                "+-------------------------+"
            )
        );
    }

    #[test]
    fn cast_date64_array_to_timestamp_overflow() {
        let overflow_value = i64::MAX / 1_000_000 + 1;
        let array: ArrayRef = Arc::new(Date64Array::from(vec![Some(overflow_value)]));
        let value = ColumnarValue::Array(array);
        let result =
            value.cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None), None);
        let err = result.expect_err("expected overflow to be detected");
        assert!(
            err.to_string()
                .contains("converted value exceeds the representable i64 range"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn cast_scientific_string_scalar_to_decimal128() {
        let value = ColumnarValue::Scalar(ScalarValue::Utf8(Some("1.5e3".to_string())));
        let casted = value
            .cast_to(&DataType::Decimal128(38, 10), None)
            .expect("scientific notation cast should succeed");

        match casted {
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(v), p, s)) => {
                assert_eq!(p, 38);
                assert_eq!(s, 10);
                assert_eq!(v, 1_500_i128 * 10_i128.pow(10));
            }
            other => panic!("unexpected cast result: {other:?}"),
        }
    }

    #[test]
    fn cast_scientific_string_array_to_decimal128() {
        let array: ArrayRef =
            Arc::new(StringArray::from(vec![Some("2.5E2"), Some("3.0"), None]));
        let value = ColumnarValue::Array(array);
        let casted = value
            .cast_to(&DataType::Decimal128(20, 2), None)
            .expect("scientific notation cast should succeed");

        match casted {
            ColumnarValue::Array(array) => {
                let decimal_array = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("expected Decimal128Array");
                assert_eq!(decimal_array.value(0), 25_000);
                assert_eq!(decimal_array.value(1), 300);
                assert!(decimal_array.is_null(2));
            }
            other => panic!("unexpected cast result: {other:?}"),
        }
    }

    #[test]
    fn cast_nan_string_scalar_to_decimal128() {
        let value = ColumnarValue::Scalar(ScalarValue::Utf8(Some("NaN".to_string())));
        let casted = value
            .cast_to(&DataType::Decimal128(38, 10), None)
            .expect("NaN cast should succeed");

        match casted {
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(v), p, s)) => {
                assert_eq!(p, 38);
                assert_eq!(s, 10);
                assert_eq!(v, PG_NUMERIC_NAN_DECIMAL128_SENTINEL);
            }
            other => panic!("unexpected cast result: {other:?}"),
        }
    }

    #[test]
    fn cast_nan_string_array_to_decimal128() {
        let array: ArrayRef =
            Arc::new(StringArray::from(vec![Some("NaN"), Some("1.25"), None]));
        let value = ColumnarValue::Array(array);
        let casted = value
            .cast_to(&DataType::Decimal128(20, 2), None)
            .expect("NaN cast should succeed");

        match casted {
            ColumnarValue::Array(array) => {
                let decimal_array = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("expected Decimal128Array");
                assert_eq!(decimal_array.value(0), PG_NUMERIC_NAN_DECIMAL128_SENTINEL);
                assert_eq!(decimal_array.value(1), 125);
                assert!(decimal_array.is_null(2));
            }
            other => panic!("unexpected cast result: {other:?}"),
        }
    }
}
