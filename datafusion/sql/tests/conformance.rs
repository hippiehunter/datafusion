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

//! SQL:2016 Conformance Test Framework
//!
//! This module provides test infrastructure for tracking DataFusion's conformance
//! to the ISO/IEC 9075:2016 SQL standard (SQL:2016).
//!
//! # Organization
//!
//! Tests are organized by SQL standard parts:
//! - Part 2: SQL/Foundation (Core SQL)
//! - Part 3: SQL/CLI (Call-Level Interface)
//! - Part 4: SQL/PSM (Persistent Stored Modules)
//! - Part 9: SQL/MED (Management of External Data)
//! - Part 11: SQL/Schemata (Information Schema)
//! - Part 14: SQL/XML (XML-related specifications)
//!
//! # Feature IDs
//!
//! Each test maps to a SQL standard feature ID:
//! - E-series: Core features (E011, E021, etc.)
//! - F-series: Extended features (F031, F041, etc.)
//! - S-series: Object-related features
//! - T-series: Temporal and other features
//!
//! # Test Macros
//!
//! - `assert_parses!` - Verify SQL parses without error
//! - `assert_plans!` - Verify SQL converts to logical plan
//! - `assert_not_implemented!` - Mark feature as not yet implemented
//! - `assert_feature_supported!` - Verify a feature works end-to-end

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::*;
use datafusion_common::config::ConfigOptions;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{DFSchema, GetExt, Result, TableReference, plan_err, not_impl_err};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, PartitionEvaluator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TableSource, TypeSignature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion_expr::planner::ExprPlanner;
use datafusion_common::ScalarValue;
use datafusion_expr::function::PartitionEvaluatorArgs;
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::{ContextProvider, ParserOptions, SqlToRel};
use sqlparser::dialect::{Dialect, GenericDialect, MsSqlDialect, PostgreSqlDialect};

// Import aggregate function stubs from datafusion_expr::test::function_stub
use datafusion_expr::test::function_stub::{
    avg_udaf, count_udaf, max_udaf, min_udaf, sum_udaf,
};

// Re-export submodules for each standard part
pub mod part2_foundation;
pub mod part4_psm;

// ============================================================================
// SQL:2016 Required Functions for Conformance
// ============================================================================

/// SQL:2016 Core aggregate functions required for conformance.
///
/// These are the aggregate functions that must be provided for a SQL implementation
/// to be considered SQL:2016 Core conformant.
pub const REQUIRED_AGGREGATE_FUNCTIONS: &[&str] = &[
    // E091 Set Functions (Core)
    "count",
    "sum",
    "avg",
    "min",
    "max",
    // Statistical aggregates (T621)
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    // Bit aggregates
    "bit_and",
    "bit_or",
    "bit_xor",
    // Array/list aggregates
    "array_agg",
    "listagg",
    "string_agg",
    // JSON aggregates (T8xx)
    "json_arrayagg",
];

/// SQL:2016 Core scalar functions required for conformance.
///
/// Downstream users must provide implementations for all these functions.
pub const REQUIRED_SCALAR_FUNCTIONS: &[&str] = &[
    // String functions (E021)
    "upper",
    "lower",
    "substring",
    "trim",
    "ltrim",
    "rtrim",
    "btrim",
    "position",
    "character_length",
    "char_length",
    "octet_length",
    "bit_length",
    "concat",
    "left",
    "right",
    "replace",
    "reverse",
    "repeat",
    "lpad",
    "rpad",
    "split_part",
    "initcap",
    "ascii",
    "chr",
    "char",
    "space",
    "translate",
    "overlay",
    // Null handling (E131)
    "coalesce",
    "nullif",
    "ifnull",
    "nvl",
    // Numeric functions (T621)
    "abs",
    "mod",
    "ceil",
    "ceiling",
    "floor",
    "round",
    "trunc",
    "truncate",
    "power",
    "sqrt",
    "exp",
    "ln",
    "log",
    "log10",
    "log2",
    "sign",
    "degrees",
    "radians",
    "pi",
    "random",
    "width_bucket",
    // Trigonometric functions
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    "sinh",
    "cosh",
    "tanh",
    // Comparison functions
    "greatest",
    "least",
    // Date/time functions (F051)
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
    "extract",
    "date_part",
    "date_add",
    "date_sub",
    "date_trunc",
    // Array functions (S091)
    "array",
    "array_append",
    "array_prepend",
    "array_remove",
    "array_replace",
    "array_distinct",
    "array_intersect",
    "array_union",
    "array_except",
    "array_position",
    "trim_array",
    "cardinality",
    // Row constructor
    "row",
    // Regex functions (F421)
    "regexp_like",
    "regexp_replace",
    "regexp_substr",
    // JSON functions (T8xx)
    "json_array",
    "json_exists",
    "json_query",
    "json_value",
    // Bit operations
    "bit_and",
    "bit_or",
    "bit_xor",
    // Misc
    "to_hex",
    "starts_with",
    "ends_with",
];

/// SQL:2016 Core window functions required for conformance.
///
/// Downstream users must provide implementations for all these functions.
pub const REQUIRED_WINDOW_FUNCTIONS: &[&str] = &[
    // T611 Window functions
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lead",
    "lag",
    "first_value",
    "last_value",
    "nth_value",
];

// ============================================================================
// ConformanceFunctionProvider Trait
// ============================================================================

/// Trait that downstream users must implement to provide SQL:2016 conformant functions.
///
/// Implementing this trait ensures your SQL implementation has all the required
/// built-in functions for SQL:2016 Core conformance. The conformance test suite
/// uses this trait to wire up function registration.
///
/// # Example
///
/// ```ignore
/// struct MyFunctionProvider;
///
/// impl ConformanceFunctionProvider for MyFunctionProvider {
///     fn get_aggregate_function(&self, name: &str) -> Option<Arc<AggregateUDF>> {
///         match name {
///             "count" => Some(my_count_udaf()),
///             "sum" => Some(my_sum_udaf()),
///             // ... etc
///             _ => None,
///         }
///     }
///     // ... implement other methods
/// }
/// ```
///
/// # Conformance
///
/// To achieve SQL:2016 conformant SQL→LogicalPlan transformation, implement this
/// trait with your function implementations. See `DataFusionFunctionProvider` as
/// a reference implementation. Use `validate_required_functions()` to verify
/// you've provided all required functions.
pub trait ConformanceFunctionProvider: Send + Sync {
    /// Get an aggregate function by name (COUNT, SUM, AVG, MIN, MAX, etc.)
    fn get_aggregate_function(&self, name: &str) -> Option<Arc<AggregateUDF>>;

    /// Get a scalar function by name (UPPER, LOWER, COALESCE, ABS, etc.)
    fn get_scalar_function(&self, name: &str) -> Option<Arc<ScalarUDF>>;

    /// Get a window function by name (ROW_NUMBER, RANK, LEAD, LAG, etc.)
    fn get_window_function(&self, name: &str) -> Option<Arc<WindowUDF>>;

    /// Validate that all required functions are provided.
    /// Returns a list of missing function names, or empty if all are present.
    fn validate_required_functions(&self) -> Vec<String> {
        let mut missing = Vec::new();

        for name in REQUIRED_AGGREGATE_FUNCTIONS {
            if self.get_aggregate_function(name).is_none() {
                missing.push(format!("aggregate:{}", name));
            }
        }
        for name in REQUIRED_SCALAR_FUNCTIONS {
            if self.get_scalar_function(name).is_none() {
                missing.push(format!("scalar:{}", name));
            }
        }
        for name in REQUIRED_WINDOW_FUNCTIONS {
            if self.get_window_function(name).is_none() {
                missing.push(format!("window:{}", name));
            }
        }

        missing
    }
}

// ============================================================================
// Scalar Function Stubs for Conformance Testing
// ============================================================================

/// Macro to create a stub scalar function.
macro_rules! stub_scalar_udf {
    ($name:ident, $fn_name:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    signature: Signature::variadic_any(Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                // Return Utf8 for string functions, Float64 for numeric, etc.
                // This is a stub - downstream should provide real implementations.
                Ok(DataType::Utf8)
            }

            fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                not_impl_err!("stub function {} should not be invoked", $fn_name)
            }
        }

        paste::paste! {
            pub fn [<$name:snake _udf>]() -> Arc<ScalarUDF> {
                static INSTANCE: std::sync::LazyLock<Arc<ScalarUDF>> =
                    std::sync::LazyLock::new(|| Arc::new(ScalarUDF::from($name::default())));
                Arc::clone(&INSTANCE)
            }
        }
    };
}

// String functions (E021)
stub_scalar_udf!(Upper, "upper");
stub_scalar_udf!(Lower, "lower");
stub_scalar_udf!(Substring, "substring");
stub_scalar_udf!(Trim, "trim");
stub_scalar_udf!(Ltrim, "ltrim");
stub_scalar_udf!(Rtrim, "rtrim");
stub_scalar_udf!(Position, "position");
stub_scalar_udf!(CharacterLength, "character_length");
stub_scalar_udf!(CharLength, "char_length");
stub_scalar_udf!(Length, "length");
stub_scalar_udf!(OctetLength, "octet_length");
stub_scalar_udf!(Strpos, "strpos");
stub_scalar_udf!(Concat, "concat");
stub_scalar_udf!(Substr, "substr");

// Null handling (E131)
stub_scalar_udf!(Coalesce, "coalesce");
stub_scalar_udf!(Nullif, "nullif");

// Numeric functions (T621)
stub_scalar_udf!(Abs, "abs");
stub_scalar_udf!(Mod, "mod");
stub_scalar_udf!(Ceil, "ceil");
stub_scalar_udf!(Ceiling, "ceiling");
stub_scalar_udf!(Floor, "floor");
stub_scalar_udf!(Round, "round");
stub_scalar_udf!(Power, "power");
stub_scalar_udf!(Sqrt, "sqrt");
stub_scalar_udf!(Exp, "exp");
stub_scalar_udf!(Ln, "ln");
stub_scalar_udf!(Log, "log");
stub_scalar_udf!(Log10, "log10");
stub_scalar_udf!(Sign, "sign");
stub_scalar_udf!(Trunc, "trunc");
stub_scalar_udf!(Truncate, "truncate");

// Date/time functions (F051)
// Note: CurrentDate, CurrentTime, CurrentTimestamp, LocalTime, LocalTimestamp, and Now
// are defined as custom implementations below with nullary signatures.
stub_scalar_udf!(Extract, "extract");
stub_scalar_udf!(DatePart, "date_part");

/// Macro to create a stub scalar function with nullary signature for datetime functions.
/// These functions can be called without arguments: CURRENT_DATE(), CURRENT_TIME(), etc.
macro_rules! stub_nullary_datetime_udf {
    ($name:ident, $fn_name:expr, $return_type:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    signature: Signature::nullary(Volatility::Stable),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                not_impl_err!("stub function {} should not be invoked", $fn_name)
            }
        }

        paste::paste! {
            pub fn [<$name:snake _udf>]() -> Arc<ScalarUDF> {
                static INSTANCE: std::sync::LazyLock<Arc<ScalarUDF>> =
                    std::sync::LazyLock::new(|| Arc::new(ScalarUDF::from($name::default())));
                Arc::clone(&INSTANCE)
            }
        }
    };
}

/// Macro to create a stub datetime function that supports optional precision argument.
/// These functions can be called as CURRENT_TIME() or CURRENT_TIME(3).
/// Precision is specified as Int64.
macro_rules! stub_optional_precision_datetime_udf {
    ($name:ident, $fn_name:expr, $return_type:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    signature: Signature::one_of(
                        vec![
                            TypeSignature::Nullary,
                            TypeSignature::Exact(vec![DataType::Int64]),
                        ],
                        Volatility::Stable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                not_impl_err!("stub function {} should not be invoked", $fn_name)
            }
        }

        paste::paste! {
            pub fn [<$name:snake _udf>]() -> Arc<ScalarUDF> {
                static INSTANCE: std::sync::LazyLock<Arc<ScalarUDF>> =
                    std::sync::LazyLock::new(|| Arc::new(ScalarUDF::from($name::default())));
                Arc::clone(&INSTANCE)
            }
        }
    };
}

// Nullary datetime functions - can be called as CURRENT_DATE(), NOW(), etc.
stub_nullary_datetime_udf!(CurrentDate, "current_date", DataType::Date32);
stub_nullary_datetime_udf!(Now, "now", DataType::Timestamp(TimeUnit::Nanosecond, None));

// Datetime functions with optional precision - can be called as CURRENT_TIME() or CURRENT_TIME(3)
stub_optional_precision_datetime_udf!(CurrentTime, "current_time", DataType::Time64(TimeUnit::Nanosecond));
stub_optional_precision_datetime_udf!(CurrentTimestamp, "current_timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None));

// CASE and conditional (F261)
stub_scalar_udf!(Case, "case");

// Additional common functions
stub_scalar_udf!(Left, "left");
stub_scalar_udf!(Right, "right");
stub_scalar_udf!(Replace, "replace");
stub_scalar_udf!(Reverse, "reverse");
stub_scalar_udf!(Repeat, "repeat");
stub_scalar_udf!(Lpad, "lpad");
stub_scalar_udf!(Rpad, "rpad");
stub_scalar_udf!(SplitPart, "split_part");
stub_scalar_udf!(StartsWith, "starts_with");
stub_scalar_udf!(EndsWith, "ends_with");
stub_scalar_udf!(Btrim, "btrim");
stub_scalar_udf!(Initcap, "initcap");
stub_scalar_udf!(Ascii, "ascii");
stub_scalar_udf!(Chr, "chr");
stub_scalar_udf!(Translate, "translate");
stub_scalar_udf!(ToHex, "to_hex");
stub_scalar_udf!(Overlay, "overlay");

// Date/time extended functions
stub_scalar_udf!(DateAdd, "date_add");
stub_scalar_udf!(DateSub, "date_sub");
stub_scalar_udf!(DateTrunc, "date_trunc");
// LocalTime and LocalTimestamp support optional precision argument
stub_optional_precision_datetime_udf!(LocalTime, "localtime", DataType::Time64(TimeUnit::Nanosecond));
stub_optional_precision_datetime_udf!(LocalTimestamp, "localtimestamp", DataType::Timestamp(TimeUnit::Nanosecond, None));

// Trigonometric functions
stub_scalar_udf!(Sin, "sin");
stub_scalar_udf!(Cos, "cos");
stub_scalar_udf!(Tan, "tan");
stub_scalar_udf!(Asin, "asin");
stub_scalar_udf!(Acos, "acos");
stub_scalar_udf!(Atan, "atan");
stub_scalar_udf!(Atan2, "atan2");
stub_scalar_udf!(Sinh, "sinh");
stub_scalar_udf!(Cosh, "cosh");
stub_scalar_udf!(Tanh, "tanh");

// More math functions
stub_scalar_udf!(Degrees, "degrees");
stub_scalar_udf!(Radians, "radians");
stub_scalar_udf!(Log2, "log2");
stub_scalar_udf!(WidthBucket, "width_bucket");
stub_scalar_udf!(Random, "random");
stub_scalar_udf!(Rand, "rand");
stub_scalar_udf!(Pi, "pi");

// Comparison functions
stub_scalar_udf!(Greatest, "greatest");
stub_scalar_udf!(Least, "least");

// Bit operations (aggregate-like but can be scalar too)
stub_scalar_udf!(BitAnd, "bit_and");
stub_scalar_udf!(BitOr, "bit_or");
stub_scalar_udf!(BitXor, "bit_xor");
stub_scalar_udf!(BitLength, "bit_length");

// Array functions
stub_scalar_udf!(Array, "array");
stub_scalar_udf!(ArrayAppend, "array_append");
stub_scalar_udf!(ArrayDistinct, "array_distinct");
stub_scalar_udf!(ArrayExcept, "array_except");
stub_scalar_udf!(ArrayIntersect, "array_intersect");
stub_scalar_udf!(ArrayPosition, "array_position");
stub_scalar_udf!(ArrayPrepend, "array_prepend");
stub_scalar_udf!(ArrayRemove, "array_remove");
stub_scalar_udf!(ArrayReplace, "array_replace");
stub_scalar_udf!(ArrayUnion, "array_union");
stub_scalar_udf!(ArraySlice, "array_slice");
stub_scalar_udf!(ArrayElement, "array_element");
stub_scalar_udf!(TrimArray, "trim_array");
stub_scalar_udf!(Cardinality, "cardinality");
stub_scalar_udf!(GetField, "get_field");

// MakeArray - special stub for array literal construction
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeArray {
    signature: Signature,
}

impl Default for MakeArray {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "make_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Return List type based on first argument type, or Null for empty arrays
        if arg_types.is_empty() {
            Ok(DataType::List(Arc::new(Field::new_list_field(DataType::Null, true))))
        } else {
            Ok(DataType::List(Arc::new(Field::new_list_field(arg_types[0].clone(), true))))
        }
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("stub function make_array should not be invoked")
    }
}

pub fn make_array_udf() -> Arc<ScalarUDF> {
    static INSTANCE: std::sync::LazyLock<Arc<ScalarUDF>> =
        std::sync::LazyLock::new(|| Arc::new(ScalarUDF::from(MakeArray::default())));
    Arc::clone(&INSTANCE)
}

// Row constructor - supports zero or more arguments
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RowConstructor {
    signature: Signature,
}

impl Default for RowConstructor {
    fn default() -> Self {
        // Use OneOf with Nullary for zero args and VariadicAny for 1+ args
        // This allows ROW() (empty) as well as ROW(a, b, c)
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RowConstructor {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "row"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return Struct type for row constructor
        Ok(DataType::Struct(Fields::empty()))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("stub function row should not be invoked")
    }
}

pub fn row_constructor_udf() -> Arc<ScalarUDF> {
    static INSTANCE: std::sync::LazyLock<Arc<ScalarUDF>> =
        std::sync::LazyLock::new(|| Arc::new(ScalarUDF::from(RowConstructor::default())));
    Arc::clone(&INSTANCE)
}

// Null handling extended
stub_scalar_udf!(Ifnull, "ifnull");
stub_scalar_udf!(Nvl, "nvl");

// String extended
stub_scalar_udf!(Char, "char");
stub_scalar_udf!(Space, "space");

// Regex functions
stub_scalar_udf!(RegexpLike, "regexp_like");
stub_scalar_udf!(RegexpReplace, "regexp_replace");
stub_scalar_udf!(RegexpSubstr, "regexp_substr");

// JSON functions
stub_scalar_udf!(JsonArray, "json_array");
stub_scalar_udf!(JsonExists, "json_exists");
stub_scalar_udf!(JsonQuery, "json_query");
stub_scalar_udf!(JsonValue, "json_value");
stub_scalar_udf!(JsonObject, "json_object");

// ============================================================================
// Additional Aggregate Function Stubs
// ============================================================================

/// Macro to create a stub aggregate function (beyond the basic ones from function_stub).
macro_rules! stub_aggregate_udf {
    ($name:ident, $fn_name:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    signature: Signature::variadic_any(Volatility::Immutable),
                }
            }
        }

        impl datafusion_expr::AggregateUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Float64)
            }

            fn accumulator(
                &self,
                _args: datafusion_expr::function::AccumulatorArgs,
            ) -> Result<Box<dyn datafusion_expr::Accumulator>> {
                not_impl_err!("stub aggregate {} should not be invoked", $fn_name)
            }

            fn state_fields(
                &self,
                _args: datafusion_expr::function::StateFieldsArgs,
            ) -> Result<Vec<Arc<Field>>> {
                not_impl_err!("stub aggregate {} should not have state_fields", $fn_name)
            }
        }

        paste::paste! {
            pub fn [<$name:snake _udaf>]() -> Arc<AggregateUDF> {
                static INSTANCE: std::sync::LazyLock<Arc<AggregateUDF>> =
                    std::sync::LazyLock::new(|| Arc::new(AggregateUDF::from($name::default())));
                Arc::clone(&INSTANCE)
            }
        }
    };
}

// Statistical aggregate functions
stub_aggregate_udf!(Stddev, "stddev");
stub_aggregate_udf!(StddevPop, "stddev_pop");
stub_aggregate_udf!(StddevSamp, "stddev_samp");
stub_aggregate_udf!(Variance, "variance");
stub_aggregate_udf!(VarPop, "var_pop");
stub_aggregate_udf!(VarSamp, "var_samp");

// Bit aggregate functions
stub_aggregate_udf!(BitAndAgg, "bit_and_agg");
stub_aggregate_udf!(BitOrAgg, "bit_or_agg");
stub_aggregate_udf!(BitXorAgg, "bit_xor_agg");

// Array/list aggregate functions
stub_aggregate_udf!(ArrayAgg, "array_agg");
stub_aggregate_udf!(ListAgg, "listagg");
stub_aggregate_udf!(StringAgg, "string_agg");

// JSON aggregate functions
stub_aggregate_udf!(JsonArrayAgg, "json_arrayagg");
stub_aggregate_udf!(JsonObjectAgg, "json_objectagg");

// ============================================================================
// Window Function Stubs for Conformance Testing
// ============================================================================

/// Macro to create a stub window function.
macro_rules! stub_window_udf {
    // Variant for zero-argument window functions (ROW_NUMBER, RANK, etc.)
    ($name:ident, $fn_name:expr, zero_arg) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    signature: Signature::nullary(Volatility::Immutable),
                }
            }
        }

        impl WindowUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn partition_evaluator(
                &self,
                _partition_evaluator_args: PartitionEvaluatorArgs,
            ) -> Result<Box<dyn PartitionEvaluator>> {
                not_impl_err!("stub window function {} should not be invoked", $fn_name)
            }

            fn field(&self, _field_args: datafusion_expr::function::WindowUDFFieldArgs) -> Result<Arc<Field>> {
                Ok(Arc::new(Field::new($fn_name, DataType::Int64, true)))
            }
        }

        paste::paste! {
            pub fn [<$name:snake _udwf>]() -> Arc<WindowUDF> {
                static INSTANCE: std::sync::LazyLock<Arc<WindowUDF>> =
                    std::sync::LazyLock::new(|| Arc::new(WindowUDF::from($name::default())));
                Arc::clone(&INSTANCE)
            }
        }
    };
    // Default variant for variadic window functions
    ($name:ident, $fn_name:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    signature: Signature::variadic_any(Volatility::Immutable),
                }
            }
        }

        impl WindowUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn partition_evaluator(
                &self,
                _partition_evaluator_args: PartitionEvaluatorArgs,
            ) -> Result<Box<dyn PartitionEvaluator>> {
                not_impl_err!("stub window function {} should not be invoked", $fn_name)
            }

            fn field(&self, _field_args: datafusion_expr::function::WindowUDFFieldArgs) -> Result<Arc<Field>> {
                Ok(Arc::new(Field::new($fn_name, DataType::Int64, true)))
            }
        }

        paste::paste! {
            pub fn [<$name:snake _udwf>]() -> Arc<WindowUDF> {
                static INSTANCE: std::sync::LazyLock<Arc<WindowUDF>> =
                    std::sync::LazyLock::new(|| Arc::new(WindowUDF::from($name::default())));
                Arc::clone(&INSTANCE)
            }
        }
    };
}

// T611 Window functions
stub_window_udf!(RowNumber, "row_number", zero_arg);
stub_window_udf!(Rank, "rank", zero_arg);
stub_window_udf!(DenseRank, "dense_rank", zero_arg);
stub_window_udf!(PercentRank, "percent_rank", zero_arg);
stub_window_udf!(CumeDist, "cume_dist", zero_arg);
stub_window_udf!(Ntile, "ntile");
stub_window_udf!(Lead, "lead");
stub_window_udf!(Lag, "lag");
stub_window_udf!(FirstValue, "first_value");
stub_window_udf!(LastValue, "last_value");
stub_window_udf!(NthValue, "nth_value");

// ============================================================================
// DataFusionFunctionProvider - Reference Implementation
// ============================================================================

/// Reference implementation of `ConformanceFunctionProvider` using stub functions.
///
/// This provider supplies stub implementations for all SQL:2016 Core functions
/// that are sufficient for parse + plan testing. The stubs will fail if actually
/// invoked during execution.
///
/// Downstream users should implement `ConformanceFunctionProvider` with their own
/// complete function implementations to achieve full SQL:2016 conformance.
pub struct DataFusionFunctionProvider;

impl ConformanceFunctionProvider for DataFusionFunctionProvider {
    fn get_aggregate_function(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        match name.to_lowercase().as_str() {
            "count" => Some(count_udaf()),
            "sum" => Some(sum_udaf()),
            "avg" | "mean" => Some(avg_udaf()),
            "min" => Some(min_udaf()),
            "max" => Some(max_udaf()),
            // Statistical functions
            "stddev" | "stddev_samp" => Some(stddev_udaf()),
            "stddev_pop" => Some(stddev_pop_udaf()),
            "variance" | "var_samp" => Some(variance_udaf()),
            "var_pop" => Some(var_pop_udaf()),
            // Bit aggregates
            "bit_and" => Some(bit_and_agg_udaf()),
            "bit_or" => Some(bit_or_agg_udaf()),
            "bit_xor" => Some(bit_xor_agg_udaf()),
            // Array/list aggregates
            "array_agg" => Some(array_agg_udaf()),
            "listagg" => Some(list_agg_udaf()),
            "string_agg" => Some(string_agg_udaf()),
            // JSON aggregates
            "json_arrayagg" => Some(json_array_agg_udaf()),
            "json_objectagg" => Some(json_object_agg_udaf()),
            _ => None,
        }
    }

    fn get_scalar_function(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        match name.to_lowercase().as_str() {
            // String functions (E021)
            "upper" => Some(upper_udf()),
            "lower" => Some(lower_udf()),
            "substring" | "substr" => Some(substring_udf()),
            "trim" => Some(trim_udf()),
            "ltrim" => Some(ltrim_udf()),
            "rtrim" => Some(rtrim_udf()),
            "position" | "strpos" => Some(position_udf()),
            "character_length" | "char_length" | "length" => Some(character_length_udf()),
            "octet_length" => Some(octet_length_udf()),
            "concat" => Some(concat_udf()),

            // Null handling (E131)
            "coalesce" => Some(coalesce_udf()),
            "nullif" => Some(nullif_udf()),

            // Numeric functions (T621)
            "abs" => Some(abs_udf()),
            "mod" => Some(mod_udf()),
            "ceil" | "ceiling" => Some(ceil_udf()),
            "floor" => Some(floor_udf()),
            "round" => Some(round_udf()),
            "power" | "pow" => Some(power_udf()),
            "sqrt" => Some(sqrt_udf()),
            "exp" => Some(exp_udf()),
            "ln" => Some(ln_udf()),
            "log" | "log10" => Some(log_udf()),
            "sign" => Some(sign_udf()),
            "trunc" | "truncate" => Some(trunc_udf()),

            // Date/time functions (F051)
            "current_date" => Some(current_date_udf()),
            "current_time" => Some(current_time_udf()),
            "current_timestamp" | "now" => Some(current_timestamp_udf()),
            "extract" | "date_part" => Some(extract_udf()),

            // Additional common functions
            "left" => Some(left_udf()),
            "right" => Some(right_udf()),
            "replace" => Some(replace_udf()),
            "reverse" => Some(reverse_udf()),
            "repeat" => Some(repeat_udf()),
            "lpad" => Some(lpad_udf()),
            "rpad" => Some(rpad_udf()),
            "split_part" => Some(split_part_udf()),
            "starts_with" => Some(starts_with_udf()),
            "ends_with" => Some(ends_with_udf()),
            "btrim" => Some(btrim_udf()),
            "initcap" => Some(initcap_udf()),
            "ascii" => Some(ascii_udf()),
            "chr" => Some(chr_udf()),
            "translate" => Some(translate_udf()),
            "to_hex" => Some(to_hex_udf()),
            "overlay" => Some(overlay_udf()),

            // Date/time extended functions
            "date_add" => Some(date_add_udf()),
            "date_sub" => Some(date_sub_udf()),
            "date_trunc" => Some(date_trunc_udf()),
            "localtime" => Some(local_time_udf()),
            "localtimestamp" => Some(local_timestamp_udf()),

            // Trigonometric functions
            "sin" => Some(sin_udf()),
            "cos" => Some(cos_udf()),
            "tan" => Some(tan_udf()),
            "asin" => Some(asin_udf()),
            "acos" => Some(acos_udf()),
            "atan" => Some(atan_udf()),
            "atan2" => Some(atan2_udf()),
            "sinh" => Some(sinh_udf()),
            "cosh" => Some(cosh_udf()),
            "tanh" => Some(tanh_udf()),

            // More math functions
            "degrees" => Some(degrees_udf()),
            "radians" => Some(radians_udf()),
            "log2" => Some(log2_udf()),
            "width_bucket" => Some(width_bucket_udf()),
            "random" => Some(random_udf()),
            "rand" => Some(rand_udf()),
            "pi" => Some(pi_udf()),

            // Comparison functions
            "greatest" => Some(greatest_udf()),
            "least" => Some(least_udf()),

            // Bit scalar functions
            "bit_and" => Some(bit_and_udf()),
            "bit_or" => Some(bit_or_udf()),
            "bit_xor" => Some(bit_xor_udf()),
            "bit_length" => Some(bit_length_udf()),

            // Array functions
            "array" => Some(array_udf()),
            "make_array" => Some(make_array_udf()),
            "array_append" => Some(array_append_udf()),
            "array_distinct" => Some(array_distinct_udf()),
            "array_except" => Some(array_except_udf()),
            "array_intersect" => Some(array_intersect_udf()),
            "array_position" => Some(array_position_udf()),
            "array_prepend" => Some(array_prepend_udf()),
            "array_remove" => Some(array_remove_udf()),
            "array_replace" => Some(array_replace_udf()),
            "array_union" => Some(array_union_udf()),
            "trim_array" => Some(trim_array_udf()),
            "cardinality" => Some(cardinality_udf()),

            // Row constructor
            "row" => Some(row_constructor_udf()),

            // Null handling extended
            "ifnull" => Some(ifnull_udf()),
            "nvl" => Some(nvl_udf()),

            // String extended
            "char" => Some(char_udf()),
            "space" => Some(space_udf()),

            // Regex functions
            "regexp_like" => Some(regexp_like_udf()),
            "regexp_replace" => Some(regexp_replace_udf()),
            "regexp_substr" => Some(regexp_substr_udf()),

            // JSON functions
            "json_array" => Some(json_array_udf()),
            "json_exists" => Some(json_exists_udf()),
            "json_query" => Some(json_query_udf()),
            "json_value" => Some(json_value_udf()),
            "json_object" => Some(json_object_udf()),

            _ => None,
        }
    }

    fn get_window_function(&self, name: &str) -> Option<Arc<WindowUDF>> {
        match name.to_lowercase().as_str() {
            "row_number" => Some(row_number_udwf()),
            "rank" => Some(rank_udwf()),
            "dense_rank" => Some(dense_rank_udwf()),
            "percent_rank" => Some(percent_rank_udwf()),
            "cume_dist" => Some(cume_dist_udwf()),
            "ntile" => Some(ntile_udwf()),
            "lead" => Some(lead_udwf()),
            "lag" => Some(lag_udwf()),
            "first_value" => Some(first_value_udwf()),
            "last_value" => Some(last_value_udwf()),
            "nth_value" => Some(nth_value_udwf()),
            _ => None,
        }
    }
}

/// Get the default function provider for conformance tests.
/// Uses DataFusion's built-in aggregate functions.
pub fn default_function_provider() -> DataFusionFunctionProvider {
    DataFusionFunctionProvider
}

// ============================================================================
// ConformanceExprPlanner - Handle SQL Standard Syntax
// ============================================================================

use datafusion_expr::planner::{PlannerResult, RawFieldAccessExpr};
use datafusion_expr::{Expr, GetFieldAccess};

/// Expression planner for SQL:2016 standard syntax.
///
/// Handles SQL standard constructs that need special planning:
/// - `SUBSTRING(x FROM y [FOR z])` → `substring(x, y, z)`
/// - `POSITION(x IN y)` → `strpos(y, x)`
/// - `EXTRACT(field FROM date)` → `extract(field, date)` / `date_part(field, date)`
/// - `OVERLAY(x PLACING y FROM z [FOR len])` → `overlay(x, y, z, len)`
/// - `TRIM([LEADING|TRAILING|BOTH] char FROM string)` → `trim(string)` variants
#[derive(Debug)]
pub struct ConformanceExprPlanner;

impl ExprPlanner for ConformanceExprPlanner {
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        // Convert SUBSTRING(string FROM start [FOR length]) to substring function call
        // args: [string, start] or [string, start, length]
        let func = substring_udf();
        match args.len() {
            2 => Ok(PlannerResult::Planned(Expr::ScalarFunction(
                datafusion_expr::expr::ScalarFunction::new_udf(func, args),
            ))),
            3 => Ok(PlannerResult::Planned(Expr::ScalarFunction(
                datafusion_expr::expr::ScalarFunction::new_udf(func, args),
            ))),
            _ => Ok(PlannerResult::Original(args)),
        }
    }

    fn plan_position(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        // Convert POSITION(substr IN string) to strpos(string, substr)
        // args: [string, substr] - note the order from the SQL parser
        if args.len() == 2 {
            let func = position_udf();
            Ok(PlannerResult::Planned(Expr::ScalarFunction(
                datafusion_expr::expr::ScalarFunction::new_udf(func, args),
            )))
        } else {
            Ok(PlannerResult::Original(args))
        }
    }

    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        // Convert EXTRACT(field FROM date) to date_part or extract function
        // args: [field, date]
        if args.len() == 2 {
            let func = extract_udf();
            Ok(PlannerResult::Planned(Expr::ScalarFunction(
                datafusion_expr::expr::ScalarFunction::new_udf(func, args),
            )))
        } else {
            Ok(PlannerResult::Original(args))
        }
    }

    fn plan_overlay(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        // Convert OVERLAY(string PLACING replacement FROM start [FOR length])
        // to overlay function call
        // args: [string, replacement, start] or [string, replacement, start, length]
        if args.len() >= 3 {
            let func = overlay_udf();
            Ok(PlannerResult::Planned(Expr::ScalarFunction(
                datafusion_expr::expr::ScalarFunction::new_udf(func, args),
            )))
        } else {
            Ok(PlannerResult::Original(args))
        }
    }

    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        _is_named_struct: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        // Convert ROW(a, b, c) or STRUCT(a, b, c) to a row() function call
        // The row() function creates a struct with fields named c0, c1, etc.
        let func = row_constructor_udf();
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(func, args),
        )))
    }

    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        // Handle array literals like [1, 2, 3] or ARRAY[...]

        // Empty arrays: create a Null scalar list
        if exprs.is_empty() {
            let empty_list = ScalarValue::new_list_nullable(&[], &DataType::Null);
            let list_scalar = ScalarValue::List(empty_list);
            return Ok(PlannerResult::Planned(Expr::Literal(list_scalar, None)));
        }

        // Check if all expressions are literals (no column references)
        let all_literals = exprs.iter().all(|e| matches!(e, Expr::Literal(_, _)));

        if all_literals {
            // For literal-only arrays, create a ScalarValue::List
            let mut values = Vec::new();
            for expr in &exprs {
                if let Expr::Literal(scalar, _) = expr {
                    values.push(scalar.clone());
                } else {
                    // Should not happen given all_literals check, but be defensive
                    let func = make_array_udf();
                    return Ok(PlannerResult::Planned(Expr::ScalarFunction(
                        datafusion_expr::expr::ScalarFunction::new_udf(func, exprs),
                    )));
                }
            }

            // Try to create a list scalar value
            // Get the data type from the first element
            if let Some(first) = values.first() {
                let data_type = first.data_type();
                let list_array = ScalarValue::new_list_nullable(&values, &data_type);
                let list_scalar = ScalarValue::List(list_array);
                return Ok(PlannerResult::Planned(Expr::Literal(list_scalar, None)));
            }
        }

        // For arrays with column references or expressions, use make_array function
        let func = make_array_udf();
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(func, exprs),
        )))
    }

    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        // Handle field access for arrays, structs, and lists
        match &expr.field_access {
            GetFieldAccess::ListRange { start, stop, stride } => {
                // Convert array[start:stop:stride] to array_slice(array, start, stop, stride)
                let func = array_slice_udf();
                let args = vec![
                    expr.expr.clone(),
                    (**start).clone(),
                    (**stop).clone(),
                    (**stride).clone(),
                ];
                Ok(PlannerResult::Planned(Expr::ScalarFunction(
                    datafusion_expr::expr::ScalarFunction::new_udf(func, args),
                )))
            }
            GetFieldAccess::ListIndex { key } => {
                // Convert array[index] to array_element(array, index)
                let func = array_element_udf();
                let args = vec![expr.expr.clone(), (**key).clone()];
                Ok(PlannerResult::Planned(Expr::ScalarFunction(
                    datafusion_expr::expr::ScalarFunction::new_udf(func, args),
                )))
            }
            GetFieldAccess::NamedStructField { name } => {
                // Convert struct.field to get_field(struct, field_name)
                let func = get_field_udf();
                let args = vec![expr.expr.clone(), datafusion_expr::lit(name.clone())];
                Ok(PlannerResult::Planned(Expr::ScalarFunction(
                    datafusion_expr::expr::ScalarFunction::new_udf(func, args),
                )))
            }
        }
    }
}

/// Get the default expression planner for conformance tests.
pub fn default_expr_planner() -> Arc<dyn ExprPlanner> {
    Arc::new(ConformanceExprPlanner)
}

// ============================================================================
// Test Macros
// ============================================================================

/// Assert that SQL text parses without error using DFParser.
///
/// This tests the parsing layer only (SQL text -> AST).
///
/// # Example
/// ```ignore
/// assert_parses!("SELECT 42");
/// assert_parses!("CREATE TABLE t (x INT)");
/// ```
#[macro_export]
macro_rules! assert_parses {
    ($sql:expr) => {{
        let result = crate::parse_sql($sql);
        assert!(
            result.is_ok(),
            "SQL should parse successfully.\nSQL: {}\nError: {:?}",
            $sql,
            result.unwrap_err()
        );
    }};
}

/// Assert that SQL text does NOT parse (expected parse error).
///
/// Use this for syntax that DataFusion intentionally doesn't support.
#[macro_export]
macro_rules! assert_parse_error {
    ($sql:expr) => {{
        let result = crate::parse_sql($sql);
        assert!(
            result.is_err(),
            "SQL should fail to parse but succeeded.\nSQL: {}",
            $sql
        );
    }};
    ($sql:expr, $expected_error:expr) => {{
        let result = crate::parse_sql($sql);
        assert!(
            result.is_err(),
            "SQL should fail to parse but succeeded.\nSQL: {}",
            $sql
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains($expected_error),
            "Error message mismatch.\nExpected to contain: {}\nActual: {}",
            $expected_error,
            err
        );
    }};
}

/// Assert that SQL text converts to a logical plan without error.
///
/// This tests both parsing and planning (SQL text -> AST -> LogicalPlan).
///
/// # Example
/// ```ignore
/// assert_plans!("SELECT 42");
/// assert_plans!("SELECT * FROM person WHERE age > 21");
/// ```
#[macro_export]
macro_rules! assert_plans {
    ($sql:expr) => {{
        let result = crate::logical_plan($sql);
        assert!(
            result.is_ok(),
            "SQL should plan successfully.\nSQL: {}\nError: {:?}",
            $sql,
            result.unwrap_err()
        );
    }};
}

/// Assert that SQL text fails to plan (expected planning error).
///
/// Use this for SQL that parses but cannot be converted to a logical plan.
#[macro_export]
macro_rules! assert_plan_error {
    ($sql:expr) => {{
        let result = crate::logical_plan($sql);
        assert!(
            result.is_err(),
            "SQL should fail to plan but succeeded.\nSQL: {}",
            $sql
        );
    }};
    ($sql:expr, $expected_error:expr) => {{
        let result = crate::logical_plan($sql);
        assert!(
            result.is_err(),
            "SQL should fail to plan but succeeded.\nSQL: {}",
            $sql
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains($expected_error),
            "Error message mismatch.\nExpected to contain: {}\nActual: {}\nSQL: {}",
            $expected_error,
            err,
            $sql
        );
    }};
}

/// Assert that a SQL:2016 feature is NOT YET implemented.
///
/// This macro is used to document missing features. When a feature is
/// implemented, this test will start failing, prompting an update to
/// a positive test.
///
/// # Example
/// ```ignore
/// // Feature T151: DISTINCT predicate
/// assert_not_implemented!(
///     "SELECT * FROM t WHERE a IS DISTINCT FROM b",
///     "T151",
///     "DISTINCT predicate"
/// );
/// ```
#[macro_export]
macro_rules! assert_not_implemented {
    ($sql:expr, $feature_id:expr, $description:expr) => {{
        let parse_result = crate::parse_sql($sql);
        let plan_result = crate::logical_plan($sql);

        // Feature is "not implemented" if either parsing or planning fails
        let is_not_implemented = parse_result.is_err() || plan_result.is_err();

        assert!(
            is_not_implemented,
            "Feature {} ({}) appears to be implemented now!\n\
             SQL: {}\n\
             Please update this test to use assert_parses! or assert_plans! instead.",
            $feature_id,
            $description,
            $sql
        );
    }};
}

/// Assert that a feature is fully supported (parses and plans).
///
/// Convenience macro combining parse + plan assertions.
#[macro_export]
macro_rules! assert_feature_supported {
    ($sql:expr, $feature_id:expr, $description:expr) => {{
        // First verify it parses
        let parse_result = crate::parse_sql($sql);
        assert!(
            parse_result.is_ok(),
            "Feature {} ({}) should parse.\nSQL: {}\nError: {:?}",
            $feature_id,
            $description,
            $sql,
            parse_result.unwrap_err()
        );

        // Then verify it plans
        let plan_result = crate::logical_plan($sql);
        assert!(
            plan_result.is_ok(),
            "Feature {} ({}) should plan.\nSQL: {}\nError: {:?}",
            $feature_id,
            $description,
            $sql,
            plan_result.unwrap_err()
        );
    }};
}

/// Assert that a SQL/PSM feature is supported using MsSqlDialect.
///
/// This macro uses MsSqlDialect which supports BEGIN/END blocks in
/// CREATE FUNCTION statements.
#[macro_export]
macro_rules! assert_psm_feature_supported {
    ($sql:expr, $feature_id:expr, $description:expr) => {{
        // First verify it parses with MsSqlDialect
        let parse_result = crate::parse_psm_sql($sql);
        assert!(
            parse_result.is_ok(),
            "PSM Feature {} ({}) should parse.\nSQL: {}\nError: {:?}",
            $feature_id,
            $description,
            $sql,
            parse_result.unwrap_err()
        );

        // Then verify it plans
        let plan_result = crate::logical_plan_psm($sql);
        assert!(
            plan_result.is_ok(),
            "PSM Feature {} ({}) should plan.\nSQL: {}\nError: {:?}",
            $feature_id,
            $description,
            $sql,
            plan_result.unwrap_err()
        );
    }};
}

/// Assert that a SQL/PSM feature parses but may not plan yet.
///
/// Use this for features where parsing works but planning is not implemented.
/// Uses MsSqlDialect which requires @ prefix for variables.
#[macro_export]
macro_rules! assert_psm_parses {
    ($sql:expr) => {{
        let result = crate::parse_psm_sql($sql);
        assert!(
            result.is_ok(),
            "SQL/PSM should parse successfully.\nSQL: {}\nError: {:?}",
            $sql,
            result.unwrap_err()
        );
    }};
}

/// Assert that PostgreSQL-style SQL parses.
///
/// PostgreSQL uses standard variable names (no @ prefix) but requires
/// $$ delimited function bodies.
#[macro_export]
macro_rules! assert_postgres_parses {
    ($sql:expr) => {{
        let result = crate::parse_postgres_sql($sql);
        assert!(
            result.is_ok(),
            "PostgreSQL SQL should parse successfully.\nSQL: {}\nError: {:?}",
            $sql,
            result.unwrap_err()
        );
    }};
}

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Mock CSV file type for testing
struct MockCsvType {}

impl GetExt for MockCsvType {
    fn get_ext(&self) -> String {
        "csv".to_string()
    }
}

impl FileType for MockCsvType {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl std::fmt::Display for MockCsvType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_ext())
    }
}

/// Mock session state for conformance tests (deprecated - use ConformanceFunctionProvider)
#[derive(Default)]
pub struct ConformanceSessionState {
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    config_options: ConfigOptions,
}

impl ConformanceSessionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_scalar_function(mut self, f: Arc<ScalarUDF>) -> Self {
        self.scalar_functions.insert(f.name().to_string(), f);
        self
    }

    pub fn with_aggregate_function(mut self, f: Arc<AggregateUDF>) -> Self {
        self.aggregate_functions
            .insert(f.name().to_string().to_lowercase(), f);
        self
    }
}

// Implement ConformanceFunctionProvider for backward compatibility
impl ConformanceFunctionProvider for ConformanceSessionState {
    fn get_aggregate_function(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions.get(name).cloned()
    }

    fn get_scalar_function(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar_functions.get(name).cloned()
    }

    fn get_window_function(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.window_functions.get(name).cloned()
    }
}

/// Empty table source for schema-only testing
struct EmptyTable {
    schema: SchemaRef,
}

impl EmptyTable {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl TableSource for EmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Context provider for conformance tests with standard test tables.
///
/// Takes a `ConformanceFunctionProvider` that supplies the actual function
/// implementations. This ensures downstream users provide real implementations
/// rather than the test suite silently using stubs.
pub struct ConformanceContextProvider<'a, F: ConformanceFunctionProvider> {
    function_provider: &'a F,
    config_options: ConfigOptions,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl<'a, F: ConformanceFunctionProvider> ConformanceContextProvider<'a, F> {
    pub fn new(function_provider: &'a F) -> Self {
        Self {
            function_provider,
            config_options: ConfigOptions::default(),
            expr_planners: vec![Arc::new(ConformanceExprPlanner)],
        }
    }

    /// Validate that all required functions are available.
    /// Returns a list of missing function names.
    pub fn validate_required_functions(&self) -> Vec<String> {
        self.function_provider.validate_required_functions()
    }
}

impl<'a, F: ConformanceFunctionProvider> ContextProvider for ConformanceContextProvider<'a, F> {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let schema_name = name.schema().unwrap_or("");
        let table_name = name.table();

        // Handle INFORMATION_SCHEMA specially (case-insensitive)
        if schema_name.eq_ignore_ascii_case("information_schema") {
            let schema = match table_name.to_lowercase().as_str() {
                "tables" => Schema::new(vec![
                    Field::new("table_catalog", DataType::Utf8, true),
                    Field::new("table_schema", DataType::Utf8, true),
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("table_type", DataType::Utf8, true),
                    Field::new("self_referencing_column_name", DataType::Utf8, true),
                    Field::new("reference_generation", DataType::Utf8, true),
                    Field::new("user_defined_type_catalog", DataType::Utf8, true),
                    Field::new("user_defined_type_schema", DataType::Utf8, true),
                    Field::new("user_defined_type_name", DataType::Utf8, true),
                    Field::new("is_insertable_into", DataType::Utf8, true),
                    Field::new("is_typed", DataType::Utf8, true),
                    Field::new("commit_action", DataType::Utf8, true),
                ]),
                "columns" => Schema::new(vec![
                    Field::new("table_catalog", DataType::Utf8, true),
                    Field::new("table_schema", DataType::Utf8, true),
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("column_name", DataType::Utf8, false),
                    Field::new("ordinal_position", DataType::Int32, false),
                    Field::new("column_default", DataType::Utf8, true),
                    Field::new("is_nullable", DataType::Utf8, true),
                    Field::new("data_type", DataType::Utf8, true),
                    Field::new("character_maximum_length", DataType::Int32, true),
                    Field::new("character_octet_length", DataType::Int32, true),
                    Field::new("numeric_precision", DataType::Int32, true),
                    Field::new("numeric_precision_radix", DataType::Int32, true),
                    Field::new("numeric_scale", DataType::Int32, true),
                    Field::new("datetime_precision", DataType::Int32, true),
                    Field::new("interval_type", DataType::Utf8, true),
                    Field::new("interval_precision", DataType::Int32, true),
                ]),
                "views" => Schema::new(vec![
                    Field::new("table_catalog", DataType::Utf8, true),
                    Field::new("table_schema", DataType::Utf8, true),
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("view_definition", DataType::Utf8, true),
                    Field::new("check_option", DataType::Utf8, true),
                    Field::new("is_updatable", DataType::Utf8, true),
                    Field::new("insertable_into", DataType::Utf8, true),
                    Field::new("is_trigger_updatable", DataType::Utf8, true),
                    Field::new("is_trigger_deletable", DataType::Utf8, true),
                    Field::new("is_trigger_insertable_into", DataType::Utf8, true),
                ]),
                "table_constraints" => Schema::new(vec![
                    Field::new("constraint_catalog", DataType::Utf8, true),
                    Field::new("constraint_schema", DataType::Utf8, true),
                    Field::new("constraint_name", DataType::Utf8, false),
                    Field::new("table_catalog", DataType::Utf8, true),
                    Field::new("table_schema", DataType::Utf8, true),
                    Field::new("table_name", DataType::Utf8, false),
                    Field::new("constraint_type", DataType::Utf8, true),
                    Field::new("is_deferrable", DataType::Utf8, true),
                    Field::new("initially_deferred", DataType::Utf8, true),
                    Field::new("enforced", DataType::Utf8, true),
                ]),
                "referential_constraints" => Schema::new(vec![
                    Field::new("constraint_catalog", DataType::Utf8, true),
                    Field::new("constraint_schema", DataType::Utf8, true),
                    Field::new("constraint_name", DataType::Utf8, false),
                    Field::new("unique_constraint_catalog", DataType::Utf8, true),
                    Field::new("unique_constraint_schema", DataType::Utf8, true),
                    Field::new("unique_constraint_name", DataType::Utf8, true),
                    Field::new("match_option", DataType::Utf8, true),
                    Field::new("update_rule", DataType::Utf8, true),
                    Field::new("delete_rule", DataType::Utf8, true),
                ]),
                "check_constraints" => Schema::new(vec![
                    Field::new("constraint_catalog", DataType::Utf8, true),
                    Field::new("constraint_schema", DataType::Utf8, true),
                    Field::new("constraint_name", DataType::Utf8, false),
                    Field::new("check_clause", DataType::Utf8, true),
                ]),
                _ => return plan_err!("Table not found: {}.{}", schema_name, table_name),
            };
            return Ok(Arc::new(EmptyTable::new(Arc::new(schema))));
        }

        // Standard test tables for conformance testing
        let schema = match table_name {
            // Basic test table with 3 columns (a, b, c)
            "t" | "t1" | "t2" | "t3" => Ok(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Utf8, true),
            ])),

            // Numeric types test table - with column 'a' as first column
            "numeric_types" => Ok(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("regular", DataType::Int32, true),
                Field::new("big", DataType::Int64, true),
                Field::new("real_col", DataType::Float32, true),
                Field::new("double_col", DataType::Float64, true),
                Field::new("decimal_col", DataType::Decimal128(10, 2), true),
            ])),

            // Character types test table
            "char_types" => Ok(Schema::new(vec![
                Field::new("char_col", DataType::Utf8, true),
                Field::new("varchar_col", DataType::Utf8, true),
                Field::new("text_col", DataType::LargeUtf8, true),
            ])),

            // Date/time types test table
            "datetime_types" => Ok(Schema::new(vec![
                Field::new("date_col", DataType::Date32, true),
                Field::new("time_col", DataType::Time64(TimeUnit::Nanosecond), true),
                Field::new(
                    "timestamp_col",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    "interval_col",
                    DataType::Interval(IntervalUnit::DayTime),
                    true,
                ),
            ])),

            // Standard person table (from existing tests) - exactly 7 columns
            "person" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("salary", DataType::Float64, false),
                Field::new(
                    "birth_date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),

            // Orders table for join tests
            "orders" => Ok(Schema::new(vec![
                Field::new("order_id", DataType::UInt32, false),
                Field::new("customer_id", DataType::UInt32, false),
                Field::new("item", DataType::Utf8, false),
                Field::new("qty", DataType::Int32, false),
                Field::new("price", DataType::Float64, false),
                Field::new("category", DataType::Utf8, true),
                Field::new("amount", DataType::Float64, true),
                Field::new("person_id", DataType::UInt32, true),
            ])),

            // Products table for join tests
            "products" => Ok(Schema::new(vec![
                Field::new("product_id", DataType::UInt32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
                Field::new("category", DataType::Utf8, true),
            ])),

            // Array types test table
            "array_types" => Ok(Schema::new(vec![
                Field::new(
                    "int_array",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                    true,
                ),
                Field::new(
                    "str_array",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                ),
            ])),

            // Struct types test table
            "struct_types" => Ok(Schema::new(vec![Field::new(
                "struct_col",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Int32, true),
                ])),
                true,
            )])),

            // JSON data test table
            "json_data" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("person_id", DataType::Int32, true),
                Field::new("data", DataType::Utf8, true), // JSON as string for now
            ])),

            // Events table for JSON testing
            "events" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("event_data", DataType::Utf8, true), // JSON as string
                Field::new("metadata", DataType::Utf8, true), // JSON as string
            ])),

            // Stock prices table for financial/analytics tests
            "stock_prices" => Ok(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, false),
                Field::new("trade_date", DataType::Date32, false),
                Field::new("open", DataType::Float64, true),
                Field::new("high", DataType::Float64, true),
                Field::new("low", DataType::Float64, true),
                Field::new("close", DataType::Float64, true),
                Field::new("volume", DataType::Int64, true),
                Field::new("price", DataType::Float64, true),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ])),

            // Employees table for HR/organizational tests
            "employees" => Ok(Schema::new(vec![
                Field::new("employee_id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("department", DataType::Utf8, true),
                Field::new("salary", DataType::Float64, true),
                Field::new("manager_id", DataType::Int32, true),
                Field::new("hire_date", DataType::Date32, true),
            ])),

            _ => plan_err!("Table not found: {}", table_name),
        };

        match schema {
            Ok(s) => Ok(Arc::new(EmptyTable::new(Arc::new(s)))),
            Err(e) => Err(e),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.function_provider.get_scalar_function(name)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.function_provider.get_aggregate_function(name)
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.function_provider.get_window_function(name)
    }

    fn options(&self) -> &ConfigOptions {
        &self.config_options
    }

    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        Ok(Arc::new(MockCsvType {}))
    }

    fn create_cte_work_table(
        &self,
        _name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(EmptyTable::new(schema)))
    }

    fn udf_names(&self) -> Vec<String> {
        // Return the required scalar functions (even if not all are implemented)
        REQUIRED_SCALAR_FUNCTIONS.iter().map(|s| s.to_string()).collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        // Return the required aggregate functions
        REQUIRED_AGGREGATE_FUNCTIONS.iter().map(|s| s.to_string()).collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        // Return the required window functions
        REQUIRED_WINDOW_FUNCTIONS.iter().map(|s| s.to_string()).collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Parse SQL text into AST statements.
///
/// Uses the default GenericDialect.
pub fn parse_sql(sql: &str) -> Result<()> {
    let _ = DFParser::parse_sql(sql)?;
    Ok(())
}

/// Parse SQL text with a specific dialect.
pub fn parse_sql_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<()> {
    let _ = DFParser::parse_sql_with_dialect(sql, dialect)?;
    Ok(())
}

/// Parse SQL/PSM text into AST statements.
///
/// Uses MsSqlDialect which supports BEGIN/END blocks in CREATE FUNCTION.
/// Note: MsSqlDialect requires @ prefix for variables (e.g., @result).
/// For standard SQL variable names, use parse_postgres_sql with $$ body syntax.
pub fn parse_psm_sql(sql: &str) -> Result<()> {
    let dialect = MsSqlDialect {};
    let _ = DFParser::parse_sql_with_dialect(sql, &dialect)?;
    Ok(())
}

/// Convert SQL/PSM text to a logical plan.
///
/// Uses MsSqlDialect for PSM syntax (BEGIN/END blocks, etc.)
/// Note: MsSqlDialect requires @ prefix for variables.
pub fn logical_plan_psm(
    sql: &str,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    let dialect = MsSqlDialect {};
    logical_plan_with_dialect_and_options(sql, &dialect, ParserOptions::default())
}

/// Parse SQL text using PostgreSQL dialect.
///
/// PostgreSQL uses $$ delimited function bodies with standard variable names.
pub fn parse_postgres_sql(sql: &str) -> Result<()> {
    let dialect = PostgreSqlDialect {};
    let _ = DFParser::parse_sql_with_dialect(sql, &dialect)?;
    Ok(())
}

/// Convert PostgreSQL SQL text to a logical plan.
pub fn logical_plan_postgres(
    sql: &str,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    let dialect = PostgreSqlDialect {};
    logical_plan_with_dialect_and_options(sql, &dialect, ParserOptions::default())
}

/// Convert SQL text to a logical plan.
///
/// Uses the default GenericDialect and DataFusionFunctionProvider.
/// Downstream users can use `logical_plan_with_provider` to use their own
/// function implementations.
pub fn logical_plan(
    sql: &str,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    logical_plan_with_options(sql, ParserOptions::default())
}

/// Convert SQL text to a logical plan with custom parser options.
pub fn logical_plan_with_options(
    sql: &str,
    options: ParserOptions,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    let dialect = GenericDialect {};
    logical_plan_with_dialect_and_options(sql, &dialect, options)
}

/// Convert SQL text to a logical plan with a specific dialect and options.
///
/// Uses the DataFusionFunctionProvider for built-in aggregate functions.
pub fn logical_plan_with_dialect_and_options(
    sql: &str,
    dialect: &dyn Dialect,
    options: ParserOptions,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    logical_plan_with_provider(sql, dialect, options, &default_function_provider())
}

/// Convert SQL text to a logical plan with a custom function provider.
///
/// This is the main entry point for downstream users who want to provide
/// their own function implementations for SQL:2016 conformance.
pub fn logical_plan_with_provider<F: ConformanceFunctionProvider>(
    sql: &str,
    dialect: &dyn Dialect,
    options: ParserOptions,
    provider: &F,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    let context = ConformanceContextProvider::new(provider);
    let planner = SqlToRel::new_with_options(&context, options);
    let mut ast = DFParser::parse_sql_with_dialect(sql, dialect)?;
    planner.statement_to_plan(ast.pop_front().unwrap())
}

// ============================================================================
// Feature Status Tracking
// ============================================================================

/// Status of a SQL standard feature
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureStatus {
    /// Feature is fully supported
    Supported,
    /// Feature is partially supported (some subfeatures work)
    Partial,
    /// Feature is not implemented
    NotImplemented,
    /// Feature is not applicable to DataFusion's use case
    NotApplicable,
}

/// Metadata about a SQL standard feature
#[derive(Debug, Clone)]
pub struct FeatureInfo {
    /// Feature ID (e.g., "E011", "T151")
    pub id: &'static str,
    /// Feature description
    pub description: &'static str,
    /// Whether this is a Core SQL feature (mandatory for conformance)
    pub is_core: bool,
    /// Current implementation status
    pub status: FeatureStatus,
    /// Optional notes about the implementation
    pub notes: Option<&'static str>,
}

impl FeatureInfo {
    pub const fn new(
        id: &'static str,
        description: &'static str,
        is_core: bool,
        status: FeatureStatus,
    ) -> Self {
        Self {
            id,
            description,
            is_core,
            status,
            notes: None,
        }
    }

    pub const fn with_notes(mut self, notes: &'static str) -> Self {
        self.notes = Some(notes);
        self
    }
}

// ============================================================================
// Tests for the test infrastructure itself
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql_basic() {
        assert!(parse_sql("SELECT 1").is_ok());
        assert!(parse_sql("SELECT * FROM t").is_ok());
        assert!(parse_sql("CREATE TABLE t (x INT)").is_ok());
    }

    #[test]
    fn test_parse_sql_error() {
        assert!(parse_sql("SELEC 1").is_err()); // typo
        assert!(parse_sql("SELECT FROM").is_err()); // incomplete
    }

    #[test]
    fn test_logical_plan_basic() {
        assert!(logical_plan("SELECT 1").is_ok());
        assert!(logical_plan("SELECT * FROM t").is_ok());
        assert!(logical_plan("SELECT a, b FROM t WHERE a > 10").is_ok());
    }

    #[test]
    fn test_logical_plan_with_joins() {
        assert!(logical_plan("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a").is_ok());
        assert!(logical_plan("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a").is_ok());
        assert!(logical_plan("SELECT * FROM t1 CROSS JOIN t2").is_ok());
    }

    #[test]
    fn test_context_provider_tables() {
        let provider = DataFusionFunctionProvider;
        let ctx = ConformanceContextProvider::new(&provider);
        assert!(ctx.get_table_source("t".into()).is_ok());
        assert!(ctx.get_table_source("person".into()).is_ok());
        assert!(ctx.get_table_source("numeric_types".into()).is_ok());
        assert!(ctx.get_table_source("nonexistent".into()).is_err());
    }

    #[test]
    fn test_function_provider_aggregates() {
        let provider = DataFusionFunctionProvider;
        // Count, sum, avg, min, max should all be available
        assert!(provider.get_aggregate_function("count").is_some());
        assert!(provider.get_aggregate_function("sum").is_some());
        assert!(provider.get_aggregate_function("avg").is_some());
        assert!(provider.get_aggregate_function("min").is_some());
        assert!(provider.get_aggregate_function("max").is_some());
        // Scalar functions are now available as stubs for conformance testing
        assert!(provider.get_scalar_function("upper").is_some());
        assert!(provider.get_scalar_function("make_array").is_some());
        // Window functions are now available as stubs for conformance testing
        assert!(provider.get_window_function("row_number").is_some());
    }

    #[test]
    fn test_logical_plan_with_aggregates() {
        // Tests that use aggregates should now work
        assert!(logical_plan("SELECT COUNT(*) FROM t").is_ok());
        assert!(logical_plan("SELECT SUM(a) FROM t").is_ok());
        assert!(logical_plan("SELECT AVG(a), MIN(b), MAX(b) FROM t").is_ok());
        assert!(logical_plan("SELECT a, COUNT(*) FROM t GROUP BY a").is_ok());
    }

    #[test]
    fn test_information_schema_case_insensitive() {
        // Test uppercase schema name
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.TABLES").is_ok());
        // Test lowercase schema name
        assert!(logical_plan("SELECT * FROM information_schema.tables").is_ok());
        // Test mixed case schema name
        assert!(logical_plan("SELECT * FROM Information_Schema.tables").is_ok());
        // Test mixed case table name
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.TaBLeS").is_ok());
    }

    #[test]
    fn test_information_schema_columns() {
        // Test all supported INFORMATION_SCHEMA tables
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.COLUMNS").is_ok());
        assert!(logical_plan("SELECT * FROM information_schema.columns").is_ok());
    }

    #[test]
    fn test_information_schema_views() {
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.VIEWS").is_ok());
        assert!(logical_plan("SELECT * FROM information_schema.views").is_ok());
    }

    #[test]
    fn test_information_schema_constraints() {
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS").is_ok());
        assert!(logical_plan("SELECT * FROM information_schema.table_constraints").is_ok());
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS").is_ok());
        assert!(logical_plan("SELECT * FROM information_schema.referential_constraints").is_ok());
        assert!(logical_plan("SELECT * FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS").is_ok());
        assert!(logical_plan("SELECT * FROM information_schema.check_constraints").is_ok());
    }

    #[test]
    fn test_information_schema_invalid_table() {
        // Test that invalid table names in INFORMATION_SCHEMA produce an error
        let result = logical_plan("SELECT * FROM INFORMATION_SCHEMA.invalid_table");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Table not found"));
        assert!(err.to_string().contains("information_schema.invalid_table"));
    }

    #[test]
    fn test_information_schema_bare_table() {
        // Test that bare table reference doesn't accidentally match INFORMATION_SCHEMA
        // This should try to find "tables" in the default schema, not INFORMATION_SCHEMA
        let result = logical_plan("SELECT * FROM tables");
        assert!(result.is_err()); // Should fail because 'tables' doesn't exist in default schema
    }

    #[test]
    fn test_information_schema_context_provider() {
        use datafusion_common::TableReference;
        let provider = DataFusionFunctionProvider;
        let ctx = ConformanceContextProvider::new(&provider);

        // Test case-insensitive schema matching
        assert!(ctx.get_table_source(TableReference::partial("INFORMATION_SCHEMA", "tables")).is_ok());
        assert!(ctx.get_table_source(TableReference::partial("information_schema", "tables")).is_ok());
        assert!(ctx.get_table_source(TableReference::partial("Information_Schema", "TABLES")).is_ok());

        // Test case-insensitive table matching
        assert!(ctx.get_table_source(TableReference::partial("INFORMATION_SCHEMA", "COLUMNS")).is_ok());
        assert!(ctx.get_table_source(TableReference::partial("information_schema", "columns")).is_ok());
        assert!(ctx.get_table_source(TableReference::partial("INFORMATION_SCHEMA", "CoLuMnS")).is_ok());

        // Test invalid table in INFORMATION_SCHEMA
        let result = ctx.get_table_source(TableReference::partial("INFORMATION_SCHEMA", "nonexistent"));
        assert!(result.is_err());

        // Test that empty schema doesn't match INFORMATION_SCHEMA
        let result = ctx.get_table_source(TableReference::bare("tables"));
        assert!(result.is_err()); // Should not find INFORMATION_SCHEMA.TABLES

        // Test Full reference (catalog.schema.table) - should also work
        assert!(ctx.get_table_source(TableReference::full("catalog", "INFORMATION_SCHEMA", "tables")).is_ok());
        assert!(ctx.get_table_source(TableReference::full("cat", "information_schema", "COLUMNS")).is_ok());
    }
}
