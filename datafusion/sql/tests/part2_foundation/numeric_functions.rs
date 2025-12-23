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

//! SQL:2016 Numeric Functions Conformance Tests
//!
//! This module tests conformance to SQL:2016 mathematical and numeric functions.
//! These functions are part of various features in the SQL standard:
//!
//! - Basic math functions (part of Core SQL)
//! - Trigonometric functions (Feature T622)
//! - Statistical functions
//! - Bit manipulation functions
//! - Other numeric utilities
//!
//! # Feature Coverage
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | T621 | Enhanced numeric functions | Partial |
//! | T622 | Trigonometric functions | Partial |
//! | T623 | General logarithmic functions | Partial |
//! | T631 | IN predicate with multiple element list | N/A |
//! | F561 | Full value expressions | Partial |
//!
//! # Test Organization
//!
//! Tests are organized by function category:
//! - Basic mathematical functions (ABS, SIGN, MOD, etc.)
//! - Rounding functions (CEIL, FLOOR, ROUND, TRUNC)
//! - Power and exponential functions (POWER, SQRT, EXP, LN, LOG)
//! - Trigonometric functions (SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2)
//! - Hyperbolic functions (SINH, COSH, TANH)
//! - Conversion functions (DEGREES, RADIANS)
//! - Statistical functions (RANDOM, GREATEST, LEAST)
//! - Bit manipulation functions
//! - Utility functions (PI, NULLIF, COALESCE, WIDTH_BUCKET)
//!
//! Each test verifies basic usage, NULL handling, and integration with expressions.

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// Basic Mathematical Functions
// ============================================================================

/// T621: ABS - absolute value
#[test]
fn t621_abs_basic() {
    assert_feature_supported!(
        "SELECT ABS(a) FROM t",
        "T621",
        "ABS function"
    );
}

/// T621: ABS with negative literal
#[test]
fn t621_abs_negative() {
    assert_feature_supported!(
        "SELECT ABS(-42) FROM t",
        "T621",
        "ABS with negative literal"
    );
}

/// T621: ABS with NULL handling
#[test]
fn t621_abs_null() {
    assert_feature_supported!(
        "SELECT ABS(NULL) FROM t",
        "T621",
        "ABS with NULL"
    );
}

/// T621: ABS with expression
#[test]
fn t621_abs_expression() {
    assert_feature_supported!(
        "SELECT ABS(a - b) FROM t",
        "T621",
        "ABS with expression"
    );
}

/// T621: ABS with floating point
#[test]
fn t621_abs_float() {
    assert_feature_supported!(
        "SELECT ABS(double_col) FROM numeric_types",
        "T621",
        "ABS with floating point"
    );
}

/// T621: MOD - modulo operation
#[test]
fn t621_mod_basic() {
    assert_feature_supported!(
        "SELECT MOD(a, 10) FROM t",
        "T621",
        "MOD function"
    );
}

/// T621: MOD with literals
#[test]
fn t621_mod_literals() {
    assert_feature_supported!(
        "SELECT MOD(17, 5) FROM t",
        "T621",
        "MOD with literals"
    );
}

/// T621: MOD with NULL handling
#[test]
fn t621_mod_null() {
    assert_feature_supported!(
        "SELECT MOD(a, NULL) FROM t",
        "T621",
        "MOD with NULL"
    );
}

/// T621: MOD with expressions
#[test]
fn t621_mod_expression() {
    assert_feature_supported!(
        "SELECT MOD(a * 2, b + 3) FROM t",
        "T621",
        "MOD with expressions"
    );
}

/// T621: SIGN - sign of number
#[test]
fn t621_sign_basic() {
    assert_feature_supported!(
        "SELECT SIGN(a) FROM t",
        "T621",
        "SIGN function"
    );
}

/// T621: SIGN with positive number
#[test]
fn t621_sign_positive() {
    assert_feature_supported!(
        "SELECT SIGN(42) FROM t",
        "T621",
        "SIGN with positive number"
    );
}

/// T621: SIGN with negative number
#[test]
fn t621_sign_negative() {
    assert_feature_supported!(
        "SELECT SIGN(-42) FROM t",
        "T621",
        "SIGN with negative number"
    );
}

/// T621: SIGN with zero
#[test]
fn t621_sign_zero() {
    assert_feature_supported!(
        "SELECT SIGN(0) FROM t",
        "T621",
        "SIGN with zero"
    );
}

/// T621: SIGN with NULL
#[test]
fn t621_sign_null() {
    assert_feature_supported!(
        "SELECT SIGN(NULL) FROM t",
        "T621",
        "SIGN with NULL"
    );
}

// ============================================================================
// Rounding Functions
// ============================================================================

/// T621: CEIL - ceiling function
#[test]
fn t621_ceil_basic() {
    assert_feature_supported!(
        "SELECT CEIL(double_col) FROM numeric_types",
        "T621",
        "CEIL function"
    );
}

/// T621: CEILING - ceiling function (alternative spelling)
#[test]
fn t621_ceiling_basic() {
    assert_feature_supported!(
        "SELECT CEILING(double_col) FROM numeric_types",
        "T621",
        "CEILING function"
    );
}

/// T621: CEIL with literal
#[test]
fn t621_ceil_literal() {
    assert_feature_supported!(
        "SELECT CEIL(3.14) FROM t",
        "T621",
        "CEIL with literal"
    );
}

/// T621: CEIL with NULL
#[test]
fn t621_ceil_null() {
    assert_feature_supported!(
        "SELECT CEIL(NULL) FROM t",
        "T621",
        "CEIL with NULL"
    );
}

/// T621: CEIL with expression
#[test]
fn t621_ceil_expression() {
    assert_feature_supported!(
        "SELECT CEIL(a / 3.0) FROM t",
        "T621",
        "CEIL with expression"
    );
}

/// T621: FLOOR - floor function
#[test]
fn t621_floor_basic() {
    assert_feature_supported!(
        "SELECT FLOOR(double_col) FROM numeric_types",
        "T621",
        "FLOOR function"
    );
}

/// T621: FLOOR with literal
#[test]
fn t621_floor_literal() {
    assert_feature_supported!(
        "SELECT FLOOR(3.14) FROM t",
        "T621",
        "FLOOR with literal"
    );
}

/// T621: FLOOR with NULL
#[test]
fn t621_floor_null() {
    assert_feature_supported!(
        "SELECT FLOOR(NULL) FROM t",
        "T621",
        "FLOOR with NULL"
    );
}

/// T621: FLOOR with negative number
#[test]
fn t621_floor_negative() {
    assert_feature_supported!(
        "SELECT FLOOR(-3.14) FROM t",
        "T621",
        "FLOOR with negative number"
    );
}

/// T621: ROUND - round to nearest integer
#[test]
fn t621_round_basic() {
    assert_feature_supported!(
        "SELECT ROUND(double_col) FROM numeric_types",
        "T621",
        "ROUND function"
    );
}

/// T621: ROUND with decimal places
#[test]
fn t621_round_with_places() {
    assert_feature_supported!(
        "SELECT ROUND(double_col, 2) FROM numeric_types",
        "T621",
        "ROUND with decimal places"
    );
}

/// T621: ROUND with literal
#[test]
fn t621_round_literal() {
    assert_feature_supported!(
        "SELECT ROUND(3.14159, 2) FROM t",
        "T621",
        "ROUND with literal"
    );
}

/// T621: ROUND with NULL
#[test]
fn t621_round_null() {
    assert_feature_supported!(
        "SELECT ROUND(NULL, 2) FROM t",
        "T621",
        "ROUND with NULL"
    );
}

/// T621: ROUND with negative decimal places
#[test]
fn t621_round_negative_places() {
    assert_feature_supported!(
        "SELECT ROUND(1234.5678, -2) FROM t",
        "T621",
        "ROUND with negative decimal places"
    );
}

/// T621: TRUNCATE - truncate to specified decimal places
#[test]
fn t621_truncate_basic() {
    assert_feature_supported!(
        "SELECT TRUNCATE(double_col, 2) FROM numeric_types",
        "T621",
        "TRUNCATE function"
    );
}

/// T621: TRUNC - truncate function (alternative spelling)
#[test]
fn t621_trunc_basic() {
    assert_feature_supported!(
        "SELECT TRUNC(double_col, 2) FROM numeric_types",
        "T621",
        "TRUNC function"
    );
}

/// T621: TRUNC with literal
#[test]
fn t621_trunc_literal() {
    assert_feature_supported!(
        "SELECT TRUNC(3.14159, 2) FROM t",
        "T621",
        "TRUNC with literal"
    );
}

/// T621: TRUNC with NULL
#[test]
fn t621_trunc_null() {
    assert_feature_supported!(
        "SELECT TRUNC(NULL, 2) FROM t",
        "T621",
        "TRUNC with NULL"
    );
}

/// T621: TRUNC without decimal places
#[test]
fn t621_trunc_no_places() {
    assert_feature_supported!(
        "SELECT TRUNC(double_col) FROM numeric_types",
        "T621",
        "TRUNC without decimal places"
    );
}

// ============================================================================
// Power and Exponential Functions
// ============================================================================

/// T621: POWER - raise to power
#[test]
fn t621_power_basic() {
    assert_feature_supported!(
        "SELECT POWER(a, 2) FROM t",
        "T621",
        "POWER function"
    );
}

/// T621: POWER with literals
#[test]
fn t621_power_literals() {
    assert_feature_supported!(
        "SELECT POWER(2, 10) FROM t",
        "T621",
        "POWER with literals"
    );
}

/// T621: POWER with NULL
#[test]
fn t621_power_null() {
    assert_feature_supported!(
        "SELECT POWER(NULL, 2) FROM t",
        "T621",
        "POWER with NULL"
    );
}

/// T621: POWER with fractional exponent
#[test]
fn t621_power_fractional() {
    assert_feature_supported!(
        "SELECT POWER(a, 0.5) FROM t",
        "T621",
        "POWER with fractional exponent"
    );
}

/// T621: POWER with negative exponent
#[test]
fn t621_power_negative() {
    assert_feature_supported!(
        "SELECT POWER(2, -3) FROM t",
        "T621",
        "POWER with negative exponent"
    );
}

/// T621: SQRT - square root
#[test]
fn t621_sqrt_basic() {
    assert_feature_supported!(
        "SELECT SQRT(a) FROM t",
        "T621",
        "SQRT function"
    );
}

/// T621: SQRT with literal
#[test]
fn t621_sqrt_literal() {
    assert_feature_supported!(
        "SELECT SQRT(16) FROM t",
        "T621",
        "SQRT with literal"
    );
}

/// T621: SQRT with NULL
#[test]
fn t621_sqrt_null() {
    assert_feature_supported!(
        "SELECT SQRT(NULL) FROM t",
        "T621",
        "SQRT with NULL"
    );
}

/// T621: SQRT with expression
#[test]
fn t621_sqrt_expression() {
    assert_feature_supported!(
        "SELECT SQRT(a * a + b * b) FROM t",
        "T621",
        "SQRT with expression"
    );
}

/// T621: EXP - exponential function
#[test]
fn t621_exp_basic() {
    assert_feature_supported!(
        "SELECT EXP(a) FROM t",
        "T621",
        "EXP function"
    );
}

/// T621: EXP with literal
#[test]
fn t621_exp_literal() {
    assert_feature_supported!(
        "SELECT EXP(1) FROM t",
        "T621",
        "EXP with literal"
    );
}

/// T621: EXP with NULL
#[test]
fn t621_exp_null() {
    assert_feature_supported!(
        "SELECT EXP(NULL) FROM t",
        "T621",
        "EXP with NULL"
    );
}

/// T623: LN - natural logarithm
#[test]
fn t623_ln_basic() {
    assert_feature_supported!(
        "SELECT LN(a) FROM t",
        "T623",
        "LN function"
    );
}

/// T623: LN with literal
#[test]
fn t623_ln_literal() {
    assert_feature_supported!(
        "SELECT LN(2.718281828) FROM t",
        "T623",
        "LN with literal"
    );
}

/// T623: LN with NULL
#[test]
fn t623_ln_null() {
    assert_feature_supported!(
        "SELECT LN(NULL) FROM t",
        "T623",
        "LN with NULL"
    );
}

/// T623: LOG - logarithm with base
#[test]
fn t623_log_basic() {
    assert_feature_supported!(
        "SELECT LOG(10, a) FROM t",
        "T623",
        "LOG with base"
    );
}

/// T623: LOG with literals
#[test]
fn t623_log_literals() {
    assert_feature_supported!(
        "SELECT LOG(2, 1024) FROM t",
        "T623",
        "LOG with literals"
    );
}

/// T623: LOG10 - base-10 logarithm
#[test]
fn t623_log10_basic() {
    assert_feature_supported!(
        "SELECT LOG10(a) FROM t",
        "T623",
        "LOG10 function"
    );
}

/// T623: LOG10 with literal
#[test]
fn t623_log10_literal() {
    assert_feature_supported!(
        "SELECT LOG10(1000) FROM t",
        "T623",
        "LOG10 with literal"
    );
}

/// T623: LOG10 with NULL
#[test]
fn t623_log10_null() {
    assert_feature_supported!(
        "SELECT LOG10(NULL) FROM t",
        "T623",
        "LOG10 with NULL"
    );
}

/// T623: LOG2 - base-2 logarithm
#[test]
fn t623_log2_basic() {
    assert_feature_supported!(
        "SELECT LOG2(a) FROM t",
        "T623",
        "LOG2 function"
    );
}

/// T623: LOG2 with literal
#[test]
fn t623_log2_literal() {
    assert_feature_supported!(
        "SELECT LOG2(1024) FROM t",
        "T623",
        "LOG2 with literal"
    );
}

// ============================================================================
// Trigonometric Functions (T622)
// ============================================================================

/// T622: SIN - sine function
#[test]
fn t622_sin_basic() {
    assert_feature_supported!(
        "SELECT SIN(a) FROM t",
        "T622",
        "SIN function"
    );
}

/// T622: SIN with literal
#[test]
fn t622_sin_literal() {
    assert_feature_supported!(
        "SELECT SIN(3.14159 / 2) FROM t",
        "T622",
        "SIN with literal"
    );
}

/// T622: SIN with NULL
#[test]
fn t622_sin_null() {
    assert_feature_supported!(
        "SELECT SIN(NULL) FROM t",
        "T622",
        "SIN with NULL"
    );
}

/// T622: COS - cosine function
#[test]
fn t622_cos_basic() {
    assert_feature_supported!(
        "SELECT COS(a) FROM t",
        "T622",
        "COS function"
    );
}

/// T622: COS with literal
#[test]
fn t622_cos_literal() {
    assert_feature_supported!(
        "SELECT COS(3.14159) FROM t",
        "T622",
        "COS with literal"
    );
}

/// T622: COS with NULL
#[test]
fn t622_cos_null() {
    assert_feature_supported!(
        "SELECT COS(NULL) FROM t",
        "T622",
        "COS with NULL"
    );
}

/// T622: TAN - tangent function
#[test]
fn t622_tan_basic() {
    assert_feature_supported!(
        "SELECT TAN(a) FROM t",
        "T622",
        "TAN function"
    );
}

/// T622: TAN with literal
#[test]
fn t622_tan_literal() {
    assert_feature_supported!(
        "SELECT TAN(3.14159 / 4) FROM t",
        "T622",
        "TAN with literal"
    );
}

/// T622: TAN with NULL
#[test]
fn t622_tan_null() {
    assert_feature_supported!(
        "SELECT TAN(NULL) FROM t",
        "T622",
        "TAN with NULL"
    );
}

/// T622: ASIN - arcsine function
#[test]
fn t622_asin_basic() {
    assert_feature_supported!(
        "SELECT ASIN(a / 100.0) FROM t",
        "T622",
        "ASIN function"
    );
}

/// T622: ASIN with literal
#[test]
fn t622_asin_literal() {
    assert_feature_supported!(
        "SELECT ASIN(0.5) FROM t",
        "T622",
        "ASIN with literal"
    );
}

/// T622: ASIN with NULL
#[test]
fn t622_asin_null() {
    assert_feature_supported!(
        "SELECT ASIN(NULL) FROM t",
        "T622",
        "ASIN with NULL"
    );
}

/// T622: ACOS - arccosine function
#[test]
fn t622_acos_basic() {
    assert_feature_supported!(
        "SELECT ACOS(a / 100.0) FROM t",
        "T622",
        "ACOS function"
    );
}

/// T622: ACOS with literal
#[test]
fn t622_acos_literal() {
    assert_feature_supported!(
        "SELECT ACOS(0.5) FROM t",
        "T622",
        "ACOS with literal"
    );
}

/// T622: ACOS with NULL
#[test]
fn t622_acos_null() {
    assert_feature_supported!(
        "SELECT ACOS(NULL) FROM t",
        "T622",
        "ACOS with NULL"
    );
}

/// T622: ATAN - arctangent function
#[test]
fn t622_atan_basic() {
    assert_feature_supported!(
        "SELECT ATAN(a) FROM t",
        "T622",
        "ATAN function"
    );
}

/// T622: ATAN with literal
#[test]
fn t622_atan_literal() {
    assert_feature_supported!(
        "SELECT ATAN(1) FROM t",
        "T622",
        "ATAN with literal"
    );
}

/// T622: ATAN with NULL
#[test]
fn t622_atan_null() {
    assert_feature_supported!(
        "SELECT ATAN(NULL) FROM t",
        "T622",
        "ATAN with NULL"
    );
}

/// T622: ATAN2 - two-argument arctangent
#[test]
fn t622_atan2_basic() {
    assert_feature_supported!(
        "SELECT ATAN2(a, b) FROM t",
        "T622",
        "ATAN2 function"
    );
}

/// T622: ATAN2 with literals
#[test]
fn t622_atan2_literals() {
    assert_feature_supported!(
        "SELECT ATAN2(1, 1) FROM t",
        "T622",
        "ATAN2 with literals"
    );
}

/// T622: ATAN2 with NULL
#[test]
fn t622_atan2_null() {
    assert_feature_supported!(
        "SELECT ATAN2(NULL, b) FROM t",
        "T622",
        "ATAN2 with NULL"
    );
}

// ============================================================================
// Hyperbolic Functions (T622)
// ============================================================================

/// T622: SINH - hyperbolic sine
#[test]
fn t622_sinh_basic() {
    assert_feature_supported!(
        "SELECT SINH(a) FROM t",
        "T622",
        "SINH function"
    );
}

/// T622: SINH with literal
#[test]
fn t622_sinh_literal() {
    assert_feature_supported!(
        "SELECT SINH(1) FROM t",
        "T622",
        "SINH with literal"
    );
}

/// T622: SINH with NULL
#[test]
fn t622_sinh_null() {
    assert_feature_supported!(
        "SELECT SINH(NULL) FROM t",
        "T622",
        "SINH with NULL"
    );
}

/// T622: COSH - hyperbolic cosine
#[test]
fn t622_cosh_basic() {
    assert_feature_supported!(
        "SELECT COSH(a) FROM t",
        "T622",
        "COSH function"
    );
}

/// T622: COSH with literal
#[test]
fn t622_cosh_literal() {
    assert_feature_supported!(
        "SELECT COSH(1) FROM t",
        "T622",
        "COSH with literal"
    );
}

/// T622: COSH with NULL
#[test]
fn t622_cosh_null() {
    assert_feature_supported!(
        "SELECT COSH(NULL) FROM t",
        "T622",
        "COSH with NULL"
    );
}

/// T622: TANH - hyperbolic tangent
#[test]
fn t622_tanh_basic() {
    assert_feature_supported!(
        "SELECT TANH(a) FROM t",
        "T622",
        "TANH function"
    );
}

/// T622: TANH with literal
#[test]
fn t622_tanh_literal() {
    assert_feature_supported!(
        "SELECT TANH(1) FROM t",
        "T622",
        "TANH with literal"
    );
}

/// T622: TANH with NULL
#[test]
fn t622_tanh_null() {
    assert_feature_supported!(
        "SELECT TANH(NULL) FROM t",
        "T622",
        "TANH with NULL"
    );
}

// ============================================================================
// Angle Conversion Functions (T622)
// ============================================================================

/// T622: DEGREES - convert radians to degrees
#[test]
fn t622_degrees_basic() {
    assert_feature_supported!(
        "SELECT DEGREES(a) FROM t",
        "T622",
        "DEGREES function"
    );
}

/// T622: DEGREES with literal
#[test]
fn t622_degrees_literal() {
    assert_feature_supported!(
        "SELECT DEGREES(3.14159) FROM t",
        "T622",
        "DEGREES with literal"
    );
}

/// T622: DEGREES with NULL
#[test]
fn t622_degrees_null() {
    assert_feature_supported!(
        "SELECT DEGREES(NULL) FROM t",
        "T622",
        "DEGREES with NULL"
    );
}

/// T622: RADIANS - convert degrees to radians
#[test]
fn t622_radians_basic() {
    assert_feature_supported!(
        "SELECT RADIANS(a) FROM t",
        "T622",
        "RADIANS function"
    );
}

/// T622: RADIANS with literal
#[test]
fn t622_radians_literal() {
    assert_feature_supported!(
        "SELECT RADIANS(180) FROM t",
        "T622",
        "RADIANS with literal"
    );
}

/// T622: RADIANS with NULL
#[test]
fn t622_radians_null() {
    assert_feature_supported!(
        "SELECT RADIANS(NULL) FROM t",
        "T622",
        "RADIANS with NULL"
    );
}

// ============================================================================
// Statistical Functions
// ============================================================================

/// Statistical: RANDOM - random number generator
#[test]
fn stat_random_basic() {
    assert_feature_supported!(
        "SELECT RANDOM() FROM t",
        "T621",
        "RANDOM function"
    );
}

/// Statistical: RAND - random number (alternative)
#[test]
fn stat_rand_basic() {
    assert_feature_supported!(
        "SELECT RAND() FROM t",
        "T621",
        "RAND function"
    );
}

/// Statistical: RANDOM in expression
#[test]
fn stat_random_expression() {
    assert_feature_supported!(
        "SELECT RANDOM() * 100 FROM t",
        "T621",
        "RANDOM in expression"
    );
}

/// Statistical: GREATEST - maximum of values
#[test]
fn stat_greatest_basic() {
    assert_feature_supported!(
        "SELECT GREATEST(a, b) FROM t",
        "T621",
        "GREATEST function"
    );
}

/// Statistical: GREATEST with multiple values
#[test]
fn stat_greatest_multiple() {
    assert_feature_supported!(
        "SELECT GREATEST(a, b, 100, 200) FROM t",
        "T621",
        "GREATEST with multiple values"
    );
}

/// Statistical: GREATEST with NULL
#[test]
fn stat_greatest_null() {
    assert_feature_supported!(
        "SELECT GREATEST(a, NULL, b) FROM t",
        "T621",
        "GREATEST with NULL"
    );
}

/// Statistical: GREATEST with literals
#[test]
fn stat_greatest_literals() {
    assert_feature_supported!(
        "SELECT GREATEST(1, 2, 3, 4, 5) FROM t",
        "T621",
        "GREATEST with literals"
    );
}

/// Statistical: LEAST - minimum of values
#[test]
fn stat_least_basic() {
    assert_feature_supported!(
        "SELECT LEAST(a, b) FROM t",
        "T621",
        "LEAST function"
    );
}

/// Statistical: LEAST with multiple values
#[test]
fn stat_least_multiple() {
    assert_feature_supported!(
        "SELECT LEAST(a, b, 100, 200) FROM t",
        "T621",
        "LEAST with multiple values"
    );
}

/// Statistical: LEAST with NULL
#[test]
fn stat_least_null() {
    assert_feature_supported!(
        "SELECT LEAST(a, NULL, b) FROM t",
        "T621",
        "LEAST with NULL"
    );
}

/// Statistical: LEAST with literals
#[test]
fn stat_least_literals() {
    assert_feature_supported!(
        "SELECT LEAST(1, 2, 3, 4, 5) FROM t",
        "T621",
        "LEAST with literals"
    );
}

// ============================================================================
// Bit Manipulation Functions
// ============================================================================

/// Bit: BIT_AND aggregate function
#[test]
fn bit_and_aggregate() {
    assert_feature_supported!(
        "SELECT BIT_AND(a) FROM t",
        "T031",
        "BIT_AND aggregate"
    );
}

/// Bit: BIT_OR aggregate function
#[test]
fn bit_or_aggregate() {
    assert_feature_supported!(
        "SELECT BIT_OR(a) FROM t",
        "T031",
        "BIT_OR aggregate"
    );
}

/// Bit: BIT_XOR aggregate function
#[test]
fn bit_xor_aggregate() {
    assert_feature_supported!(
        "SELECT BIT_XOR(a) FROM t",
        "T031",
        "BIT_XOR aggregate"
    );
}

/// Bit: Bitwise AND operator
#[test]
fn bit_and_operator() {
    assert_feature_supported!(
        "SELECT a & b FROM t",
        "T031",
        "Bitwise AND operator"
    );
}

/// Bit: Bitwise OR operator
#[test]
fn bit_or_operator() {
    assert_feature_supported!(
        "SELECT a | b FROM t",
        "T031",
        "Bitwise OR operator"
    );
}

/// Bit: Bitwise XOR operator
#[test]
fn bit_xor_operator() {
    assert_feature_supported!(
        "SELECT a ^ b FROM t",
        "T031",
        "Bitwise XOR operator"
    );
}

/// Bit: Bitwise NOT operator
#[test]
fn bit_not_operator() {
    assert_feature_supported!(
        "SELECT ~a FROM t",
        "T031",
        "Bitwise NOT operator"
    );
}

/// Bit: Left shift operator
#[test]
fn bit_left_shift() {
    assert_feature_supported!(
        "SELECT a << 2 FROM t",
        "T031",
        "Left shift operator"
    );
}

/// Bit: Right shift operator
#[test]
fn bit_right_shift() {
    assert_feature_supported!(
        "SELECT a >> 2 FROM t",
        "T031",
        "Right shift operator"
    );
}

/// Bit: Complex bitwise expression
#[test]
fn bit_complex_expression() {
    assert_feature_supported!(
        "SELECT (a & b) | (c ^ 0xFF) FROM t",
        "T031",
        "Complex bitwise expression"
    );
}

/// Bit: Bitwise operators in WHERE clause
#[test]
fn bit_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a & 0x0F) = 0",
        "T031",
        "Bitwise operators in WHERE"
    );
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Utility: PI constant
#[test]
fn util_pi_basic() {
    assert_feature_supported!(
        "SELECT PI() FROM t",
        "T621",
        "PI function"
    );
}

/// Utility: PI in calculation
#[test]
fn util_pi_calculation() {
    assert_feature_supported!(
        "SELECT 2 * PI() * a FROM t",
        "T621",
        "PI in calculation"
    );
}

/// Utility: NULLIF - return NULL if equal
#[test]
fn util_nullif_basic() {
    assert_feature_supported!(
        "SELECT NULLIF(a, b) FROM t",
        "T621",
        "NULLIF function"
    );
}

/// Utility: NULLIF with literals
#[test]
fn util_nullif_literals() {
    assert_feature_supported!(
        "SELECT NULLIF(a, 0) FROM t",
        "T621",
        "NULLIF with literal"
    );
}

/// Utility: NULLIF with NULL
#[test]
fn util_nullif_null() {
    assert_feature_supported!(
        "SELECT NULLIF(NULL, a) FROM t",
        "T621",
        "NULLIF with NULL"
    );
}

/// Utility: COALESCE - return first non-NULL
#[test]
fn util_coalesce_basic() {
    assert_feature_supported!(
        "SELECT COALESCE(a, b) FROM t",
        "T621",
        "COALESCE function"
    );
}

/// Utility: COALESCE with multiple values
#[test]
fn util_coalesce_multiple() {
    assert_feature_supported!(
        "SELECT COALESCE(a, b, 0) FROM t",
        "T621",
        "COALESCE with multiple values"
    );
}

/// Utility: COALESCE with all NULLs
#[test]
fn util_coalesce_all_nulls() {
    assert_feature_supported!(
        "SELECT COALESCE(NULL, NULL, NULL) FROM t",
        "T621",
        "COALESCE with all NULLs"
    );
}

/// Utility: COALESCE with literals
#[test]
fn util_coalesce_literals() {
    assert_feature_supported!(
        "SELECT COALESCE(NULL, 'default', 'fallback') FROM t",
        "T621",
        "COALESCE with literals"
    );
}

/// Utility: NVL - Oracle-style NULL replacement
#[test]
fn util_nvl_basic() {
    assert_feature_supported!(
        "SELECT NVL(a, 0) FROM t",
        "T621",
        "NVL function"
    );
}

/// Utility: NVL with NULL
#[test]
fn util_nvl_null() {
    assert_feature_supported!(
        "SELECT NVL(NULL, b) FROM t",
        "T621",
        "NVL with NULL"
    );
}

/// Utility: IFNULL - MySQL-style NULL replacement
#[test]
fn util_ifnull_basic() {
    assert_feature_supported!(
        "SELECT IFNULL(a, 0) FROM t",
        "T621",
        "IFNULL function"
    );
}

/// Utility: IFNULL with NULL
#[test]
fn util_ifnull_null() {
    assert_feature_supported!(
        "SELECT IFNULL(NULL, b) FROM t",
        "T621",
        "IFNULL with NULL"
    );
}

/// Utility: WIDTH_BUCKET - histogram bucket
#[test]
fn util_width_bucket_basic() {
    assert_feature_supported!(
        "SELECT WIDTH_BUCKET(a, 0, 100, 10) FROM t",
        "T621",
        "WIDTH_BUCKET function"
    );
}

/// Utility: WIDTH_BUCKET with expression
#[test]
fn util_width_bucket_expression() {
    assert_feature_supported!(
        "SELECT WIDTH_BUCKET(price, 0.0, 1000.0, 20) FROM orders",
        "T621",
        "WIDTH_BUCKET with expression"
    );
}

/// Utility: WIDTH_BUCKET with NULL
#[test]
fn util_width_bucket_null() {
    assert_feature_supported!(
        "SELECT WIDTH_BUCKET(NULL, 0, 100, 10) FROM t",
        "T621",
        "WIDTH_BUCKET with NULL"
    );
}

// ============================================================================
// Combined and Complex Scenarios
// ============================================================================

/// Combined: Multiple math functions
#[test]
fn combined_multiple_functions() {
    assert_feature_supported!(
        "SELECT ABS(a), SIGN(b), ROUND(SQRT(a * a + b * b), 2) FROM t",
        "T621",
        "Multiple math functions"
    );
}

/// Combined: Trigonometric identity
#[test]
fn combined_trig_identity() {
    assert_feature_supported!(
        "SELECT SIN(a) * SIN(a) + COS(a) * COS(a) FROM t",
        "T622",
        "Trigonometric identity"
    );
}

/// Combined: Nested functions
#[test]
fn combined_nested_functions() {
    assert_feature_supported!(
        "SELECT CEIL(LOG10(ABS(a) + 1)) FROM t",
        "T621",
        "Nested math functions"
    );
}

/// Combined: Math in WHERE clause
#[test]
fn combined_math_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ABS(a - b) < 10",
        "T621",
        "Math functions in WHERE"
    );
}

/// Combined: Math in ORDER BY
#[test]
fn combined_math_in_order_by() {
    assert_feature_supported!(
        "SELECT a, b FROM t ORDER BY ABS(a - b) DESC",
        "T621",
        "Math functions in ORDER BY"
    );
}

/// Combined: Math with aggregates
#[test]
fn combined_math_with_aggregates() {
    assert_feature_supported!(
        "SELECT SQRT(SUM(a * a)) FROM t",
        "T621",
        "Math with aggregates"
    );
}

/// Combined: Math in GROUP BY
#[test]
fn combined_math_in_group_by() {
    assert_feature_supported!(
        "SELECT SIGN(a), COUNT(*) FROM t GROUP BY SIGN(a)",
        "T621",
        "Math functions in GROUP BY"
    );
}

/// Combined: Math in HAVING clause
#[test]
fn combined_math_in_having() {
    assert_feature_supported!(
        "SELECT a FROM t GROUP BY a HAVING ABS(SUM(b)) > 100",
        "T621",
        "Math functions in HAVING"
    );
}

/// Combined: Math with CASE expression
#[test]
fn combined_math_with_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 0 THEN SQRT(a) ELSE 0 END FROM t",
        "T621",
        "Math with CASE"
    );
}

/// Combined: Math in subquery
#[test]
fn combined_math_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > (SELECT AVG(ABS(b)) FROM t)",
        "T621",
        "Math in subquery"
    );
}

/// Combined: Distance calculation (Euclidean)
#[test]
fn combined_distance_calculation() {
    assert_feature_supported!(
        "SELECT SQRT(POWER(t1.a - t2.a, 2) + POWER(t1.b - t2.b, 2)) FROM t t1 CROSS JOIN t t2",
        "T621",
        "Distance calculation"
    );
}

/// Combined: Angle and distance (polar coordinates)
#[test]
fn combined_polar_coordinates() {
    assert_feature_supported!(
        "SELECT SQRT(a * a + b * b) AS r, ATAN2(b, a) AS theta FROM t",
        "T622",
        "Polar coordinates calculation"
    );
}

/// Combined: Statistical with COALESCE
#[test]
fn combined_coalesce_with_math() {
    assert_feature_supported!(
        "SELECT SQRT(COALESCE(a, 0)) FROM t",
        "T621",
        "COALESCE with math"
    );
}

/// Combined: NULLIF for division by zero protection
#[test]
fn combined_nullif_division() {
    assert_feature_supported!(
        "SELECT a / NULLIF(b, 0) FROM t",
        "T621",
        "NULLIF for division protection"
    );
}

/// Combined: Rounding chain
#[test]
fn combined_rounding_chain() {
    assert_feature_supported!(
        "SELECT CEIL(a), FLOOR(a), ROUND(a, 2), TRUNC(a, 1) FROM numeric_types",
        "T621",
        "Multiple rounding functions"
    );
}

/// Combined: Logarithm family
#[test]
fn combined_logarithms() {
    assert_feature_supported!(
        "SELECT LN(a), LOG10(a), LOG2(a), LOG(2, a) FROM t WHERE a > 0",
        "T623",
        "Multiple logarithm functions"
    );
}

/// Combined: All trigonometric functions
#[test]
fn combined_all_trig() {
    assert_feature_supported!(
        "SELECT SIN(a), COS(a), TAN(a), ASIN(a/10), ACOS(a/10), ATAN(a), ATAN2(a, b) FROM t",
        "T622",
        "All trigonometric functions"
    );
}

/// Combined: All hyperbolic functions
#[test]
fn combined_all_hyperbolic() {
    assert_feature_supported!(
        "SELECT SINH(a), COSH(a), TANH(a) FROM t",
        "T622",
        "All hyperbolic functions"
    );
}

/// Combined: Angle conversion round-trip
#[test]
fn combined_angle_conversion() {
    assert_feature_supported!(
        "SELECT DEGREES(RADIANS(a)) FROM t",
        "T622",
        "Angle conversion round-trip"
    );
}

/// Combined: Bit operations chain
#[test]
fn combined_bit_operations() {
    assert_feature_supported!(
        "SELECT a & b, a | b, a ^ b, ~a, a << 1, a >> 1 FROM t",
        "T031",
        "Multiple bit operations"
    );
}

/// Combined: GREATEST and LEAST together
#[test]
fn combined_greatest_least() {
    assert_feature_supported!(
        "SELECT GREATEST(a, b, 0), LEAST(a, b, 100) FROM t",
        "T621",
        "GREATEST and LEAST together"
    );
}

/// Combined: Complex numeric expression
#[test]
fn combined_complex_expression() {
    assert_feature_supported!(
        "SELECT ROUND(SQRT(POWER(ABS(a - b), 2) + POWER(ABS(COALESCE(a, 0)), 2)), 3) FROM t",
        "T621",
        "Complex nested numeric expression"
    );
}

/// Combined: Math functions with NULL handling
#[test]
fn combined_null_handling() {
    assert_feature_supported!(
        "SELECT COALESCE(SQRT(NULLIF(a, 0)), 0) FROM t",
        "T621",
        "Math with comprehensive NULL handling"
    );
}

/// Combined: Width bucket with binning
#[test]
fn combined_histogram_binning() {
    assert_feature_supported!(
        "SELECT WIDTH_BUCKET(price, 0, 1000, 10) AS bucket, COUNT(*) FROM orders GROUP BY WIDTH_BUCKET(price, 0, 1000, 10)",
        "T621",
        "Histogram binning with WIDTH_BUCKET"
    );
}

// ============================================================================
// Edge Cases and Special Values
// ============================================================================

/// Edge: Division by zero with NULLIF
#[test]
fn edge_division_by_zero() {
    assert_feature_supported!(
        "SELECT 10 / NULLIF(a, 0) FROM t",
        "T621",
        "Division by zero protection"
    );
}

/// Edge: SQRT of negative number handling
#[test]
fn edge_sqrt_negative() {
    assert_feature_supported!(
        "SELECT SQRT(a) FROM t WHERE a >= 0",
        "T621",
        "SQRT with non-negative filter"
    );
}

/// Edge: LOG of zero or negative
#[test]
fn edge_log_positive() {
    assert_feature_supported!(
        "SELECT LOG10(a) FROM t WHERE a > 0",
        "T623",
        "LOG with positive filter"
    );
}

/// Edge: Overflow protection with LEAST
#[test]
fn edge_overflow_protection() {
    assert_feature_supported!(
        "SELECT LEAST(a * b, 2147483647) FROM t",
        "T621",
        "Overflow protection with LEAST"
    );
}

/// Edge: Underflow handling with GREATEST
#[test]
fn edge_underflow_handling() {
    assert_feature_supported!(
        "SELECT GREATEST(a / 1000000, 0.000001) FROM t",
        "T621",
        "Underflow handling with GREATEST"
    );
}

/// Edge: Very small numbers with EXP
#[test]
fn edge_exp_small() {
    assert_feature_supported!(
        "SELECT EXP(-100) FROM t",
        "T621",
        "EXP with very small result"
    );
}

/// Edge: Very large numbers with POWER
#[test]
fn edge_power_large() {
    assert_feature_supported!(
        "SELECT POWER(2, 10) FROM t",
        "T621",
        "POWER with large result"
    );
}

/// Edge: ATAN2 with both zero
#[test]
fn edge_atan2_zero() {
    assert_feature_supported!(
        "SELECT ATAN2(0, 0) FROM t",
        "T622",
        "ATAN2 with both arguments zero"
    );
}

/// Edge: MOD with negative numbers
#[test]
fn edge_mod_negative() {
    assert_feature_supported!(
        "SELECT MOD(-17, 5), MOD(17, -5), MOD(-17, -5) FROM t",
        "T621",
        "MOD with negative arguments"
    );
}

/// Edge: ROUND with extreme decimal places
#[test]
fn edge_round_extreme() {
    assert_feature_supported!(
        "SELECT ROUND(3.14159265359, 10) FROM t",
        "T621",
        "ROUND with many decimal places"
    );
}
