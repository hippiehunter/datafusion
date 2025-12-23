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

//! SQL:2016 Feature E011 - Numeric data types
//!
//! ISO/IEC 9075-2:2016 Section 4.4
//!
//! This feature covers the basic numeric data types required by Core SQL:
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E011-01 | INTEGER and SMALLINT | Supported |
//! | E011-02 | REAL, DOUBLE PRECISION, FLOAT | Supported |
//! | E011-03 | DECIMAL and NUMERIC | Supported |
//! | E011-04 | Arithmetic operators | Supported |
//! | E011-05 | Numeric comparison | Supported |
//! | E011-06 | Implicit casting among numeric types | Supported |
//!
//! All E011 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// E011-01: INTEGER and SMALLINT data types
// ============================================================================

/// E011-01: INTEGER data type in column definition
#[test]
fn e011_01_integer_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x INTEGER)",
        "E011-01",
        "INTEGER data type"
    );
}

/// E011-01: INT abbreviation for INTEGER
#[test]
fn e011_01_int_abbreviation() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT)",
        "E011-01",
        "INT abbreviation"
    );
}

/// E011-01: SMALLINT data type in column definition
#[test]
fn e011_01_smallint_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x SMALLINT)",
        "E011-01",
        "SMALLINT data type"
    );
}

/// E011-01: INTEGER literal in SELECT
#[test]
fn e011_01_integer_literal() {
    assert_feature_supported!(
        "SELECT 42",
        "E011-01",
        "INTEGER literal"
    );
}

/// E011-01: Negative integer literal
#[test]
fn e011_01_negative_integer() {
    assert_feature_supported!(
        "SELECT -42",
        "E011-01",
        "Negative integer literal"
    );
}

// ============================================================================
// E011-02: REAL, DOUBLE PRECISION, and FLOAT data types
// ============================================================================

/// E011-02: REAL data type in column definition
#[test]
fn e011_02_real_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x REAL)",
        "E011-02",
        "REAL data type"
    );
}

/// E011-02: DOUBLE PRECISION data type in column definition
#[test]
fn e011_02_double_precision_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x DOUBLE PRECISION)",
        "E011-02",
        "DOUBLE PRECISION data type"
    );
}

/// E011-02: DOUBLE as abbreviation for DOUBLE PRECISION
#[test]
fn e011_02_double_abbreviation() {
    assert_feature_supported!(
        "CREATE TABLE t (x DOUBLE)",
        "E011-02",
        "DOUBLE abbreviation"
    );
}

/// E011-02: FLOAT data type in column definition
#[test]
fn e011_02_float_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x FLOAT)",
        "E011-02",
        "FLOAT data type"
    );
}

/// E011-02: FLOAT with precision
#[test]
fn e011_02_float_with_precision() {
    assert_feature_supported!(
        "CREATE TABLE t (x FLOAT(24))",
        "E011-02",
        "FLOAT with precision"
    );
}

/// E011-02: Floating-point literal
#[test]
fn e011_02_float_literal() {
    assert_feature_supported!(
        "SELECT 3.14159",
        "E011-02",
        "Floating-point literal"
    );
}

/// E011-02: Scientific notation literal
#[test]
fn e011_02_scientific_notation() {
    assert_feature_supported!(
        "SELECT 1.23E10",
        "E011-02",
        "Scientific notation"
    );
}

// ============================================================================
// E011-03: DECIMAL and NUMERIC data types
// ============================================================================

/// E011-03: DECIMAL data type without precision
#[test]
fn e011_03_decimal_no_precision() {
    assert_feature_supported!(
        "CREATE TABLE t (x DECIMAL)",
        "E011-03",
        "DECIMAL without precision"
    );
}

/// E011-03: DECIMAL with precision only
#[test]
fn e011_03_decimal_precision() {
    assert_feature_supported!(
        "CREATE TABLE t (x DECIMAL(10))",
        "E011-03",
        "DECIMAL with precision"
    );
}

/// E011-03: DECIMAL with precision and scale
#[test]
fn e011_03_decimal_precision_scale() {
    assert_feature_supported!(
        "CREATE TABLE t (x DECIMAL(10, 2))",
        "E011-03",
        "DECIMAL with precision and scale"
    );
}

/// E011-03: NUMERIC data type (synonym for DECIMAL)
#[test]
fn e011_03_numeric_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x NUMERIC(10, 2))",
        "E011-03",
        "NUMERIC data type"
    );
}

/// E011-03: DEC abbreviation for DECIMAL
#[test]
fn e011_03_dec_abbreviation() {
    // GAP: DataFusion does not currently support the DEC abbreviation
    assert_feature_supported!(
        "CREATE TABLE t (x DEC(10, 2))",
        "E011-03",
        "DEC abbreviation"
    );
}

// ============================================================================
// E011-04: Arithmetic operators
// ============================================================================

/// E011-04: Addition operator
#[test]
fn e011_04_addition() {
    assert_feature_supported!(
        "SELECT 1 + 2",
        "E011-04",
        "Addition operator"
    );
}

/// E011-04: Subtraction operator
#[test]
fn e011_04_subtraction() {
    assert_feature_supported!(
        "SELECT 5 - 3",
        "E011-04",
        "Subtraction operator"
    );
}

/// E011-04: Multiplication operator
#[test]
fn e011_04_multiplication() {
    assert_feature_supported!(
        "SELECT 4 * 3",
        "E011-04",
        "Multiplication operator"
    );
}

/// E011-04: Division operator
#[test]
fn e011_04_division() {
    assert_feature_supported!(
        "SELECT 10 / 2",
        "E011-04",
        "Division operator"
    );
}

/// E011-04: Modulo operator (standard uses MOD function, but % is common)
#[test]
fn e011_04_modulo() {
    assert_feature_supported!(
        "SELECT 10 % 3",
        "E011-04",
        "Modulo operator"
    );
}

/// E011-04: Unary plus
#[test]
fn e011_04_unary_plus() {
    assert_feature_supported!(
        "SELECT +42",
        "E011-04",
        "Unary plus"
    );
}

/// E011-04: Unary minus
#[test]
fn e011_04_unary_minus() {
    assert_feature_supported!(
        "SELECT -42",
        "E011-04",
        "Unary minus"
    );
}

/// E011-04: Arithmetic with columns
#[test]
fn e011_04_column_arithmetic() {
    assert_feature_supported!(
        "SELECT a + b, a - b, a * b FROM t",
        "E011-04",
        "Column arithmetic"
    );
}

/// E011-04: Complex expression with parentheses
#[test]
fn e011_04_complex_expression() {
    assert_feature_supported!(
        "SELECT (regular + big) * (small - 1) FROM numeric_types",
        "E011-04",
        "Complex expression"
    );
}

// ============================================================================
// E011-05: Numeric comparison
// ============================================================================

/// E011-05: Equality comparison
#[test]
fn e011_05_equality() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a = 42",
        "E011-05",
        "Equality comparison"
    );
}

/// E011-05: Inequality comparison (<>)
#[test]
fn e011_05_inequality() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a <> 42",
        "E011-05",
        "Inequality comparison"
    );
}

/// E011-05: Not equal comparison (!=)
#[test]
fn e011_05_not_equal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a != 42",
        "E011-05",
        "Not equal comparison"
    );
}

/// E011-05: Less than comparison
#[test]
fn e011_05_less_than() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a < 42",
        "E011-05",
        "Less than comparison"
    );
}

/// E011-05: Less than or equal comparison
#[test]
fn e011_05_less_than_or_equal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a <= 42",
        "E011-05",
        "Less than or equal comparison"
    );
}

/// E011-05: Greater than comparison
#[test]
fn e011_05_greater_than() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > 42",
        "E011-05",
        "Greater than comparison"
    );
}

/// E011-05: Greater than or equal comparison
#[test]
fn e011_05_greater_than_or_equal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a >= 42",
        "E011-05",
        "Greater than or equal comparison"
    );
}

/// E011-05: Comparison between columns
#[test]
fn e011_05_column_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > b",
        "E011-05",
        "Column comparison"
    );
}

// ============================================================================
// E011-06: Implicit casting among numeric data types
// ============================================================================

/// E011-06: Integer to float comparison (implicit cast)
#[test]
fn e011_06_int_float_comparison() {
    assert_feature_supported!(
        "SELECT * FROM numeric_types WHERE regular = real_col",
        "E011-06",
        "Integer to float comparison"
    );
}

/// E011-06: Integer to decimal comparison (implicit cast)
#[test]
fn e011_06_int_decimal_comparison() {
    assert_feature_supported!(
        "SELECT * FROM numeric_types WHERE regular = decimal_col",
        "E011-06",
        "Integer to decimal comparison"
    );
}

/// E011-06: Mixed numeric arithmetic (implicit cast)
#[test]
fn e011_06_mixed_arithmetic() {
    assert_feature_supported!(
        "SELECT regular + real_col FROM numeric_types",
        "E011-06",
        "Mixed numeric arithmetic"
    );
}

/// E011-06: Integer literal compared to decimal column
#[test]
fn e011_06_literal_decimal_comparison() {
    assert_feature_supported!(
        "SELECT * FROM numeric_types WHERE decimal_col > 100",
        "E011-06",
        "Literal to decimal comparison"
    );
}

// ============================================================================
// Additional numeric tests (T071 BIGINT - optional but commonly needed)
// ============================================================================

/// T071: BIGINT data type (optional feature, but widely used)
#[test]
fn t071_bigint_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x BIGINT)",
        "T071",
        "BIGINT data type"
    );
}

/// T071: BIGINT literal (large number)
#[test]
fn t071_bigint_literal() {
    assert_feature_supported!(
        "SELECT 9223372036854775807",
        "T071",
        "BIGINT literal"
    );
}

// ============================================================================
// Summary Tests - Verify overall E011 support
// ============================================================================

#[test]
fn e011_summary_all_subfeatures() {
    // This test verifies that all E011 subfeatures work together
    // in a realistic scenario

    // Create table with all numeric types
    assert_plans!("CREATE TABLE all_nums (
        int_col INTEGER,
        small_col SMALLINT,
        big_col BIGINT,
        real_col REAL,
        double_col DOUBLE PRECISION,
        float_col FLOAT,
        decimal_col DECIMAL(10, 2),
        numeric_col NUMERIC(8, 4)
    )");

    // Query with arithmetic and comparisons using the numeric_types table columns
    assert_plans!(
        "SELECT regular + small AS sum_result,
                double_col * decimal_col AS product,
                big / 2 AS half
         FROM numeric_types
         WHERE regular > 0 AND decimal_col < 1000.00"
    );
}
