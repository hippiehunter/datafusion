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

//! SQL:2016 Feature F201 - CAST function
//!
//! ISO/IEC 9075-2:2016 Section 6.13
//!
//! This feature covers the CAST function for explicit type conversion:
//! - CAST(expr AS type) syntax
//! - Casting between numeric types
//! - Casting between string and numeric types
//! - Casting between date/time types
//! - Casting to and from NULL
//!
//! F201 is a CORE feature (mandatory for SQL:2016 conformance).

use crate::assert_feature_supported;

// ============================================================================
// F201: Basic CAST syntax
// ============================================================================

/// F201: Basic CAST to INTEGER
#[test]
fn f201_cast_to_int() {
    assert_feature_supported!(
        "SELECT CAST(a AS INTEGER) FROM t",
        "F201",
        "CAST to INTEGER"
    );
}

/// F201: Basic CAST to VARCHAR
#[test]
fn f201_cast_to_varchar() {
    assert_feature_supported!(
        "SELECT CAST(a AS VARCHAR) FROM t",
        "F201",
        "CAST to VARCHAR"
    );
}

/// F201: CAST literal value
#[test]
fn f201_cast_literal() {
    assert_feature_supported!(
        "SELECT CAST(42 AS VARCHAR)",
        "F201",
        "CAST literal value"
    );
}

/// F201: CAST in WHERE clause
#[test]
fn f201_cast_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE CAST(c AS INTEGER) > 10",
        "F201",
        "CAST in WHERE clause"
    );
}

/// F201: CAST in ORDER BY
#[test]
fn f201_cast_in_order_by() {
    assert_feature_supported!(
        "SELECT * FROM t ORDER BY CAST(c AS INTEGER)",
        "F201",
        "CAST in ORDER BY"
    );
}

/// F201: CAST with AS alias
#[test]
fn f201_cast_with_alias() {
    assert_feature_supported!(
        "SELECT CAST(a AS VARCHAR) AS text_value FROM t",
        "F201",
        "CAST with column alias"
    );
}

// ============================================================================
// F201: Numeric type conversions
// ============================================================================

/// F201: CAST INT to BIGINT
#[test]
fn f201_cast_int_to_bigint() {
    assert_feature_supported!(
        "SELECT CAST(regular AS BIGINT) FROM numeric_types",
        "F201",
        "CAST INTEGER to BIGINT"
    );
}

/// F201: CAST BIGINT to INT
#[test]
fn f201_cast_bigint_to_int() {
    assert_feature_supported!(
        "SELECT CAST(big AS INTEGER) FROM numeric_types",
        "F201",
        "CAST BIGINT to INTEGER"
    );
}

/// F201: CAST INT to SMALLINT
#[test]
fn f201_cast_int_to_smallint() {
    assert_feature_supported!(
        "SELECT CAST(regular AS SMALLINT) FROM numeric_types",
        "F201",
        "CAST INTEGER to SMALLINT"
    );
}

/// F201: CAST INT to TINYINT
#[test]
fn f201_cast_int_to_tinyint() {
    assert_feature_supported!(
        "SELECT CAST(regular AS TINYINT) FROM numeric_types",
        "F201",
        "CAST INTEGER to TINYINT"
    );
}

/// F201: CAST between all integer types
#[test]
fn f201_cast_integer_chain() {
    assert_feature_supported!(
        "SELECT CAST(CAST(CAST(tiny AS SMALLINT) AS INTEGER) AS BIGINT) FROM numeric_types",
        "F201",
        "Chained integer CAST"
    );
}

/// F201: CAST INT to FLOAT
#[test]
fn f201_cast_int_to_float() {
    assert_feature_supported!(
        "SELECT CAST(regular AS FLOAT) FROM numeric_types",
        "F201",
        "CAST INTEGER to FLOAT"
    );
}

/// F201: CAST INT to DOUBLE
#[test]
fn f201_cast_int_to_double() {
    assert_feature_supported!(
        "SELECT CAST(regular AS DOUBLE) FROM numeric_types",
        "F201",
        "CAST INTEGER to DOUBLE"
    );
}

/// F201: CAST FLOAT to INT
#[test]
fn f201_cast_float_to_int() {
    assert_feature_supported!(
        "SELECT CAST(real_col AS INTEGER) FROM numeric_types",
        "F201",
        "CAST FLOAT to INTEGER"
    );
}

/// F201: CAST DOUBLE to INT
#[test]
fn f201_cast_double_to_int() {
    assert_feature_supported!(
        "SELECT CAST(double_col AS INTEGER) FROM numeric_types",
        "F201",
        "CAST DOUBLE to INTEGER"
    );
}

/// F201: CAST FLOAT to DOUBLE
#[test]
fn f201_cast_float_to_double() {
    assert_feature_supported!(
        "SELECT CAST(real_col AS DOUBLE) FROM numeric_types",
        "F201",
        "CAST FLOAT to DOUBLE"
    );
}

/// F201: CAST DOUBLE to FLOAT
#[test]
fn f201_cast_double_to_float() {
    assert_feature_supported!(
        "SELECT CAST(double_col AS FLOAT) FROM numeric_types",
        "F201",
        "CAST DOUBLE to FLOAT"
    );
}

/// F201: CAST to DECIMAL
#[test]
fn f201_cast_to_decimal() {
    assert_feature_supported!(
        "SELECT CAST(regular AS DECIMAL(10, 2)) FROM numeric_types",
        "F201",
        "CAST to DECIMAL with precision and scale"
    );
}

/// F201: CAST DECIMAL to INT
#[test]
fn f201_cast_decimal_to_int() {
    assert_feature_supported!(
        "SELECT CAST(decimal_col AS INTEGER) FROM numeric_types",
        "F201",
        "CAST DECIMAL to INTEGER"
    );
}

/// F201: CAST DECIMAL to FLOAT
#[test]
fn f201_cast_decimal_to_float() {
    assert_feature_supported!(
        "SELECT CAST(decimal_col AS FLOAT) FROM numeric_types",
        "F201",
        "CAST DECIMAL to FLOAT"
    );
}

/// F201: CAST literal numeric with precision
#[test]
fn f201_cast_literal_decimal() {
    assert_feature_supported!(
        "SELECT CAST(123.456 AS DECIMAL(5, 2))",
        "F201",
        "CAST literal to DECIMAL"
    );
}

// ============================================================================
// F201: String to numeric conversions
// ============================================================================

/// F201: CAST string to INTEGER
#[test]
fn f201_cast_string_to_int() {
    assert_feature_supported!(
        "SELECT CAST('123' AS INTEGER)",
        "F201",
        "CAST string to INTEGER"
    );
}

/// F201: CAST string to BIGINT
#[test]
fn f201_cast_string_to_bigint() {
    assert_feature_supported!(
        "SELECT CAST('9223372036854775807' AS BIGINT)",
        "F201",
        "CAST string to BIGINT"
    );
}

/// F201: CAST string to FLOAT
#[test]
fn f201_cast_string_to_float() {
    assert_feature_supported!(
        "SELECT CAST('123.45' AS FLOAT)",
        "F201",
        "CAST string to FLOAT"
    );
}

/// F201: CAST string to DOUBLE
#[test]
fn f201_cast_string_to_double() {
    assert_feature_supported!(
        "SELECT CAST('123.456789' AS DOUBLE)",
        "F201",
        "CAST string to DOUBLE"
    );
}

/// F201: CAST string to DECIMAL
#[test]
fn f201_cast_string_to_decimal() {
    assert_feature_supported!(
        "SELECT CAST('123.45' AS DECIMAL(10, 2))",
        "F201",
        "CAST string to DECIMAL"
    );
}

/// F201: CAST varchar column to numeric
#[test]
fn f201_cast_varchar_column_to_numeric() {
    assert_feature_supported!(
        "SELECT CAST(c AS INTEGER) FROM t WHERE CAST(c AS INTEGER) > 0",
        "F201",
        "CAST VARCHAR column to numeric"
    );
}

/// F201: CAST string with whitespace
#[test]
fn f201_cast_string_whitespace() {
    assert_feature_supported!(
        "SELECT CAST('  42  ' AS INTEGER)",
        "F201",
        "CAST string with whitespace to INTEGER"
    );
}

// ============================================================================
// F201: Numeric to string conversions
// ============================================================================

/// F201: CAST INTEGER to VARCHAR
#[test]
fn f201_cast_int_to_string() {
    assert_feature_supported!(
        "SELECT CAST(regular AS VARCHAR) FROM numeric_types",
        "F201",
        "CAST INTEGER to VARCHAR"
    );
}

/// F201: CAST BIGINT to VARCHAR
#[test]
fn f201_cast_bigint_to_string() {
    assert_feature_supported!(
        "SELECT CAST(big AS VARCHAR) FROM numeric_types",
        "F201",
        "CAST BIGINT to VARCHAR"
    );
}

/// F201: CAST FLOAT to VARCHAR
#[test]
fn f201_cast_float_to_string() {
    assert_feature_supported!(
        "SELECT CAST(real_col AS VARCHAR) FROM numeric_types",
        "F201",
        "CAST FLOAT to VARCHAR"
    );
}

/// F201: CAST DOUBLE to VARCHAR
#[test]
fn f201_cast_double_to_string() {
    assert_feature_supported!(
        "SELECT CAST(double_col AS VARCHAR) FROM numeric_types",
        "F201",
        "CAST DOUBLE to VARCHAR"
    );
}

/// F201: CAST DECIMAL to VARCHAR
#[test]
fn f201_cast_decimal_to_string() {
    assert_feature_supported!(
        "SELECT CAST(decimal_col AS VARCHAR) FROM numeric_types",
        "F201",
        "CAST DECIMAL to VARCHAR"
    );
}

/// F201: CAST negative number to string
#[test]
fn f201_cast_negative_to_string() {
    assert_feature_supported!(
        "SELECT CAST(-123 AS VARCHAR)",
        "F201",
        "CAST negative number to VARCHAR"
    );
}

/// F201: CAST with VARCHAR length
#[test]
fn f201_cast_varchar_length() {
    assert_feature_supported!(
        "SELECT CAST(regular AS VARCHAR(10)) FROM numeric_types",
        "F201",
        "CAST to VARCHAR with length"
    );
}

// ============================================================================
// F201: Date/Time conversions
// ============================================================================

/// F201: CAST string to DATE
#[test]
fn f201_cast_string_to_date() {
    assert_feature_supported!(
        "SELECT CAST('2024-01-15' AS DATE)",
        "F201",
        "CAST string to DATE"
    );
}

/// F201: CAST string to TIMESTAMP
#[test]
fn f201_cast_string_to_timestamp() {
    assert_feature_supported!(
        "SELECT CAST('2024-01-15 10:30:00' AS TIMESTAMP)",
        "F201",
        "CAST string to TIMESTAMP"
    );
}

/// F201: CAST DATE to VARCHAR
#[test]
fn f201_cast_date_to_string() {
    assert_feature_supported!(
        "SELECT CAST(date_col AS VARCHAR) FROM datetime_types",
        "F201",
        "CAST DATE to VARCHAR"
    );
}

/// F201: CAST TIMESTAMP to VARCHAR
#[test]
fn f201_cast_timestamp_to_string() {
    assert_feature_supported!(
        "SELECT CAST(timestamp_col AS VARCHAR) FROM datetime_types",
        "F201",
        "CAST TIMESTAMP to VARCHAR"
    );
}

/// F201: CAST TIMESTAMP to DATE
#[test]
fn f201_cast_timestamp_to_date() {
    assert_feature_supported!(
        "SELECT CAST(timestamp_col AS DATE) FROM datetime_types",
        "F201",
        "CAST TIMESTAMP to DATE"
    );
}

/// F201: CAST DATE to TIMESTAMP
#[test]
fn f201_cast_date_to_timestamp() {
    assert_feature_supported!(
        "SELECT CAST(date_col AS TIMESTAMP) FROM datetime_types",
        "F201",
        "CAST DATE to TIMESTAMP"
    );
}

/// F201: CAST string to TIME
#[test]
fn f201_cast_string_to_time() {
    assert_feature_supported!(
        "SELECT CAST('10:30:00' AS TIME)",
        "F201",
        "CAST string to TIME"
    );
}

/// F201: CAST TIME to VARCHAR
#[test]
fn f201_cast_time_to_string() {
    assert_feature_supported!(
        "SELECT CAST(time_col AS VARCHAR) FROM datetime_types",
        "F201",
        "CAST TIME to VARCHAR"
    );
}

// ============================================================================
// F201: Boolean conversions
// ============================================================================

/// F201: CAST boolean to INTEGER
#[test]
fn f201_cast_boolean_to_int() {
    assert_feature_supported!(
        "SELECT CAST(TRUE AS INTEGER)",
        "F201",
        "CAST BOOLEAN to INTEGER"
    );
}

/// F201: CAST INTEGER to boolean
#[test]
fn f201_cast_int_to_boolean() {
    assert_feature_supported!(
        "SELECT CAST(1 AS BOOLEAN)",
        "F201",
        "CAST INTEGER to BOOLEAN"
    );
}

/// F201: CAST boolean to VARCHAR
#[test]
fn f201_cast_boolean_to_string() {
    assert_feature_supported!(
        "SELECT CAST(TRUE AS VARCHAR)",
        "F201",
        "CAST BOOLEAN to VARCHAR"
    );
}

/// F201: CAST string to boolean
#[test]
fn f201_cast_string_to_boolean() {
    assert_feature_supported!(
        "SELECT CAST('true' AS BOOLEAN)",
        "F201",
        "CAST string to BOOLEAN"
    );
}

// ============================================================================
// F201: NULL handling
// ============================================================================

/// F201: CAST NULL to INTEGER
#[test]
fn f201_cast_null_to_int() {
    assert_feature_supported!(
        "SELECT CAST(NULL AS INTEGER)",
        "F201",
        "CAST NULL to INTEGER"
    );
}

/// F201: CAST NULL to VARCHAR
#[test]
fn f201_cast_null_to_varchar() {
    assert_feature_supported!(
        "SELECT CAST(NULL AS VARCHAR)",
        "F201",
        "CAST NULL to VARCHAR"
    );
}

/// F201: CAST NULL to DATE
#[test]
fn f201_cast_null_to_date() {
    assert_feature_supported!(
        "SELECT CAST(NULL AS DATE)",
        "F201",
        "CAST NULL to DATE"
    );
}

/// F201: CAST nullable column
#[test]
fn f201_cast_nullable_column() {
    assert_feature_supported!(
        "SELECT CAST(a AS VARCHAR) FROM t WHERE a IS NOT NULL",
        "F201",
        "CAST nullable column"
    );
}

// ============================================================================
// F201: CAST in expressions
// ============================================================================

/// F201: CAST in arithmetic expression
#[test]
fn f201_cast_in_arithmetic() {
    assert_feature_supported!(
        "SELECT CAST(a AS FLOAT) + CAST(b AS FLOAT) FROM t",
        "F201",
        "CAST in arithmetic expression"
    );
}

/// F201: CAST result of arithmetic
#[test]
fn f201_cast_arithmetic_result() {
    assert_feature_supported!(
        "SELECT CAST(a + b AS VARCHAR) FROM t",
        "F201",
        "CAST result of arithmetic"
    );
}

/// F201: CAST in comparison
#[test]
fn f201_cast_in_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE CAST(c AS INTEGER) > CAST(a AS INTEGER)",
        "F201",
        "CAST in comparison"
    );
}

/// F201: CAST in CASE expression
#[test]
fn f201_cast_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN CAST(c AS INTEGER) > 10 THEN 'high' ELSE 'low' END FROM t",
        "F201",
        "CAST in CASE expression"
    );
}

/// F201: CAST of CASE result
#[test]
fn f201_cast_case_result() {
    assert_feature_supported!(
        "SELECT CAST(CASE WHEN a > 10 THEN a ELSE b END AS VARCHAR) FROM t",
        "F201",
        "CAST of CASE result"
    );
}

/// F201: CAST in aggregate function
#[test]
fn f201_cast_in_aggregate() {
    assert_feature_supported!(
        "SELECT AVG(CAST(c AS DOUBLE)) FROM t",
        "F201",
        "CAST in aggregate function"
    );
}

/// F201: CAST aggregate result
#[test]
fn f201_cast_aggregate_result() {
    assert_feature_supported!(
        "SELECT CAST(AVG(a) AS INTEGER) FROM t",
        "F201",
        "CAST aggregate result"
    );
}

// ============================================================================
// F201: Nested CAST
// ============================================================================

/// F201: Double CAST
#[test]
fn f201_double_cast() {
    assert_feature_supported!(
        "SELECT CAST(CAST(a AS VARCHAR) AS INTEGER) FROM t",
        "F201",
        "Double CAST (round-trip)"
    );
}

/// F201: Triple CAST
#[test]
fn f201_triple_cast() {
    assert_feature_supported!(
        "SELECT CAST(CAST(CAST(regular AS VARCHAR) AS FLOAT) AS INTEGER) FROM numeric_types",
        "F201",
        "Triple CAST"
    );
}

/// F201: CAST in subquery
#[test]
fn f201_cast_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > (SELECT CAST(AVG(b) AS INTEGER) FROM t)",
        "F201",
        "CAST in scalar subquery"
    );
}

// ============================================================================
// F201: CAST with JOIN
// ============================================================================

/// F201: CAST in JOIN condition
#[test]
fn f201_cast_in_join() {
    assert_feature_supported!(
        "SELECT * FROM t1 JOIN t2 ON CAST(t1.c AS INTEGER) = t2.a",
        "F201",
        "CAST in JOIN condition"
    );
}

/// F201: CAST both sides of JOIN
#[test]
fn f201_cast_both_join_sides() {
    assert_feature_supported!(
        "SELECT * FROM t1 JOIN t2 ON CAST(t1.a AS VARCHAR) = CAST(t2.a AS VARCHAR)",
        "F201",
        "CAST both sides of JOIN"
    );
}

/// F201: CAST in joined column selection
#[test]
fn f201_cast_joined_columns() {
    assert_feature_supported!(
        "SELECT CAST(t1.a AS VARCHAR) AS t1_str, CAST(t2.a AS VARCHAR) AS t2_str FROM t1 JOIN t2 ON t1.b = t2.b",
        "F201",
        "CAST columns from joined tables"
    );
}

// ============================================================================
// F201: CAST with GROUP BY and HAVING
// ============================================================================

/// F201: CAST in GROUP BY
#[test]
fn f201_cast_in_group_by() {
    assert_feature_supported!(
        "SELECT CAST(a AS VARCHAR), COUNT(*) FROM t GROUP BY CAST(a AS VARCHAR)",
        "F201",
        "CAST in GROUP BY"
    );
}

/// F201: CAST in HAVING
#[test]
fn f201_cast_in_having() {
    assert_feature_supported!(
        "SELECT state, COUNT(*) FROM person GROUP BY state HAVING CAST(COUNT(*) AS FLOAT) > 10.5",
        "F201",
        "CAST in HAVING clause"
    );
}

/// F201: CAST aggregate in HAVING
#[test]
fn f201_cast_aggregate_having() {
    assert_feature_supported!(
        "SELECT state, AVG(salary) FROM person GROUP BY state HAVING CAST(AVG(salary) AS INTEGER) > 50000",
        "F201",
        "CAST aggregate in HAVING"
    );
}

// ============================================================================
// F201: TRY_CAST (if supported)
// ============================================================================

/// F201: TRY_CAST basic usage
#[test]
fn f201_try_cast_basic() {
    assert_feature_supported!(
        "SELECT TRY_CAST(c AS INTEGER) FROM t",
        "F201",
        "TRY_CAST to INTEGER"
    );
}

/// F201: TRY_CAST invalid string
#[test]
fn f201_try_cast_invalid() {
    assert_feature_supported!(
        "SELECT TRY_CAST('invalid' AS INTEGER)",
        "F201",
        "TRY_CAST invalid string to INTEGER"
    );
}

/// F201: TRY_CAST with COALESCE
#[test]
fn f201_try_cast_coalesce() {
    assert_feature_supported!(
        "SELECT COALESCE(TRY_CAST(c AS INTEGER), 0) FROM t",
        "F201",
        "TRY_CAST with COALESCE for default"
    );
}

/// F201: TRY_CAST in WHERE
#[test]
fn f201_try_cast_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE TRY_CAST(c AS INTEGER) IS NOT NULL",
        "F201",
        "TRY_CAST in WHERE clause"
    );
}

// ============================================================================
// F201: Type aliases and variants
// ============================================================================

/// F201: CAST to INT (alias for INTEGER)
#[test]
fn f201_cast_to_int_alias() {
    assert_feature_supported!(
        "SELECT CAST(a AS INT) FROM t",
        "F201",
        "CAST to INT (INTEGER alias)"
    );
}

/// F201: CAST to CHAR
#[test]
fn f201_cast_to_char() {
    assert_feature_supported!(
        "SELECT CAST(a AS CHAR(10)) FROM t",
        "F201",
        "CAST to CHAR with length"
    );
}

/// F201: CAST to TEXT
#[test]
fn f201_cast_to_text() {
    assert_feature_supported!(
        "SELECT CAST(a AS TEXT) FROM t",
        "F201",
        "CAST to TEXT"
    );
}

/// F201: CAST to REAL (alias for FLOAT)
#[test]
fn f201_cast_to_real() {
    assert_feature_supported!(
        "SELECT CAST(regular AS REAL) FROM numeric_types",
        "F201",
        "CAST to REAL (FLOAT alias)"
    );
}

/// F201: CAST to DOUBLE PRECISION
#[test]
fn f201_cast_to_double_precision() {
    assert_feature_supported!(
        "SELECT CAST(regular AS DOUBLE PRECISION) FROM numeric_types",
        "F201",
        "CAST to DOUBLE PRECISION"
    );
}

// ============================================================================
// F201: CAST with special numeric values
// ============================================================================

/// F201: CAST zero
#[test]
fn f201_cast_zero() {
    assert_feature_supported!(
        "SELECT CAST(0 AS VARCHAR)",
        "F201",
        "CAST zero to VARCHAR"
    );
}

/// F201: CAST negative number
#[test]
fn f201_cast_negative() {
    assert_feature_supported!(
        "SELECT CAST(-123.45 AS INTEGER)",
        "F201",
        "CAST negative FLOAT to INTEGER"
    );
}

/// F201: CAST scientific notation
#[test]
fn f201_cast_scientific() {
    assert_feature_supported!(
        "SELECT CAST(1.23e5 AS INTEGER)",
        "F201",
        "CAST scientific notation to INTEGER"
    );
}

// ============================================================================
// F201: Complex real-world scenarios
// ============================================================================

/// F201: Multiple CAST in complex query
#[test]
fn f201_combined_multiple_casts() {
    assert_feature_supported!(
        "SELECT \
         CAST(id AS VARCHAR) AS id_str, \
         CAST(age AS FLOAT) / CAST(12 AS FLOAT) AS age_in_years, \
         CAST(salary AS DECIMAL(10, 2)) AS formatted_salary, \
         CAST(birth_date AS VARCHAR) AS birth_date_str \
         FROM person \
         WHERE CAST(state AS VARCHAR) IN ('CA', 'NY') \
         ORDER BY CAST(salary AS INTEGER) DESC",
        "F201",
        "Multiple CAST in complex query"
    );
}

/// F201: CAST with type conversion chain
#[test]
fn f201_combined_conversion_chain() {
    assert_feature_supported!(
        "SELECT \
         a, \
         CAST(a AS VARCHAR) AS a_str, \
         CAST(CAST(a AS VARCHAR) AS INTEGER) AS round_trip, \
         CAST(CAST(CAST(a AS FLOAT) AS VARCHAR) AS DOUBLE) AS multi_convert \
         FROM t \
         WHERE CAST(b AS VARCHAR) LIKE '%5%'",
        "F201",
        "Type conversion chain"
    );
}

/// F201: CAST in analytical query
#[test]
fn f201_combined_analytical() {
    assert_feature_supported!(
        "SELECT \
         state, \
         CAST(COUNT(*) AS VARCHAR) AS count_str, \
         CAST(AVG(salary) AS DECIMAL(10, 2)) AS avg_salary, \
         CAST(MIN(age) AS VARCHAR) || '-' || CAST(MAX(age) AS VARCHAR) AS age_range \
         FROM person \
         GROUP BY state \
         HAVING CAST(AVG(salary) AS INTEGER) > 50000 \
         ORDER BY CAST(COUNT(*) AS FLOAT) DESC",
        "F201",
        "CAST in analytical query with aggregates"
    );
}

/// F201: CAST for data cleaning
#[test]
fn f201_combined_data_cleaning() {
    assert_feature_supported!(
        "SELECT \
         COALESCE(TRY_CAST(c AS INTEGER), 0) AS cleaned_int, \
         CASE \
           WHEN TRY_CAST(c AS INTEGER) IS NULL THEN 'Invalid' \
           ELSE CAST(CAST(c AS INTEGER) AS VARCHAR) \
         END AS validation_result \
         FROM t",
        "F201",
        "CAST for data cleaning and validation"
    );
}

/// F201: CAST with date arithmetic
#[test]
fn f201_combined_date_arithmetic() {
    assert_feature_supported!(
        "SELECT \
         birth_date, \
         CAST(birth_date AS DATE) AS birth_date_only, \
         CAST(CAST(birth_date AS DATE) AS VARCHAR) AS formatted_date \
         FROM person \
         WHERE CAST(birth_date AS DATE) > CAST('1990-01-01' AS DATE)",
        "F201",
        "CAST with date operations"
    );
}

/// F201: Cross-type joins with CAST
#[test]
fn f201_combined_cross_type_join() {
    assert_feature_supported!(
        "SELECT \
         p.first_name, \
         o.order_id, \
         CAST(p.id AS VARCHAR) AS person_id_str, \
         CAST(o.customer_id AS VARCHAR) AS customer_id_str \
         FROM person p \
         JOIN orders o ON CAST(p.id AS VARCHAR) = CAST(o.customer_id AS VARCHAR) \
         WHERE CAST(o.qty AS FLOAT) > 0.5",
        "F201",
        "Cross-type joins with CAST"
    );
}

/// F201: CAST in window function context
#[test]
fn f201_combined_window_function() {
    assert_feature_supported!(
        "SELECT \
         first_name, \
         salary, \
         CAST(salary AS INTEGER) AS salary_int \
         FROM person \
         ORDER BY CAST(age AS FLOAT) / CAST(100 AS FLOAT)",
        "F201",
        "CAST in window function context"
    );
}

/// F201: String concatenation with CAST
#[test]
fn f201_combined_string_concat() {
    assert_feature_supported!(
        "SELECT \
         first_name || ' (Age: ' || CAST(age AS VARCHAR) || ', Salary: $' || CAST(salary AS VARCHAR) || ')' AS full_info \
         FROM person",
        "F201",
        "String concatenation with CAST"
    );
}
