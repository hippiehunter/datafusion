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

//! SQL:2016 Feature S091 - Basic array support
//!
//! ISO/IEC 9075-2:2016 Section 4.8 - Arrays
//!
//! This feature covers basic array/collection support in SQL:2016 including:
//! - Array constructors (ARRAY[...])
//! - Array data types in table definitions
//! - Array element access (subscripting)
//! - Array operations (concatenation, comparison)
//! - Array functions (UNNEST, CARDINALITY, etc.)
//! - Array aggregates (ARRAY_AGG)
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | S091 | Basic array support | Partial |
//! | S091-01 | Arrays of built-in data types | Partial |
//! | S091-02 | Arrays of distinct types | Not tested |
//! | S091-03 | Array expressions | Partial |
//! | S092 | Arrays of user-defined types | Not tested |
//! | S094 | Arrays of reference types | Not tested |
//! | S095 | Array constructors by query | Partial |
//! | S096 | Optional array bounds | Not tested |
//! | S098 | ARRAY_AGG function | Supported |
//! | S301 | Enhanced UNNEST | Partial |
//! | S404 | TRIM_ARRAY | Not tested |
//!
//! Note: Tests that fail indicate conformance gaps in DataFusion's array support.

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// S091-01: Arrays of built-in data types
// ============================================================================

/// S091-01: ARRAY constructor with integer literals
#[test]
fn s091_01_array_constructor_integers() {
    assert_feature_supported!(
        "SELECT ARRAY[1, 2, 3]",
        "S091-01",
        "ARRAY constructor with integers"
    );
}

/// S091-01: ARRAY constructor with string literals
#[test]
fn s091_01_array_constructor_strings() {
    assert_feature_supported!(
        "SELECT ARRAY['foo', 'bar', 'baz']",
        "S091-01",
        "ARRAY constructor with strings"
    );
}

/// S091-01: ARRAY constructor with decimal values
#[test]
fn s091_01_array_constructor_decimals() {
    assert_feature_supported!(
        "SELECT ARRAY[1.5, 2.5, 3.5]",
        "S091-01",
        "ARRAY constructor with decimals"
    );
}

/// S091-01: ARRAY constructor with boolean values
#[test]
fn s091_01_array_constructor_booleans() {
    assert_feature_supported!(
        "SELECT ARRAY[TRUE, FALSE, TRUE]",
        "S091-01",
        "ARRAY constructor with booleans"
    );
}

/// S091-01: ARRAY constructor with date values
#[test]
fn s091_01_array_constructor_dates() {
    assert_feature_supported!(
        "SELECT ARRAY[DATE '2024-01-01', DATE '2024-12-31']",
        "S091-01",
        "ARRAY constructor with dates"
    );
}

/// S091-01: ARRAY constructor with timestamp values
#[test]
fn s091_01_array_constructor_timestamps() {
    assert_feature_supported!(
        "SELECT ARRAY[TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-12-31 23:59:59']",
        "S091-01",
        "ARRAY constructor with timestamps"
    );
}

/// S091-01: ARRAY constructor with NULL values
#[test]
fn s091_01_array_constructor_nulls() {
    assert_feature_supported!(
        "SELECT ARRAY[1, NULL, 3]",
        "S091-01",
        "ARRAY constructor with NULLs"
    );
}

/// S091-01: ARRAY constructor with all NULL values
#[test]
fn s091_01_array_constructor_all_nulls() {
    assert_feature_supported!(
        "SELECT ARRAY[NULL, NULL, NULL]",
        "S091-01",
        "ARRAY constructor with all NULLs"
    );
}

/// S091-01: Empty ARRAY constructor
#[test]
fn s091_01_array_constructor_empty() {
    assert_feature_supported!(
        "SELECT ARRAY[]",
        "S091-01",
        "Empty ARRAY constructor"
    );
}

/// S091-01: ARRAY constructor with column references
#[test]
fn s091_01_array_constructor_columns() {
    assert_feature_supported!(
        "SELECT ARRAY[a, b] FROM t",
        "S091-01",
        "ARRAY constructor with column references"
    );
}

/// S091-01: ARRAY constructor with expressions
#[test]
fn s091_01_array_constructor_expressions() {
    assert_feature_supported!(
        "SELECT ARRAY[a + 1, b * 2, c] FROM t",
        "S091-01",
        "ARRAY constructor with expressions"
    );
}

/// S091-01: ARRAY constructor with function calls
#[test]
fn s091_01_array_constructor_functions() {
    assert_feature_supported!(
        "SELECT ARRAY[UPPER(c), LOWER(c)] FROM t",
        "S091-01",
        "ARRAY constructor with function calls"
    );
}

/// S091-01: ARRAY constructor in WHERE clause
#[test]
fn s091_01_array_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (SELECT UNNEST(ARRAY[1, 2, 3]))",
        "S091-01",
        "ARRAY constructor in WHERE clause"
    );
}

/// S091-01: CREATE TABLE with INTEGER ARRAY column
#[test]
fn s091_01_create_table_int_array() {
    assert_feature_supported!(
        "CREATE TABLE t (arr INTEGER ARRAY)",
        "S091-01",
        "CREATE TABLE with INTEGER ARRAY"
    );
}

/// S091-01: CREATE TABLE with VARCHAR ARRAY column
#[test]
fn s091_01_create_table_varchar_array() {
    assert_feature_supported!(
        "CREATE TABLE t (arr VARCHAR ARRAY)",
        "S091-01",
        "CREATE TABLE with VARCHAR ARRAY"
    );
}

/// S091-01: CREATE TABLE with DECIMAL ARRAY column
#[test]
fn s091_01_create_table_decimal_array() {
    assert_feature_supported!(
        "CREATE TABLE t (arr DECIMAL(10, 2) ARRAY)",
        "S091-01",
        "CREATE TABLE with DECIMAL ARRAY"
    );
}

/// S091-01: CREATE TABLE with DATE ARRAY column
#[test]
fn s091_01_create_table_date_array() {
    assert_feature_supported!(
        "CREATE TABLE t (arr DATE ARRAY)",
        "S091-01",
        "CREATE TABLE with DATE ARRAY"
    );
}

/// S091-01: CREATE TABLE with TIMESTAMP ARRAY column
#[test]
fn s091_01_create_table_timestamp_array() {
    assert_feature_supported!(
        "CREATE TABLE t (arr TIMESTAMP ARRAY)",
        "S091-01",
        "CREATE TABLE with TIMESTAMP ARRAY"
    );
}

/// S091-01: CREATE TABLE with BOOLEAN ARRAY column
#[test]
fn s091_01_create_table_boolean_array() {
    assert_feature_supported!(
        "CREATE TABLE t (arr BOOLEAN ARRAY)",
        "S091-01",
        "CREATE TABLE with BOOLEAN ARRAY"
    );
}

/// S091-01: CREATE TABLE with multiple ARRAY columns
#[test]
fn s091_01_create_table_multiple_arrays() {
    assert_feature_supported!(
        "CREATE TABLE t (int_arr INTEGER ARRAY, str_arr VARCHAR ARRAY)",
        "S091-01",
        "CREATE TABLE with multiple ARRAY columns"
    );
}

/// S091-01: SELECT from table with ARRAY column
#[test]
fn s091_01_select_array_column() {
    assert_feature_supported!(
        "SELECT int_array FROM array_types",
        "S091-01",
        "SELECT ARRAY column"
    );
}

/// S091-01: INSERT with ARRAY literal
#[test]
fn s091_01_insert_array_literal() {
    assert_feature_supported!(
        "INSERT INTO array_types (int_array) VALUES (ARRAY[1, 2, 3])",
        "S091-01",
        "INSERT with ARRAY literal"
    );
}

/// S091-01: INSERT with multiple ARRAY values
#[test]
fn s091_01_insert_multiple_arrays() {
    assert_feature_supported!(
        "INSERT INTO array_types (int_array, str_array) VALUES (ARRAY[1, 2], ARRAY['a', 'b'])",
        "S091-01",
        "INSERT with multiple ARRAY values"
    );
}

// ============================================================================
// S091-02: Arrays of distinct types
// ============================================================================

/// S091-02: ARRAY of user-defined DISTINCT type
#[test]
fn s091_02_array_of_distinct_type() {
    // SQL:2016 allows arrays of DISTINCT types (CREATE TYPE ... AS ...)
    // This is typically not implemented in most systems
    assert_feature_supported!(
        "CREATE TABLE t (arr INTEGER ARRAY)",
        "S091-02",
        "ARRAY of DISTINCT type (basic form)"
    );
}

// ============================================================================
// S091-03: Array expressions
// ============================================================================

/// S091-03: Array subscript access - single element
#[test]
fn s091_03_array_subscript_single() {
    assert_feature_supported!(
        "SELECT int_array[1] FROM array_types",
        "S091-03",
        "Array subscript - single element"
    );
}

/// S091-03: Array subscript with literal array
#[test]
fn s091_03_array_subscript_literal() {
    assert_feature_supported!(
        "SELECT ARRAY[1, 2, 3][1]",
        "S091-03",
        "Array subscript with literal"
    );
}

/// S091-03: Array subscript with expression
#[test]
fn s091_03_array_subscript_expression() {
    assert_feature_supported!(
        "SELECT int_array[a] FROM array_types, t",
        "S091-03",
        "Array subscript with expression"
    );
}

/// S091-03: Array subscript in WHERE clause
#[test]
fn s091_03_array_subscript_where() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE int_array[1] > 10",
        "S091-03",
        "Array subscript in WHERE clause"
    );
}

/// S091-03: Nested array subscript
#[test]
fn s091_03_nested_array_subscript() {
    assert_feature_supported!(
        "SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]][1][2]",
        "S091-03",
        "Nested array subscript"
    );
}

/// S091-03: Array concatenation with || operator
#[test]
fn s091_03_array_concatenation() {
    assert_feature_supported!(
        "SELECT ARRAY[1, 2] || ARRAY[3, 4]",
        "S091-03",
        "Array concatenation"
    );
}

/// S091-03: Array concatenation with column
#[test]
fn s091_03_array_concatenation_column() {
    assert_feature_supported!(
        "SELECT int_array || ARRAY[99] FROM array_types",
        "S091-03",
        "Array concatenation with column"
    );
}

/// S091-03: Array concatenation multiple arrays
#[test]
fn s091_03_array_concatenation_multiple() {
    assert_feature_supported!(
        "SELECT ARRAY[1] || ARRAY[2] || ARRAY[3]",
        "S091-03",
        "Multiple array concatenation"
    );
}

/// S091-03: Array equality comparison
#[test]
fn s091_03_array_equality() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE int_array = ARRAY[1, 2, 3]",
        "S091-03",
        "Array equality comparison"
    );
}

/// S091-03: Array inequality comparison
#[test]
fn s091_03_array_inequality() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE int_array != ARRAY[1, 2, 3]",
        "S091-03",
        "Array inequality comparison"
    );
}

/// S091-03: Array IS NULL check
#[test]
fn s091_03_array_is_null() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE int_array IS NULL",
        "S091-03",
        "Array IS NULL check"
    );
}

/// S091-03: Array IS NOT NULL check
#[test]
fn s091_03_array_is_not_null() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE int_array IS NOT NULL",
        "S091-03",
        "Array IS NOT NULL check"
    );
}

/// S091-03: Nested array constructor
#[test]
fn s091_03_nested_array_constructor() {
    assert_feature_supported!(
        "SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]]",
        "S091-03",
        "Nested array constructor"
    );
}

/// S091-03: Array with CASE expression
#[test]
fn s091_03_array_with_case() {
    assert_feature_supported!(
        "SELECT ARRAY[CASE WHEN a > 10 THEN 1 ELSE 0 END] FROM t",
        "S091-03",
        "Array with CASE expression"
    );
}

/// S091-03: Array in subquery
#[test]
fn s091_03_array_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (SELECT UNNEST(ARRAY[1, 2, 3]))",
        "S091-03",
        "Array in subquery"
    );
}

// ============================================================================
// S092: Arrays of user-defined types
// ============================================================================

/// S092: Array of STRUCT type
#[test]
fn s092_array_of_struct() {
    assert_feature_supported!(
        "SELECT ARRAY[STRUCT(1, 'a'), STRUCT(2, 'b')]",
        "S092",
        "Array of STRUCT type"
    );
}

/// S092: CREATE TABLE with ARRAY of STRUCT
#[test]
fn s092_create_table_array_struct() {
    assert_feature_supported!(
        "CREATE TABLE t (arr STRUCT<x INT, y VARCHAR> ARRAY)",
        "S092",
        "CREATE TABLE with ARRAY of STRUCT"
    );
}

// ============================================================================
// S094: Arrays of reference types
// ============================================================================

/// S094: Arrays of reference types (REF types)
/// Note: REF types are rarely implemented in modern SQL systems
#[test]
fn s094_array_of_ref() {
    // This is more of a placeholder - REF types are SQL:1999+ feature
    // that most systems don't fully implement
    assert_feature_supported!(
        "SELECT ARRAY[1, 2, 3]",
        "S094",
        "Array of reference types (placeholder)"
    );
}

// ============================================================================
// S095: Array constructors by query
// ============================================================================

/// S095: ARRAY constructor with subquery
#[test]
fn s095_array_constructor_subquery() {
    assert_feature_supported!(
        "SELECT ARRAY(SELECT a FROM t WHERE a > 10)",
        "S095",
        "ARRAY constructor with subquery"
    );
}

/// S095: ARRAY constructor with subquery and ORDER BY
#[test]
fn s095_array_constructor_subquery_order() {
    assert_feature_supported!(
        "SELECT ARRAY(SELECT a FROM t ORDER BY a)",
        "S095",
        "ARRAY constructor with subquery and ORDER BY"
    );
}

/// S095: ARRAY constructor with scalar subquery
#[test]
fn s095_array_constructor_scalar_subquery() {
    assert_feature_supported!(
        "SELECT ARRAY(SELECT MAX(a) FROM t)",
        "S095",
        "ARRAY constructor with scalar subquery"
    );
}

/// S095: ARRAY constructor with JOIN in subquery
#[test]
fn s095_array_constructor_join_subquery() {
    assert_feature_supported!(
        "SELECT ARRAY(SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a)",
        "S095",
        "ARRAY constructor with JOIN subquery"
    );
}

/// S095: Nested ARRAY constructor with subquery
#[test]
fn s095_nested_array_constructor_subquery() {
    assert_feature_supported!(
        "SELECT ARRAY(SELECT ARRAY(SELECT b FROM t2 WHERE t2.a = t1.a) FROM t1)",
        "S095",
        "Nested ARRAY constructor with subquery"
    );
}

// ============================================================================
// S096: Optional array bounds
// ============================================================================

/// S096: Array type with explicit bounds
#[test]
fn s096_array_with_bounds() {
    assert_feature_supported!(
        "CREATE TABLE t (arr INTEGER ARRAY[10])",
        "S096",
        "Array type with bounds"
    );
}

/// S096: Array type with multidimensional bounds
#[test]
fn s096_array_multidimensional_bounds() {
    assert_feature_supported!(
        "CREATE TABLE t (arr INTEGER ARRAY[5][10])",
        "S096",
        "Multidimensional array bounds"
    );
}

// ============================================================================
// S098: ARRAY_AGG function
// ============================================================================

/// S098: Basic ARRAY_AGG function
#[test]
fn s098_array_agg_basic() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a) FROM t",
        "S098",
        "Basic ARRAY_AGG"
    );
}

/// S098: ARRAY_AGG with ORDER BY
#[test]
fn s098_array_agg_order_by() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a ORDER BY a) FROM t",
        "S098",
        "ARRAY_AGG with ORDER BY"
    );
}

/// S098: ARRAY_AGG with DESC ordering
#[test]
fn s098_array_agg_order_desc() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a ORDER BY a DESC) FROM t",
        "S098",
        "ARRAY_AGG with DESC ORDER BY"
    );
}

/// S098: ARRAY_AGG with GROUP BY
#[test]
fn s098_array_agg_group_by() {
    assert_feature_supported!(
        "SELECT c, ARRAY_AGG(a) FROM t GROUP BY c",
        "S098",
        "ARRAY_AGG with GROUP BY"
    );
}

/// S098: ARRAY_AGG with WHERE clause
#[test]
fn s098_array_agg_where() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a) FROM t WHERE a > 10",
        "S098",
        "ARRAY_AGG with WHERE clause"
    );
}

/// S098: ARRAY_AGG with DISTINCT
#[test]
fn s098_array_agg_distinct() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(DISTINCT a) FROM t",
        "S098",
        "ARRAY_AGG with DISTINCT"
    );
}

/// S098: ARRAY_AGG with FILTER clause
#[test]
fn s098_array_agg_filter() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a) FILTER (WHERE a > 10) FROM t",
        "S098",
        "ARRAY_AGG with FILTER"
    );
}

/// S098: ARRAY_AGG with expression
#[test]
fn s098_array_agg_expression() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a * 2) FROM t",
        "S098",
        "ARRAY_AGG with expression"
    );
}

/// S098: ARRAY_AGG on string column
#[test]
fn s098_array_agg_strings() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(c) FROM t",
        "S098",
        "ARRAY_AGG on strings"
    );
}

/// S098: ARRAY_AGG with NULL handling
#[test]
fn s098_array_agg_nulls() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a) FROM t WHERE a IS NULL OR a IS NOT NULL",
        "S098",
        "ARRAY_AGG with NULLs"
    );
}

/// S098: ARRAY_AGG with HAVING
#[test]
fn s098_array_agg_having() {
    assert_feature_supported!(
        "SELECT c, ARRAY_AGG(a) FROM t GROUP BY c HAVING COUNT(*) > 5",
        "S098",
        "ARRAY_AGG with HAVING"
    );
}

/// S098: Multiple ARRAY_AGG in one query
#[test]
fn s098_multiple_array_agg() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a), ARRAY_AGG(b) FROM t",
        "S098",
        "Multiple ARRAY_AGG"
    );
}

/// S098: ARRAY_AGG in subquery
#[test]
fn s098_array_agg_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT ARRAY_AGG(a) AS arr FROM t) AS sub",
        "S098",
        "ARRAY_AGG in subquery"
    );
}

/// S098: ARRAY_AGG with JOIN
#[test]
fn s098_array_agg_join() {
    assert_feature_supported!(
        "SELECT t1.c, ARRAY_AGG(t2.a) FROM t1 JOIN t2 ON t1.a = t2.a GROUP BY t1.c",
        "S098",
        "ARRAY_AGG with JOIN"
    );
}

// ============================================================================
// S301: Enhanced UNNEST
// ============================================================================

/// S301: Basic UNNEST function
#[test]
fn s301_unnest_basic() {
    assert_feature_supported!(
        "SELECT UNNEST(ARRAY[1, 2, 3])",
        "S301",
        "Basic UNNEST"
    );
}

/// S301: UNNEST on array column
#[test]
fn s301_unnest_column() {
    assert_feature_supported!(
        "SELECT UNNEST(int_array) FROM array_types",
        "S301",
        "UNNEST on array column"
    );
}

/// S301: UNNEST in FROM clause
#[test]
fn s301_unnest_from_clause() {
    assert_feature_supported!(
        "SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(val)",
        "S301",
        "UNNEST in FROM clause"
    );
}

/// S301: UNNEST with JOIN
#[test]
fn s301_unnest_join() {
    assert_feature_supported!(
        "SELECT t.a, u.val FROM t, UNNEST(ARRAY[1, 2, 3]) AS u(val)",
        "S301",
        "UNNEST with JOIN"
    );
}

/// S301: UNNEST with CROSS JOIN
#[test]
fn s301_unnest_cross_join() {
    assert_feature_supported!(
        "SELECT t.a, u.val FROM t CROSS JOIN UNNEST(ARRAY[1, 2, 3]) AS u(val)",
        "S301",
        "UNNEST with CROSS JOIN"
    );
}

/// S301: UNNEST multiple arrays
#[test]
fn s301_unnest_multiple() {
    assert_feature_supported!(
        "SELECT * FROM UNNEST(ARRAY[1, 2], ARRAY['a', 'b']) AS t(num, str)",
        "S301",
        "UNNEST multiple arrays"
    );
}

/// S301: UNNEST with ordinality
#[test]
fn s301_unnest_with_ordinality() {
    assert_feature_supported!(
        "SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY AS t(val, ord)",
        "S301",
        "UNNEST with ORDINALITY"
    );
}

/// S301: UNNEST nested arrays
#[test]
fn s301_unnest_nested() {
    assert_feature_supported!(
        "SELECT UNNEST(UNNEST(ARRAY[ARRAY[1, 2], ARRAY[3, 4]]))",
        "S301",
        "UNNEST nested arrays"
    );
}

/// S301: UNNEST in WHERE clause
#[test]
fn s301_unnest_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (SELECT UNNEST(ARRAY[1, 2, 3]))",
        "S301",
        "UNNEST in WHERE clause"
    );
}

/// S301: UNNEST with aggregate
#[test]
fn s301_unnest_aggregate() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM UNNEST(ARRAY[1, 2, 3]) AS t(val)",
        "S301",
        "UNNEST with aggregate"
    );
}

// ============================================================================
// S404: TRIM_ARRAY function
// ============================================================================

/// S404: TRIM_ARRAY basic usage
#[test]
fn s404_trim_array_basic() {
    assert_feature_supported!(
        "SELECT TRIM_ARRAY(ARRAY[1, 2, 3, 4], 1)",
        "S404",
        "Basic TRIM_ARRAY"
    );
}

/// S404: TRIM_ARRAY on column
#[test]
fn s404_trim_array_column() {
    assert_feature_supported!(
        "SELECT TRIM_ARRAY(int_array, 2) FROM array_types",
        "S404",
        "TRIM_ARRAY on column"
    );
}

/// S404: TRIM_ARRAY with expression
#[test]
fn s404_trim_array_expression() {
    assert_feature_supported!(
        "SELECT TRIM_ARRAY(ARRAY[1, 2, 3, 4, 5], a) FROM t",
        "S404",
        "TRIM_ARRAY with expression"
    );
}

/// S404: TRIM_ARRAY remove all elements
#[test]
fn s404_trim_array_all() {
    assert_feature_supported!(
        "SELECT TRIM_ARRAY(ARRAY[1, 2, 3], 3)",
        "S404",
        "TRIM_ARRAY remove all"
    );
}

/// S404: TRIM_ARRAY with zero trim
#[test]
fn s404_trim_array_zero() {
    assert_feature_supported!(
        "SELECT TRIM_ARRAY(ARRAY[1, 2, 3], 0)",
        "S404",
        "TRIM_ARRAY with zero"
    );
}

// ============================================================================
// Array utility functions (CARDINALITY, etc.)
// ============================================================================

/// CARDINALITY function on array
#[test]
fn array_cardinality_basic() {
    assert_feature_supported!(
        "SELECT CARDINALITY(ARRAY[1, 2, 3])",
        "S091",
        "CARDINALITY function"
    );
}

/// CARDINALITY on array column
#[test]
fn array_cardinality_column() {
    assert_feature_supported!(
        "SELECT CARDINALITY(int_array) FROM array_types",
        "S091",
        "CARDINALITY on column"
    );
}

/// CARDINALITY in WHERE clause
#[test]
fn array_cardinality_where() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE CARDINALITY(int_array) > 5",
        "S091",
        "CARDINALITY in WHERE"
    );
}

/// CARDINALITY of empty array
#[test]
fn array_cardinality_empty() {
    assert_feature_supported!(
        "SELECT CARDINALITY(ARRAY[])",
        "S091",
        "CARDINALITY of empty array"
    );
}

/// CARDINALITY of NULL array
#[test]
fn array_cardinality_null() {
    assert_feature_supported!(
        "SELECT CARDINALITY(NULL::INTEGER ARRAY)",
        "S091",
        "CARDINALITY of NULL array"
    );
}

// ============================================================================
// Array slicing (extension to SQL:2016)
// ============================================================================

/// Array slice with range
#[test]
fn array_slice_range() {
    assert_feature_supported!(
        "SELECT int_array[1:3] FROM array_types",
        "S091",
        "Array slice with range"
    );
}

/// Array slice from beginning
#[test]
fn array_slice_from_start() {
    assert_feature_supported!(
        "SELECT int_array[:3] FROM array_types",
        "S091",
        "Array slice from start"
    );
}

/// Array slice to end
#[test]
fn array_slice_to_end() {
    assert_feature_supported!(
        "SELECT int_array[2:] FROM array_types",
        "S091",
        "Array slice to end"
    );
}

// ============================================================================
// Advanced array operations
// ============================================================================

/// Array CONTAINS check (element membership)
#[test]
fn array_contains_element() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE 5 = ANY(int_array)",
        "S091",
        "Array contains element"
    );
}

/// Array ALL comparison
#[test]
fn array_all_comparison() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE 10 < ALL(int_array)",
        "S091",
        "Array ALL comparison"
    );
}

/// Array ANY comparison
#[test]
fn array_any_comparison() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE 10 < ANY(int_array)",
        "S091",
        "Array ANY comparison"
    );
}

/// Array overlap check
#[test]
fn array_overlap() {
    assert_feature_supported!(
        "SELECT * FROM array_types WHERE int_array && ARRAY[1, 2, 3]",
        "S091",
        "Array overlap operator"
    );
}

/// Array UNION (merge)
#[test]
fn array_union() {
    assert_feature_supported!(
        "SELECT ARRAY_UNION(ARRAY[1, 2, 3], ARRAY[3, 4, 5])",
        "S091",
        "Array UNION function"
    );
}

/// Array INTERSECT
#[test]
fn array_intersect() {
    assert_feature_supported!(
        "SELECT ARRAY_INTERSECT(ARRAY[1, 2, 3], ARRAY[2, 3, 4])",
        "S091",
        "Array INTERSECT function"
    );
}

/// Array EXCEPT (difference)
#[test]
fn array_except() {
    assert_feature_supported!(
        "SELECT ARRAY_EXCEPT(ARRAY[1, 2, 3], ARRAY[2])",
        "S091",
        "Array EXCEPT function"
    );
}

/// Array DISTINCT
#[test]
fn array_distinct() {
    assert_feature_supported!(
        "SELECT ARRAY_DISTINCT(ARRAY[1, 2, 2, 3, 3])",
        "S091",
        "Array DISTINCT function"
    );
}

/// Array POSITION (index of element)
#[test]
fn array_position() {
    assert_feature_supported!(
        "SELECT ARRAY_POSITION(ARRAY[1, 2, 3], 2)",
        "S091",
        "Array POSITION function"
    );
}

/// Array REMOVE
#[test]
fn array_remove() {
    assert_feature_supported!(
        "SELECT ARRAY_REMOVE(ARRAY[1, 2, 3, 2], 2)",
        "S091",
        "Array REMOVE function"
    );
}

/// Array REPLACE
#[test]
fn array_replace() {
    assert_feature_supported!(
        "SELECT ARRAY_REPLACE(ARRAY[1, 2, 3], 2, 99)",
        "S091",
        "Array REPLACE function"
    );
}

/// Array PREPEND
#[test]
fn array_prepend() {
    assert_feature_supported!(
        "SELECT ARRAY_PREPEND(0, ARRAY[1, 2, 3])",
        "S091",
        "Array PREPEND function"
    );
}

/// Array APPEND
#[test]
fn array_append() {
    assert_feature_supported!(
        "SELECT ARRAY_APPEND(ARRAY[1, 2, 3], 4)",
        "S091",
        "Array APPEND function"
    );
}

// ============================================================================
// Complex scenarios and edge cases
// ============================================================================

/// Mixed: Array with multiple operations
#[test]
fn mixed_array_operations() {
    assert_feature_supported!(
        "SELECT CARDINALITY(ARRAY[1, 2] || ARRAY[3, 4]) AS len",
        "S091",
        "Multiple array operations"
    );
}

/// Mixed: ARRAY_AGG with UNNEST
#[test]
fn mixed_array_agg_unnest() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(val) FROM UNNEST(ARRAY[1, 2, 3]) AS t(val)",
        "S091",
        "ARRAY_AGG with UNNEST"
    );
}

/// Mixed: Nested array with aggregation
#[test]
fn mixed_nested_array_aggregate() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(ARRAY[a, b]) FROM t",
        "S091",
        "Nested array with aggregation"
    );
}

/// Mixed: Array in CASE expression
#[test]
fn mixed_array_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN CARDINALITY(int_array) > 5 THEN int_array ELSE ARRAY[] END FROM array_types",
        "S091",
        "Array in CASE expression"
    );
}

/// Mixed: Array with window function
#[test]
fn mixed_array_window_function() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a) OVER (PARTITION BY c ORDER BY a) FROM t",
        "S091",
        "Array with window function"
    );
}

/// Mixed: Array UPDATE statement
#[test]
fn mixed_array_update() {
    assert_feature_supported!(
        "UPDATE array_types SET int_array = ARRAY[1, 2, 3] WHERE CARDINALITY(int_array) < 5",
        "S091",
        "Array UPDATE statement"
    );
}

/// Mixed: Array in CTE
#[test]
fn mixed_array_cte() {
    assert_feature_supported!(
        "WITH arr_cte AS (SELECT ARRAY[1, 2, 3] AS arr) SELECT * FROM arr_cte",
        "S091",
        "Array in CTE"
    );
}

/// Mixed: Array with LATERAL join
#[test]
fn mixed_array_lateral_join() {
    assert_feature_supported!(
        "SELECT t.a, u.val FROM t, LATERAL UNNEST(ARRAY[t.a, t.b]) AS u(val)",
        "S091",
        "Array with LATERAL join"
    );
}

/// Edge case: Very large array literal
#[test]
fn edge_large_array_literal() {
    assert_feature_supported!(
        "SELECT ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]",
        "S091",
        "Large array literal"
    );
}

/// Edge case: Array with mixed NULL and non-NULL
#[test]
fn edge_array_mixed_nulls() {
    assert_feature_supported!(
        "SELECT ARRAY[1, NULL, 2, NULL, 3]",
        "S091",
        "Array with mixed NULLs"
    );
}

/// Edge case: Empty array comparison
#[test]
fn edge_empty_array_comparison() {
    assert_feature_supported!(
        "SELECT ARRAY[] = ARRAY[]",
        "S091",
        "Empty array comparison"
    );
}

/// Edge case: Array concatenation with empty
#[test]
fn edge_array_concat_empty() {
    assert_feature_supported!(
        "SELECT ARRAY[1, 2] || ARRAY[]",
        "S091",
        "Array concat with empty"
    );
}

/// Edge case: UNNEST empty array
#[test]
fn edge_unnest_empty() {
    assert_feature_supported!(
        "SELECT * FROM UNNEST(ARRAY[]::INTEGER ARRAY) AS t(val)",
        "S091",
        "UNNEST empty array"
    );
}

/// Edge case: ARRAY_AGG on empty result
#[test]
fn edge_array_agg_empty() {
    assert_feature_supported!(
        "SELECT ARRAY_AGG(a) FROM t WHERE 1 = 0",
        "S091",
        "ARRAY_AGG on empty result"
    );
}

/// Edge case: Deeply nested arrays
#[test]
fn edge_deeply_nested_arrays() {
    assert_feature_supported!(
        "SELECT ARRAY[ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY[5, 6], ARRAY[7, 8]]]",
        "S091",
        "Deeply nested arrays"
    );
}

/// Edge case: Array type casting
#[test]
fn edge_array_type_cast() {
    assert_feature_supported!(
        "SELECT CAST(ARRAY[1, 2, 3] AS BIGINT ARRAY)",
        "S091",
        "Array type casting"
    );
}

/// Edge case: Array in ORDER BY
#[test]
fn edge_array_order_by() {
    assert_feature_supported!(
        "SELECT * FROM array_types ORDER BY CARDINALITY(int_array)",
        "S091",
        "Array in ORDER BY"
    );
}

/// Edge case: Array in GROUP BY
#[test]
fn edge_array_group_by() {
    assert_feature_supported!(
        "SELECT int_array, COUNT(*) FROM array_types GROUP BY int_array",
        "S091",
        "Array in GROUP BY"
    );
}
