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

//! SQL:2016 Feature T151 - DISTINCT predicate
//!
//! ISO/IEC 9075-2:2016 Section 8.18
//!
//! This feature provides the DISTINCT predicate which performs NULL-safe comparison:
//! - IS DISTINCT FROM: Returns TRUE if values differ (treating NULL as a comparable value)
//! - IS NOT DISTINCT FROM: Returns TRUE if values are the same (NULL equals NULL)
//!
//! Key differences from standard equality:
//! - `NULL = NULL` returns NULL (unknown)
//! - `NULL IS DISTINCT FROM NULL` returns FALSE (they are not distinct)
//! - `NULL IS NOT DISTINCT FROM NULL` returns TRUE (they are the same)
//!
//! The DISTINCT predicate is essential for NULL-safe comparisons and is commonly
//! used in:
//! - JOIN conditions that need to match NULL values
//! - Uniqueness checks that include NULL
//! - Data validation and comparison logic
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | T151 | DISTINCT predicate | Not Implemented |
//!
//! All tests in this module are expected to FAIL as DataFusion does not currently
//! implement the DISTINCT predicate. These tests document the conformance gap.

use crate::assert_feature_supported;

// ============================================================================
// T151: Basic DISTINCT predicate
// ============================================================================

/// T151: Basic IS DISTINCT FROM with integers
#[test]
fn t151_is_distinct_from_basic() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM b",
        "T151",
        "Basic IS DISTINCT FROM"
    );
}

/// T151: IS NOT DISTINCT FROM with integers
#[test]
fn t151_is_not_distinct_from_basic() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT DISTINCT FROM b",
        "T151",
        "IS NOT DISTINCT FROM"
    );
}

/// T151: IS DISTINCT FROM with literal values
#[test]
fn t151_is_distinct_from_literal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM 5",
        "T151",
        "IS DISTINCT FROM with literal"
    );
}

/// T151: IS NOT DISTINCT FROM with literal values
#[test]
fn t151_is_not_distinct_from_literal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT DISTINCT FROM 10",
        "T151",
        "IS NOT DISTINCT FROM with literal"
    );
}

// ============================================================================
// T151: NULL handling (the key feature)
// ============================================================================

/// T151: IS DISTINCT FROM NULL
///
/// This should return TRUE for non-NULL values and FALSE for NULL values
#[test]
fn t151_is_distinct_from_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM NULL",
        "T151",
        "IS DISTINCT FROM NULL"
    );
}

/// T151: IS NOT DISTINCT FROM NULL
///
/// This is equivalent to IS NULL but more explicit about NULL comparison
#[test]
fn t151_is_not_distinct_from_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT DISTINCT FROM NULL",
        "T151",
        "IS NOT DISTINCT FROM NULL"
    );
}

/// T151: Comparison showing NULL-safe equality
///
/// Demonstrates the key difference: IS NOT DISTINCT FROM treats NULL = NULL as TRUE
#[test]
fn t151_null_safe_equality() {
    assert_feature_supported!(
        "SELECT * FROM t1 JOIN t2 ON t1.a IS NOT DISTINCT FROM t2.b",
        "T151",
        "NULL-safe JOIN using IS NOT DISTINCT FROM"
    );
}

/// T151: IS DISTINCT FROM in WHERE with nullable columns
#[test]
fn t151_nullable_columns() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name IS DISTINCT FROM last_name",
        "T151",
        "IS DISTINCT FROM with nullable columns"
    );
}

// ============================================================================
// T151: Different data types
// ============================================================================

/// T151: IS DISTINCT FROM with strings
#[test]
fn t151_distinct_strings() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name IS DISTINCT FROM 'John'",
        "T151",
        "IS DISTINCT FROM with strings"
    );
}

/// T151: IS NOT DISTINCT FROM with strings
#[test]
fn t151_not_distinct_strings() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE state IS NOT DISTINCT FROM 'CA'",
        "T151",
        "IS NOT DISTINCT FROM with strings"
    );
}

/// T151: IS DISTINCT FROM with numeric types
#[test]
fn t151_distinct_numeric() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM 3.14",
        "T151",
        "IS DISTINCT FROM with numeric"
    );
}

/// T151: IS DISTINCT FROM with boolean values
#[test]
fn t151_distinct_boolean() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE flag IS DISTINCT FROM TRUE",
        "T151",
        "IS DISTINCT FROM with boolean"
    );
}

// ============================================================================
// T151: In complex expressions
// ============================================================================

/// T151: IS DISTINCT FROM in SELECT clause
#[test]
fn t151_distinct_in_select() {
    assert_feature_supported!(
        "SELECT a, b, a IS DISTINCT FROM b AS are_different FROM t",
        "T151",
        "IS DISTINCT FROM in SELECT clause"
    );
}

/// T151: IS NOT DISTINCT FROM in SELECT clause
#[test]
fn t151_not_distinct_in_select() {
    assert_feature_supported!(
        "SELECT a, b, a IS NOT DISTINCT FROM b AS are_same FROM t",
        "T151",
        "IS NOT DISTINCT FROM in SELECT clause"
    );
}

/// T151: IS DISTINCT FROM with expressions
#[test]
fn t151_distinct_expressions() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a + b IS DISTINCT FROM c * d",
        "T151",
        "IS DISTINCT FROM with expressions"
    );
}

/// T151: IS DISTINCT FROM in CASE expression
#[test]
fn t151_distinct_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS DISTINCT FROM b THEN 'different' ELSE 'same' END FROM t",
        "T151",
        "IS DISTINCT FROM in CASE"
    );
}

/// T151: IS NOT DISTINCT FROM in CASE expression
#[test]
fn t151_not_distinct_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS NOT DISTINCT FROM NULL THEN 'is null' ELSE 'not null' END FROM t",
        "T151",
        "IS NOT DISTINCT FROM in CASE"
    );
}

// ============================================================================
// T151: Boolean combinations
// ============================================================================

/// T151: IS DISTINCT FROM with AND
#[test]
fn t151_distinct_and() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM b AND b IS DISTINCT FROM c",
        "T151",
        "IS DISTINCT FROM with AND"
    );
}

/// T151: IS DISTINCT FROM with OR
#[test]
fn t151_distinct_or() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM 1 OR a IS DISTINCT FROM 2",
        "T151",
        "IS DISTINCT FROM with OR"
    );
}

/// T151: Mixed DISTINCT and standard predicates
#[test]
fn t151_mixed_predicates() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM b AND c > 10 OR d IS NULL",
        "T151",
        "Mixed DISTINCT and standard predicates"
    );
}

// ============================================================================
// T151: Comparison with standard equality operators
// ============================================================================

/// T151: Demonstrate difference between = and IS NOT DISTINCT FROM
///
/// Key differences:
/// - `a = b` returns NULL when either operand is NULL
/// - `a IS NOT DISTINCT FROM b` returns TRUE/FALSE even with NULLs
#[test]
fn t151_vs_equals() {
    assert_feature_supported!(
        "SELECT a = b AS standard_eq, a IS NOT DISTINCT FROM b AS null_safe_eq FROM t",
        "T151",
        "Comparison: = vs IS NOT DISTINCT FROM"
    );
}

/// T151: Demonstrate difference between <> and IS DISTINCT FROM
///
/// Key differences:
/// - `a <> b` returns NULL when either operand is NULL
/// - `a IS DISTINCT FROM b` returns TRUE/FALSE even with NULLs
#[test]
fn t151_vs_not_equals() {
    assert_feature_supported!(
        "SELECT a <> b AS standard_neq, a IS DISTINCT FROM b AS null_safe_neq FROM t",
        "T151",
        "Comparison: <> vs IS DISTINCT FROM"
    );
}

// ============================================================================
// T151: Practical use cases
// ============================================================================

/// T151: NULL-safe JOIN
///
/// Unlike standard JOIN which doesn't match NULL values,
/// IS NOT DISTINCT FROM allows NULL-to-NULL matches
#[test]
fn t151_null_safe_join() {
    assert_feature_supported!(
        "SELECT * FROM t1 \
         JOIN t2 ON t1.nullable_key IS NOT DISTINCT FROM t2.nullable_key",
        "T151",
        "NULL-safe JOIN"
    );
}

/// T151: Find rows where value changed (excluding NULL->NULL)
#[test]
fn t151_detect_changes() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE old_value IS DISTINCT FROM new_value",
        "T151",
        "Detect value changes with IS DISTINCT FROM"
    );
}

/// T151: Find rows where value stayed the same (including NULL->NULL)
#[test]
fn t151_detect_no_change() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE old_value IS NOT DISTINCT FROM new_value",
        "T151",
        "Detect no change with IS NOT DISTINCT FROM"
    );
}

/// T151: Uniqueness check including NULLs
///
/// GROUP BY treats NULLs as distinct, but we might want to find duplicate NULLs
#[test]
fn t151_uniqueness_with_nulls() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t \
         GROUP BY a \
         HAVING COUNT(*) > 1 OR (a IS NOT DISTINCT FROM NULL AND COUNT(*) > 1)",
        "T151",
        "Uniqueness check including NULLs"
    );
}

/// T151: Coalesce alternative using IS NOT DISTINCT FROM
#[test]
fn t151_coalesce_alternative() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS NOT DISTINCT FROM NULL THEN 0 ELSE a END FROM t",
        "T151",
        "COALESCE-like behavior with IS NOT DISTINCT FROM"
    );
}

// ============================================================================
// T151: Edge cases
// ============================================================================

/// T151: IS DISTINCT FROM with subquery result
#[test]
fn t151_distinct_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS DISTINCT FROM (SELECT MAX(b) FROM t)",
        "T151",
        "IS DISTINCT FROM with scalar subquery"
    );
}

/// T151: IS DISTINCT FROM both sides NULL literal
#[test]
fn t151_null_distinct_null() {
    assert_feature_supported!(
        "SELECT NULL IS DISTINCT FROM NULL AS result",
        "T151",
        "NULL IS DISTINCT FROM NULL (should be FALSE)"
    );
}

/// T151: IS NOT DISTINCT FROM both sides NULL literal
#[test]
fn t151_null_not_distinct_null() {
    assert_feature_supported!(
        "SELECT NULL IS NOT DISTINCT FROM NULL AS result",
        "T151",
        "NULL IS NOT DISTINCT FROM NULL (should be TRUE)"
    );
}

/// T151: Chained DISTINCT comparisons
#[test]
fn t151_chained_distinct() {
    assert_feature_supported!(
        "SELECT * FROM t \
         WHERE a IS NOT DISTINCT FROM b \
         AND b IS NOT DISTINCT FROM c \
         AND c IS NOT DISTINCT FROM d",
        "T151",
        "Chained IS NOT DISTINCT FROM comparisons"
    );
}

/// T151: DISTINCT in HAVING clause
#[test]
fn t151_distinct_in_having() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t \
         GROUP BY a \
         HAVING MAX(b) IS DISTINCT FROM MIN(c)",
        "T151",
        "IS DISTINCT FROM in HAVING clause"
    );
}

/// T151: DISTINCT with CAST
#[test]
fn t151_distinct_with_cast() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE CAST(a AS VARCHAR) IS DISTINCT FROM CAST(b AS VARCHAR)",
        "T151",
        "IS DISTINCT FROM with CAST"
    );
}

// ============================================================================
// T151: Complex scenarios
// ============================================================================

/// T151: Multiple DISTINCT predicates in complex query
#[test]
fn t151_complex_query() {
    assert_feature_supported!(
        "SELECT * FROM person p \
         WHERE p.first_name IS NOT DISTINCT FROM 'John' \
         AND p.middle_name IS DISTINCT FROM NULL \
         AND p.last_name IS DISTINCT FROM p.maiden_name \
         AND (p.spouse_name IS NOT DISTINCT FROM NULL OR p.spouse_name IS DISTINCT FROM p.first_name)",
        "T151",
        "Complex query with multiple DISTINCT predicates"
    );
}

/// T151: DISTINCT in UPDATE statement
#[test]
fn t151_distinct_in_update() {
    assert_feature_supported!(
        "UPDATE t SET a = b WHERE a IS DISTINCT FROM b",
        "T151",
        "IS DISTINCT FROM in UPDATE"
    );
}

/// T151: DISTINCT in DELETE statement
#[test]
fn t151_distinct_in_delete() {
    assert_feature_supported!(
        "DELETE FROM t WHERE a IS NOT DISTINCT FROM NULL",
        "T151",
        "IS NOT DISTINCT FROM in DELETE"
    );
}

/// T151: DISTINCT with aggregate functions
#[test]
fn t151_distinct_with_aggregates() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM t WHERE a IS DISTINCT FROM AVG(a) OVER ()",
        "T151",
        "IS DISTINCT FROM with window function"
    );
}
