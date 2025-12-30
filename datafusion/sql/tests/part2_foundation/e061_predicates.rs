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

//! SQL:2016 Feature E061 - Basic predicates and search conditions
//!
//! ISO/IEC 9075-2:2016 Section 8.1
//!
//! This feature covers the basic predicates and search conditions required by Core SQL:
//! - Comparison predicates (=, <>, <, >, <=, >=)
//! - BETWEEN predicate
//! - IN predicate
//! - LIKE predicate
//! - NULL predicate
//! - Quantified comparison (ANY, ALL)
//! - EXISTS predicate
//! - Subqueries in predicates
//! - Correlated subqueries
//! - Boolean operators (AND, OR, NOT)
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E061-01 | Comparison predicate | Supported |
//! | E061-02 | BETWEEN predicate | Supported |
//! | E061-03 | IN predicate with list of values | Supported |
//! | E061-04 | LIKE predicate | Supported |
//! | E061-05 | LIKE predicate ESCAPE clause | Supported |
//! | E061-06 | NULL predicate | Supported |
//! | E061-07 | Quantified comparison predicate | Supported |
//! | E061-08 | EXISTS predicate | Supported |
//! | E061-09 | Subqueries in comparison predicate | Supported |
//! | E061-11 | Subqueries in IN predicate | Supported |
//! | E061-12 | Subqueries in quantified comparison | Supported |
//! | E061-13 | Correlated subqueries | Supported |
//! | E061-14 | Search condition | Supported |
//!
//! All E061 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::assert_feature_supported;

// ============================================================================
// E061-01: Comparison predicate
// ============================================================================

/// E061-01: Equality comparison (=)
#[test]
fn e061_01_equals() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a = 5",
        "E061-01",
        "Equality comparison"
    );
}

/// E061-01: Not equal comparison (<>)
#[test]
fn e061_01_not_equal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a <> 5",
        "E061-01",
        "Not equal (<>) comparison"
    );
}

/// E061-01: Not equal alternative (!=)
#[test]
fn e061_01_not_equal_alt() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a != 5",
        "E061-01",
        "Not equal (!=) comparison"
    );
}

/// E061-01: Less than comparison (<)
#[test]
fn e061_01_less_than() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a < 10",
        "E061-01",
        "Less than comparison"
    );
}

/// E061-01: Greater than comparison (>)
#[test]
fn e061_01_greater_than() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > 10",
        "E061-01",
        "Greater than comparison"
    );
}

/// E061-01: Less than or equal (<=)
#[test]
fn e061_01_less_equal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a <= 10",
        "E061-01",
        "Less than or equal comparison"
    );
}

/// E061-01: Greater than or equal (>=)
#[test]
fn e061_01_greater_equal() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a >= 10",
        "E061-01",
        "Greater than or equal comparison"
    );
}

/// E061-01: Column to column comparison
#[test]
fn e061_01_column_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > b",
        "E061-01",
        "Column to column comparison"
    );
}

/// E061-01: String comparison
#[test]
fn e061_01_string_comparison() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name = 'John'",
        "E061-01",
        "String comparison"
    );
}

/// E061-01: Comparison with expression
#[test]
fn e061_01_expression_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a + b > 100",
        "E061-01",
        "Comparison with expression"
    );
}

// ============================================================================
// E061-02: BETWEEN predicate
// ============================================================================

/// E061-02: Basic BETWEEN
#[test]
fn e061_02_between_basic() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a BETWEEN 10 AND 20",
        "E061-02",
        "Basic BETWEEN predicate"
    );
}

/// E061-02: NOT BETWEEN
#[test]
fn e061_02_not_between() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a NOT BETWEEN 10 AND 20",
        "E061-02",
        "NOT BETWEEN predicate"
    );
}

/// E061-02: BETWEEN with expressions
#[test]
fn e061_02_between_expressions() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a + b BETWEEN 50 AND 100",
        "E061-02",
        "BETWEEN with expression"
    );
}

/// E061-02: BETWEEN with columns
#[test]
fn e061_02_between_columns() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a BETWEEN t1.b AND t1.c",
        "E061-02",
        "BETWEEN with column bounds"
    );
}

/// E061-02: BETWEEN with strings
#[test]
fn e061_02_between_strings() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name BETWEEN 'A' AND 'M'",
        "E061-02",
        "BETWEEN with strings"
    );
}

// ============================================================================
// E061-03: IN predicate with list of values
// ============================================================================

/// E061-03: Basic IN with integers
#[test]
fn e061_03_in_basic() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (1, 2, 3, 4, 5)",
        "E061-03",
        "Basic IN predicate"
    );
}

/// E061-03: NOT IN
#[test]
fn e061_03_not_in() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a NOT IN (1, 2, 3)",
        "E061-03",
        "NOT IN predicate"
    );
}

/// E061-03: IN with strings
#[test]
fn e061_03_in_strings() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE state IN ('CA', 'NY', 'TX')",
        "E061-03",
        "IN with strings"
    );
}

/// E061-03: IN with single value
#[test]
fn e061_03_in_single() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (42)",
        "E061-03",
        "IN with single value"
    );
}

/// E061-03: IN with expressions
#[test]
fn e061_03_in_expressions() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a + b IN (10, 20, 30)",
        "E061-03",
        "IN with expression"
    );
}

/// E061-03: IN with mixed types (implicit cast)
#[test]
fn e061_03_in_mixed() {
    assert_feature_supported!(
        "SELECT * FROM numeric_types WHERE regular IN (1, 2.5, 3)",
        "E061-03",
        "IN with mixed numeric types"
    );
}

// ============================================================================
// E061-04: LIKE predicate
// ============================================================================

/// E061-04: Basic LIKE with %
#[test]
fn e061_04_like_percent() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE 'J%'",
        "E061-04",
        "LIKE with % wildcard"
    );
}

/// E061-04: LIKE with underscore
#[test]
fn e061_04_like_underscore() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE 'J_hn'",
        "E061-04",
        "LIKE with _ wildcard"
    );
}

/// E061-04: LIKE with both wildcards
#[test]
fn e061_04_like_both() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE '%a_e%'",
        "E061-04",
        "LIKE with both wildcards"
    );
}

/// E061-04: NOT LIKE
#[test]
fn e061_04_not_like() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name NOT LIKE 'J%'",
        "E061-04",
        "NOT LIKE predicate"
    );
}

/// E061-04: LIKE with exact match (no wildcards)
#[test]
fn e061_04_like_exact() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE 'John'",
        "E061-04",
        "LIKE without wildcards"
    );
}

/// E061-04: LIKE at beginning
#[test]
fn e061_04_like_prefix() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE last_name LIKE 'Smith%'",
        "E061-04",
        "LIKE prefix match"
    );
}

/// E061-04: LIKE at end
#[test]
fn e061_04_like_suffix() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE last_name LIKE '%son'",
        "E061-04",
        "LIKE suffix match"
    );
}

/// E061-04: LIKE contains
#[test]
fn e061_04_like_contains() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE '%oh%'",
        "E061-04",
        "LIKE contains match"
    );
}

// ============================================================================
// E061-05: LIKE predicate ESCAPE clause
// ============================================================================

/// E061-05: LIKE with ESCAPE clause
#[test]
fn e061_05_like_escape() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c LIKE '50!%' ESCAPE '!'",
        "E061-05",
        "LIKE with ESCAPE"
    );
}

/// E061-05: LIKE ESCAPE for underscore
#[test]
fn e061_05_like_escape_underscore() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c LIKE 'test!_file' ESCAPE '!'",
        "E061-05",
        "LIKE ESCAPE underscore"
    );
}

/// E061-05: LIKE ESCAPE with backslash
#[test]
fn e061_05_like_escape_backslash() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c LIKE '100\\%' ESCAPE '\\'",
        "E061-05",
        "LIKE ESCAPE with backslash"
    );
}

/// E061-05: NOT LIKE with ESCAPE
#[test]
fn e061_05_not_like_escape() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c NOT LIKE '%!%%' ESCAPE '!'",
        "E061-05",
        "NOT LIKE with ESCAPE"
    );
}

// ============================================================================
// E061-06: NULL predicate
// ============================================================================

/// E061-06: IS NULL
#[test]
fn e061_06_is_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NULL",
        "E061-06",
        "IS NULL predicate"
    );
}

/// E061-06: IS NOT NULL
#[test]
fn e061_06_is_not_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT NULL",
        "E061-06",
        "IS NOT NULL predicate"
    );
}

/// E061-06: IS NULL with expression
#[test]
fn e061_06_is_null_expression() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a + b IS NULL",
        "E061-06",
        "IS NULL with expression"
    );
}

/// E061-06: Multiple NULL checks
#[test]
fn e061_06_multiple_null_checks() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NULL AND b IS NOT NULL",
        "E061-06",
        "Multiple NULL predicates"
    );
}

// ============================================================================
// E061-07: Quantified comparison predicate (ANY, SOME, ALL)
// ============================================================================

/// E061-07: Comparison with ANY
#[test]
fn e061_07_any_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a > ANY (SELECT b FROM t2)",
        "E061-07",
        "Comparison with ANY"
    );
}

/// E061-07: Comparison with SOME (synonym for ANY)
#[test]
fn e061_07_some_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a > SOME (SELECT b FROM t2)",
        "E061-07",
        "Comparison with SOME"
    );
}

/// E061-07: Comparison with ALL
#[test]
fn e061_07_all_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a > ALL (SELECT b FROM t2)",
        "E061-07",
        "Comparison with ALL"
    );
}

/// E061-07: Equals ANY
#[test]
fn e061_07_equals_any() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age = ANY (SELECT age FROM person WHERE state = 'CA')",
        "E061-07",
        "Equals ANY"
    );
}

/// E061-07: Not equals ALL
#[test]
fn e061_07_not_equals_all() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a <> ALL (SELECT b FROM t2)",
        "E061-07",
        "Not equals ALL"
    );
}

/// E061-07: Less than ANY
#[test]
fn e061_07_less_than_any() {
    assert_feature_supported!(
        "SELECT * FROM numeric_types WHERE regular < ANY (SELECT small FROM numeric_types)",
        "E061-07",
        "Less than ANY"
    );
}

/// E061-07: Greater than or equal ALL
#[test]
fn e061_07_gte_all() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a >= ALL (SELECT b FROM t2)",
        "E061-07",
        "Greater than or equal ALL"
    );
}

// ============================================================================
// E061-08: EXISTS predicate
// ============================================================================

/// E061-08: Basic EXISTS
#[test]
fn e061_08_exists_basic() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2)",
        "E061-08",
        "Basic EXISTS predicate"
    );
}

/// E061-08: NOT EXISTS
#[test]
fn e061_08_not_exists() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE NOT EXISTS (SELECT * FROM t2 WHERE t2.a = t1.a)",
        "E061-08",
        "NOT EXISTS predicate"
    );
}

/// E061-08: EXISTS with correlation
#[test]
fn e061_08_exists_correlated() {
    assert_feature_supported!(
        "SELECT * FROM person p WHERE EXISTS (SELECT * FROM orders o WHERE o.customer_id = p.id)",
        "E061-08",
        "EXISTS with correlated subquery"
    );
}

/// E061-08: EXISTS with multiple conditions
#[test]
fn e061_08_exists_multiple() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE EXISTS (SELECT * FROM orders WHERE customer_id = person.id AND qty > 10)",
        "E061-08",
        "EXISTS with multiple conditions"
    );
}

// ============================================================================
// E061-09: Subqueries in comparison predicate
// ============================================================================

/// E061-09: Scalar subquery in comparison
#[test]
fn e061_09_scalar_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > (SELECT AVG(b) FROM t)",
        "E061-09",
        "Scalar subquery in comparison"
    );
}

/// E061-09: Scalar subquery equals
#[test]
fn e061_09_scalar_equals() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age = (SELECT MAX(age) FROM person)",
        "E061-09",
        "Scalar subquery equals"
    );
}

/// E061-09: Scalar subquery with column reference
#[test]
fn e061_09_scalar_column_ref() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a < (SELECT MAX(b) FROM t2 WHERE t2.c = t1.c)",
        "E061-09",
        "Scalar subquery with correlation"
    );
}

/// E061-09: Multiple scalar subqueries
#[test]
fn e061_09_multiple_scalars() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a BETWEEN (SELECT MIN(b) FROM t) AND (SELECT MAX(b) FROM t)",
        "E061-09",
        "Multiple scalar subqueries"
    );
}

// ============================================================================
// E061-11: Subqueries in IN predicate
// ============================================================================

/// E061-11: IN with subquery
#[test]
fn e061_11_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a IN (SELECT b FROM t2)",
        "E061-11",
        "IN with subquery"
    );
}

/// E061-11: NOT IN with subquery
#[test]
fn e061_11_not_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a NOT IN (SELECT b FROM t2)",
        "E061-11",
        "NOT IN with subquery"
    );
}

/// E061-11: IN with filtered subquery
#[test]
fn e061_11_in_filtered_subquery() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE id IN (SELECT customer_id FROM orders WHERE qty > 10)",
        "E061-11",
        "IN with filtered subquery"
    );
}

/// E061-11: IN with aggregate subquery
#[test]
fn e061_11_in_aggregate_subquery() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE state IN (SELECT state FROM person GROUP BY state HAVING COUNT(*) > 100)",
        "E061-11",
        "IN with aggregate subquery"
    );
}

// ============================================================================
// E061-12: Subqueries in quantified comparison
// ============================================================================

/// E061-12: ANY with complex subquery
#[test]
fn e061_12_any_complex() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary > ANY (SELECT AVG(salary) FROM person GROUP BY state)",
        "E061-12",
        "ANY with aggregate subquery"
    );
}

/// E061-12: ALL with filtered subquery
#[test]
fn e061_12_all_filtered() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age >= ALL (SELECT age FROM person WHERE state = 'CA')",
        "E061-12",
        "ALL with filtered subquery"
    );
}

/// E061-12: SOME with join subquery
#[test]
fn e061_12_some_join() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a = SOME (SELECT t2.b FROM t2 JOIN t3 ON t2.c = t3.c)",
        "E061-12",
        "SOME with join subquery"
    );
}

// ============================================================================
// E061-13: Correlated subqueries
// ============================================================================

/// E061-13: Correlated subquery in WHERE
#[test]
fn e061_13_correlated_where() {
    assert_feature_supported!(
        "SELECT * FROM person p WHERE salary > (SELECT AVG(salary) FROM person WHERE state = p.state)",
        "E061-13",
        "Correlated subquery in WHERE"
    );
}

/// E061-13: Correlated EXISTS
#[test]
fn e061_13_correlated_exists() {
    assert_feature_supported!(
        "SELECT * FROM person p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = p.id AND o.qty > 5)",
        "E061-13",
        "Correlated EXISTS"
    );
}

/// E061-13: Correlated IN
#[test]
fn e061_13_correlated_in() {
    assert_feature_supported!(
        "SELECT * FROM person p WHERE p.state IN (SELECT state FROM person WHERE age > p.age)",
        "E061-13",
        "Correlated IN subquery"
    );
}

/// E061-13: Correlated quantified comparison
#[test]
fn e061_13_correlated_any() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE a > ANY (SELECT b FROM t2 WHERE t2.c = t1.c)",
        "E061-13",
        "Correlated ANY"
    );
}

/// E061-13: Double correlation
#[test]
fn e061_13_double_correlation() {
    assert_feature_supported!(
        "SELECT * FROM person p1 WHERE salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state AND p2.age < p1.age)",
        "E061-13",
        "Double correlated subquery"
    );
}

// ============================================================================
// E061-14: Search condition (AND, OR, NOT)
// ============================================================================

/// E061-14: AND condition
#[test]
fn e061_14_and() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age > 21 AND state = 'CA'",
        "E061-14",
        "AND condition"
    );
}

/// E061-14: OR condition
#[test]
fn e061_14_or() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE state = 'CA' OR state = 'NY'",
        "E061-14",
        "OR condition"
    );
}

/// E061-14: NOT condition
#[test]
fn e061_14_not() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE NOT (a > 10)",
        "E061-14",
        "NOT condition"
    );
}

/// E061-14: Complex boolean expression
#[test]
fn e061_14_complex_boolean() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE (age > 21 AND state = 'CA') OR (age > 18 AND state = 'NY')",
        "E061-14",
        "Complex boolean expression"
    );
}

/// E061-14: Nested boolean conditions
#[test]
fn e061_14_nested_boolean() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE NOT (age < 18 OR (state = 'CA' AND salary < 50000))",
        "E061-14",
        "Nested boolean conditions"
    );
}

/// E061-14: AND with multiple predicates
#[test]
fn e061_14_multiple_and() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age >= 18 AND age <= 65 AND state = 'CA' AND salary > 40000",
        "E061-14",
        "Multiple AND conditions"
    );
}

/// E061-14: OR with multiple predicates
#[test]
fn e061_14_multiple_or() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE state = 'CA' OR state = 'NY' OR state = 'TX' OR state = 'FL'",
        "E061-14",
        "Multiple OR conditions"
    );
}

/// E061-14: Mixed AND/OR with precedence
#[test]
fn e061_14_precedence() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age > 30 AND salary > 50000 OR state = 'CA'",
        "E061-14",
        "AND/OR precedence"
    );
}

/// E061-14: Boolean with NULL predicate
#[test]
fn e061_14_boolean_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a IS NOT NULL AND b > 10) OR c IS NULL",
        "E061-14",
        "Boolean with NULL predicate"
    );
}

// ============================================================================
// E061 Combined Tests
// ============================================================================

/// E061: Complex WHERE clause with multiple predicate types
#[test]
fn e061_combined_predicates() {
    assert_feature_supported!(
        "SELECT * FROM person \
         WHERE age BETWEEN 25 AND 65 \
         AND state IN ('CA', 'NY', 'TX') \
         AND first_name LIKE 'J%' \
         AND salary IS NOT NULL \
         AND id NOT IN (SELECT customer_id FROM orders WHERE qty < 5)",
        "E061",
        "Combined predicates"
    );
}

/// E061: Multiple subquery types
#[test]
fn e061_combined_subqueries() {
    assert_feature_supported!(
        "SELECT * FROM person p \
         WHERE salary > (SELECT AVG(salary) FROM person) \
         AND state IN (SELECT state FROM person GROUP BY state HAVING COUNT(*) > 10) \
         AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = p.id) \
         AND age > ALL (SELECT age FROM person WHERE state = 'CA' AND salary < 30000)",
        "E061",
        "Combined subquery predicates"
    );
}

/// E061: Complex correlated subquery scenario
#[test]
fn e061_combined_correlated() {
    assert_feature_supported!(
        "SELECT * FROM person p1 \
         WHERE salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state) \
         AND NOT EXISTS (SELECT 1 FROM person p3 WHERE p3.state = p1.state AND p3.salary > p1.salary * 2) \
         AND age >= SOME (SELECT age FROM person p4 WHERE p4.state = p1.state AND p4.id <> p1.id)",
        "E061",
        "Complex correlated subqueries"
    );
}

/// E061: Boolean logic with diverse predicates
#[test]
fn e061_combined_boolean() {
    assert_feature_supported!(
        "SELECT * FROM person \
         WHERE (age > 21 AND state = 'CA') \
         OR (age > 18 AND state IN ('NY', 'NJ') AND salary > 40000) \
         OR (first_name LIKE 'A%' AND last_name NOT LIKE '%son' AND age BETWEEN 30 AND 50) \
         AND id IS NOT NULL",
        "E061",
        "Complex boolean search conditions"
    );
}

/// E061: All predicate types in single query
#[test]
fn e061_combined_all_types() {
    assert_feature_supported!(
        "SELECT * FROM person p \
         WHERE p.age = 30 \
         AND p.salary BETWEEN 40000 AND 80000 \
         AND p.state IN ('CA', 'NY') \
         AND p.first_name LIKE 'J%n' ESCAPE '!' \
         AND p.last_name IS NOT NULL \
         AND p.salary > ANY (SELECT salary FROM person WHERE state = 'TX') \
         AND EXISTS (SELECT 1 FROM orders WHERE customer_id = p.id) \
         AND NOT (p.age < 25 OR p.salary < 35000)",
        "E061",
        "All E061 predicate types"
    );
}
