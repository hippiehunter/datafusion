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

//! SQL:2016 Feature F591 - Derived tables
//!
//! ISO/IEC 9075-2:2016 Section 7.6
//!
//! This feature covers derived tables (subqueries in the FROM clause):
//! - Basic derived table with table alias
//! - Derived table with column aliases
//! - Derived table with complex queries (joins, aggregates, etc.)
//! - Nested derived tables
//! - Multiple derived tables in FROM
//! - Derived table with window functions
//!
//! F591 is a CORE feature (mandatory for SQL:2016 conformance).
//!
//! Also includes tests for:
//! - F641: Row and table constructors (VALUES clause)
//! - T441: ABS and MOD functions
//! - T461: Symmetric BETWEEN
//! - T631: IN predicate with one list element
//! - F571: Truth value tests (IS TRUE, IS FALSE, IS UNKNOWN)
//! - F481: Expanded NULL predicate

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// F591: Derived tables (subqueries in FROM clause)
// ============================================================================

/// F591: Basic derived table with table alias
#[test]
fn f591_basic_derived_table() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t) AS dt",
        "F591",
        "Basic derived table"
    );
}

/// F591: Derived table with WHERE clause
#[test]
fn f591_derived_table_with_where() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t WHERE a > 10) AS dt",
        "F591",
        "Derived table with WHERE"
    );
}

/// F591: Derived table with column aliases
#[test]
fn f591_derived_table_column_aliases() {
    assert_feature_supported!(
        "SELECT x, y FROM (SELECT a, b FROM t) AS dt(x, y)",
        "F591",
        "Derived table with column aliases"
    );
}

/// F591: Derived table with explicit column list
#[test]
fn f591_derived_table_explicit_columns() {
    assert_feature_supported!(
        "SELECT col1, col2 FROM (SELECT a, b, c FROM t) AS dt(col1, col2, col3)",
        "F591",
        "Derived table with explicit column names"
    );
}

/// F591: Derived table with renamed columns
#[test]
fn f591_derived_table_renamed_columns() {
    assert_feature_supported!(
        "SELECT first, second FROM (SELECT a AS first, b AS second FROM t) AS dt",
        "F591",
        "Derived table with renamed columns"
    );
}

/// F591: Derived table with aggregation
#[test]
fn f591_derived_table_aggregate() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, AVG(salary) AS avg_salary FROM person GROUP BY state) AS dt",
        "F591",
        "Derived table with aggregation"
    );
}

/// F591: Derived table with COUNT
#[test]
fn f591_derived_table_count() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, COUNT(*) AS cnt FROM person GROUP BY state) AS dt",
        "F591",
        "Derived table with COUNT"
    );
}

/// F591: Derived table with multiple aggregates
#[test]
fn f591_derived_table_multiple_aggregates() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, MIN(age) AS min_age, MAX(age) AS max_age, AVG(salary) AS avg_sal FROM person GROUP BY state) AS dt",
        "F591",
        "Derived table with multiple aggregates"
    );
}

/// F591: Derived table with HAVING
#[test]
fn f591_derived_table_having() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, COUNT(*) AS cnt FROM person GROUP BY state HAVING COUNT(*) > 10) AS dt",
        "F591",
        "Derived table with HAVING"
    );
}

/// F591: Derived table with ORDER BY
#[test]
fn f591_derived_table_order_by() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, age FROM person ORDER BY age DESC) AS dt",
        "F591",
        "Derived table with ORDER BY"
    );
}

/// F591: Derived table with LIMIT
#[test]
fn f591_derived_table_limit() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, salary FROM person ORDER BY salary DESC LIMIT 10) AS dt",
        "F591",
        "Derived table with LIMIT"
    );
}

/// F591: Derived table with DISTINCT
#[test]
fn f591_derived_table_distinct() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT DISTINCT state FROM person) AS dt",
        "F591",
        "Derived table with DISTINCT"
    );
}

/// F591: Derived table with join
#[test]
fn f591_derived_table_join() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT p.first_name, o.item FROM person p JOIN orders o ON p.id = o.customer_id) AS dt",
        "F591",
        "Derived table with join"
    );
}

/// F591: Derived table with LEFT JOIN
#[test]
fn f591_derived_table_left_join() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT p.first_name, o.qty FROM person p LEFT JOIN orders o ON p.id = o.customer_id) AS dt",
        "F591",
        "Derived table with LEFT JOIN"
    );
}

/// F591: Derived table with CROSS JOIN
#[test]
fn f591_derived_table_cross_join() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT t1.a, t2.b FROM t1 CROSS JOIN t2) AS dt",
        "F591",
        "Derived table with CROSS JOIN"
    );
}

/// F591: Nested derived tables (2 levels)
#[test]
fn f591_nested_derived_tables() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (SELECT a, b FROM t) AS inner_dt) AS outer_dt",
        "F591",
        "Nested derived tables"
    );
}

/// F591: Nested derived tables (3 levels)
#[test]
fn f591_deeply_nested_derived_tables() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT a FROM t) AS dt1) AS dt2) AS dt3",
        "F591",
        "Deeply nested derived tables"
    );
}

/// F591: Nested derived tables with column renaming
#[test]
fn f591_nested_with_renaming() {
    assert_feature_supported!(
        "SELECT x FROM (SELECT col1 AS x FROM (SELECT a AS col1 FROM t) AS inner_dt) AS outer_dt",
        "F591",
        "Nested derived tables with renaming"
    );
}

/// F591: Nested derived tables with filtering
#[test]
fn f591_nested_with_filtering() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (SELECT a, b FROM t WHERE a > 5) AS dt1 WHERE b < 20) AS dt2",
        "F591",
        "Nested derived tables with filtering at each level"
    );
}

/// F591: Multiple derived tables in FROM
#[test]
fn f591_multiple_derived_tables() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t) AS dt1, (SELECT b FROM t) AS dt2",
        "F591",
        "Multiple derived tables (comma join)"
    );
}

/// F591: Join between derived tables
#[test]
fn f591_join_derived_tables() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t1) AS dt1 JOIN (SELECT c, d FROM t2) AS dt2 ON dt1.a = dt2.c",
        "F591",
        "Join between derived tables"
    );
}

/// F591: LEFT JOIN between derived tables
#[test]
fn f591_left_join_derived_tables() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT id, first_name FROM person) AS p LEFT JOIN (SELECT customer_id, item FROM orders) AS o ON p.id = o.customer_id",
        "F591",
        "LEFT JOIN between derived tables"
    );
}

/// F591: Multiple derived tables with aggregates
#[test]
fn f591_multiple_derived_aggregates() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, AVG(age) AS avg_age FROM person GROUP BY state) AS dt1 JOIN (SELECT state, AVG(salary) AS avg_sal FROM person GROUP BY state) AS dt2 ON dt1.state = dt2.state",
        "F591",
        "Join between derived tables with aggregates"
    );
}

/// F591: Derived table with UNION
#[test]
fn f591_derived_table_union() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 UNION SELECT b FROM t2) AS dt",
        "F591",
        "Derived table with UNION"
    );
}

/// F591: Derived table with UNION ALL
#[test]
fn f591_derived_table_union_all() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 UNION ALL SELECT b FROM t2) AS dt",
        "F591",
        "Derived table with UNION ALL"
    );
}

/// F591: Derived table with INTERSECT
#[test]
fn f591_derived_table_intersect() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 INTERSECT SELECT b FROM t2) AS dt",
        "F591",
        "Derived table with INTERSECT"
    );
}

/// F591: Derived table with EXCEPT
#[test]
fn f591_derived_table_except() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 EXCEPT SELECT b FROM t2) AS dt",
        "F591",
        "Derived table with EXCEPT"
    );
}

/// F591: Derived table with window functions
#[test]
fn f591_derived_table_window() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank FROM person) AS dt",
        "F591",
        "Derived table with window function"
    );
}

/// F591: Derived table with multiple window functions
#[test]
fn f591_derived_table_multiple_windows() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank, AVG(salary) OVER () AS avg_salary FROM person) AS dt",
        "F591",
        "Derived table with multiple window functions"
    );
}

/// F591: Derived table with partitioned window
#[test]
fn f591_derived_table_partitioned_window() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, first_name, salary, RANK() OVER (PARTITION BY state ORDER BY salary DESC) AS state_rank FROM person) AS dt",
        "F591",
        "Derived table with partitioned window"
    );
}

/// F591: Filtering derived table results
#[test]
fn f591_filter_derived_table() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, AVG(salary) AS avg_sal FROM person GROUP BY state) AS dt WHERE avg_sal > 50000",
        "F591",
        "Filter derived table results"
    );
}

/// F591: Derived table in WHERE subquery
#[test]
fn f591_derived_in_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary > (SELECT AVG(avg_sal) FROM (SELECT state, AVG(salary) AS avg_sal FROM person GROUP BY state) AS dt)",
        "F591",
        "Derived table in WHERE subquery"
    );
}

/// F591: Derived table with scalar subquery
#[test]
fn f591_derived_with_scalar_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, salary, (SELECT AVG(salary) FROM person) AS overall_avg FROM person) AS dt",
        "F591",
        "Derived table containing scalar subquery"
    );
}

/// F591: Derived table with correlated subquery
#[test]
fn f591_derived_with_correlated() {
    assert_feature_supported!(
        "SELECT * FROM person p WHERE EXISTS (SELECT 1 FROM (SELECT customer_id FROM orders WHERE qty > 10) AS dt WHERE dt.customer_id = p.id)",
        "F591",
        "Derived table in correlated subquery"
    );
}

/// F591: Complex query with multiple derived tables
#[test]
fn f591_complex_multiple_derived() {
    assert_feature_supported!(
        "SELECT dt1.state, dt1.avg_age, dt2.avg_sal \
         FROM (SELECT state, AVG(age) AS avg_age FROM person GROUP BY state) AS dt1 \
         JOIN (SELECT state, AVG(salary) AS avg_sal FROM person GROUP BY state) AS dt2 \
         ON dt1.state = dt2.state \
         WHERE dt1.avg_age > 30 AND dt2.avg_sal > 40000",
        "F591",
        "Complex query with multiple derived tables"
    );
}

/// F591: Derived table with CASE expression
#[test]
fn f591_derived_with_case() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, CASE WHEN age < 30 THEN 'Young' WHEN age < 50 THEN 'Middle' ELSE 'Senior' END AS age_group FROM person) AS dt",
        "F591",
        "Derived table with CASE expression"
    );
}

/// F591: Derived table with COALESCE
#[test]
fn f591_derived_with_coalesce() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, COALESCE(b, 0) AS b_or_zero FROM t) AS dt",
        "F591",
        "Derived table with COALESCE"
    );
}

/// F591: Derived table with arithmetic
#[test]
fn f591_derived_with_arithmetic() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b, a + b AS sum, a * b AS product FROM t) AS dt",
        "F591",
        "Derived table with arithmetic expressions"
    );
}

/// F591: Derived table with string operations
#[test]
fn f591_derived_with_string_ops() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name || ' ' || last_name AS full_name FROM person) AS dt",
        "F591",
        "Derived table with string concatenation"
    );
}

// ============================================================================
// F641: Row and table constructors (VALUES clause)
// ============================================================================

/// F641: Basic VALUES clause
#[test]
fn f641_values_basic() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(x, y)",
        "F641",
        "Basic VALUES clause"
    );
}

/// F641: VALUES with single row
#[test]
fn f641_values_single_row() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (42, 'answer')) AS t(num, text)",
        "F641",
        "VALUES with single row"
    );
}

/// F641: VALUES with multiple columns
#[test]
fn f641_values_multiple_columns() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1, 'a', 10.5), (2, 'b', 20.7), (3, 'c', 30.9)) AS t(id, name, value)",
        "F641",
        "VALUES with multiple columns"
    );
}

/// F641: VALUES without table alias
#[test]
fn f641_values_no_alias() {
    assert_feature_supported!(
        "VALUES (1, 2), (3, 4)",
        "F641",
        "VALUES without table alias"
    );
}

/// F641: VALUES with column aliases
#[test]
fn f641_values_with_aliases() {
    assert_feature_supported!(
        "SELECT x, y FROM (VALUES (10, 20), (30, 40)) AS t(x, y)",
        "F641",
        "VALUES with column aliases"
    );
}

/// F641: VALUES in FROM clause
#[test]
fn f641_values_in_from() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1), (2), (3)) AS nums(n)",
        "F641",
        "VALUES as table source"
    );
}

/// F641: VALUES with NULL
#[test]
fn f641_values_with_null() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1, 'a'), (2, NULL), (3, 'c')) AS t(id, name)",
        "F641",
        "VALUES with NULL values"
    );
}

/// F641: VALUES with expressions
#[test]
fn f641_values_with_expressions() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1 + 1, 'a'), (2 * 3, 'b')) AS t(num, text)",
        "F641",
        "VALUES with expressions"
    );
}

/// F641: VALUES joined with table
#[test]
fn f641_values_join_table() {
    assert_feature_supported!(
        "SELECT * FROM person p JOIN (VALUES (1), (2), (3)) AS ids(id) ON p.id = ids.id",
        "F641",
        "VALUES joined with table"
    );
}

/// F641: Multiple VALUES in query
#[test]
fn f641_multiple_values() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1, 'a')) AS t1(x, y) JOIN (VALUES (1, 'b')) AS t2(x, z) ON t1.x = t2.x",
        "F641",
        "Multiple VALUES in query"
    );
}

/// F641: VALUES with WHERE clause
#[test]
fn f641_values_with_where() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name) WHERE id > 1",
        "F641",
        "VALUES with WHERE clause"
    );
}

/// F641: VALUES with ORDER BY
#[test]
fn f641_values_with_order_by() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (3, 'c'), (1, 'a'), (2, 'b')) AS t(id, name) ORDER BY id",
        "F641",
        "VALUES with ORDER BY"
    );
}

/// F641: VALUES with aggregation
#[test]
fn f641_values_with_aggregate() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM (VALUES (1), (2), (3), (4), (5)) AS nums(n)",
        "F641",
        "VALUES with aggregation"
    );
}

/// F641: Row constructor in comparison
#[test]
fn f641_row_constructor_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a, b) = (1, 2)",
        "F641",
        "Row constructor in comparison"
    );
}

/// F641: Row constructor with greater than
#[test]
fn f641_row_constructor_gt() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a, b) > (5, 10)",
        "F641",
        "Row constructor with greater than"
    );
}

/// F641: Row constructor in IN predicate
#[test]
fn f641_row_constructor_in() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a, b) IN ((1, 2), (3, 4), (5, 6))",
        "F641",
        "Row constructor in IN predicate"
    );
}

/// F641: Row constructor in INSERT
#[test]
fn f641_row_constructor_insert() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 'test')",
        "F641",
        "Row constructor in INSERT"
    );
}

/// F641: Multiple rows in INSERT
#[test]
fn f641_multiple_rows_insert() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 'a'), (3, 4, 'b'), (5, 6, 'c')",
        "F641",
        "Multiple rows in INSERT"
    );
}

// ============================================================================
// T441: ABS and MOD functions
// ============================================================================

/// T441: ABS function with positive number
#[test]
fn t441_abs_positive() {
    assert_feature_supported!(
        "SELECT ABS(5) FROM t",
        "T441",
        "ABS with positive number"
    );
}

/// T441: ABS function with negative number
#[test]
fn t441_abs_negative() {
    assert_feature_supported!(
        "SELECT ABS(-5) FROM t",
        "T441",
        "ABS with negative number"
    );
}

/// T441: ABS function with column
#[test]
fn t441_abs_column() {
    assert_feature_supported!(
        "SELECT ABS(a) FROM t",
        "T441",
        "ABS with column"
    );
}

/// T441: ABS function with expression
#[test]
fn t441_abs_expression() {
    assert_feature_supported!(
        "SELECT ABS(a - b) FROM t",
        "T441",
        "ABS with expression"
    );
}

/// T441: ABS function in WHERE clause
#[test]
fn t441_abs_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ABS(a) > 10",
        "T441",
        "ABS in WHERE clause"
    );
}

/// T441: MOD function basic
#[test]
fn t441_mod_basic() {
    assert_feature_supported!(
        "SELECT MOD(10, 3) FROM t",
        "T441",
        "MOD function basic"
    );
}

/// T441: MOD function with columns
#[test]
fn t441_mod_columns() {
    assert_feature_supported!(
        "SELECT MOD(a, b) FROM t",
        "T441",
        "MOD with columns"
    );
}

/// T441: MOD function with expressions
#[test]
fn t441_mod_expressions() {
    assert_feature_supported!(
        "SELECT MOD(a + 10, b - 2) FROM t",
        "T441",
        "MOD with expressions"
    );
}

/// T441: MOD function in WHERE clause
#[test]
fn t441_mod_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE MOD(a, 2) = 0",
        "T441",
        "MOD in WHERE clause (even numbers)"
    );
}

/// T441: Combined ABS and MOD
#[test]
fn t441_abs_mod_combined() {
    assert_feature_supported!(
        "SELECT ABS(MOD(a, b)) FROM t",
        "T441",
        "Combined ABS and MOD"
    );
}

/// T441: ABS with decimal
#[test]
fn t441_abs_decimal() {
    assert_feature_supported!(
        "SELECT ABS(-10.5) FROM t",
        "T441",
        "ABS with decimal"
    );
}

/// T441: MOD in ORDER BY
#[test]
fn t441_mod_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY MOD(a, 10)",
        "T441",
        "MOD in ORDER BY"
    );
}

// ============================================================================
// T461: Symmetric BETWEEN
// ============================================================================

/// T461: BETWEEN SYMMETRIC basic
#[test]
fn t461_between_symmetric_basic() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a BETWEEN SYMMETRIC 5 AND 10",
        "T461",
        "BETWEEN SYMMETRIC basic"
    );
}

/// T461: BETWEEN SYMMETRIC reversed bounds
#[test]
fn t461_between_symmetric_reversed() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a BETWEEN SYMMETRIC 10 AND 5",
        "T461",
        "BETWEEN SYMMETRIC with reversed bounds"
    );
}

/// T461: NOT BETWEEN SYMMETRIC
#[test]
fn t461_not_between_symmetric() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a NOT BETWEEN SYMMETRIC 5 AND 10",
        "T461",
        "NOT BETWEEN SYMMETRIC"
    );
}

/// T461: BETWEEN SYMMETRIC with columns
#[test]
fn t461_between_symmetric_columns() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a BETWEEN SYMMETRIC b AND c",
        "T461",
        "BETWEEN SYMMETRIC with column bounds"
    );
}

/// T461: BETWEEN SYMMETRIC with expressions
#[test]
fn t461_between_symmetric_expressions() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age BETWEEN SYMMETRIC salary / 1000 AND salary / 500",
        "T461",
        "BETWEEN SYMMETRIC with expressions"
    );
}

// ============================================================================
// T631: IN predicate with one list element
// ============================================================================

/// T631: IN with single element
#[test]
fn t631_in_single_element() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (42)",
        "T631",
        "IN with single element"
    );
}

/// T631: IN with single string
#[test]
fn t631_in_single_string() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE state IN ('CA')",
        "T631",
        "IN with single string"
    );
}

/// T631: NOT IN with single element
#[test]
fn t631_not_in_single_element() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a NOT IN (0)",
        "T631",
        "NOT IN with single element"
    );
}

/// T631: IN single element vs equals
#[test]
fn t631_in_single_vs_equals() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (5) AND b = 10",
        "T631",
        "IN single element combined with equals"
    );
}

// ============================================================================
// F571: Truth value tests (IS TRUE, IS FALSE, IS UNKNOWN)
// ============================================================================

/// F571: IS TRUE predicate
#[test]
fn f571_is_true() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS TRUE",
        "F571",
        "IS TRUE predicate"
    );
}

/// F571: IS FALSE predicate
#[test]
fn f571_is_false() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS FALSE",
        "F571",
        "IS FALSE predicate"
    );
}

/// F571: IS UNKNOWN predicate
#[test]
fn f571_is_unknown() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS UNKNOWN",
        "F571",
        "IS UNKNOWN predicate"
    );
}

/// F571: IS NOT TRUE
#[test]
fn f571_is_not_true() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS NOT TRUE",
        "F571",
        "IS NOT TRUE predicate"
    );
}

/// F571: IS NOT FALSE
#[test]
fn f571_is_not_false() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS NOT FALSE",
        "F571",
        "IS NOT FALSE predicate"
    );
}

/// F571: IS NOT UNKNOWN
#[test]
fn f571_is_not_unknown() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS NOT UNKNOWN",
        "F571",
        "IS NOT UNKNOWN predicate"
    );
}

/// F571: IS TRUE with column comparison
#[test]
fn f571_is_true_column() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a = b) IS TRUE",
        "F571",
        "IS TRUE with column comparison"
    );
}

/// F571: IS FALSE with NULL handling
#[test]
fn f571_is_false_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a IS NULL) IS FALSE",
        "F571",
        "IS FALSE with NULL check"
    );
}

/// F571: IS UNKNOWN with NULL comparison
#[test]
fn f571_is_unknown_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a = NULL) IS UNKNOWN",
        "F571",
        "IS UNKNOWN with NULL comparison"
    );
}

/// F571: Truth test in SELECT
#[test]
fn f571_truth_in_select() {
    assert_feature_supported!(
        "SELECT a, (a > 10) IS TRUE AS is_large FROM t",
        "F571",
        "Truth test in SELECT clause"
    );
}

/// F571: Multiple truth tests
#[test]
fn f571_multiple_truth_tests() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a > 5) IS TRUE AND (b < 10) IS NOT FALSE",
        "F571",
        "Multiple truth value tests"
    );
}

// ============================================================================
// F481: Expanded NULL predicate
// ============================================================================

/// F481: IS NULL basic
#[test]
fn f481_is_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NULL",
        "F481",
        "IS NULL predicate"
    );
}

/// F481: IS NOT NULL basic
#[test]
fn f481_is_not_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT NULL",
        "F481",
        "IS NOT NULL predicate"
    );
}

/// F481: IS NULL with expression
#[test]
fn f481_is_null_expression() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a + b) IS NULL",
        "F481",
        "IS NULL with expression"
    );
}

/// F481: IS NULL with function result
#[test]
fn f481_is_null_function() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE NULLIF(a, b) IS NULL",
        "F481",
        "IS NULL with function result"
    );
}

/// F481: IS NULL in CASE
#[test]
fn f481_is_null_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS NULL THEN 0 ELSE a END FROM t",
        "F481",
        "IS NULL in CASE expression"
    );
}

/// F481: Multiple NULL checks
#[test]
fn f481_multiple_null_checks() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NULL",
        "F481",
        "Multiple NULL predicates"
    );
}

/// F481: NULL predicate with OR
#[test]
fn f481_null_with_or() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NULL OR a > 10",
        "F481",
        "NULL predicate with OR"
    );
}

// ============================================================================
// Combined/Complex Tests
// ============================================================================

/// Combined: Derived table with VALUES
#[test]
fn combined_derived_values() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(id, name)) AS dt WHERE id > 1",
        "F591/F641",
        "Derived table wrapping VALUES"
    );
}

/// Combined: Derived table with ABS and MOD
#[test]
fn combined_derived_abs_mod() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, ABS(a) AS abs_a, MOD(a, 10) AS mod_a FROM t) AS dt WHERE mod_a = 0",
        "F591/T441",
        "Derived table with ABS and MOD"
    );
}

/// Combined: VALUES with truth tests
#[test]
fn combined_values_truth() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(a, b) WHERE (a < b) IS TRUE",
        "F641/F571",
        "VALUES with truth test"
    );
}

/// Combined: Complex query with multiple features
#[test]
fn combined_complex_all_features() {
    assert_feature_supported!(
        "SELECT dt.state, dt.avg_sal, dt.diff \
         FROM (SELECT state, AVG(salary) AS avg_sal, ABS(AVG(salary) - 50000) AS diff \
               FROM person \
               WHERE age BETWEEN SYMMETRIC 25 AND 65 \
                 AND state IN ('CA', 'NY', 'TX') \
                 AND first_name IS NOT NULL \
                 AND (salary > 30000) IS TRUE \
               GROUP BY state \
               HAVING COUNT(*) > 5 AND MOD(COUNT(*), 2) = 0) AS dt \
         WHERE dt.avg_sal > 40000 \
         ORDER BY dt.diff",
        "F591/F641/T441/T461/T631/F571/F481",
        "Complex query combining all features"
    );
}

/// Combined: Nested derived tables with row constructors
#[test]
fn combined_nested_derived_row_constructor() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (SELECT a, b FROM t WHERE (a, b) IN ((1, 2), (3, 4))) AS inner_dt) AS outer_dt",
        "F591/F641",
        "Nested derived tables with row constructors"
    );
}

/// Combined: JOIN between VALUES tables
#[test]
fn combined_values_join() {
    assert_feature_supported!(
        "SELECT t1.id, t1.name, t2.value \
         FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t1(id, name) \
         JOIN (VALUES (1, 100), (2, 200)) AS t2(id, value) \
         ON t1.id = t2.id",
        "F641",
        "JOIN between VALUES tables"
    );
}

/// Combined: Derived table with window and math functions
#[test]
fn combined_derived_window_math() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, salary, ABS(salary - AVG(salary) OVER (PARTITION BY state)) AS deviation, ROW_NUMBER() OVER (PARTITION BY state ORDER BY salary DESC) AS rank FROM person) AS dt WHERE rank <= 3",
        "F591/T441",
        "Derived table with window and ABS"
    );
}

/// Combined: All predicate types
#[test]
fn combined_all_predicates() {
    assert_feature_supported!(
        "SELECT * FROM t \
         WHERE a BETWEEN SYMMETRIC 5 AND 15 \
           AND b IN (10) \
           AND c IS NOT NULL \
           AND (a > b) IS TRUE \
           AND MOD(a, 2) = 0",
        "T461/T631/F481/F571/T441",
        "All predicate types combined"
    );
}
