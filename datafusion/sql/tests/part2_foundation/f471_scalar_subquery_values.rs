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

//! SQL:2016 Feature F471 - Scalar subquery values
//!
//! ISO/IEC 9075-2:2016 Section 7.11
//!
//! This feature covers scalar subqueries that return a single value:
//! - Scalar subquery in SELECT list
//! - Scalar subquery in WHERE clause
//! - Scalar subquery in expressions
//! - Correlated scalar subqueries
//! - Scalar subqueries with aggregates
//!
//! F471 is a CORE feature (mandatory for SQL:2016 conformance).

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// F471: Scalar subquery in SELECT list
// ============================================================================

/// F471: Basic scalar subquery in SELECT
#[test]
fn f471_scalar_in_select_basic() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(a) FROM t) FROM t1",
        "F471",
        "Basic scalar subquery in SELECT"
    );
}

/// F471: Scalar subquery with aggregate
#[test]
fn f471_scalar_aggregate() {
    assert_feature_supported!(
        "SELECT (SELECT AVG(salary) FROM person) AS avg_salary FROM person",
        "F471",
        "Scalar subquery with AVG aggregate"
    );
}

/// F471: Multiple scalar subqueries in SELECT
#[test]
fn f471_multiple_scalar_select() {
    assert_feature_supported!(
        "SELECT (SELECT MIN(age) FROM person), (SELECT MAX(age) FROM person), (SELECT AVG(salary) FROM person) FROM person",
        "F471",
        "Multiple scalar subqueries in SELECT"
    );
}

/// F471: Scalar subquery with COUNT
#[test]
fn f471_scalar_count() {
    assert_feature_supported!(
        "SELECT (SELECT COUNT(*) FROM orders) AS total_orders FROM person",
        "F471",
        "Scalar subquery with COUNT"
    );
}

/// F471: Scalar subquery with SUM
#[test]
fn f471_scalar_sum() {
    assert_feature_supported!(
        "SELECT (SELECT SUM(qty) FROM orders) AS total_quantity FROM person",
        "F471",
        "Scalar subquery with SUM"
    );
}

/// F471: Scalar subquery returning single row
#[test]
fn f471_scalar_single_row() {
    assert_feature_supported!(
        "SELECT (SELECT first_name FROM person WHERE id = 1) AS name FROM t",
        "F471",
        "Scalar subquery returning single row"
    );
}

/// F471: Scalar subquery with LIMIT 1
#[test]
fn f471_scalar_limit() {
    assert_feature_supported!(
        "SELECT (SELECT salary FROM person ORDER BY salary DESC LIMIT 1) AS top_salary FROM t",
        "F471",
        "Scalar subquery with LIMIT 1"
    );
}

/// F471: Scalar subquery mixed with regular columns
#[test]
fn f471_scalar_mixed_columns() {
    assert_feature_supported!(
        "SELECT first_name, age, (SELECT AVG(age) FROM person) AS avg_age FROM person",
        "F471",
        "Scalar subquery mixed with columns"
    );
}

// ============================================================================
// F471: Scalar subquery in WHERE clause
// ============================================================================

/// F471: Scalar subquery in WHERE with equality
#[test]
fn f471_scalar_where_equals() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary = (SELECT MAX(salary) FROM person)",
        "F471",
        "Scalar subquery in WHERE with equality"
    );
}

/// F471: Scalar subquery in WHERE with comparison
#[test]
fn f471_scalar_where_greater() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary > (SELECT AVG(salary) FROM person)",
        "F471",
        "Scalar subquery in WHERE with greater than"
    );
}

/// F471: Scalar subquery in WHERE with less than
#[test]
fn f471_scalar_where_less() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age < (SELECT AVG(age) FROM person)",
        "F471",
        "Scalar subquery in WHERE with less than"
    );
}

/// F471: Multiple scalar subqueries in WHERE
#[test]
fn f471_scalar_where_multiple() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age > (SELECT AVG(age) FROM person) AND salary < (SELECT MAX(salary) FROM person)",
        "F471",
        "Multiple scalar subqueries in WHERE"
    );
}

/// F471: Scalar subquery in WHERE with AND
#[test]
fn f471_scalar_where_and() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary BETWEEN (SELECT MIN(salary) FROM person WHERE state = 'CA') AND (SELECT MAX(salary) FROM person WHERE state = 'CA')",
        "F471",
        "Scalar subqueries in BETWEEN"
    );
}

/// F471: Scalar subquery in WHERE with OR
#[test]
fn f471_scalar_where_or() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary > (SELECT AVG(salary) FROM person WHERE state = 'CA') OR salary > (SELECT AVG(salary) FROM person WHERE state = 'NY')",
        "F471",
        "Scalar subqueries in OR condition"
    );
}

/// F471: Scalar subquery with NOT
#[test]
fn f471_scalar_where_not() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age <> (SELECT MAX(age) FROM person)",
        "F471",
        "Scalar subquery with not equals"
    );
}

// ============================================================================
// F471: Scalar subquery in expressions
// ============================================================================

/// F471: Scalar subquery in arithmetic
#[test]
fn f471_scalar_arithmetic() {
    assert_feature_supported!(
        "SELECT salary - (SELECT AVG(salary) FROM person) AS diff_from_avg FROM person",
        "F471",
        "Scalar subquery in arithmetic"
    );
}

/// F471: Scalar subquery in multiplication
#[test]
fn f471_scalar_multiply() {
    assert_feature_supported!(
        "SELECT a * (SELECT MAX(b) FROM t) FROM t1",
        "F471",
        "Scalar subquery in multiplication"
    );
}

/// F471: Scalar subquery in division
#[test]
fn f471_scalar_divide() {
    assert_feature_supported!(
        "SELECT salary / (SELECT AVG(salary) FROM person) AS salary_ratio FROM person",
        "F471",
        "Scalar subquery in division"
    );
}

/// F471: Multiple scalar subqueries in expression
#[test]
fn f471_scalar_complex_expr() {
    assert_feature_supported!(
        "SELECT (salary - (SELECT AVG(salary) FROM person)) / (SELECT MAX(salary) FROM person) AS normalized FROM person",
        "F471",
        "Multiple scalar subqueries in expression"
    );
}

/// F471: Scalar subquery in CASE condition
#[test]
fn f471_scalar_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN salary > (SELECT AVG(salary) FROM person) THEN 'above' ELSE 'below' END FROM person",
        "F471",
        "Scalar subquery in CASE condition"
    );
}

/// F471: Scalar subquery in CASE result
#[test]
fn f471_scalar_case_result() {
    assert_feature_supported!(
        "SELECT CASE WHEN age > 30 THEN (SELECT AVG(salary) FROM person WHERE age > 30) ELSE (SELECT AVG(salary) FROM person WHERE age <= 30) END FROM person",
        "F471",
        "Scalar subquery in CASE result"
    );
}

/// F471: Scalar subquery with COALESCE
#[test]
fn f471_scalar_coalesce() {
    assert_feature_supported!(
        "SELECT COALESCE((SELECT MAX(a) FROM t), 0) FROM t1",
        "F471",
        "Scalar subquery with COALESCE"
    );
}

/// F471: Scalar subquery with NULLIF
#[test]
fn f471_scalar_nullif() {
    assert_feature_supported!(
        "SELECT NULLIF(a, (SELECT AVG(a) FROM t)) FROM t",
        "F471",
        "Scalar subquery with NULLIF"
    );
}

// ============================================================================
// F471: Correlated scalar subqueries
// ============================================================================

/// F471: Basic correlated scalar subquery
#[test]
fn f471_correlated_basic() {
    assert_feature_supported!(
        "SELECT first_name, (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state) AS state_avg FROM person p1",
        "F471",
        "Basic correlated scalar subquery"
    );
}

/// F471: Correlated scalar in WHERE
#[test]
fn f471_correlated_where() {
    assert_feature_supported!(
        "SELECT * FROM person p1 WHERE salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state)",
        "F471",
        "Correlated scalar in WHERE"
    );
}

/// F471: Multiple correlations
#[test]
fn f471_correlated_multiple() {
    assert_feature_supported!(
        "SELECT * FROM person p1 WHERE salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state AND p2.age < p1.age)",
        "F471",
        "Correlated scalar with multiple conditions"
    );
}

/// F471: Correlated scalar with aggregate
#[test]
fn f471_correlated_aggregate() {
    assert_feature_supported!(
        "SELECT first_name, salary, (SELECT COUNT(*) FROM person p2 WHERE p2.salary > p1.salary) AS rank FROM person p1",
        "F471",
        "Correlated scalar for ranking"
    );
}

/// F471: Correlated scalar in expression
#[test]
fn f471_correlated_expression() {
    assert_feature_supported!(
        "SELECT first_name, salary - (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state) AS diff FROM person p1",
        "F471",
        "Correlated scalar in expression"
    );
}

/// F471: Nested correlated scalar subqueries
#[test]
fn f471_correlated_nested() {
    assert_feature_supported!(
        "SELECT * FROM person p1 WHERE salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state AND p2.age > (SELECT AVG(age) FROM person p3 WHERE p3.state = p2.state))",
        "F471",
        "Nested correlated scalar subqueries"
    );
}

/// F471: Correlated scalar with EXISTS
#[test]
fn f471_correlated_with_exists() {
    assert_feature_supported!(
        "SELECT * FROM person p WHERE salary > (SELECT AVG(salary) FROM person WHERE state = p.state) AND EXISTS (SELECT 1 FROM orders WHERE customer_id = p.id)",
        "F471",
        "Correlated scalar with EXISTS"
    );
}

/// F471: Self-referencing correlated scalar
#[test]
fn f471_correlated_self_reference() {
    assert_feature_supported!(
        "SELECT first_name, (SELECT COUNT(*) FROM person p2 WHERE p2.state = p1.state AND p2.id <> p1.id) AS others_in_state FROM person p1",
        "F471",
        "Self-referencing correlated scalar"
    );
}

// ============================================================================
// F471: Scalar subquery in ORDER BY
// ============================================================================

/// F471: Scalar subquery in ORDER BY
#[test]
fn f471_scalar_order_by() {
    assert_feature_supported!(
        "SELECT * FROM person ORDER BY salary - (SELECT AVG(salary) FROM person)",
        "F471",
        "Scalar subquery in ORDER BY"
    );
}

/// F471: Correlated scalar in ORDER BY
#[test]
fn f471_correlated_order_by() {
    assert_feature_supported!(
        "SELECT * FROM person p1 ORDER BY (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state)",
        "F471",
        "Correlated scalar in ORDER BY"
    );
}

/// F471: Multiple scalar subqueries in ORDER BY
#[test]
fn f471_scalar_order_by_multiple() {
    assert_feature_supported!(
        "SELECT * FROM person ORDER BY (SELECT MAX(age) FROM person) - age, salary - (SELECT AVG(salary) FROM person)",
        "F471",
        "Multiple scalar subqueries in ORDER BY"
    );
}

// ============================================================================
// F471: Scalar subquery in HAVING
// ============================================================================

/// F471: Scalar subquery in HAVING
#[test]
fn f471_scalar_having() {
    assert_feature_supported!(
        "SELECT state, AVG(salary) FROM person GROUP BY state HAVING AVG(salary) > (SELECT AVG(salary) FROM person)",
        "F471",
        "Scalar subquery in HAVING"
    );
}

/// F471: Correlated scalar in HAVING
#[test]
fn f471_correlated_having() {
    assert_feature_supported!(
        "SELECT state, COUNT(*) FROM person p1 GROUP BY state HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT state, COUNT(*) AS cnt FROM person GROUP BY state) AS state_counts)",
        "F471",
        "Scalar subquery in HAVING with derived table"
    );
}

/// F471: Multiple scalar subqueries in HAVING
#[test]
fn f471_scalar_having_multiple() {
    assert_feature_supported!(
        "SELECT state, AVG(salary) FROM person GROUP BY state HAVING AVG(salary) > (SELECT AVG(salary) FROM person) AND COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM person GROUP BY state) AS x)",
        "F471",
        "Multiple scalar subqueries in HAVING"
    );
}

// ============================================================================
// F471: Scalar subquery in JOIN
// ============================================================================

/// F471: Scalar subquery in JOIN condition
#[test]
fn f471_scalar_join_condition() {
    assert_feature_supported!(
        "SELECT * FROM person p JOIN orders o ON p.id = o.customer_id AND o.price > (SELECT AVG(price) FROM orders)",
        "F471",
        "Scalar subquery in JOIN condition"
    );
}

/// F471: Correlated scalar in JOIN
#[test]
fn f471_correlated_join() {
    assert_feature_supported!(
        "SELECT * FROM person p JOIN orders o ON p.id = o.customer_id AND o.qty > (SELECT AVG(qty) FROM orders WHERE customer_id = p.id)",
        "F471",
        "Correlated scalar in JOIN condition"
    );
}

/// F471: Scalar subquery selecting from joined tables
#[test]
fn f471_scalar_from_join() {
    assert_feature_supported!(
        "SELECT (SELECT AVG(o.price) FROM orders o WHERE o.customer_id = p.id) AS avg_order_price FROM person p",
        "F471",
        "Scalar subquery referencing outer query"
    );
}

// ============================================================================
// F471: Scalar subquery with different data types
// ============================================================================

/// F471: Scalar subquery returning string
#[test]
fn f471_scalar_string() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(first_name) FROM person) AS max_name FROM t",
        "F471",
        "Scalar subquery returning string"
    );
}

/// F471: Scalar subquery returning date
#[test]
fn f471_scalar_date() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(birth_date) FROM person) AS latest_birth FROM t",
        "F471",
        "Scalar subquery returning date"
    );
}

/// F471: Scalar subquery in string concatenation
#[test]
fn f471_scalar_concat() {
    assert_feature_supported!(
        "SELECT first_name || ' (Avg age: ' || (SELECT AVG(age) FROM person) || ')' FROM person",
        "F471",
        "Scalar subquery in string concatenation"
    );
}

/// F471: Scalar subquery with CAST
#[test]
fn f471_scalar_cast() {
    assert_feature_supported!(
        "SELECT CAST((SELECT AVG(salary) FROM person) AS INTEGER) AS avg_salary_int FROM t",
        "F471",
        "Scalar subquery with CAST"
    );
}

// ============================================================================
// F471: Scalar subquery with filtering
// ============================================================================

/// F471: Scalar subquery with WHERE
#[test]
fn f471_scalar_filtered() {
    assert_feature_supported!(
        "SELECT (SELECT AVG(salary) FROM person WHERE state = 'CA') AS ca_avg FROM t",
        "F471",
        "Scalar subquery with WHERE filter"
    );
}

/// F471: Scalar subquery with multiple filters
#[test]
fn f471_scalar_multiple_filters() {
    assert_feature_supported!(
        "SELECT (SELECT AVG(salary) FROM person WHERE state = 'CA' AND age > 30) AS ca_senior_avg FROM t",
        "F471",
        "Scalar subquery with multiple filters"
    );
}

/// F471: Scalar subquery with GROUP BY
#[test]
fn f471_scalar_group_by() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(avg_sal) FROM (SELECT AVG(salary) AS avg_sal FROM person GROUP BY state) AS x) FROM t",
        "F471",
        "Scalar subquery with GROUP BY"
    );
}

/// F471: Scalar subquery with HAVING
#[test]
fn f471_scalar_with_having() {
    assert_feature_supported!(
        "SELECT (SELECT COUNT(*) FROM (SELECT state FROM person GROUP BY state HAVING COUNT(*) > 10) AS x) FROM t",
        "F471",
        "Scalar subquery with HAVING"
    );
}

// ============================================================================
// F471: Scalar subquery with NULL handling
// ============================================================================

/// F471: Scalar subquery returning NULL
#[test]
fn f471_scalar_null_result() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(a) FROM t WHERE a < 0) AS max_negative FROM t1",
        "F471",
        "Scalar subquery potentially returning NULL"
    );
}

/// F471: Scalar subquery with IS NULL
#[test]
fn f471_scalar_is_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (SELECT MAX(a) FROM t WHERE a < 0) IS NULL",
        "F471",
        "Scalar subquery with IS NULL check"
    );
}

/// F471: Scalar subquery with COALESCE for NULL
#[test]
fn f471_scalar_coalesce_null() {
    assert_feature_supported!(
        "SELECT COALESCE((SELECT MAX(a) FROM t WHERE a < 0), 0) AS result FROM t1",
        "F471",
        "Scalar subquery with COALESCE for NULL handling"
    );
}

/// F471: Scalar subquery in NULL comparison
#[test]
fn f471_scalar_null_comparison() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE salary > COALESCE((SELECT AVG(salary) FROM person WHERE state = 'XX'), 0)",
        "F471",
        "Scalar subquery in NULL-safe comparison"
    );
}

// ============================================================================
// F471: Scalar subquery with DISTINCT
// ============================================================================

/// F471: Scalar subquery with DISTINCT
#[test]
fn f471_scalar_distinct() {
    assert_feature_supported!(
        "SELECT (SELECT COUNT(DISTINCT state) FROM person) AS state_count FROM t",
        "F471",
        "Scalar subquery with DISTINCT"
    );
}

/// F471: Scalar subquery with MAX DISTINCT
#[test]
fn f471_scalar_max_distinct() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(DISTINCT age) FROM person) AS max_age FROM t",
        "F471",
        "Scalar subquery with MAX DISTINCT"
    );
}

// ============================================================================
// F471: Scalar subquery in SET operations
// ============================================================================

/// F471: Scalar subquery in UNION
#[test]
fn f471_scalar_union() {
    assert_feature_supported!(
        "SELECT (SELECT MAX(a) FROM t) AS max_val FROM t1 UNION SELECT (SELECT MIN(a) FROM t) FROM t2",
        "F471",
        "Scalar subquery in UNION"
    );
}

/// F471: Scalar subquery filtering UNION
#[test]
fn f471_scalar_filter_union() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 UNION SELECT b FROM t2) AS u WHERE u.a > (SELECT AVG(a) FROM t)",
        "F471",
        "Scalar subquery filtering UNION result"
    );
}

// ============================================================================
// F471: Scalar subquery in INSERT
// ============================================================================

/// F471: Scalar subquery in INSERT VALUES
#[test]
fn f471_scalar_insert_values() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES ((SELECT MAX(a) FROM t1) + 1, 10, 'test')",
        "F471",
        "Scalar subquery in INSERT VALUES"
    );
}

/// F471: Multiple scalar subqueries in INSERT
#[test]
fn f471_scalar_insert_multiple() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES ((SELECT MAX(a) FROM t1), (SELECT AVG(b) FROM t2), 'test')",
        "F471",
        "Multiple scalar subqueries in INSERT"
    );
}

// ============================================================================
// F471: Scalar subquery in UPDATE
// ============================================================================

/// F471: Scalar subquery in UPDATE SET
#[test]
fn f471_scalar_update_set() {
    assert_feature_supported!(
        "UPDATE t SET a = (SELECT MAX(a) FROM t1) WHERE b > 10",
        "F471",
        "Scalar subquery in UPDATE SET"
    );
}

/// F471: Correlated scalar in UPDATE
#[test]
fn f471_correlated_update() {
    assert_feature_supported!(
        "UPDATE person SET salary = (SELECT AVG(salary) FROM person p2 WHERE p2.state = person.state) WHERE age < 25",
        "F471",
        "Correlated scalar subquery in UPDATE"
    );
}

/// F471: Scalar subquery in UPDATE WHERE
#[test]
fn f471_scalar_update_where() {
    assert_feature_supported!(
        "UPDATE person SET salary = salary * 1.1 WHERE salary < (SELECT AVG(salary) FROM person)",
        "F471",
        "Scalar subquery in UPDATE WHERE"
    );
}

// ============================================================================
// F471: Scalar subquery in DELETE
// ============================================================================

/// F471: Scalar subquery in DELETE WHERE
#[test]
fn f471_scalar_delete() {
    assert_feature_supported!(
        "DELETE FROM person WHERE age < (SELECT AVG(age) FROM person)",
        "F471",
        "Scalar subquery in DELETE WHERE"
    );
}

/// F471: Correlated scalar in DELETE
#[test]
fn f471_correlated_delete() {
    assert_feature_supported!(
        "DELETE FROM person p1 WHERE salary < (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state)",
        "F471",
        "Correlated scalar subquery in DELETE"
    );
}

// ============================================================================
// F471: Complex scalar subquery scenarios
// ============================================================================

/// F471: Scalar subquery in derived table
#[test]
fn f471_scalar_derived_table() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, salary - (SELECT AVG(salary) FROM person) AS diff FROM person) AS x WHERE diff > 0",
        "F471",
        "Scalar subquery in derived table"
    );
}

/// F471: Multiple levels of scalar subqueries
#[test]
fn f471_scalar_nested_levels() {
    assert_feature_supported!(
        "SELECT (SELECT AVG(salary) FROM person WHERE salary > (SELECT AVG(salary) FROM person WHERE age > 30)) FROM t",
        "F471",
        "Nested scalar subqueries"
    );
}

/// F471: Scalar subquery with window function context
#[test]
fn f471_scalar_window_context() {
    assert_feature_supported!(
        "SELECT first_name, salary, (SELECT AVG(salary) FROM person) AS overall_avg FROM person ORDER BY salary DESC",
        "F471",
        "Scalar subquery in query with ordering"
    );
}

/// F471: Complex correlated scenario
#[test]
fn f471_combined_complex_correlated() {
    assert_feature_supported!(
        "SELECT \
         p.first_name, \
         p.state, \
         p.salary, \
         (SELECT AVG(salary) FROM person WHERE state = p.state) AS state_avg, \
         (SELECT COUNT(*) FROM person WHERE state = p.state AND salary > p.salary) AS higher_earners_in_state, \
         (SELECT MAX(salary) FROM person WHERE state = p.state) - p.salary AS diff_from_state_max \
         FROM person p \
         WHERE p.salary > (SELECT AVG(salary) FROM person WHERE state = p.state) \
         ORDER BY (SELECT COUNT(*) FROM orders WHERE customer_id = p.id) DESC",
        "F471",
        "Complex correlated scalar subqueries"
    );
}

/// F471: All scalar subquery positions
#[test]
fn f471_combined_all_positions() {
    assert_feature_supported!(
        "SELECT \
         first_name, \
         (SELECT AVG(age) FROM person) AS avg_age \
         FROM person \
         WHERE salary > (SELECT AVG(salary) FROM person) \
         ORDER BY (SELECT MAX(salary) FROM person) - salary",
        "F471",
        "Scalar subqueries in SELECT, WHERE, and ORDER BY"
    );
}

/// F471: Real-world analytical scenario
#[test]
fn f471_combined_analytical() {
    assert_feature_supported!(
        "SELECT \
         state, \
         first_name, \
         salary, \
         CASE \
           WHEN salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state) * 1.5 \
           THEN 'Top earner' \
           WHEN salary > (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state) \
           THEN 'Above average' \
           ELSE 'Below average' \
         END AS category, \
         salary - (SELECT AVG(salary) FROM person p2 WHERE p2.state = p1.state) AS diff_from_state_avg, \
         CAST(salary / (SELECT AVG(salary) FROM person) * 100 AS INTEGER) AS pct_of_overall_avg \
         FROM person p1 \
         WHERE age > (SELECT AVG(age) FROM person) \
         AND EXISTS (SELECT 1 FROM orders WHERE customer_id = p1.id) \
         ORDER BY (SELECT COUNT(*) FROM person p2 WHERE p2.state = p1.state) DESC, salary DESC",
        "F471",
        "Real-world analytical query with scalar subqueries"
    );
}

/// F471: Scalar subqueries for data quality
#[test]
fn f471_combined_data_quality() {
    assert_feature_supported!(
        "SELECT \
         first_name, \
         age, \
         salary, \
         CASE \
           WHEN age > (SELECT AVG(age) FROM person) + (SELECT STDDEV(age) FROM person) * 2 \
           THEN 'Age outlier' \
           WHEN salary > (SELECT AVG(salary) FROM person) + (SELECT STDDEV(salary) FROM person) * 2 \
           THEN 'Salary outlier' \
           ELSE 'Normal' \
         END AS outlier_status \
         FROM person \
         WHERE age IS NOT NULL AND salary IS NOT NULL",
        "F471",
        "Scalar subqueries for outlier detection"
    );
}
