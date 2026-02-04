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

//! SQL:2016 Feature E101 - Basic data manipulation
//!
//! ISO/IEC 9075-2:2016 Section 14 (Data Manipulation)
//!
//! This feature covers the basic data manipulation statements required by Core SQL:
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E101-01 | INSERT statement | Supported |
//! | E101-03 | Searched UPDATE statement | Supported |
//! | E101-04 | Searched DELETE statement | Supported |
//!
//! Related features:
//! - F222: INSERT statement: DEFAULT VALUES clause
//! - F312: MERGE statement
//! - T641: Multiple column assignment in UPDATE
//!
//! E101 is a CORE feature (mandatory for SQL:2016 conformance).
//!
//! Tests that fail indicate gaps in DataFusion's SQL:2016 conformance.

use crate::{assert_plans, assert_feature_supported};

// ============================================================================
// E101-01: INSERT statement
// ============================================================================

/// E101-01: Basic INSERT with VALUES clause
#[test]
fn e101_01_insert_values_basic() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
        "E101-01",
        "Basic INSERT with VALUES"
    );
}

/// E101-01: INSERT with explicit column list
#[test]
fn e101_01_insert_with_columns() {
    assert_feature_supported!(
        "INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date)
         VALUES (1, 'John', 'Doe', 30, 'CA', 50000.0, TIMESTAMP '2000-01-01 00:00:00')",
        "E101-01",
        "INSERT with explicit column list"
    );
}

/// E101-01: INSERT with column subset (partial column list)
#[test]
fn e101_01_insert_column_subset() {
    assert_feature_supported!(
        "INSERT INTO person (id, first_name, last_name) VALUES (1, 'John', 'Doe')",
        "E101-01",
        "INSERT with column subset"
    );
}

/// E101-01: Multi-row INSERT
#[test]
fn e101_01_insert_multi_row() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)",
        "E101-01",
        "Multi-row INSERT"
    );
}

/// E101-01: Multi-row INSERT with explicit columns
#[test]
fn e101_01_insert_multi_row_with_columns() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 'x'), (3, 4, 'y')",
        "E101-01",
        "Multi-row INSERT with columns"
    );
}

/// E101-01: INSERT with expressions in VALUES
#[test]
fn e101_01_insert_with_expressions() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1 + 2, 3 * 4, UPPER('test'))",
        "E101-01",
        "INSERT with expressions"
    );
}

/// E101-01: INSERT with arithmetic expressions
#[test]
fn e101_01_insert_arithmetic_expressions() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (10 + 5, 20 - 3, 25 + 25)",
        "E101-01",
        "INSERT with arithmetic expressions"
    );
}

/// E101-01: INSERT with NULL values
#[test]
fn e101_01_insert_with_nulls() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, NULL, 'test')",
        "E101-01",
        "INSERT with NULL values"
    );
}

/// E101-01: INSERT ... SELECT statement
#[test]
fn e101_01_insert_select() {
    assert_feature_supported!(
        "INSERT INTO t1 SELECT * FROM t2",
        "E101-01",
        "INSERT ... SELECT"
    );
}

/// E101-01: INSERT ... SELECT with WHERE clause
#[test]
fn e101_01_insert_select_filtered() {
    assert_feature_supported!(
        "INSERT INTO t1 SELECT * FROM t2 WHERE a > 10",
        "E101-01",
        "INSERT ... SELECT with WHERE"
    );
}

/// E101-01: INSERT ... SELECT with column mapping
#[test]
fn e101_01_insert_select_column_mapping() {
    assert_feature_supported!(
        "INSERT INTO t1 (a, b) SELECT b, a FROM t2",
        "E101-01",
        "INSERT ... SELECT with column mapping"
    );
}

/// E101-01: INSERT ... SELECT with expressions
#[test]
fn e101_01_insert_select_expressions() {
    assert_feature_supported!(
        "INSERT INTO t1 (a, b, c) SELECT a * 2, b + 10, UPPER(c) FROM t2",
        "E101-01",
        "INSERT ... SELECT with expressions"
    );
}

/// E101-01: INSERT ... SELECT with aggregation
#[test]
fn e101_01_insert_select_aggregation() {
    assert_feature_supported!(
        "INSERT INTO t1 (a, b) SELECT a, COUNT(*) FROM t2 GROUP BY a",
        "E101-01",
        "INSERT ... SELECT with aggregation"
    );
}

/// E101-01: INSERT ... SELECT with JOIN
#[test]
fn e101_01_insert_select_join() {
    assert_feature_supported!(
        "INSERT INTO orders (order_id, customer_id, item, qty, price)
         SELECT o.order_id, p.id, o.item, o.qty, o.price
         FROM orders o
         JOIN person p ON o.customer_id = p.id",
        "E101-01",
        "INSERT ... SELECT with JOIN"
    );
}

/// E101-01: INSERT with subquery in VALUES
#[test]
fn e101_01_insert_subquery_values() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES ((SELECT MAX(a) FROM t2), 10, 'test')",
        "E101-01",
        "INSERT with subquery in VALUES"
    );
}

// ============================================================================
// F222: INSERT statement: DEFAULT VALUES clause
// ============================================================================

/// F222: INSERT DEFAULT VALUES
#[test]
fn f222_insert_default_values() {
    assert_feature_supported!(
        "INSERT INTO t DEFAULT VALUES",
        "F222",
        "INSERT DEFAULT VALUES"
    );
}

/// F222: INSERT with DEFAULT keyword for specific columns
#[test]
fn f222_insert_default_keyword() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, DEFAULT, 'test')",
        "F222",
        "INSERT with DEFAULT keyword"
    );
}

/// F222: INSERT with mixed DEFAULT and values
#[test]
fn f222_insert_mixed_default() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (DEFAULT, 42, DEFAULT)",
        "F222",
        "INSERT with mixed DEFAULT"
    );
}

// ============================================================================
// E101-03: Searched UPDATE statement
// ============================================================================

/// E101-03: Basic UPDATE statement
#[test]
fn e101_03_update_basic() {
    assert_feature_supported!(
        "UPDATE t SET a = 10",
        "E101-03",
        "Basic UPDATE"
    );
}

/// E101-03: UPDATE with WHERE clause
#[test]
fn e101_03_update_with_where() {
    assert_feature_supported!(
        "UPDATE t SET a = 10 WHERE b > 5",
        "E101-03",
        "UPDATE with WHERE"
    );
}

/// E101-03: UPDATE with complex WHERE condition
#[test]
fn e101_03_update_complex_where() {
    assert_feature_supported!(
        "UPDATE person SET salary = 60000.0 WHERE age > 30 AND state = 'CA'",
        "E101-03",
        "UPDATE with complex WHERE"
    );
}

/// E101-03: UPDATE multiple columns
#[test]
fn e101_03_update_multiple_columns() {
    assert_feature_supported!(
        "UPDATE person SET first_name = 'Jane', last_name = 'Smith'",
        "E101-03",
        "UPDATE multiple columns"
    );
}

/// E101-03: UPDATE with multiple columns and WHERE
#[test]
fn e101_03_update_multiple_where() {
    assert_feature_supported!(
        "UPDATE person SET first_name = 'Jane', last_name = 'Smith', age = 25 WHERE id = 1",
        "E101-03",
        "UPDATE multiple columns with WHERE"
    );
}

/// E101-03: UPDATE with expression
#[test]
fn e101_03_update_with_expression() {
    assert_feature_supported!(
        "UPDATE person SET age = age + 1",
        "E101-03",
        "UPDATE with expression"
    );
}

/// E101-03: UPDATE with arithmetic expression
#[test]
fn e101_03_update_arithmetic() {
    assert_feature_supported!(
        "UPDATE person SET salary = salary * 1.1 WHERE state = 'CA'",
        "E101-03",
        "UPDATE with arithmetic"
    );
}

/// E101-03: UPDATE with complex expression
#[test]
fn e101_03_update_complex_expression() {
    assert_feature_supported!(
        "UPDATE person SET salary = salary * 1.1 + 5000 WHERE age > 40",
        "E101-03",
        "UPDATE with complex expression"
    );
}

/// E101-03: UPDATE with column reference in expression
#[test]
fn e101_03_update_column_reference() {
    assert_feature_supported!(
        "UPDATE t SET a = b + 10",
        "E101-03",
        "UPDATE with column reference"
    );
}

/// E101-03: UPDATE with CASE expression
#[test]
fn e101_03_update_case_expression() {
    assert_feature_supported!(
        "UPDATE person SET salary = CASE
            WHEN state = 'CA' THEN salary * 1.2
            WHEN state = 'NY' THEN salary * 1.15
            ELSE salary * 1.1
         END",
        "E101-03",
        "UPDATE with CASE expression"
    );
}

/// E101-03: UPDATE with NULL
#[test]
fn e101_03_update_null() {
    assert_feature_supported!(
        "UPDATE t SET b = NULL WHERE a = 1",
        "E101-03",
        "UPDATE with NULL"
    );
}

/// E101-03: UPDATE with subquery
#[test]
fn e101_03_update_subquery() {
    assert_feature_supported!(
        "UPDATE person SET salary = (SELECT AVG(salary) FROM person WHERE state = 'CA') WHERE id = 1",
        "E101-03",
        "UPDATE with subquery"
    );
}

/// E101-03: UPDATE with scalar subquery in expression
#[test]
fn e101_03_update_scalar_subquery_expression() {
    assert_feature_supported!(
        "UPDATE t SET a = (SELECT MAX(a) FROM t2) + 10",
        "E101-03",
        "UPDATE with scalar subquery in expression"
    );
}

/// E101-03: UPDATE with IN subquery in WHERE
#[test]
fn e101_03_update_in_subquery() {
    assert_feature_supported!(
        "UPDATE person SET salary = salary * 1.1 WHERE id IN (SELECT customer_id FROM orders)",
        "E101-03",
        "UPDATE with IN subquery"
    );
}

/// E101-03: UPDATE with EXISTS subquery
#[test]
fn e101_03_update_exists_subquery() {
    assert_feature_supported!(
        "UPDATE person SET salary = salary * 1.2
         WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = person.id)",
        "E101-03",
        "UPDATE with EXISTS subquery"
    );
}

// ============================================================================
// T641: Multiple column assignment in UPDATE
// ============================================================================

/// T641: UPDATE with tuple assignment
#[test]
fn t641_update_tuple_assignment() {
    // SQL:2016 allows UPDATE SET (col1, col2) = (val1, val2)
    // This is different from UPDATE SET col1 = val1, col2 = val2
    assert_feature_supported!(
        "UPDATE t SET (a, b) = (10, 20)",
        "T641",
        "UPDATE with tuple assignment"
    );
}

/// T641: UPDATE with tuple assignment and subquery
#[test]
fn t641_update_tuple_subquery() {
    assert_feature_supported!(
        "UPDATE t SET (a, b) = (SELECT MAX(a), MIN(b) FROM t2)",
        "T641",
        "UPDATE tuple with subquery"
    );
}

/// T641: UPDATE with tuple assignment and WHERE
#[test]
fn t641_update_tuple_where() {
    assert_feature_supported!(
        "UPDATE t SET (a, b) = (100, 200) WHERE c = 'test'",
        "T641",
        "UPDATE tuple with WHERE"
    );
}

// ============================================================================
// E101-04: Searched DELETE statement
// ============================================================================

/// E101-04: DELETE without WHERE (delete all rows)
#[test]
fn e101_04_delete_all() {
    assert_feature_supported!(
        "DELETE FROM t",
        "E101-04",
        "DELETE all rows"
    );
}

/// E101-04: DELETE with WHERE clause
#[test]
fn e101_04_delete_with_where() {
    assert_feature_supported!(
        "DELETE FROM t WHERE a > 10",
        "E101-04",
        "DELETE with WHERE"
    );
}

/// E101-04: DELETE with equality condition
#[test]
fn e101_04_delete_equality() {
    assert_feature_supported!(
        "DELETE FROM person WHERE id = 1",
        "E101-04",
        "DELETE with equality"
    );
}

/// E101-04: DELETE with complex WHERE condition
#[test]
fn e101_04_delete_complex_where() {
    assert_feature_supported!(
        "DELETE FROM person WHERE age > 65 AND state = 'FL'",
        "E101-04",
        "DELETE with complex WHERE"
    );
}

/// E101-04: DELETE with OR condition
#[test]
fn e101_04_delete_or_condition() {
    assert_feature_supported!(
        "DELETE FROM person WHERE age < 18 OR age > 65",
        "E101-04",
        "DELETE with OR condition"
    );
}

/// E101-04: DELETE with NULL comparison
#[test]
fn e101_04_delete_null_comparison() {
    assert_feature_supported!(
        "DELETE FROM t WHERE b IS NULL",
        "E101-04",
        "DELETE with NULL comparison"
    );
}

/// E101-04: DELETE with NOT NULL comparison
#[test]
fn e101_04_delete_not_null() {
    assert_feature_supported!(
        "DELETE FROM t WHERE b IS NOT NULL",
        "E101-04",
        "DELETE with NOT NULL"
    );
}

/// E101-04: DELETE with BETWEEN
#[test]
fn e101_04_delete_between() {
    assert_feature_supported!(
        "DELETE FROM person WHERE age BETWEEN 18 AND 65",
        "E101-04",
        "DELETE with BETWEEN"
    );
}

/// E101-04: DELETE with IN list
#[test]
fn e101_04_delete_in_list() {
    assert_feature_supported!(
        "DELETE FROM person WHERE state IN ('CA', 'NY', 'TX')",
        "E101-04",
        "DELETE with IN list"
    );
}

/// E101-04: DELETE with LIKE
#[test]
fn e101_04_delete_like() {
    assert_feature_supported!(
        "DELETE FROM person WHERE first_name LIKE 'J%'",
        "E101-04",
        "DELETE with LIKE"
    );
}

/// E101-04: DELETE with subquery in WHERE
#[test]
fn e101_04_delete_subquery() {
    assert_feature_supported!(
        "DELETE FROM person WHERE id IN (SELECT customer_id FROM orders WHERE qty > 100)",
        "E101-04",
        "DELETE with subquery"
    );
}

/// E101-04: DELETE with EXISTS subquery
#[test]
fn e101_04_delete_exists() {
    assert_feature_supported!(
        "DELETE FROM person WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = person.id AND qty > 100)",
        "E101-04",
        "DELETE with EXISTS"
    );
}

/// E101-04: DELETE with NOT EXISTS subquery
#[test]
fn e101_04_delete_not_exists() {
    assert_feature_supported!(
        "DELETE FROM person WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = person.id)",
        "E101-04",
        "DELETE with NOT EXISTS"
    );
}

/// E101-04: DELETE with scalar subquery comparison
#[test]
fn e101_04_delete_scalar_subquery() {
    assert_feature_supported!(
        "DELETE FROM person WHERE salary > (SELECT AVG(salary) FROM person)",
        "E101-04",
        "DELETE with scalar subquery"
    );
}

/// E101-04: DELETE with complex expression in WHERE
#[test]
fn e101_04_delete_complex_expression() {
    assert_feature_supported!(
        "DELETE FROM person WHERE salary * 0.2 > 10000 AND age + 5 < 70",
        "E101-04",
        "DELETE with complex expression"
    );
}

// ============================================================================
// F312: MERGE statement
// ============================================================================

/// F312: Basic MERGE statement
#[test]
fn f312_merge_basic() {
    assert_feature_supported!(
        "MERGE INTO t1
         USING t2
         ON t1.a = t2.a
         WHEN MATCHED THEN UPDATE SET t1.b = t2.b
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (t2.a, t2.b, t2.c)",
        "F312",
        "Basic MERGE statement"
    );
}

/// F312: MERGE with UPDATE only
#[test]
fn f312_merge_update_only() {
    assert_feature_supported!(
        "MERGE INTO person
         USING orders
         ON person.id = orders.customer_id
         WHEN MATCHED THEN UPDATE SET person.salary = person.salary + 1000",
        "F312",
        "MERGE with UPDATE only"
    );
}

/// F312: MERGE with INSERT only
#[test]
fn f312_merge_insert_only() {
    assert_feature_supported!(
        "MERGE INTO t1
         USING t2
         ON t1.a = t2.a
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (t2.a, t2.b, t2.c)",
        "F312",
        "MERGE with INSERT only"
    );
}

/// F312: MERGE with subquery as source
#[test]
fn f312_merge_subquery_source() {
    assert_feature_supported!(
        "MERGE INTO t1
         USING (SELECT a, b, c FROM t2 WHERE a > 10) AS src
         ON t1.a = src.a
         WHEN MATCHED THEN UPDATE SET t1.b = src.b",
        "F312",
        "MERGE with subquery source"
    );
}

/// F312: MERGE with multiple column updates
#[test]
fn f312_merge_multiple_updates() {
    assert_feature_supported!(
        "MERGE INTO person
         USING orders
         ON person.id = orders.customer_id
         WHEN MATCHED THEN UPDATE SET
            person.salary = person.salary + 1000,
            person.state = 'CA'",
        "F312",
        "MERGE with multiple updates"
    );
}

/// F312: MERGE with DELETE when matched
#[test]
fn f312_merge_delete() {
    assert_feature_supported!(
        "MERGE INTO t1
         USING t2
         ON t1.a = t2.a
         WHEN MATCHED THEN DELETE",
        "F312",
        "MERGE with DELETE"
    );
}

/// F312: MERGE with conditional UPDATE
#[test]
fn f312_merge_conditional_update() {
    assert_feature_supported!(
        "MERGE INTO t1
         USING t2
         ON t1.a = t2.a
         WHEN MATCHED AND t2.b > 10 THEN UPDATE SET t1.c = t2.c
         WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (t2.a, t2.b, t2.c)",
        "F312",
        "MERGE with conditional UPDATE"
    );
}

/// F312: MERGE with expressions in UPDATE
#[test]
fn f312_merge_update_expressions() {
    assert_feature_supported!(
        "MERGE INTO person
         USING orders
         ON person.id = orders.customer_id
         WHEN MATCHED THEN UPDATE SET person.salary = person.salary * 1.1 + orders.price",
        "F312",
        "MERGE with expressions"
    );
}

// ============================================================================
// ON CONFLICT clause (PostgreSQL/SQLite upsert syntax)
// ============================================================================

/// ON CONFLICT DO NOTHING - silently skip conflicting rows
#[test]
fn on_conflict_do_nothing_basic() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT DO NOTHING",
        "E101-01+",
        "INSERT with ON CONFLICT DO NOTHING"
    );
}

/// ON CONFLICT (column) DO NOTHING - skip on specific column conflict
#[test]
fn on_conflict_column_do_nothing() {
    assert_feature_supported!(
        "INSERT INTO person (id, first_name, last_name) VALUES (1, 'John', 'Doe') ON CONFLICT (id) DO NOTHING",
        "E101-01+",
        "INSERT with ON CONFLICT (column) DO NOTHING"
    );
}

/// ON CONFLICT DO UPDATE - update conflicting rows
#[test]
fn on_conflict_do_update_basic() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a) DO UPDATE SET b = 10",
        "E101-01+",
        "INSERT with ON CONFLICT DO UPDATE"
    );
}

/// ON CONFLICT DO UPDATE with EXCLUDED pseudo-table
#[test]
fn on_conflict_do_update_excluded() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b, c = EXCLUDED.c",
        "E101-01+",
        "INSERT with ON CONFLICT DO UPDATE using EXCLUDED"
    );
}

/// ON CONFLICT DO UPDATE with expression
#[test]
fn on_conflict_do_update_expression() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a) DO UPDATE SET b = t.b + EXCLUDED.b",
        "E101-01+",
        "INSERT with ON CONFLICT DO UPDATE expression"
    );
}

/// ON CONFLICT DO UPDATE with WHERE clause
#[test]
fn on_conflict_do_update_where() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b WHERE t.c > 0",
        "E101-01+",
        "INSERT with ON CONFLICT DO UPDATE WHERE"
    );
}

/// ON CONFLICT with multiple conflict columns
#[test]
fn on_conflict_multiple_columns() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a, b) DO UPDATE SET c = EXCLUDED.c",
        "E101-01+",
        "INSERT with ON CONFLICT multiple columns"
    );
}

/// ON CONFLICT DO UPDATE with multiple assignments
#[test]
fn on_conflict_multiple_assignments() {
    assert_feature_supported!(
        "INSERT INTO person (id, first_name, last_name, age)
         VALUES (1, 'John', 'Doe', 30)
         ON CONFLICT (id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            age = EXCLUDED.age",
        "E101-01+",
        "INSERT with ON CONFLICT multiple assignments"
    );
}

/// ON CONFLICT with multi-row INSERT
#[test]
fn on_conflict_multi_row() {
    assert_feature_supported!(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3), (4, 5, 6) ON CONFLICT (a) DO NOTHING",
        "E101-01+",
        "Multi-row INSERT with ON CONFLICT"
    );
}

/// ON CONFLICT with INSERT ... SELECT
#[test]
fn on_conflict_insert_select() {
    assert_feature_supported!(
        "INSERT INTO t1 (a, b, c) SELECT a, b, c FROM t2 ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b",
        "E101-01+",
        "INSERT SELECT with ON CONFLICT"
    );
}

// ============================================================================
// Summary Tests - Verify overall E101 support
// ============================================================================

#[test]
fn e101_summary_all_dml_operations() {
    // This test verifies that all E101 subfeatures work together
    // in realistic scenarios

    // Test INSERT variations
    assert_plans!("INSERT INTO t (a, b, c) VALUES (1, 2, 3)");
    assert_plans!("INSERT INTO t (a, b) VALUES (1, 2)");
    assert_plans!("INSERT INTO t1 SELECT * FROM t2");

    // Test UPDATE variations
    assert_plans!("UPDATE t SET a = 10 WHERE b > 5");
    assert_plans!("UPDATE person SET salary = salary * 1.1, age = age + 1 WHERE state = 'CA'");

    // Test DELETE variations
    assert_plans!("DELETE FROM t WHERE a > 10");
    assert_plans!("DELETE FROM person WHERE id IN (SELECT customer_id FROM orders)");
}

#[test]
fn e101_summary_complex_scenarios() {
    // Complex real-world scenarios combining multiple features

    // Multi-row insert with expressions
    assert_plans!(
        "INSERT INTO t (a, b, c) VALUES
         (1 + 1, 2 * 2, UPPER('test')),
         (10, 20, 'direct'),
         (NULL, DEFAULT, 'mixed')"
    );

    // Update with subquery and complex WHERE
    assert_plans!(
        "UPDATE person
         SET salary = (SELECT AVG(salary) * 1.2 FROM person WHERE state = person.state)
         WHERE age > 30
           AND EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = person.id)"
    );

    // Delete with correlated subquery
    assert_plans!(
        "DELETE FROM orders
         WHERE price < (SELECT AVG(price) FROM orders o2 WHERE o2.item = orders.item) * 0.5"
    );
}

#[test]
fn e101_summary_insert_select_combinations() {
    // INSERT ... SELECT with various query features

    // With aggregation and GROUP BY
    assert_plans!(
        "INSERT INTO t1 (a, b)
         SELECT state, COUNT(*)
         FROM person
         GROUP BY state"
    );

    // With JOIN
    assert_plans!(
        "INSERT INTO t1 (a, b, c)
         SELECT p.id, p.first_name, o.order_id
         FROM person p
         JOIN orders o ON p.id = o.customer_id
         WHERE o.qty > 10"
    );

    // With UNION
    assert_plans!(
        "INSERT INTO t1 (a, b, c)
         SELECT a, b, c FROM t2 WHERE a > 0
         UNION ALL
         SELECT a, b, c FROM t3 WHERE a < 0"
    );
}
