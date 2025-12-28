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

//! SQL:2016 Features T121 (WITH clause) and T131 (Recursive query)
//!
//! ISO/IEC 9075-2:2016 Section 7.16
//!
//! These features provide Common Table Expressions (CTEs), which allow
//! defining temporary named result sets that can be referenced within
//! a single SQL statement.
//!
//! # T121: WITH clause (non-recursive)
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | T121 | WITH clause in query expression | Supported |
//!
//! # T131: Recursive query
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | T131 | WITH RECURSIVE | Gap |
//!
//! # Common Table Expressions (CTEs)
//!
//! CTEs provide several benefits:
//! - Improved query readability by breaking complex queries into logical steps
//! - Ability to reference the same subquery multiple times without duplication
//! - Support for recursive queries (WITH RECURSIVE)
//! - Better query organization and maintenance
//!
//! # Syntax
//!
//! ```sql
//! WITH cte_name AS (
//!     SELECT ...
//! )
//! SELECT * FROM cte_name;
//!
//! -- Multiple CTEs
//! WITH cte1 AS (...),
//!      cte2 AS (...)
//! SELECT ...;
//!
//! -- Recursive CTE
//! WITH RECURSIVE cte AS (
//!     -- Base case
//!     SELECT ...
//!     UNION ALL
//!     -- Recursive case
//!     SELECT ... FROM cte ...
//! )
//! SELECT * FROM cte;
//! ```

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// T121: WITH clause (non-recursive CTEs)
// ============================================================================

/// T121: Basic CTE in query expression
#[test]
fn t121_basic_cte() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t) \
         SELECT * FROM cte",
        "T121",
        "Basic CTE"
    );
}

/// T121: CTE with WHERE clause
#[test]
fn t121_cte_with_where() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t WHERE a > 10) \
         SELECT * FROM cte",
        "T121",
        "CTE with WHERE clause"
    );
}

/// T121: CTE with aggregation
#[test]
fn t121_cte_with_aggregation() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, COUNT(*) AS cnt FROM t GROUP BY a) \
         SELECT * FROM cte WHERE cnt > 5",
        "T121",
        "CTE with aggregation"
    );
}

/// T121: CTE with JOIN
#[test]
fn t121_cte_with_join() {
    assert_feature_supported!(
        "WITH cte AS (SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.a = t2.a) \
         SELECT * FROM cte",
        "T121",
        "CTE with JOIN"
    );
}

/// T121: Multiple CTEs
#[test]
fn t121_multiple_ctes() {
    assert_feature_supported!(
        "WITH cte1 AS (SELECT a FROM t), \
              cte2 AS (SELECT b FROM t) \
         SELECT * FROM cte1, cte2",
        "T121",
        "Multiple CTEs"
    );
}

/// T121: CTE referencing another CTE (nested)
#[test]
fn t121_nested_ctes() {
    assert_feature_supported!(
        "WITH cte1 AS (SELECT a, b FROM t), \
              cte2 AS (SELECT a FROM cte1 WHERE b > 10) \
         SELECT * FROM cte2",
        "T121",
        "Nested CTEs"
    );
}

/// T121: CTE with column aliases
#[test]
fn t121_cte_with_column_aliases() {
    assert_feature_supported!(
        "WITH cte(col1, col2) AS (SELECT a, b FROM t) \
         SELECT col1, col2 FROM cte",
        "T121",
        "CTE with column aliases"
    );
}

/// T121: CTE referenced multiple times
#[test]
fn t121_cte_referenced_multiple_times() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t) \
         SELECT cte1.a, cte2.b \
         FROM cte AS cte1 \
         JOIN cte AS cte2 ON cte1.a = cte2.a",
        "T121",
        "CTE referenced multiple times"
    );
}

/// T121: CTE in subquery
#[test]
fn t121_cte_in_subquery() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t) \
         SELECT * FROM cte WHERE a IN (SELECT a FROM t WHERE b > 5)",
        "T121",
        "CTE with subquery"
    );
}

/// T121: CTE with DISTINCT
#[test]
fn t121_cte_with_distinct() {
    assert_feature_supported!(
        "WITH cte AS (SELECT DISTINCT a FROM t) \
         SELECT * FROM cte",
        "T121",
        "CTE with DISTINCT"
    );
}

/// T121: CTE with ORDER BY
#[test]
fn t121_cte_with_order_by() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t ORDER BY a) \
         SELECT * FROM cte",
        "T121",
        "CTE with ORDER BY"
    );
}

/// T121: CTE with LIMIT
#[test]
fn t121_cte_with_limit() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t LIMIT 10) \
         SELECT * FROM cte",
        "T121",
        "CTE with LIMIT"
    );
}

/// T121: CTE with UNION
#[test]
fn t121_cte_with_union() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a FROM t1 UNION SELECT a FROM t2) \
         SELECT * FROM cte",
        "T121",
        "CTE with UNION"
    );
}

/// T121: CTE in INSERT statement
#[test]
fn t121_cte_in_insert() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t1) \
         INSERT INTO t2 (a, b) SELECT * FROM cte",
        "T121",
        "CTE in INSERT"
    );
}

/// T121: CTE with complex expressions
#[test]
fn t121_cte_with_complex_expressions() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b, a + b AS sum_val, a * b AS prod_val FROM t) \
         SELECT * FROM cte WHERE sum_val > 100",
        "T121",
        "CTE with complex expressions"
    );
}

/// T121: CTE with CASE expression
#[test]
fn t121_cte_with_case() {
    assert_feature_supported!(
        "WITH cte AS (
           SELECT a,
                  CASE WHEN b > 10 THEN 'high' ELSE 'low' END AS category
           FROM t
         ) \
         SELECT * FROM cte WHERE category = 'high'",
        "T121",
        "CTE with CASE expression"
    );
}

/// T121: Multiple CTEs with different complexity
#[test]
fn t121_multiple_ctes_complex() {
    assert_feature_supported!(
        "WITH \
           high_value AS (SELECT * FROM t WHERE a > 100), \
           low_value AS (SELECT * FROM t WHERE a <= 100), \
           summary AS (
             SELECT 'high' AS category, COUNT(*) AS cnt FROM high_value
             UNION ALL
             SELECT 'low' AS category, COUNT(*) AS cnt FROM low_value
           ) \
         SELECT * FROM summary",
        "T121",
        "Multiple CTEs with different complexity"
    );
}

/// T121: CTE with LEFT JOIN
#[test]
fn t121_cte_with_left_join() {
    assert_feature_supported!(
        "WITH cte AS (
           SELECT p.id, p.first_name, o.order_id
           FROM person p
           LEFT JOIN orders o ON p.id = o.customer_id
         ) \
         SELECT * FROM cte",
        "T121",
        "CTE with LEFT JOIN"
    );
}

/// T121: CTE with self-join
#[test]
fn t121_cte_with_self_join() {
    assert_feature_supported!(
        "WITH cte AS (
           SELECT a.id, a.first_name, b.first_name AS other_name
           FROM person a
           JOIN person b ON a.id = b.id + 1
         ) \
         SELECT * FROM cte",
        "T121",
        "CTE with self-join"
    );
}

/// T121: CTE with window functions
#[test]
fn t121_cte_with_window_functions() {
    assert_feature_supported!(
        "WITH cte AS (
           SELECT a, b, ROW_NUMBER() OVER (ORDER BY a) AS row_num
           FROM t
         ) \
         SELECT * FROM cte WHERE row_num <= 10",
        "T121",
        "CTE with window functions"
    );
}

/// T121: CTE with HAVING clause
#[test]
fn t121_cte_with_having() {
    assert_feature_supported!(
        "WITH cte AS (
           SELECT state, AVG(salary) AS avg_salary
           FROM person
           GROUP BY state
           HAVING AVG(salary) > 50000
         ) \
         SELECT * FROM cte",
        "T121",
        "CTE with HAVING clause"
    );
}

// ============================================================================
// T131: WITH RECURSIVE (Recursive CTEs)
// ============================================================================

/// T131: Basic recursive CTE
#[test]
fn t131_basic_recursive_cte() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE cte AS (
           SELECT 1 AS n
           UNION ALL
           SELECT n + 1 FROM cte WHERE n < 10
         ) \
         SELECT * FROM cte",
        "T131",
        "Basic recursive CTE"
    );
}

/// T131: Recursive CTE with base case and recursive case
#[test]
fn t131_recursive_with_base_and_recursive() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE numbers AS (
           -- Base case
           SELECT 1 AS n
           UNION ALL
           -- Recursive case
           SELECT n + 1 FROM numbers WHERE n < 100
         ) \
         SELECT * FROM numbers",
        "T131",
        "Recursive CTE with base and recursive case"
    );
}

/// T131: Recursive CTE for hierarchical data (organization chart)
#[test]
fn t131_recursive_org_chart() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE org_chart AS (
           -- Base case: top-level employees
           SELECT id, first_name, id AS manager_id, 0 AS level
           FROM person
           WHERE id = 1
           UNION ALL
           -- Recursive case: employees reporting to current level
           SELECT p.id, p.first_name, p.id AS manager_id, oc.level + 1
           FROM person p
           JOIN org_chart oc ON p.id = oc.manager_id + 1
           WHERE oc.level < 5
         ) \
         SELECT * FROM org_chart",
        "T131",
        "Recursive CTE for org chart"
    );
}

/// T131: Recursive CTE for tree traversal
#[test]
fn t131_recursive_tree_traversal() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE tree AS (
           SELECT id, first_name, id AS parent_id, 1 AS depth
           FROM person
           WHERE id = 1
           UNION ALL
           SELECT p.id, p.first_name, t.id AS parent_id, t.depth + 1
           FROM person p
           JOIN tree t ON p.id = t.id + 1
         ) \
         SELECT * FROM tree",
        "T131",
        "Recursive CTE for tree traversal"
    );
}

/// T131: Recursive CTE with UNION (distinct, removes duplicates)
#[test]
fn t131_recursive_union_distinct() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE cte AS (
           SELECT a FROM t WHERE a = 1
           UNION
           SELECT a + 1 FROM cte WHERE a < 10
         ) \
         SELECT * FROM cte",
        "T131",
        "Recursive CTE with UNION distinct"
    );
}

/// T131: Recursive CTE with multiple columns
#[test]
fn t131_recursive_multiple_columns() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE sequence AS (
           SELECT 1 AS n, 1 AS factorial
           UNION ALL
           SELECT n + 1, factorial * (n + 1)
           FROM sequence
           WHERE n < 10
         ) \
         SELECT * FROM sequence",
        "T131",
        "Recursive CTE with multiple columns"
    );
}

/// T131: Recursive CTE for path finding
#[test]
fn t131_recursive_path_finding() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE paths AS (
           SELECT id, first_name, CAST(first_name AS VARCHAR) AS path
           FROM person
           WHERE id = 1
           UNION ALL
           SELECT p.id, p.first_name, paths.path || ' -> ' || p.first_name
           FROM person p
           JOIN paths ON p.id = paths.id + 1
         ) \
         SELECT * FROM paths",
        "T131",
        "Recursive CTE for path finding"
    );
}

/// T131: Recursive CTE with WHERE clause in recursive part
#[test]
fn t131_recursive_with_where() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE filtered AS (
           SELECT a, b FROM t WHERE a = 1
           UNION ALL
           SELECT t.a, t.b
           FROM t
           JOIN filtered ON t.a = filtered.a + 1
           WHERE t.b > 0
         ) \
         SELECT * FROM filtered",
        "T131",
        "Recursive CTE with WHERE in recursive part"
    );
}

/// T131: Recursive CTE with aggregation
#[test]
fn t131_recursive_with_aggregation() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE running_totals AS (
           SELECT a, b, b AS running_total
           FROM t
           WHERE a = 1
           UNION ALL
           SELECT t.a, t.b, rt.running_total + t.b
           FROM t
           JOIN running_totals rt ON t.a = rt.a + 1
         ) \
         SELECT * FROM running_totals",
        "T131",
        "Recursive CTE with running totals"
    );
}

/// T131: Recursive CTE for Fibonacci sequence
#[test]
fn t131_recursive_fibonacci() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE fibonacci AS (
           SELECT 1 AS n, 0 AS fib_n, 1 AS fib_n_plus_1
           UNION ALL
           SELECT n + 1, fib_n_plus_1, fib_n + fib_n_plus_1
           FROM fibonacci
           WHERE n < 20
         ) \
         SELECT n, fib_n FROM fibonacci",
        "T131",
        "Recursive CTE for Fibonacci sequence"
    );
}

/// T131: Recursive CTE with cycle detection
#[test]
fn t131_recursive_cycle_detection() {
    // GAP: DataFusion does not currently support WITH RECURSIVE or CYCLE clause
    assert_feature_supported!(
        "WITH RECURSIVE paths AS (
           SELECT id, first_name, ARRAY[id] AS path
           FROM person
           WHERE id = 1
           UNION ALL
           SELECT p.id, p.first_name, paths.path || p.id
           FROM person p
           JOIN paths ON p.id = paths.id + 1
           WHERE NOT (p.id = ANY(paths.path))
         ) \
         SELECT * FROM paths",
        "T131",
        "Recursive CTE with cycle detection"
    );
}

/// T131: Recursive CTE with multiple levels
#[test]
fn t131_recursive_multiple_levels() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE hierarchy AS (
           SELECT id, first_name, 0 AS level
           FROM person
           WHERE id = 1
           UNION ALL
           SELECT p.id, p.first_name, h.level + 1
           FROM person p
           JOIN hierarchy h ON p.id = h.id + 1
           WHERE h.level < 10
         ) \
         SELECT * FROM hierarchy ORDER BY level, id",
        "T131",
        "Recursive CTE with multiple levels"
    );
}

/// T131: Recursive CTE for bill of materials (BOM)
#[test]
fn t131_recursive_bill_of_materials() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE bom AS (
           SELECT order_id, item, qty, 1 AS level
           FROM orders
           WHERE order_id = 1
           UNION ALL
           SELECT o.order_id, o.item, o.qty * bom.qty, bom.level + 1
           FROM orders o
           JOIN bom ON o.order_id = bom.order_id + 1
           WHERE bom.level < 5
         ) \
         SELECT * FROM bom",
        "T131",
        "Recursive CTE for bill of materials"
    );
}

/// T131: Recursive CTE with LEFT JOIN
#[test]
fn t131_recursive_with_left_join() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE tree AS (
           SELECT id, first_name, 1 AS level
           FROM person
           WHERE id = 1
           UNION ALL
           SELECT p.id, p.first_name, t.level + 1
           FROM tree t
           LEFT JOIN person p ON p.id = t.id + 1
           WHERE t.level < 10 AND p.id IS NOT NULL
         ) \
         SELECT * FROM tree",
        "T131",
        "Recursive CTE with LEFT JOIN"
    );
}

/// T131: Multiple recursive CTEs
#[test]
fn t131_multiple_recursive_ctes() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE \
           cte1 AS (
             SELECT 1 AS n
             UNION ALL
             SELECT n + 1 FROM cte1 WHERE n < 10
           ), \
           cte2 AS (
             SELECT 1 AS m
             UNION ALL
             SELECT m * 2 FROM cte2 WHERE m < 100
           ) \
         SELECT cte1.n, cte2.m FROM cte1, cte2",
        "T131",
        "Multiple recursive CTEs"
    );
}

// ============================================================================
// Combined T121/T131 Tests
// ============================================================================

/// T121/T131: Mix of recursive and non-recursive CTEs
#[test]
fn t121_t131_mixed_ctes() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH RECURSIVE \
           base_data AS (SELECT a, b FROM t WHERE a > 10), \
           recursive_data AS (
             SELECT a FROM base_data WHERE a = 11
             UNION ALL
             SELECT bd.a
             FROM base_data bd
             JOIN recursive_data rd ON bd.a = rd.a + 1
             WHERE rd.a < 20
           ) \
         SELECT * FROM recursive_data",
        "T121/T131",
        "Mix of recursive and non-recursive CTEs"
    );
}

/// T121/T131: Recursive CTE using non-recursive CTE
#[test]
fn t121_t131_recursive_using_non_recursive() {
    // GAP: DataFusion does not currently support WITH RECURSIVE
    assert_feature_supported!(
        "WITH \
           filtered AS (SELECT * FROM person WHERE age > 18), \
           RECURSIVE ancestors AS (
             SELECT id, first_name, 0 AS generation
             FROM filtered
             WHERE id = 1
             UNION ALL
             SELECT f.id, f.first_name, a.generation + 1
             FROM filtered f
             JOIN ancestors a ON f.id = a.id + 1
           ) \
         SELECT * FROM ancestors",
        "T121/T131",
        "Recursive CTE using non-recursive CTE"
    );
}

/// T121: CTE in UPDATE statement
#[test]
fn t121_cte_in_update() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a, b FROM t1 WHERE a > 10) \
         UPDATE t2 SET b = cte.b FROM cte WHERE t2.a = cte.a",
        "T121",
        "CTE in UPDATE"
    );
}

/// T121: CTE in DELETE statement
#[test]
fn t121_cte_in_delete() {
    assert_feature_supported!(
        "WITH cte AS (SELECT a FROM t1 WHERE b > 100) \
         DELETE FROM t2 WHERE a IN (SELECT a FROM cte)",
        "T121",
        "CTE in DELETE"
    );
}

/// T121: Deeply nested CTEs (5 levels)
#[test]
fn t121_deeply_nested_ctes() {
    assert_feature_supported!(
        "WITH \
           cte1 AS (SELECT a FROM t), \
           cte2 AS (SELECT a FROM cte1 WHERE a > 10), \
           cte3 AS (SELECT a FROM cte2 WHERE a > 20), \
           cte4 AS (SELECT a FROM cte3 WHERE a > 30), \
           cte5 AS (SELECT a FROM cte4 WHERE a > 40) \
         SELECT * FROM cte5",
        "T121",
        "Deeply nested CTEs"
    );
}
