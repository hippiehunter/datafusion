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

//! SQL:2016 Features F850-F867 - ORDER BY and LIMIT/OFFSET/FETCH clauses
//!
//! ISO/IEC 9075-2:2016 Section 7.13 (query expression) and 14.1 (declare cursor)
//!
//! This feature set covers ordering and limiting result sets:
//! - ORDER BY clause (column references, expressions, aliases, ordinal positions)
//! - ORDER BY with ASC/DESC
//! - ORDER BY with NULLS FIRST/NULLS LAST
//! - LIMIT and OFFSET clauses
//! - FETCH FIRST/NEXT syntax (SQL:2008)
//! - OFFSET...ROWS FETCH...ROWS syntax
//! - FETCH FIRST WITH TIES option
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | F850 | Top-level ORDER BY in query expression | Supported |
//! | F851 | ORDER BY in subqueries | Supported |
//! | F852 | Top-level ORDER BY in views | Supported |
//! | F855 | Nested ORDER BY in query expression | Supported |
//! | F856 | Nested FETCH FIRST in query expression | Partial |
//! | F857 | Top-level FETCH FIRST in query expression | Partial |
//! | F858 | FETCH FIRST in subqueries | Partial |
//! | F859 | Top-level FETCH FIRST in views | Partial |
//! | F860 | Dynamic FETCH FIRST row count | Not Tested |
//! | F861 | Top-level OFFSET in query expression | Supported |
//! | F862 | OFFSET in subqueries | Supported |
//! | F865 | Dynamic offset row count | Not Tested |
//! | F867 | FETCH FIRST WITH TIES option | Not Tested |

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// F850: Top-level ORDER BY in query expression
// ============================================================================

/// F850-01: Basic ORDER BY with single column
#[test]
fn f850_01_order_by_single_column() {
    assert_feature_supported!(
        "SELECT a, b FROM t ORDER BY a",
        "F850-01",
        "ORDER BY single column"
    );
}

/// F850-02: ORDER BY with multiple columns
#[test]
fn f850_02_order_by_multiple_columns() {
    assert_feature_supported!(
        "SELECT first_name, last_name, age FROM person ORDER BY last_name, first_name",
        "F850-02",
        "ORDER BY multiple columns"
    );
}

/// F850-03: ORDER BY with ASC
#[test]
fn f850_03_order_by_asc() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a ASC",
        "F850-03",
        "ORDER BY ASC"
    );
}

/// F850-04: ORDER BY with DESC
#[test]
fn f850_04_order_by_desc() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a DESC",
        "F850-04",
        "ORDER BY DESC"
    );
}

/// F850-05: ORDER BY with mixed ASC/DESC
#[test]
fn f850_05_order_by_mixed_asc_desc() {
    assert_feature_supported!(
        "SELECT first_name, last_name, age FROM person ORDER BY last_name ASC, age DESC",
        "F850-05",
        "ORDER BY mixed ASC/DESC"
    );
}

/// F850-06: ORDER BY with expression
#[test]
fn f850_06_order_by_expression() {
    assert_feature_supported!(
        "SELECT first_name, salary FROM person ORDER BY salary * 12",
        "F850-06",
        "ORDER BY expression"
    );
}

/// F850-07: ORDER BY with aggregate expression
#[test]
fn f850_07_order_by_aggregate() {
    assert_feature_supported!(
        "SELECT state, COUNT(*) as cnt FROM person GROUP BY state ORDER BY COUNT(*)",
        "F850-07",
        "ORDER BY aggregate"
    );
}

/// F850-08: ORDER BY with column alias
#[test]
fn f850_08_order_by_alias() {
    assert_feature_supported!(
        "SELECT first_name, salary * 12 AS annual_salary FROM person ORDER BY annual_salary",
        "F850-08",
        "ORDER BY column alias"
    );
}

/// F850-09: ORDER BY with ordinal position
#[test]
fn f850_09_order_by_ordinal() {
    assert_feature_supported!(
        "SELECT first_name, last_name FROM person ORDER BY 2, 1",
        "F850-09",
        "ORDER BY ordinal position"
    );
}

/// F850-10: ORDER BY with NULLS FIRST
#[test]
fn f850_10_order_by_nulls_first() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a NULLS FIRST",
        "F850-10",
        "ORDER BY NULLS FIRST"
    );
}

/// F850-11: ORDER BY with NULLS LAST
#[test]
fn f850_11_order_by_nulls_last() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a NULLS LAST",
        "F850-11",
        "ORDER BY NULLS LAST"
    );
}

/// F850-12: ORDER BY with DESC NULLS FIRST
#[test]
fn f850_12_order_by_desc_nulls_first() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a DESC NULLS FIRST",
        "F850-12",
        "ORDER BY DESC NULLS FIRST"
    );
}

/// F850-13: ORDER BY with multiple columns and null ordering
#[test]
fn f850_13_order_by_multiple_null_ordering() {
    assert_feature_supported!(
        "SELECT a, b FROM t ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST",
        "F850-13",
        "ORDER BY multiple columns with null ordering"
    );
}

/// F850-14: ORDER BY with DISTINCT
#[test]
fn f850_14_order_by_with_distinct() {
    assert_feature_supported!(
        "SELECT DISTINCT state FROM person ORDER BY state",
        "F850-14",
        "ORDER BY with DISTINCT"
    );
}

/// F850-15: ORDER BY in UNION query
#[test]
fn f850_15_order_by_in_union() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a",
        "F850-15",
        "ORDER BY in UNION"
    );
}

/// F850-16: ORDER BY in INTERSECT query
#[test]
fn f850_16_order_by_in_intersect() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2 ORDER BY a",
        "F850-16",
        "ORDER BY in INTERSECT"
    );
}

/// F850-17: ORDER BY in EXCEPT query
#[test]
fn f850_17_order_by_in_except() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2 ORDER BY a",
        "F850-17",
        "ORDER BY in EXCEPT"
    );
}

// ============================================================================
// F851: ORDER BY in subqueries
// ============================================================================

/// F851-01: ORDER BY in scalar subquery
#[test]
fn f851_01_order_by_in_scalar_subquery() {
    assert_feature_supported!(
        "SELECT (SELECT a FROM t ORDER BY a LIMIT 1) as first_a FROM t1",
        "F851-01",
        "ORDER BY in scalar subquery"
    );
}

/// F851-02: ORDER BY in subquery in FROM clause
#[test]
fn f851_02_order_by_in_from_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t ORDER BY a) AS subq",
        "F851-02",
        "ORDER BY in FROM subquery"
    );
}

/// F851-03: ORDER BY in EXISTS subquery
#[test]
fn f851_03_order_by_in_exists_subquery() {
    assert_feature_supported!(
        "SELECT a FROM t WHERE EXISTS (SELECT b FROM t1 WHERE t1.a = t.a ORDER BY b LIMIT 1)",
        "F851-03",
        "ORDER BY in EXISTS subquery"
    );
}

/// F851-04: ORDER BY in IN subquery
#[test]
fn f851_04_order_by_in_in_subquery() {
    assert_feature_supported!(
        "SELECT a FROM t WHERE a IN (SELECT b FROM t1 ORDER BY b)",
        "F851-04",
        "ORDER BY in IN subquery"
    );
}

/// F851-05: ORDER BY in correlated subquery
#[test]
fn f851_05_order_by_in_correlated_subquery() {
    assert_feature_supported!(
        "SELECT a, (SELECT MAX(b) FROM t1 WHERE t1.a = t.a ORDER BY b LIMIT 1) FROM t",
        "F851-05",
        "ORDER BY in correlated subquery"
    );
}

// ============================================================================
// F852: Top-level ORDER BY in views
// ============================================================================

/// F852-01: CREATE VIEW with ORDER BY
#[test]
fn f852_01_view_with_order_by() {
    assert_feature_supported!(
        "CREATE VIEW sorted_person AS SELECT * FROM person ORDER BY last_name, first_name",
        "F852-01",
        "CREATE VIEW with ORDER BY"
    );
}

/// F852-02: CREATE VIEW with ORDER BY and LIMIT
#[test]
fn f852_02_view_with_order_by_limit() {
    assert_feature_supported!(
        "CREATE VIEW top_earners AS SELECT first_name, salary FROM person ORDER BY salary DESC LIMIT 10",
        "F852-02",
        "CREATE VIEW with ORDER BY and LIMIT"
    );
}

// ============================================================================
// F855: Nested ORDER BY in query expression
// ============================================================================

/// F855-01: ORDER BY in nested query expression
#[test]
fn f855_01_nested_order_by() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (SELECT a, b FROM t ORDER BY a) ORDER BY b)",
        "F855-01",
        "Nested ORDER BY"
    );
}

/// F855-02: ORDER BY in CTE with ORDER BY in main query
#[test]
fn f855_02_order_by_in_cte_and_main() {
    assert_feature_supported!(
        "WITH ordered_people AS (SELECT * FROM person ORDER BY age) SELECT * FROM ordered_people ORDER BY salary",
        "F855-02",
        "ORDER BY in CTE and main query"
    );
}

/// F855-03: ORDER BY in multiple CTEs
#[test]
fn f855_03_order_by_in_multiple_ctes() {
    assert_feature_supported!(
        "WITH cte1 AS (SELECT * FROM t1 ORDER BY a), cte2 AS (SELECT * FROM t2 ORDER BY b) SELECT * FROM cte1 JOIN cte2 ON cte1.a = cte2.a ORDER BY cte1.a",
        "F855-03",
        "ORDER BY in multiple CTEs"
    );
}

// ============================================================================
// F856: Nested FETCH FIRST in query expression
// ============================================================================

/// F856-01: FETCH FIRST in nested subquery
#[test]
fn f856_01_nested_fetch_first() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t ORDER BY a FETCH FIRST 5 ROWS ONLY)",
        "F856-01",
        "FETCH FIRST in nested subquery"
    );
}

/// F856-02: FETCH FIRST in CTE
#[test]
fn f856_02_fetch_first_in_cte() {
    assert_feature_supported!(
        "WITH limited AS (SELECT * FROM person ORDER BY salary DESC FETCH FIRST 10 ROWS ONLY) SELECT * FROM limited",
        "F856-02",
        "FETCH FIRST in CTE"
    );
}

// ============================================================================
// F857: Top-level FETCH FIRST in query expression
// ============================================================================

/// F857-01: Basic FETCH FIRST
#[test]
fn f857_01_basic_fetch_first() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a FETCH FIRST 10 ROWS ONLY",
        "F857-01",
        "Basic FETCH FIRST"
    );
}

/// F857-02: FETCH NEXT (synonym for FETCH FIRST)
#[test]
fn f857_02_fetch_next() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a FETCH NEXT 5 ROWS ONLY",
        "F857-02",
        "FETCH NEXT"
    );
}

/// F857-03: FETCH FIRST 1 ROW
#[test]
fn f857_03_fetch_first_one_row() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a FETCH FIRST 1 ROW ONLY",
        "F857-03",
        "FETCH FIRST 1 ROW"
    );
}

/// F857-04: FETCH FIRST without ORDER BY
#[test]
fn f857_04_fetch_first_without_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t FETCH FIRST 10 ROWS ONLY",
        "F857-04",
        "FETCH FIRST without ORDER BY"
    );
}

/// F857-05: FETCH FIRST with complex expression
#[test]
fn f857_05_fetch_first_complex() {
    assert_feature_supported!(
        "SELECT first_name, salary FROM person WHERE age > 25 ORDER BY salary DESC FETCH FIRST 20 ROWS ONLY",
        "F857-05",
        "FETCH FIRST with complex query"
    );
}

// ============================================================================
// F858: FETCH FIRST in subqueries
// ============================================================================

/// F858-01: FETCH FIRST in scalar subquery
#[test]
fn f858_01_fetch_first_in_scalar_subquery() {
    assert_feature_supported!(
        "SELECT (SELECT a FROM t ORDER BY a FETCH FIRST 1 ROW ONLY) as first_a FROM t1",
        "F858-01",
        "FETCH FIRST in scalar subquery"
    );
}

/// F858-02: FETCH FIRST in FROM subquery
#[test]
fn f858_02_fetch_first_in_from_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t ORDER BY a FETCH FIRST 5 ROWS ONLY) AS subq",
        "F858-02",
        "FETCH FIRST in FROM subquery"
    );
}

/// F858-03: FETCH FIRST in EXISTS subquery
#[test]
fn f858_03_fetch_first_in_exists_subquery() {
    assert_feature_supported!(
        "SELECT a FROM t WHERE EXISTS (SELECT b FROM t1 WHERE t1.a = t.a ORDER BY b FETCH FIRST 1 ROW ONLY)",
        "F858-03",
        "FETCH FIRST in EXISTS subquery"
    );
}

// ============================================================================
// F859: Top-level FETCH FIRST in views
// ============================================================================

/// F859-01: CREATE VIEW with FETCH FIRST
#[test]
fn f859_01_view_with_fetch_first() {
    assert_feature_supported!(
        "CREATE VIEW top_10_earners AS SELECT first_name, salary FROM person ORDER BY salary DESC FETCH FIRST 10 ROWS ONLY",
        "F859-01",
        "CREATE VIEW with FETCH FIRST"
    );
}

/// F859-02: CREATE VIEW with FETCH NEXT
#[test]
fn f859_02_view_with_fetch_next() {
    assert_feature_supported!(
        "CREATE VIEW youngest_5 AS SELECT first_name, age FROM person ORDER BY age FETCH NEXT 5 ROWS ONLY",
        "F859-02",
        "CREATE VIEW with FETCH NEXT"
    );
}

// ============================================================================
// F860: Dynamic FETCH FIRST row count
// ============================================================================

/// F860-01: FETCH FIRST with parameter marker
#[test]
fn f860_01_fetch_first_with_parameter() {
    // Note: DataFusion may not support parameter markers in FETCH clause
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a FETCH FIRST ? ROWS ONLY",
        "F860-01",
        "FETCH FIRST with parameter"
    );
}

// ============================================================================
// F861: Top-level OFFSET in query expression
// ============================================================================

/// F861-01: OFFSET with ROWS
#[test]
fn f861_01_offset_with_rows() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET 5 ROWS",
        "F861-01",
        "OFFSET with ROWS"
    );
}

/// F861-02: OFFSET without ROWS keyword
#[test]
fn f861_02_offset_without_rows() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET 10",
        "F861-02",
        "OFFSET without ROWS"
    );
}

/// F861-03: OFFSET with FETCH FIRST
#[test]
fn f861_03_offset_with_fetch_first() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY",
        "F861-03",
        "OFFSET with FETCH FIRST"
    );
}

/// F861-04: OFFSET 0
#[test]
fn f861_04_offset_zero() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET 0 ROWS",
        "F861-04",
        "OFFSET 0"
    );
}

/// F861-05: OFFSET with complex query
#[test]
fn f861_05_offset_complex() {
    assert_feature_supported!(
        "SELECT first_name, salary FROM person WHERE age > 30 ORDER BY salary DESC OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY",
        "F861-05",
        "OFFSET with complex query"
    );
}

// ============================================================================
// F862: OFFSET in subqueries
// ============================================================================

/// F862-01: OFFSET in scalar subquery
#[test]
fn f862_01_offset_in_scalar_subquery() {
    assert_feature_supported!(
        "SELECT (SELECT a FROM t ORDER BY a OFFSET 1 ROWS FETCH FIRST 1 ROW ONLY) FROM t1",
        "F862-01",
        "OFFSET in scalar subquery"
    );
}

/// F862-02: OFFSET in FROM subquery
#[test]
fn f862_02_offset_in_from_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t ORDER BY a OFFSET 5 ROWS) AS subq",
        "F862-02",
        "OFFSET in FROM subquery"
    );
}

/// F862-03: OFFSET with FETCH in FROM subquery
#[test]
fn f862_03_offset_fetch_in_from_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b FROM t ORDER BY a OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY) AS subq",
        "F862-03",
        "OFFSET with FETCH in FROM subquery"
    );
}

// ============================================================================
// F865: Dynamic offset row count
// ============================================================================

/// F865-01: OFFSET with parameter marker
#[test]
fn f865_01_offset_with_parameter() {
    // Note: DataFusion may not support parameter markers in OFFSET clause
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET ? ROWS",
        "F865-01",
        "OFFSET with parameter"
    );
}

/// F865-02: OFFSET and FETCH with parameters
#[test]
fn f865_02_offset_fetch_with_parameters() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET ? ROWS FETCH FIRST ? ROWS ONLY",
        "F865-02",
        "OFFSET and FETCH with parameters"
    );
}

// ============================================================================
// F867: FETCH FIRST WITH TIES option
// ============================================================================

/// F867-01: FETCH FIRST WITH TIES
#[test]
fn f867_01_fetch_first_with_ties() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a FETCH FIRST 10 ROWS WITH TIES",
        "F867-01",
        "FETCH FIRST WITH TIES"
    );
}

/// F867-02: FETCH NEXT WITH TIES
#[test]
fn f867_02_fetch_next_with_ties() {
    assert_feature_supported!(
        "SELECT first_name, salary FROM person ORDER BY salary DESC FETCH NEXT 5 ROWS WITH TIES",
        "F867-02",
        "FETCH NEXT WITH TIES"
    );
}

/// F867-03: FETCH WITH TIES and OFFSET
#[test]
fn f867_03_fetch_with_ties_and_offset() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET 5 ROWS FETCH FIRST 10 ROWS WITH TIES",
        "F867-03",
        "FETCH WITH TIES and OFFSET"
    );
}

/// F867-04: FETCH WITH TIES on multiple columns
#[test]
fn f867_04_fetch_with_ties_multiple_columns() {
    assert_feature_supported!(
        "SELECT state, salary FROM person ORDER BY state, salary DESC FETCH FIRST 10 ROWS WITH TIES",
        "F867-04",
        "FETCH WITH TIES on multiple columns"
    );
}

// ============================================================================
// LIMIT/OFFSET (PostgreSQL/MySQL syntax - non-standard but widely supported)
// ============================================================================

/// LIMIT: Basic LIMIT clause
#[test]
fn limit_01_basic_limit() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a LIMIT 10",
        "LIMIT",
        "Basic LIMIT"
    );
}

/// LIMIT: LIMIT without ORDER BY
#[test]
fn limit_02_limit_without_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t LIMIT 5",
        "LIMIT",
        "LIMIT without ORDER BY"
    );
}

/// LIMIT: LIMIT with OFFSET
#[test]
fn limit_03_limit_with_offset() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a LIMIT 10 OFFSET 20",
        "LIMIT",
        "LIMIT with OFFSET"
    );
}

/// LIMIT: LIMIT 0 (fetch no rows)
#[test]
fn limit_04_limit_zero() {
    assert_feature_supported!(
        "SELECT a FROM t LIMIT 0",
        "LIMIT",
        "LIMIT 0"
    );
}

/// LIMIT: LIMIT ALL
#[test]
fn limit_05_limit_all() {
    assert_feature_supported!(
        "SELECT a FROM t LIMIT ALL",
        "LIMIT",
        "LIMIT ALL"
    );
}

/// LIMIT: LIMIT in subquery
#[test]
fn limit_06_limit_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t ORDER BY a LIMIT 5) AS subq",
        "LIMIT",
        "LIMIT in subquery"
    );
}

/// LIMIT: LIMIT in UNION
#[test]
fn limit_07_limit_in_union() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 10",
        "LIMIT",
        "LIMIT in UNION"
    );
}

/// LIMIT: LIMIT with complex query
#[test]
fn limit_08_limit_complex() {
    assert_feature_supported!(
        "SELECT first_name, COUNT(*) as cnt FROM person GROUP BY first_name HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 5",
        "LIMIT",
        "LIMIT with complex query"
    );
}

/// LIMIT: OFFSET without LIMIT (fetch all remaining rows)
#[test]
fn limit_09_offset_without_limit() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a OFFSET 10",
        "LIMIT",
        "OFFSET without LIMIT"
    );
}

// ============================================================================
// Advanced ORDER BY scenarios
// ============================================================================

/// Advanced: ORDER BY with CASE expression
#[test]
fn advanced_01_order_by_case() {
    assert_feature_supported!(
        "SELECT first_name, age FROM person ORDER BY CASE WHEN age < 30 THEN 1 ELSE 2 END, age",
        "F850",
        "ORDER BY with CASE expression"
    );
}

/// Advanced: ORDER BY with CAST
#[test]
fn advanced_02_order_by_cast() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY CAST(a AS VARCHAR)",
        "F850",
        "ORDER BY with CAST"
    );
}

/// Advanced: ORDER BY with string functions
#[test]
fn advanced_03_order_by_string_function() {
    assert_feature_supported!(
        "SELECT first_name FROM person ORDER BY LOWER(first_name)",
        "F850",
        "ORDER BY with string function"
    );
}

/// Advanced: ORDER BY with arithmetic expression
#[test]
fn advanced_04_order_by_arithmetic() {
    assert_feature_supported!(
        "SELECT a, b FROM t ORDER BY a + b * 2",
        "F850",
        "ORDER BY with arithmetic expression"
    );
}

/// Advanced: ORDER BY with window function
#[test]
fn advanced_05_order_by_with_window_function() {
    assert_feature_supported!(
        "SELECT first_name, salary, ROW_NUMBER() OVER (ORDER BY salary) as rn FROM person ORDER BY rn",
        "F850",
        "ORDER BY with window function"
    );
}

/// Advanced: ORDER BY in lateral subquery
#[test]
fn advanced_06_order_by_in_lateral() {
    assert_feature_supported!(
        "SELECT * FROM person p, LATERAL (SELECT * FROM orders o WHERE o.customer_id = p.id ORDER BY o.price DESC LIMIT 1)",
        "F851",
        "ORDER BY in LATERAL subquery"
    );
}

/// Advanced: Multiple ORDER BY in complex query
#[test]
fn advanced_07_multiple_order_by_complex() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT state, AVG(salary) as avg_sal FROM person GROUP BY state ORDER BY avg_sal DESC LIMIT 10) AS top_states ORDER BY state",
        "F855",
        "Multiple ORDER BY in complex query"
    );
}

/// Advanced: ORDER BY with COALESCE
#[test]
fn advanced_08_order_by_coalesce() {
    assert_feature_supported!(
        "SELECT a, b FROM t ORDER BY COALESCE(a, 0)",
        "F850",
        "ORDER BY with COALESCE"
    );
}

/// Advanced: ORDER BY with subquery expression
#[test]
fn advanced_09_order_by_subquery_expression() {
    assert_feature_supported!(
        "SELECT first_name FROM person p ORDER BY (SELECT COUNT(*) FROM orders o WHERE o.customer_id = p.id)",
        "F850",
        "ORDER BY with subquery expression"
    );
}

/// Advanced: FETCH with percent (if supported)
#[test]
fn advanced_10_fetch_first_percent() {
    // Note: This is an extension; SQL standard FETCH uses row count only
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a FETCH FIRST 10 PERCENT ROWS ONLY",
        "F857",
        "FETCH FIRST with percent"
    );
}

// ============================================================================
// Combined complex scenarios
// ============================================================================

/// Combined: Full pagination scenario
#[test]
fn combined_01_full_pagination() {
    assert_feature_supported!(
        "SELECT first_name, last_name, salary FROM person WHERE state = 'CA' ORDER BY salary DESC NULLS LAST OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY",
        "F850-F861",
        "Full pagination with ORDER BY, OFFSET, and FETCH"
    );
}

/// Combined: Complex subquery with ordering and limiting
#[test]
fn combined_02_complex_subquery_ordering() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT first_name, salary, state, ROW_NUMBER() OVER (PARTITION BY state ORDER BY salary DESC) as rank FROM person) WHERE rank <= 3 ORDER BY state, rank",
        "F850-F855",
        "Complex subquery with ordering and ranking"
    );
}

/// Combined: UNION with individual and combined ORDER BY
#[test]
fn combined_03_union_multiple_order_by() {
    assert_feature_supported!(
        "(SELECT a, b FROM t1 ORDER BY a LIMIT 5) UNION (SELECT a, b FROM t2 ORDER BY b LIMIT 5) ORDER BY a",
        "F850-F855",
        "UNION with multiple ORDER BY clauses"
    );
}

/// Combined: CTE with ordering used in main query
#[test]
fn combined_04_cte_ordering_main_query() {
    assert_feature_supported!(
        "WITH ranked AS (SELECT state, first_name, salary, RANK() OVER (PARTITION BY state ORDER BY salary DESC) as r FROM person) SELECT state, first_name, salary FROM ranked WHERE r <= 5 ORDER BY state, r",
        "F850-F855",
        "CTE with ordering in main query"
    );
}

/// Combined: Multiple set operations with ordering
#[test]
fn combined_05_multiple_set_operations_ordering() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 INTERSECT SELECT a FROM t3 ORDER BY a DESC LIMIT 100",
        "F850",
        "Multiple set operations with ordering"
    );
}

/// Combined: Nested subqueries with different ordering
#[test]
fn combined_06_nested_different_ordering() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT * FROM (SELECT * FROM person ORDER BY age) ORDER BY salary) ORDER BY first_name LIMIT 10",
        "F855",
        "Nested subqueries with different ordering"
    );
}

/// Combined: JOIN with ORDER BY on joined columns
#[test]
fn combined_07_join_order_by_joined_columns() {
    assert_feature_supported!(
        "SELECT p.first_name, o.item FROM person p JOIN orders o ON p.id = o.customer_id ORDER BY p.last_name, o.price DESC",
        "F850",
        "JOIN with ORDER BY on multiple table columns"
    );
}

/// Combined: Aggregate with HAVING and complex ORDER BY
#[test]
fn combined_08_aggregate_having_complex_order() {
    assert_feature_supported!(
        "SELECT state, AVG(salary) as avg_sal, COUNT(*) as cnt FROM person GROUP BY state HAVING COUNT(*) >= 10 ORDER BY AVG(salary) DESC, state LIMIT 20",
        "F850",
        "Aggregate with HAVING and complex ORDER BY"
    );
}
