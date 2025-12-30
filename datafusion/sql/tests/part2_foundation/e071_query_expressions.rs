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

//! SQL:2016 Feature E071 - Basic query expressions
//!
//! ISO/IEC 9075-2:2016 Section 7.13
//!
//! This feature covers basic query expressions including table operators
//! (UNION, EXCEPT) and their use in queries and subqueries.
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E071-01 | UNION DISTINCT table operator | Supported |
//! | E071-02 | UNION ALL table operator | Supported |
//! | E071-03 | EXCEPT DISTINCT table operator | Supported |
//! | E071-05 | Columns combined via table operators need not have exactly same data type | Partial |
//! | E071-06 | Table operators in subqueries | Supported |
//!
//! All E071 subfeatures are CORE features (mandatory for SQL:2016 conformance).
//!
//! Additional related features tested:
//! - F302: INTERSECT table operator (DISTINCT)
//! - F304: EXCEPT ALL table operator
//! - F305: INTERSECT ALL table operator

use crate::assert_feature_supported;

// ============================================================================
// E071-01: UNION DISTINCT table operator
// ============================================================================

/// E071-01: Basic UNION DISTINCT
#[test]
fn e071_01_union_distinct() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2",
        "E071-01",
        "UNION DISTINCT table operator"
    );
}

/// E071-01: Explicit UNION DISTINCT keyword
#[test]
fn e071_01_union_distinct_explicit() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION DISTINCT SELECT a FROM t2",
        "E071-01",
        "UNION DISTINCT with explicit keyword"
    );
}

/// E071-01: UNION DISTINCT with multiple columns
#[test]
fn e071_01_union_distinct_multiple_columns() {
    assert_feature_supported!(
        "SELECT a, b FROM t1 UNION SELECT a, b FROM t2",
        "E071-01",
        "UNION DISTINCT with multiple columns"
    );
}

/// E071-01: UNION DISTINCT with WHERE clauses
#[test]
fn e071_01_union_distinct_with_where() {
    assert_feature_supported!(
        "SELECT a FROM t1 WHERE a > 10 UNION SELECT a FROM t2 WHERE a < 100",
        "E071-01",
        "UNION DISTINCT with WHERE clauses"
    );
}

/// E071-01: UNION DISTINCT with ORDER BY
#[test]
fn e071_01_union_distinct_with_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a",
        "E071-01",
        "UNION DISTINCT with ORDER BY"
    );
}

/// E071-01: Multiple UNION DISTINCT operations
#[test]
fn e071_01_multiple_union_distinct() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 UNION SELECT a FROM t",
        "E071-01",
        "Multiple UNION DISTINCT operations"
    );
}

/// E071-01: UNION DISTINCT with expressions
#[test]
fn e071_01_union_distinct_expressions() {
    assert_feature_supported!(
        "SELECT a + 1 FROM t1 UNION SELECT b * 2 FROM t2",
        "E071-01",
        "UNION DISTINCT with expressions"
    );
}

// ============================================================================
// E071-02: UNION ALL table operator
// ============================================================================

/// E071-02: Basic UNION ALL
#[test]
fn e071_02_union_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
        "E071-02",
        "UNION ALL table operator"
    );
}

/// E071-02: UNION ALL with multiple columns
#[test]
fn e071_02_union_all_multiple_columns() {
    assert_feature_supported!(
        "SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2",
        "E071-02",
        "UNION ALL with multiple columns"
    );
}

/// E071-02: UNION ALL with WHERE clauses
#[test]
fn e071_02_union_all_with_where() {
    assert_feature_supported!(
        "SELECT a FROM t1 WHERE a > 10 UNION ALL SELECT a FROM t2 WHERE a < 100",
        "E071-02",
        "UNION ALL with WHERE clauses"
    );
}

/// E071-02: UNION ALL with ORDER BY
#[test]
fn e071_02_union_all_with_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a",
        "E071-02",
        "UNION ALL with ORDER BY"
    );
}

/// E071-02: Multiple UNION ALL operations
#[test]
fn e071_02_multiple_union_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2 UNION ALL SELECT a FROM t",
        "E071-02",
        "Multiple UNION ALL operations"
    );
}

/// E071-02: Mixed UNION and UNION ALL
#[test]
fn e071_02_mixed_union_union_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 UNION ALL SELECT a FROM t",
        "E071-02",
        "Mixed UNION and UNION ALL"
    );
}

/// E071-02: UNION ALL with LIMIT
#[test]
fn e071_02_union_all_with_limit() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2 LIMIT 10",
        "E071-02",
        "UNION ALL with LIMIT"
    );
}

// ============================================================================
// E071-03: EXCEPT DISTINCT table operator
// ============================================================================

/// E071-03: Basic EXCEPT DISTINCT
#[test]
fn e071_03_except_distinct() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
        "E071-03",
        "EXCEPT DISTINCT table operator"
    );
}

/// E071-03: Explicit EXCEPT DISTINCT keyword
#[test]
fn e071_03_except_distinct_explicit() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT DISTINCT SELECT a FROM t2",
        "E071-03",
        "EXCEPT DISTINCT with explicit keyword"
    );
}

/// E071-03: EXCEPT DISTINCT with multiple columns
#[test]
fn e071_03_except_distinct_multiple_columns() {
    assert_feature_supported!(
        "SELECT a, b FROM t1 EXCEPT SELECT a, b FROM t2",
        "E071-03",
        "EXCEPT DISTINCT with multiple columns"
    );
}

/// E071-03: EXCEPT DISTINCT with WHERE clauses
#[test]
fn e071_03_except_distinct_with_where() {
    assert_feature_supported!(
        "SELECT a FROM t1 WHERE a > 10 EXCEPT SELECT a FROM t2 WHERE a < 100",
        "E071-03",
        "EXCEPT DISTINCT with WHERE clauses"
    );
}

/// E071-03: EXCEPT DISTINCT with ORDER BY
#[test]
fn e071_03_except_distinct_with_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2 ORDER BY a",
        "E071-03",
        "EXCEPT DISTINCT with ORDER BY"
    );
}

/// E071-03: Multiple EXCEPT DISTINCT operations
#[test]
fn e071_03_multiple_except_distinct() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2 EXCEPT SELECT a FROM t",
        "E071-03",
        "Multiple EXCEPT DISTINCT operations"
    );
}

/// E071-03: EXCEPT DISTINCT with expressions
#[test]
fn e071_03_except_distinct_expressions() {
    assert_feature_supported!(
        "SELECT a + 1 FROM t1 EXCEPT SELECT b * 2 FROM t2",
        "E071-03",
        "EXCEPT DISTINCT with expressions"
    );
}

// ============================================================================
// E071-05: Columns combined via table operators need not have exactly same data type
// ============================================================================

/// E071-05: UNION with different numeric types
#[test]
fn e071_05_union_different_numeric_types() {
    assert_feature_supported!(
        "SELECT CAST(a AS INTEGER) FROM t1 UNION SELECT CAST(a AS BIGINT) FROM t2",
        "E071-05",
        "UNION with different numeric types"
    );
}

/// E071-05: UNION with compatible character types
#[test]
fn e071_05_union_compatible_char_types() {
    assert_feature_supported!(
        "SELECT CAST(a AS VARCHAR(10)) FROM t1 UNION SELECT CAST(a AS VARCHAR(20)) FROM t2",
        "E071-05",
        "UNION with compatible character types"
    );
}

/// E071-05: EXCEPT with different numeric types
#[test]
fn e071_05_except_different_numeric_types() {
    assert_feature_supported!(
        "SELECT CAST(a AS INTEGER) FROM t1 EXCEPT SELECT CAST(a AS BIGINT) FROM t2",
        "E071-05",
        "EXCEPT with different numeric types"
    );
}

/// E071-05: UNION with NULL and typed column
#[test]
fn e071_05_union_null_and_typed() {
    assert_feature_supported!(
        "SELECT NULL FROM t1 UNION SELECT a FROM t2",
        "E071-05",
        "UNION with NULL and typed column"
    );
}

/// E071-05: UNION ALL with different precision decimals
#[test]
fn e071_05_union_all_decimal_precision() {
    assert_feature_supported!(
        "SELECT CAST(a AS DECIMAL(10, 2)) FROM t1 UNION ALL SELECT CAST(a AS DECIMAL(12, 3)) FROM t2",
        "E071-05",
        "UNION ALL with different precision decimals"
    );
}

// ============================================================================
// E071-06: Table operators in subqueries
// ============================================================================

/// E071-06: UNION in subquery in FROM clause
#[test]
fn e071_06_union_in_from_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS combined",
        "E071-06",
        "UNION in FROM subquery"
    );
}

/// E071-06: UNION in subquery in WHERE clause
#[test]
fn e071_06_union_in_where_subquery() {
    assert_feature_supported!(
        "SELECT a FROM t WHERE a IN (SELECT a FROM t1 UNION SELECT a FROM t2)",
        "E071-06",
        "UNION in WHERE subquery"
    );
}

/// E071-06: EXCEPT in subquery in FROM clause
#[test]
fn e071_06_except_in_from_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 EXCEPT SELECT a FROM t2) AS diff",
        "E071-06",
        "EXCEPT in FROM subquery"
    );
}

/// E071-06: UNION ALL in subquery with JOIN
#[test]
fn e071_06_union_all_in_subquery_with_join() {
    assert_feature_supported!(
        "SELECT t.a FROM t JOIN (SELECT a FROM t1 UNION ALL SELECT a FROM t2) AS u ON t.a = u.a",
        "E071-06",
        "UNION ALL in subquery with JOIN"
    );
}

/// E071-06: Nested table operators in subquery
#[test]
fn e071_06_nested_table_operators_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 UNION SELECT a FROM t2 EXCEPT SELECT a FROM t) AS s",
        "E071-06",
        "Nested table operators in subquery"
    );
}

/// E071-06: UNION in scalar subquery
#[test]
fn e071_06_union_in_scalar_subquery() {
    assert_feature_supported!(
        "SELECT a, (SELECT COUNT(*) FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS u) FROM t",
        "E071-06",
        "UNION in scalar subquery"
    );
}

/// E071-06: Table operator in EXISTS subquery
#[test]
fn e071_06_table_operator_in_exists() {
    assert_feature_supported!(
        "SELECT a FROM t WHERE EXISTS (SELECT a FROM t1 UNION SELECT a FROM t2)",
        "E071-06",
        "Table operator in EXISTS subquery"
    );
}

// ============================================================================
// F302: INTERSECT table operator (DISTINCT)
// ============================================================================

/// F302: Basic INTERSECT DISTINCT
#[test]
fn f302_intersect_distinct() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
        "F302",
        "INTERSECT DISTINCT table operator"
    );
}

/// F302: Explicit INTERSECT DISTINCT keyword
#[test]
fn f302_intersect_distinct_explicit() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT DISTINCT SELECT a FROM t2",
        "F302",
        "INTERSECT DISTINCT with explicit keyword"
    );
}

/// F302: INTERSECT with multiple columns
#[test]
fn f302_intersect_multiple_columns() {
    assert_feature_supported!(
        "SELECT a, b FROM t1 INTERSECT SELECT a, b FROM t2",
        "F302",
        "INTERSECT with multiple columns"
    );
}

/// F302: INTERSECT with WHERE clauses
#[test]
fn f302_intersect_with_where() {
    assert_feature_supported!(
        "SELECT a FROM t1 WHERE a > 10 INTERSECT SELECT a FROM t2 WHERE a < 100",
        "F302",
        "INTERSECT with WHERE clauses"
    );
}

/// F302: INTERSECT with ORDER BY
#[test]
fn f302_intersect_with_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2 ORDER BY a",
        "F302",
        "INTERSECT with ORDER BY"
    );
}

/// F302: Multiple INTERSECT operations
#[test]
fn f302_multiple_intersect() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2 INTERSECT SELECT a FROM t",
        "F302",
        "Multiple INTERSECT operations"
    );
}

/// F302: INTERSECT in subquery
#[test]
fn f302_intersect_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 INTERSECT SELECT a FROM t2) AS common",
        "F302",
        "INTERSECT in subquery"
    );
}

// ============================================================================
// F304: EXCEPT ALL table operator
// ============================================================================

/// F304: Basic EXCEPT ALL
#[test]
fn f304_except_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2",
        "F304",
        "EXCEPT ALL table operator"
    );
}

/// F304: EXCEPT ALL with multiple columns
#[test]
fn f304_except_all_multiple_columns() {
    assert_feature_supported!(
        "SELECT a, b FROM t1 EXCEPT ALL SELECT a, b FROM t2",
        "F304",
        "EXCEPT ALL with multiple columns"
    );
}

/// F304: EXCEPT ALL with WHERE clauses
#[test]
fn f304_except_all_with_where() {
    assert_feature_supported!(
        "SELECT a FROM t1 WHERE a > 10 EXCEPT ALL SELECT a FROM t2 WHERE a < 100",
        "F304",
        "EXCEPT ALL with WHERE clauses"
    );
}

/// F304: EXCEPT ALL with ORDER BY
#[test]
fn f304_except_all_with_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2 ORDER BY a",
        "F304",
        "EXCEPT ALL with ORDER BY"
    );
}

/// F304: Multiple EXCEPT ALL operations
#[test]
fn f304_multiple_except_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2 EXCEPT ALL SELECT a FROM t",
        "F304",
        "Multiple EXCEPT ALL operations"
    );
}

/// F304: EXCEPT ALL in subquery
#[test]
fn f304_except_all_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2) AS diff",
        "F304",
        "EXCEPT ALL in subquery"
    );
}

// ============================================================================
// F305: INTERSECT ALL table operator
// ============================================================================

/// F305: Basic INTERSECT ALL
#[test]
fn f305_intersect_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT ALL SELECT a FROM t2",
        "F305",
        "INTERSECT ALL table operator"
    );
}

/// F305: INTERSECT ALL with multiple columns
#[test]
fn f305_intersect_all_multiple_columns() {
    assert_feature_supported!(
        "SELECT a, b FROM t1 INTERSECT ALL SELECT a, b FROM t2",
        "F305",
        "INTERSECT ALL with multiple columns"
    );
}

/// F305: INTERSECT ALL with WHERE clauses
#[test]
fn f305_intersect_all_with_where() {
    assert_feature_supported!(
        "SELECT a FROM t1 WHERE a > 10 INTERSECT ALL SELECT a FROM t2 WHERE a < 100",
        "F305",
        "INTERSECT ALL with WHERE clauses"
    );
}

/// F305: INTERSECT ALL with ORDER BY
#[test]
fn f305_intersect_all_with_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT ALL SELECT a FROM t2 ORDER BY a",
        "F305",
        "INTERSECT ALL with ORDER BY"
    );
}

/// F305: Multiple INTERSECT ALL operations
#[test]
fn f305_multiple_intersect_all() {
    assert_feature_supported!(
        "SELECT a FROM t1 INTERSECT ALL SELECT a FROM t2 INTERSECT ALL SELECT a FROM t",
        "F305",
        "Multiple INTERSECT ALL operations"
    );
}

/// F305: INTERSECT ALL in subquery
#[test]
fn f305_intersect_all_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a FROM t1 INTERSECT ALL SELECT a FROM t2) AS common",
        "F305",
        "INTERSECT ALL in subquery"
    );
}

// ============================================================================
// Mixed and complex table operator scenarios
// ============================================================================

/// Complex: All set operators combined
#[test]
fn mixed_all_set_operators() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 INTERSECT SELECT a FROM t EXCEPT SELECT a FROM person",
        "E071",
        "All set operators combined"
    );
}

/// Complex: Parenthesized table operators
#[test]
fn mixed_parenthesized_table_operators() {
    assert_feature_supported!(
        "(SELECT a FROM t1 UNION SELECT a FROM t2) EXCEPT SELECT a FROM t",
        "E071",
        "Parenthesized table operators"
    );
}

/// Complex: Table operators with JOINs
#[test]
fn mixed_table_operators_with_joins() {
    assert_feature_supported!(
        "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a UNION SELECT person.id FROM person JOIN orders ON person.id = orders.customer_id",
        "E071",
        "Table operators with JOINs"
    );
}

/// Complex: Table operators with aggregates
#[test]
fn mixed_table_operators_with_aggregates() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS u",
        "E071",
        "Table operators with aggregates"
    );
}

/// Complex: UNION with DISTINCT in individual queries
#[test]
fn mixed_union_with_distinct_queries() {
    assert_feature_supported!(
        "SELECT DISTINCT a FROM t1 UNION SELECT DISTINCT a FROM t2",
        "E071",
        "UNION with DISTINCT in individual queries"
    );
}

/// Complex: Table operators with LIMIT and OFFSET
#[test]
fn mixed_table_operators_with_limit_offset() {
    assert_feature_supported!(
        "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 10 OFFSET 5",
        "E071",
        "Table operators with LIMIT and OFFSET"
    );
}
