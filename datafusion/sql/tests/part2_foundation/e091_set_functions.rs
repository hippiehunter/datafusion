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

//! SQL:2016 Feature E091 - Set functions
//!
//! ISO/IEC 9075-2:2016 Section 10.9
//!
//! This feature covers the basic set functions (aggregates) required by Core SQL:
//! - AVG: Average of non-NULL values
//! - COUNT: Count of rows or non-NULL values
//! - MAX: Maximum value
//! - MIN: Minimum value
//! - SUM: Sum of non-NULL values
//! - ALL and DISTINCT quantifiers
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E091-01 | AVG function | Supported |
//! | E091-02 | COUNT function | Supported |
//! | E091-03 | MAX function | Supported |
//! | E091-04 | MIN function | Supported |
//! | E091-05 | SUM function | Supported |
//! | E091-06 | ALL quantifier | Supported |
//! | E091-07 | DISTINCT quantifier | Supported |
//!
//! All E091 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::assert_feature_supported;

// ============================================================================
// E091-01: AVG function
// ============================================================================

/// E091-01: Basic AVG function
#[test]
fn e091_01_avg_basic() {
    assert_feature_supported!(
        "SELECT AVG(a) FROM t",
        "E091-01",
        "AVG function"
    );
}

/// E091-01: AVG with WHERE clause
#[test]
fn e091_01_avg_with_where() {
    assert_feature_supported!(
        "SELECT AVG(a) FROM t WHERE a > 10",
        "E091-01",
        "AVG with WHERE clause"
    );
}

/// E091-01: AVG with GROUP BY
#[test]
fn e091_01_avg_with_group_by() {
    assert_feature_supported!(
        "SELECT category, AVG(price) FROM orders GROUP BY category",
        "E091-01",
        "AVG with GROUP BY"
    );
}

/// E091-01: AVG with HAVING
#[test]
fn e091_01_avg_with_having() {
    assert_feature_supported!(
        "SELECT category, AVG(price) FROM orders GROUP BY category HAVING AVG(price) > 100",
        "E091-01",
        "AVG with HAVING"
    );
}

/// E091-01: AVG of integer column
#[test]
fn e091_01_avg_integer() {
    assert_feature_supported!(
        "SELECT AVG(id) FROM person",
        "E091-01",
        "AVG of integer column"
    );
}

/// E091-01: AVG of decimal column
#[test]
fn e091_01_avg_decimal() {
    assert_feature_supported!(
        "SELECT AVG(CAST(a AS DECIMAL(10, 2))) FROM numeric_types",
        "E091-01",
        "AVG of decimal column"
    );
}

/// E091-01: AVG of floating point column
#[test]
fn e091_01_avg_float() {
    assert_feature_supported!(
        "SELECT AVG(CAST(a AS DOUBLE)) FROM numeric_types",
        "E091-01",
        "AVG of floating point column"
    );
}

/// E091-01: Multiple AVG functions in select list
#[test]
fn e091_01_multiple_avg() {
    assert_feature_supported!(
        "SELECT AVG(a), AVG(b) FROM t",
        "E091-01",
        "Multiple AVG functions"
    );
}

/// E091-01: AVG in subquery
#[test]
fn e091_01_avg_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > (SELECT AVG(a) FROM t)",
        "E091-01",
        "AVG in subquery"
    );
}

/// E091-01: AVG with expression
#[test]
fn e091_01_avg_expression() {
    assert_feature_supported!(
        "SELECT AVG(a * 2 + b) FROM t",
        "E091-01",
        "AVG with expression"
    );
}

// ============================================================================
// E091-02: COUNT function
// ============================================================================

/// E091-02: COUNT(*) - count all rows
#[test]
fn e091_02_count_star() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM t",
        "E091-02",
        "COUNT(*)"
    );
}

/// E091-02: COUNT(column) - count non-NULL values
#[test]
fn e091_02_count_column() {
    assert_feature_supported!(
        "SELECT COUNT(a) FROM t",
        "E091-02",
        "COUNT(column)"
    );
}

/// E091-02: COUNT with WHERE clause
#[test]
fn e091_02_count_with_where() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM t WHERE a > 10",
        "E091-02",
        "COUNT with WHERE clause"
    );
}

/// E091-02: COUNT with GROUP BY
#[test]
fn e091_02_count_with_group_by() {
    assert_feature_supported!(
        "SELECT category, COUNT(*) FROM orders GROUP BY category",
        "E091-02",
        "COUNT with GROUP BY"
    );
}

/// E091-02: COUNT with HAVING
#[test]
fn e091_02_count_with_having() {
    assert_feature_supported!(
        "SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 5",
        "E091-02",
        "COUNT with HAVING"
    );
}

/// E091-02: Multiple COUNT functions
#[test]
fn e091_02_multiple_count() {
    assert_feature_supported!(
        "SELECT COUNT(*), COUNT(a), COUNT(b) FROM t",
        "E091-02",
        "Multiple COUNT functions"
    );
}

/// E091-02: COUNT in subquery
#[test]
fn e091_02_count_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (SELECT COUNT(*) FROM t1) > 10",
        "E091-02",
        "COUNT in subquery"
    );
}

/// E091-02: COUNT with expression
#[test]
fn e091_02_count_expression() {
    assert_feature_supported!(
        "SELECT COUNT(a + b) FROM t",
        "E091-02",
        "COUNT with expression"
    );
}

/// E091-02: COUNT with CASE expression
#[test]
fn e091_02_count_case() {
    assert_feature_supported!(
        "SELECT COUNT(CASE WHEN a > 10 THEN 1 END) FROM t",
        "E091-02",
        "COUNT with CASE expression"
    );
}

// ============================================================================
// E091-03: MAX function
// ============================================================================

/// E091-03: Basic MAX function
#[test]
fn e091_03_max_basic() {
    assert_feature_supported!(
        "SELECT MAX(a) FROM t",
        "E091-03",
        "MAX function"
    );
}

/// E091-03: MAX with WHERE clause
#[test]
fn e091_03_max_with_where() {
    assert_feature_supported!(
        "SELECT MAX(a) FROM t WHERE a > 10",
        "E091-03",
        "MAX with WHERE clause"
    );
}

/// E091-03: MAX with GROUP BY
#[test]
fn e091_03_max_with_group_by() {
    assert_feature_supported!(
        "SELECT category, MAX(price) FROM orders GROUP BY category",
        "E091-03",
        "MAX with GROUP BY"
    );
}

/// E091-03: MAX with HAVING
#[test]
fn e091_03_max_with_having() {
    assert_feature_supported!(
        "SELECT category, MAX(price) FROM orders GROUP BY category HAVING MAX(price) > 1000",
        "E091-03",
        "MAX with HAVING"
    );
}

/// E091-03: MAX of integer column
#[test]
fn e091_03_max_integer() {
    assert_feature_supported!(
        "SELECT MAX(id) FROM person",
        "E091-03",
        "MAX of integer column"
    );
}

/// E091-03: MAX of string column
#[test]
fn e091_03_max_string() {
    assert_feature_supported!(
        "SELECT MAX(name) FROM person",
        "E091-03",
        "MAX of string column"
    );
}

/// E091-03: MAX of decimal column
#[test]
fn e091_03_max_decimal() {
    assert_feature_supported!(
        "SELECT MAX(CAST(a AS DECIMAL(10, 2))) FROM numeric_types",
        "E091-03",
        "MAX of decimal column"
    );
}

/// E091-03: Multiple MAX functions
#[test]
fn e091_03_multiple_max() {
    assert_feature_supported!(
        "SELECT MAX(a), MAX(b) FROM t",
        "E091-03",
        "Multiple MAX functions"
    );
}

/// E091-03: MAX in subquery
#[test]
fn e091_03_max_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a = (SELECT MAX(a) FROM t)",
        "E091-03",
        "MAX in subquery"
    );
}

/// E091-03: MAX with expression
#[test]
fn e091_03_max_expression() {
    assert_feature_supported!(
        "SELECT MAX(a * 2 + b) FROM t",
        "E091-03",
        "MAX with expression"
    );
}

// ============================================================================
// E091-04: MIN function
// ============================================================================

/// E091-04: Basic MIN function
#[test]
fn e091_04_min_basic() {
    assert_feature_supported!(
        "SELECT MIN(a) FROM t",
        "E091-04",
        "MIN function"
    );
}

/// E091-04: MIN with WHERE clause
#[test]
fn e091_04_min_with_where() {
    assert_feature_supported!(
        "SELECT MIN(a) FROM t WHERE a > 10",
        "E091-04",
        "MIN with WHERE clause"
    );
}

/// E091-04: MIN with GROUP BY
#[test]
fn e091_04_min_with_group_by() {
    assert_feature_supported!(
        "SELECT category, MIN(price) FROM orders GROUP BY category",
        "E091-04",
        "MIN with GROUP BY"
    );
}

/// E091-04: MIN with HAVING
#[test]
fn e091_04_min_with_having() {
    assert_feature_supported!(
        "SELECT category, MIN(price) FROM orders GROUP BY category HAVING MIN(price) < 10",
        "E091-04",
        "MIN with HAVING"
    );
}

/// E091-04: MIN of integer column
#[test]
fn e091_04_min_integer() {
    assert_feature_supported!(
        "SELECT MIN(id) FROM person",
        "E091-04",
        "MIN of integer column"
    );
}

/// E091-04: MIN of string column
#[test]
fn e091_04_min_string() {
    assert_feature_supported!(
        "SELECT MIN(name) FROM person",
        "E091-04",
        "MIN of string column"
    );
}

/// E091-04: MIN of decimal column
#[test]
fn e091_04_min_decimal() {
    assert_feature_supported!(
        "SELECT MIN(CAST(a AS DECIMAL(10, 2))) FROM numeric_types",
        "E091-04",
        "MIN of decimal column"
    );
}

/// E091-04: Multiple MIN functions
#[test]
fn e091_04_multiple_min() {
    assert_feature_supported!(
        "SELECT MIN(a), MIN(b) FROM t",
        "E091-04",
        "Multiple MIN functions"
    );
}

/// E091-04: MIN in subquery
#[test]
fn e091_04_min_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a = (SELECT MIN(a) FROM t)",
        "E091-04",
        "MIN in subquery"
    );
}

/// E091-04: MIN with expression
#[test]
fn e091_04_min_expression() {
    assert_feature_supported!(
        "SELECT MIN(a * 2 + b) FROM t",
        "E091-04",
        "MIN with expression"
    );
}

// ============================================================================
// E091-05: SUM function
// ============================================================================

/// E091-05: Basic SUM function
#[test]
fn e091_05_sum_basic() {
    assert_feature_supported!(
        "SELECT SUM(a) FROM t",
        "E091-05",
        "SUM function"
    );
}

/// E091-05: SUM with WHERE clause
#[test]
fn e091_05_sum_with_where() {
    assert_feature_supported!(
        "SELECT SUM(a) FROM t WHERE a > 10",
        "E091-05",
        "SUM with WHERE clause"
    );
}

/// E091-05: SUM with GROUP BY
#[test]
fn e091_05_sum_with_group_by() {
    assert_feature_supported!(
        "SELECT category, SUM(price) FROM orders GROUP BY category",
        "E091-05",
        "SUM with GROUP BY"
    );
}

/// E091-05: SUM with HAVING
#[test]
fn e091_05_sum_with_having() {
    assert_feature_supported!(
        "SELECT category, SUM(price) FROM orders GROUP BY category HAVING SUM(price) > 10000",
        "E091-05",
        "SUM with HAVING"
    );
}

/// E091-05: SUM of integer column
#[test]
fn e091_05_sum_integer() {
    assert_feature_supported!(
        "SELECT SUM(id) FROM person",
        "E091-05",
        "SUM of integer column"
    );
}

/// E091-05: SUM of decimal column
#[test]
fn e091_05_sum_decimal() {
    assert_feature_supported!(
        "SELECT SUM(CAST(a AS DECIMAL(10, 2))) FROM numeric_types",
        "E091-05",
        "SUM of decimal column"
    );
}

/// E091-05: SUM of floating point column
#[test]
fn e091_05_sum_float() {
    assert_feature_supported!(
        "SELECT SUM(CAST(a AS DOUBLE)) FROM numeric_types",
        "E091-05",
        "SUM of floating point column"
    );
}

/// E091-05: Multiple SUM functions
#[test]
fn e091_05_multiple_sum() {
    assert_feature_supported!(
        "SELECT SUM(a), SUM(b) FROM t",
        "E091-05",
        "Multiple SUM functions"
    );
}

/// E091-05: SUM in subquery
#[test]
fn e091_05_sum_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > (SELECT SUM(a) FROM t1)",
        "E091-05",
        "SUM in subquery"
    );
}

/// E091-05: SUM with expression
#[test]
fn e091_05_sum_expression() {
    assert_feature_supported!(
        "SELECT SUM(a * 2 + b) FROM t",
        "E091-05",
        "SUM with expression"
    );
}

// ============================================================================
// E091-06: ALL quantifier
// ============================================================================

/// E091-06: COUNT(ALL column)
#[test]
fn e091_06_count_all() {
    assert_feature_supported!(
        "SELECT COUNT(ALL a) FROM t",
        "E091-06",
        "COUNT(ALL column)"
    );
}

/// E091-06: SUM(ALL column)
#[test]
fn e091_06_sum_all() {
    assert_feature_supported!(
        "SELECT SUM(ALL a) FROM t",
        "E091-06",
        "SUM(ALL column)"
    );
}

/// E091-06: AVG(ALL column)
#[test]
fn e091_06_avg_all() {
    assert_feature_supported!(
        "SELECT AVG(ALL a) FROM t",
        "E091-06",
        "AVG(ALL column)"
    );
}

/// E091-06: MAX(ALL column)
#[test]
fn e091_06_max_all() {
    assert_feature_supported!(
        "SELECT MAX(ALL a) FROM t",
        "E091-06",
        "MAX(ALL column)"
    );
}

/// E091-06: MIN(ALL column)
#[test]
fn e091_06_min_all() {
    assert_feature_supported!(
        "SELECT MIN(ALL a) FROM t",
        "E091-06",
        "MIN(ALL column)"
    );
}

/// E091-06: ALL with expression
#[test]
fn e091_06_all_with_expression() {
    assert_feature_supported!(
        "SELECT SUM(ALL a + b) FROM t",
        "E091-06",
        "ALL with expression"
    );
}

/// E091-06: ALL with GROUP BY
#[test]
fn e091_06_all_with_group_by() {
    assert_feature_supported!(
        "SELECT category, COUNT(ALL price) FROM orders GROUP BY category",
        "E091-06",
        "ALL with GROUP BY"
    );
}

// ============================================================================
// E091-07: DISTINCT quantifier
// ============================================================================

/// E091-07: COUNT(DISTINCT column)
#[test]
fn e091_07_count_distinct() {
    assert_feature_supported!(
        "SELECT COUNT(DISTINCT a) FROM t",
        "E091-07",
        "COUNT(DISTINCT column)"
    );
}

/// E091-07: SUM(DISTINCT column)
#[test]
fn e091_07_sum_distinct() {
    assert_feature_supported!(
        "SELECT SUM(DISTINCT a) FROM t",
        "E091-07",
        "SUM(DISTINCT column)"
    );
}

/// E091-07: AVG(DISTINCT column)
#[test]
fn e091_07_avg_distinct() {
    assert_feature_supported!(
        "SELECT AVG(DISTINCT a) FROM t",
        "E091-07",
        "AVG(DISTINCT column)"
    );
}

/// E091-07: MAX(DISTINCT column)
#[test]
fn e091_07_max_distinct() {
    assert_feature_supported!(
        "SELECT MAX(DISTINCT a) FROM t",
        "E091-07",
        "MAX(DISTINCT column)"
    );
}

/// E091-07: MIN(DISTINCT column)
#[test]
fn e091_07_min_distinct() {
    assert_feature_supported!(
        "SELECT MIN(DISTINCT a) FROM t",
        "E091-07",
        "MIN(DISTINCT column)"
    );
}

/// E091-07: DISTINCT with expression
#[test]
fn e091_07_distinct_with_expression() {
    assert_feature_supported!(
        "SELECT COUNT(DISTINCT a + b) FROM t",
        "E091-07",
        "DISTINCT with expression"
    );
}

/// E091-07: DISTINCT with GROUP BY
#[test]
fn e091_07_distinct_with_group_by() {
    assert_feature_supported!(
        "SELECT category, COUNT(DISTINCT customer_id) FROM orders GROUP BY category",
        "E091-07",
        "DISTINCT with GROUP BY"
    );
}

/// E091-07: Multiple DISTINCT aggregates
#[test]
fn e091_07_multiple_distinct() {
    assert_feature_supported!(
        "SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM t",
        "E091-07",
        "Multiple DISTINCT aggregates"
    );
}

/// E091-07: DISTINCT in HAVING clause
#[test]
fn e091_07_distinct_in_having() {
    assert_feature_supported!(
        "SELECT category FROM orders GROUP BY category HAVING COUNT(DISTINCT customer_id) > 5",
        "E091-07",
        "DISTINCT in HAVING clause"
    );
}

// ============================================================================
// Mixed aggregate function scenarios
// ============================================================================

/// Mixed: All basic aggregates in one query
#[test]
fn mixed_all_basic_aggregates() {
    assert_feature_supported!(
        "SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM t",
        "E091",
        "All basic aggregates"
    );
}

/// Mixed: Aggregates with both ALL and DISTINCT
#[test]
fn mixed_all_and_distinct_aggregates() {
    assert_feature_supported!(
        "SELECT COUNT(ALL a), COUNT(DISTINCT a), SUM(ALL b), SUM(DISTINCT b) FROM t",
        "E091",
        "Aggregates with ALL and DISTINCT"
    );
}

/// Mixed: Nested aggregates in subquery
#[test]
fn mixed_nested_aggregates() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a > (SELECT AVG(a) FROM t1 WHERE b < (SELECT MAX(b) FROM t2))",
        "E091",
        "Nested aggregates in subquery"
    );
}

/// Mixed: Aggregates with JOIN
#[test]
fn mixed_aggregates_with_join() {
    assert_feature_supported!(
        "SELECT p.name, COUNT(*), SUM(o.amount) FROM person p JOIN orders o ON p.id = o.customer_id GROUP BY p.name",
        "E091",
        "Aggregates with JOIN"
    );
}

/// Mixed: Aggregates in CASE expression
#[test]
fn mixed_aggregates_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN COUNT(*) > 10 THEN 'many' ELSE 'few' END FROM t",
        "E091",
        "Aggregates in CASE expression"
    );
}

/// Mixed: Multiple GROUP BY columns with aggregates
#[test]
fn mixed_multiple_group_by_aggregates() {
    assert_feature_supported!(
        "SELECT category, customer_id, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY category, customer_id",
        "E091",
        "Multiple GROUP BY columns with aggregates"
    );
}

/// Mixed: Aggregates with ORDER BY aggregate result
#[test]
fn mixed_aggregates_order_by_aggregate() {
    assert_feature_supported!(
        "SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category ORDER BY COUNT(*) DESC",
        "E091",
        "Aggregates with ORDER BY aggregate result"
    );
}

/// Mixed: Aggregates with HAVING and complex condition
#[test]
fn mixed_aggregates_complex_having() {
    assert_feature_supported!(
        "SELECT category, COUNT(*), AVG(price) FROM orders GROUP BY category HAVING COUNT(*) > 10 AND AVG(price) < 100",
        "E091",
        "Aggregates with complex HAVING"
    );
}

// ============================================================================
// FILTER clause (SQL:2003 addition, commonly supported)
// ============================================================================

/// FILTER: COUNT with FILTER clause
#[test]
fn filter_count_with_filter() {
    assert_feature_supported!(
        "SELECT COUNT(*) FILTER (WHERE a > 10) FROM t",
        "T612",
        "COUNT with FILTER clause"
    );
}

/// FILTER: SUM with FILTER clause
#[test]
fn filter_sum_with_filter() {
    assert_feature_supported!(
        "SELECT SUM(a) FILTER (WHERE a > 10) FROM t",
        "T612",
        "SUM with FILTER clause"
    );
}

/// FILTER: AVG with FILTER clause
#[test]
fn filter_avg_with_filter() {
    assert_feature_supported!(
        "SELECT AVG(price) FILTER (WHERE price > 100) FROM orders",
        "T612",
        "AVG with FILTER clause"
    );
}

/// FILTER: MAX with FILTER clause
#[test]
fn filter_max_with_filter() {
    assert_feature_supported!(
        "SELECT MAX(a) FILTER (WHERE a < 1000) FROM t",
        "T612",
        "MAX with FILTER clause"
    );
}

/// FILTER: MIN with FILTER clause
#[test]
fn filter_min_with_filter() {
    assert_feature_supported!(
        "SELECT MIN(a) FILTER (WHERE a > 0) FROM t",
        "T612",
        "MIN with FILTER clause"
    );
}

/// FILTER: Multiple aggregates with different filters
#[test]
fn filter_multiple_with_different_filters() {
    assert_feature_supported!(
        "SELECT COUNT(*) FILTER (WHERE a > 10), SUM(b) FILTER (WHERE b < 100) FROM t",
        "T612",
        "Multiple aggregates with different filters"
    );
}

/// FILTER: FILTER with GROUP BY
#[test]
fn filter_with_group_by() {
    assert_feature_supported!(
        "SELECT category, COUNT(*) FILTER (WHERE price > 100) FROM orders GROUP BY category",
        "T612",
        "FILTER with GROUP BY"
    );
}

/// FILTER: DISTINCT with FILTER
#[test]
fn filter_distinct_with_filter() {
    assert_feature_supported!(
        "SELECT COUNT(DISTINCT a) FILTER (WHERE a > 10) FROM t",
        "T612",
        "DISTINCT with FILTER"
    );
}

/// FILTER: Complex filter condition
#[test]
fn filter_complex_condition() {
    assert_feature_supported!(
        "SELECT SUM(amount) FILTER (WHERE category = 'electronics' AND price > 500) FROM orders",
        "T612",
        "FILTER with complex condition"
    );
}

/// FILTER: FILTER in HAVING clause
#[test]
fn filter_in_having() {
    assert_feature_supported!(
        "SELECT category FROM orders GROUP BY category HAVING COUNT(*) FILTER (WHERE price > 100) > 5",
        "T612",
        "FILTER in HAVING clause"
    );
}

// ============================================================================
// Edge cases and special scenarios
// ============================================================================

/// Edge case: Aggregate on empty table
#[test]
fn edge_aggregate_empty_table() {
    assert_feature_supported!(
        "SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM t WHERE 1 = 0",
        "E091",
        "Aggregate on empty result set"
    );
}

/// Edge case: Aggregate with all NULL values
#[test]
fn edge_aggregate_all_nulls() {
    assert_feature_supported!(
        "SELECT COUNT(NULL), SUM(NULL), AVG(NULL), MIN(NULL), MAX(NULL) FROM t",
        "E091",
        "Aggregate with NULL values"
    );
}

/// Edge case: COUNT(*) vs COUNT(column) difference
#[test]
fn edge_count_star_vs_column() {
    assert_feature_supported!(
        "SELECT COUNT(*), COUNT(a), COUNT(b) FROM t",
        "E091",
        "COUNT(*) vs COUNT(column)"
    );
}

/// Edge case: Aggregate in ORDER BY without being in SELECT
#[test]
fn edge_aggregate_in_order_by_only() {
    assert_feature_supported!(
        "SELECT category FROM orders GROUP BY category ORDER BY COUNT(*)",
        "E091",
        "Aggregate in ORDER BY not in SELECT"
    );
}

/// Edge case: Single row aggregate without GROUP BY
#[test]
fn edge_single_row_aggregate() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM (SELECT 1) AS single_row",
        "E091",
        "Single row aggregate"
    );
}

/// Edge case: Aggregate over UNION result
#[test]
fn edge_aggregate_over_union() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM (SELECT a FROM t1 UNION SELECT a FROM t2) AS combined",
        "E091",
        "Aggregate over UNION result"
    );
}

/// Edge case: GROUP BY with no aggregate in SELECT
#[test]
fn edge_group_by_no_aggregate() {
    assert_feature_supported!(
        "SELECT category FROM orders GROUP BY category",
        "E091",
        "GROUP BY without aggregate in SELECT"
    );
}

/// Edge case: Aggregate with NULLIF
#[test]
fn edge_aggregate_with_nullif() {
    assert_feature_supported!(
        "SELECT AVG(NULLIF(a, 0)) FROM t",
        "E091",
        "Aggregate with NULLIF"
    );
}

/// Edge case: Aggregate with COALESCE
#[test]
fn edge_aggregate_with_coalesce() {
    assert_feature_supported!(
        "SELECT SUM(COALESCE(a, 0)) FROM t",
        "E091",
        "Aggregate with COALESCE"
    );
}
