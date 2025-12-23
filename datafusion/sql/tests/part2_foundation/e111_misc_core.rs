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

//! SQL:2016 Miscellaneous Core Features - E111, E131, E161, F131
//!
//! This module tests several essential SQL:2016 core features:
//!
//! # E111 - Single row SELECT statement
//! ISO/IEC 9075-2:2016 Section 7.6
//! SELECT statements without a FROM clause (value expressions)
//!
//! # E131 - NULL value support
//! ISO/IEC 9075-2:2016 Section 4.5
//! Comprehensive NULL semantics including three-valued logic
//!
//! # E161 - SQL comments
//! ISO/IEC 9075-2:2016 Section 5.2
//! Single-line (--) and multi-line (/* */) comment support
//!
//! # F131 - Grouped operations
//! ISO/IEC 9075-2:2016 Section 7.9
//! Advanced GROUP BY and HAVING capabilities
//!
//! All features tested here are CORE features (mandatory for SQL:2016 conformance).

use crate::assert_feature_supported;

// ============================================================================
// E111 - Single row SELECT statement
// ============================================================================

/// E111: SELECT without FROM - literal integer
#[test]
fn e111_select_literal_integer() {
    assert_feature_supported!(
        "SELECT 1",
        "E111",
        "SELECT without FROM - integer literal"
    );
}

/// E111: SELECT without FROM - literal string
#[test]
fn e111_select_literal_string() {
    assert_feature_supported!(
        "SELECT 'hello'",
        "E111",
        "SELECT without FROM - string literal"
    );
}

/// E111: SELECT without FROM - arithmetic expression
#[test]
fn e111_select_expression_arithmetic() {
    assert_feature_supported!(
        "SELECT 1 + 2",
        "E111",
        "SELECT without FROM - arithmetic"
    );
}

/// E111: SELECT without FROM - complex expression
#[test]
fn e111_select_expression_complex() {
    assert_feature_supported!(
        "SELECT 1 + 2 * 3 - 4 / 2",
        "E111",
        "SELECT without FROM - complex expression"
    );
}

/// E111: SELECT without FROM - function call
#[test]
fn e111_select_function_current_date() {
    assert_feature_supported!(
        "SELECT CURRENT_DATE",
        "E111",
        "SELECT without FROM - CURRENT_DATE"
    );
}

/// E111: SELECT without FROM - function call CURRENT_TIME
#[test]
fn e111_select_function_current_time() {
    assert_feature_supported!(
        "SELECT CURRENT_TIME",
        "E111",
        "SELECT without FROM - CURRENT_TIME"
    );
}

/// E111: SELECT without FROM - function call CURRENT_TIMESTAMP
#[test]
fn e111_select_function_current_timestamp() {
    assert_feature_supported!(
        "SELECT CURRENT_TIMESTAMP",
        "E111",
        "SELECT without FROM - CURRENT_TIMESTAMP"
    );
}

/// E111: SELECT without FROM - multiple values
#[test]
fn e111_select_multiple_values() {
    assert_feature_supported!(
        "SELECT 1, 'a', 3.14",
        "E111",
        "SELECT without FROM - multiple values"
    );
}

/// E111: SELECT without FROM - multiple expressions
#[test]
fn e111_select_multiple_expressions() {
    assert_feature_supported!(
        "SELECT 1 + 1 AS two, 2 * 3 AS six, 'hello' AS greeting",
        "E111",
        "SELECT without FROM - multiple expressions with aliases"
    );
}

/// E111: SELECT without FROM - boolean literal
#[test]
fn e111_select_boolean() {
    assert_feature_supported!(
        "SELECT TRUE, FALSE",
        "E111",
        "SELECT without FROM - boolean literals"
    );
}

/// E111: SELECT without FROM - NULL literal
#[test]
fn e111_select_null() {
    assert_feature_supported!(
        "SELECT NULL",
        "E111",
        "SELECT without FROM - NULL literal"
    );
}

/// E111: SELECT without FROM - string concatenation
#[test]
fn e111_select_string_concat() {
    assert_feature_supported!(
        "SELECT 'hello' || ' ' || 'world'",
        "E111",
        "SELECT without FROM - string concatenation"
    );
}

/// E111: SELECT without FROM - scalar function
#[test]
fn e111_select_scalar_function() {
    assert_feature_supported!(
        "SELECT UPPER('hello')",
        "E111",
        "SELECT without FROM - scalar function"
    );
}

/// E111: SELECT without FROM - CAST expression
#[test]
fn e111_select_cast() {
    assert_feature_supported!(
        "SELECT CAST(42 AS VARCHAR)",
        "E111",
        "SELECT without FROM - CAST"
    );
}

/// E111: SELECT without FROM - CASE expression
#[test]
fn e111_select_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END",
        "E111",
        "SELECT without FROM - CASE expression"
    );
}

/// E111: SELECT without FROM - nested expressions
#[test]
fn e111_select_nested() {
    assert_feature_supported!(
        "SELECT (1 + 2) * (3 + 4) AS result",
        "E111",
        "SELECT without FROM - nested expressions"
    );
}

// ============================================================================
// E131 - NULL value support
// ============================================================================

/// E131: NULL literal in SELECT
#[test]
fn e131_null_literal() {
    assert_feature_supported!(
        "SELECT NULL",
        "E131",
        "NULL literal"
    );
}

/// E131: NULL literal with alias
#[test]
fn e131_null_literal_alias() {
    assert_feature_supported!(
        "SELECT NULL AS null_value FROM t",
        "E131",
        "NULL literal with alias"
    );
}

/// E131: Multiple NULL values
#[test]
fn e131_multiple_nulls() {
    assert_feature_supported!(
        "SELECT NULL, NULL AS n1, NULL AS n2 FROM t",
        "E131",
        "Multiple NULL values"
    );
}

/// E131: NULL in arithmetic expression
#[test]
fn e131_null_in_arithmetic() {
    assert_feature_supported!(
        "SELECT a + NULL FROM t",
        "E131",
        "NULL in arithmetic expression"
    );
}

/// E131: NULL in comparison
#[test]
fn e131_null_in_comparison() {
    assert_feature_supported!(
        "SELECT a = NULL FROM t",
        "E131",
        "NULL in comparison (returns NULL)"
    );
}

/// E131: IS NULL predicate
#[test]
fn e131_is_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NULL",
        "E131",
        "IS NULL predicate"
    );
}

/// E131: IS NOT NULL predicate
#[test]
fn e131_is_not_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IS NOT NULL",
        "E131",
        "IS NOT NULL predicate"
    );
}

/// E131: IS NULL with expression
#[test]
fn e131_is_null_expression() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a + b IS NULL",
        "E131",
        "IS NULL with expression"
    );
}

/// E131: NULL in INSERT statement
#[test]
fn e131_null_in_insert() {
    assert_feature_supported!(
        "INSERT INTO t (a, b) VALUES (1, NULL)",
        "E131",
        "NULL in INSERT VALUES"
    );
}

/// E131: NULL in UPDATE statement
#[test]
fn e131_null_in_update() {
    assert_feature_supported!(
        "UPDATE t SET a = NULL WHERE b = 1",
        "E131",
        "NULL in UPDATE SET"
    );
}

/// E131: NULL handling in COUNT aggregate
#[test]
fn e131_null_count() {
    assert_feature_supported!(
        "SELECT COUNT(a) FROM t",
        "E131",
        "COUNT ignores NULLs"
    );
}

/// E131: NULL handling in COUNT(*)
#[test]
fn e131_null_count_star() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM t",
        "E131",
        "COUNT(*) includes NULL rows"
    );
}

/// E131: NULL handling in SUM aggregate
#[test]
fn e131_null_sum() {
    assert_feature_supported!(
        "SELECT SUM(a) FROM t",
        "E131",
        "SUM ignores NULLs"
    );
}

/// E131: NULL handling in AVG aggregate
#[test]
fn e131_null_avg() {
    assert_feature_supported!(
        "SELECT AVG(a) FROM t",
        "E131",
        "AVG ignores NULLs"
    );
}

/// E131: NULL handling in MIN aggregate
#[test]
fn e131_null_min() {
    assert_feature_supported!(
        "SELECT MIN(a) FROM t",
        "E131",
        "MIN ignores NULLs"
    );
}

/// E131: NULL handling in MAX aggregate
#[test]
fn e131_null_max() {
    assert_feature_supported!(
        "SELECT MAX(a) FROM t",
        "E131",
        "MAX ignores NULLs"
    );
}

/// E131: COALESCE with NULL values
#[test]
fn e131_coalesce_null() {
    assert_feature_supported!(
        "SELECT COALESCE(NULL, NULL, a, NULL, b) FROM t",
        "E131",
        "COALESCE with NULL values"
    );
}

/// E131: COALESCE all NULLs
#[test]
fn e131_coalesce_all_null() {
    assert_feature_supported!(
        "SELECT COALESCE(NULL, NULL, NULL) FROM t",
        "E131",
        "COALESCE with all NULLs"
    );
}

/// E131: NULLIF function
#[test]
fn e131_nullif() {
    assert_feature_supported!(
        "SELECT NULLIF(a, 0) FROM t",
        "E131",
        "NULLIF function"
    );
}

/// E131: NULL in CASE WHEN condition
#[test]
fn e131_null_case_condition() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS NULL THEN 0 ELSE a END FROM t",
        "E131",
        "NULL in CASE condition"
    );
}

/// E131: NULL in CASE result
#[test]
fn e131_null_case_result() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN a ELSE NULL END FROM t",
        "E131",
        "NULL in CASE result"
    );
}

/// E131: NULL in simple CASE
#[test]
fn e131_null_simple_case() {
    assert_feature_supported!(
        "SELECT CASE a WHEN NULL THEN 'null' ELSE 'not null' END FROM t",
        "E131",
        "NULL in simple CASE (won't match)"
    );
}

/// E131: Three-valued logic - NULL AND TRUE
#[test]
fn e131_null_and_true() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a IS NULL) AND (b > 0)",
        "E131",
        "NULL AND TRUE = NULL"
    );
}

/// E131: Three-valued logic - NULL OR TRUE
#[test]
fn e131_null_or_true() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE (a IS NULL) OR (b > 0)",
        "E131",
        "NULL OR TRUE = TRUE"
    );
}

/// E131: Three-valued logic - NOT NULL
#[test]
fn e131_not_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE NOT (a = NULL)",
        "E131",
        "NOT NULL = NULL"
    );
}

/// E131: NULL in string concatenation
#[test]
fn e131_null_concat() {
    assert_feature_supported!(
        "SELECT a || NULL FROM t",
        "E131",
        "NULL in string concatenation"
    );
}

/// E131: NULL comparison semantics
#[test]
fn e131_null_comparison() {
    assert_feature_supported!(
        "SELECT NULL = NULL, NULL <> NULL, NULL > NULL FROM t",
        "E131",
        "NULL comparison semantics (all return NULL)"
    );
}

/// E131: NULL in BETWEEN
#[test]
fn e131_null_between() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a BETWEEN NULL AND 10",
        "E131",
        "NULL in BETWEEN predicate"
    );
}

/// E131: NULL in IN list
#[test]
fn e131_null_in_list() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE a IN (1, NULL, 3)",
        "E131",
        "NULL in IN list"
    );
}

/// E131: NULL with DISTINCT
#[test]
fn e131_null_distinct() {
    assert_feature_supported!(
        "SELECT DISTINCT a FROM t",
        "E131",
        "DISTINCT treats NULLs as equal"
    );
}

/// E131: NULL in GROUP BY
#[test]
fn e131_null_group_by() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "E131",
        "GROUP BY groups NULLs together"
    );
}

/// E131: NULL in ORDER BY
#[test]
fn e131_null_order_by() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a",
        "E131",
        "NULL handling in ORDER BY"
    );
}

/// E131: NULL in ORDER BY with NULLS FIRST
#[test]
fn e131_null_order_nulls_first() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a NULLS FIRST",
        "E131",
        "ORDER BY with NULLS FIRST"
    );
}

/// E131: NULL in ORDER BY with NULLS LAST
#[test]
fn e131_null_order_nulls_last() {
    assert_feature_supported!(
        "SELECT a FROM t ORDER BY a NULLS LAST",
        "E131",
        "ORDER BY with NULLS LAST"
    );
}

/// E131: NULL in JOIN condition
#[test]
fn e131_null_join() {
    assert_feature_supported!(
        "SELECT * FROM t t1 JOIN t t2 ON t1.a = t2.a",
        "E131",
        "NULL in JOIN (NULLs don't match)"
    );
}

/// E131: Complex NULL handling
#[test]
fn e131_complex_null() {
    assert_feature_supported!(
        "SELECT COALESCE(NULLIF(a, 0), b, 1) FROM t WHERE a IS NOT NULL OR b IS NULL",
        "E131",
        "Complex NULL handling"
    );
}

// ============================================================================
// E161 - SQL comments
// ============================================================================

/// E161: Single-line comment before SELECT
#[test]
fn e161_single_line_before() {
    assert_feature_supported!(
        "-- This is a comment\nSELECT 1",
        "E161",
        "Single-line comment before SELECT"
    );
}

/// E161: Single-line comment after SELECT
#[test]
fn e161_single_line_after() {
    assert_feature_supported!(
        "SELECT 1 -- comment after query",
        "E161",
        "Single-line comment at end"
    );
}

/// E161: Single-line comment in middle of query
#[test]
fn e161_single_line_middle() {
    assert_feature_supported!(
        "SELECT a -- select column a\nFROM t",
        "E161",
        "Single-line comment in query"
    );
}

/// E161: Multiple single-line comments
#[test]
fn e161_multiple_single_line() {
    assert_feature_supported!(
        "-- First comment\n-- Second comment\nSELECT 1",
        "E161",
        "Multiple single-line comments"
    );
}

/// E161: Single-line comment with special characters
#[test]
fn e161_single_line_special_chars() {
    assert_feature_supported!(
        "-- Comment with special chars: @#$%^&*()\nSELECT 1",
        "E161",
        "Single-line comment with special characters"
    );
}

/// E161: Multi-line comment before SELECT
#[test]
fn e161_multi_line_before() {
    assert_feature_supported!(
        "/* This is a multi-line comment */\nSELECT 1",
        "E161",
        "Multi-line comment before SELECT"
    );
}

/// E161: Multi-line comment after SELECT
#[test]
fn e161_multi_line_after() {
    assert_feature_supported!(
        "SELECT 1 /* comment */",
        "E161",
        "Multi-line comment after query"
    );
}

/// E161: Multi-line comment spanning lines
#[test]
fn e161_multi_line_spanning() {
    assert_feature_supported!(
        "/* This comment\nspans multiple\nlines */\nSELECT 1",
        "E161",
        "Multi-line comment spanning lines"
    );
}

/// E161: Multi-line comment in SELECT list
#[test]
fn e161_multi_line_select_list() {
    assert_feature_supported!(
        "SELECT a, /* comment */ b FROM t",
        "E161",
        "Multi-line comment in SELECT list"
    );
}

/// E161: Multi-line comment in WHERE clause
#[test]
fn e161_multi_line_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE /* filter */ a > 0",
        "E161",
        "Multi-line comment in WHERE"
    );
}

/// E161: Multiple multi-line comments
#[test]
fn e161_multiple_multi_line() {
    assert_feature_supported!(
        "/* First */ SELECT /* Second */ 1",
        "E161",
        "Multiple multi-line comments"
    );
}

/// E161: Multi-line comment with special characters
#[test]
fn e161_multi_line_special_chars() {
    assert_feature_supported!(
        "/* Special: @#$%^&*() */\nSELECT 1",
        "E161",
        "Multi-line comment with special chars"
    );
}

/// E161: Comments in complex query
#[test]
fn e161_complex_query_comments() {
    assert_feature_supported!(
        "-- Get all persons\nSELECT /* columns */ first_name, last_name\nFROM person -- from person table\nWHERE age > 21 /* adults only */",
        "E161",
        "Comments in complex query"
    );
}

/// E161: Comment before INSERT
#[test]
fn e161_comment_insert() {
    assert_feature_supported!(
        "-- Insert a new record\nINSERT INTO t (a, b) VALUES (1, 2)",
        "E161",
        "Comment before INSERT"
    );
}

/// E161: Comment before UPDATE
#[test]
fn e161_comment_update() {
    assert_feature_supported!(
        "/* Update records */ UPDATE t SET a = 1 WHERE b = 2",
        "E161",
        "Comment before UPDATE"
    );
}

/// E161: Comment before DELETE
#[test]
fn e161_comment_delete() {
    assert_feature_supported!(
        "-- Delete old records\nDELETE FROM t WHERE a < 0",
        "E161",
        "Comment before DELETE"
    );
}

/// E161: Comment before CREATE TABLE
#[test]
fn e161_comment_create() {
    assert_feature_supported!(
        "/* Create new table */\nCREATE TABLE test_table (id INT, name VARCHAR)",
        "E161",
        "Comment before CREATE TABLE"
    );
}

/// E161: Empty multi-line comment
#[test]
fn e161_empty_multi_line() {
    assert_feature_supported!(
        "/**/SELECT 1",
        "E161",
        "Empty multi-line comment"
    );
}

/// E161: Comment with dashes inside
#[test]
fn e161_dashes_inside_comment() {
    assert_feature_supported!(
        "/* Comment with -- dashes */\nSELECT 1",
        "E161",
        "Multi-line comment containing dashes"
    );
}

/// E161: Mixed comment styles
#[test]
fn e161_mixed_comments() {
    assert_feature_supported!(
        "-- Single line\n/* Multi line */\nSELECT 1 -- end comment",
        "E161",
        "Mixed comment styles"
    );
}

// ============================================================================
// F131 - Grouped operations
// ============================================================================

/// F131-01: WHERE, GROUP BY, HAVING in queries with grouped views
/// Testing basic GROUP BY with WHERE and HAVING
#[test]
fn f131_01_where_group_having() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t WHERE b > 0 GROUP BY a HAVING COUNT(*) > 1",
        "F131-01",
        "WHERE, GROUP BY, HAVING"
    );
}

/// F131-01: GROUP BY with filtered results
#[test]
fn f131_01_group_with_where() {
    assert_feature_supported!(
        "SELECT state, COUNT(*) FROM person WHERE age > 18 GROUP BY state",
        "F131-01",
        "GROUP BY with WHERE filter"
    );
}

/// F131-01: HAVING with aggregate condition
#[test]
fn f131_01_having_aggregate() {
    assert_feature_supported!(
        "SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 100",
        "F131-01",
        "HAVING with aggregate condition"
    );
}

/// F131-01: HAVING with multiple conditions
#[test]
fn f131_01_having_multiple() {
    assert_feature_supported!(
        "SELECT a, COUNT(*), AVG(b) FROM t GROUP BY a HAVING COUNT(*) > 5 AND AVG(b) > 10",
        "F131-01",
        "HAVING with multiple conditions"
    );
}

/// F131-02: Multiple tables in queries with grouped views
#[test]
fn f131_02_multiple_tables() {
    assert_feature_supported!(
        "SELECT person.state, COUNT(*) \
         FROM person JOIN orders ON person.id = orders.person_id \
         GROUP BY person.state",
        "F131-02",
        "GROUP BY with JOIN"
    );
}

/// F131-02: GROUP BY with multiple JOINs
#[test]
fn f131_02_multiple_joins() {
    assert_feature_supported!(
        "SELECT p.state, COUNT(*) \
         FROM person p \
         JOIN orders o ON p.id = o.person_id \
         GROUP BY p.state \
         HAVING COUNT(*) > 10",
        "F131-02",
        "Multiple tables with GROUP BY and HAVING"
    );
}

/// F131-03: Set functions in queries with grouped views
#[test]
fn f131_03_set_functions() {
    assert_feature_supported!(
        "SELECT a, COUNT(*), SUM(b), AVG(c), MIN(d), MAX(e) FROM t GROUP BY a",
        "F131-03",
        "Multiple set functions with GROUP BY"
    );
}

/// F131-03: Complex aggregates in GROUP BY
#[test]
fn f131_03_complex_aggregates() {
    assert_feature_supported!(
        "SELECT state, COUNT(DISTINCT first_name), AVG(age), SUM(salary) \
         FROM person \
         GROUP BY state",
        "F131-03",
        "Complex aggregates in GROUP BY"
    );
}

/// F131-04: Subqueries with GROUP BY and HAVING
#[test]
fn f131_04_subquery_group() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, COUNT(*) as cnt FROM t GROUP BY a HAVING COUNT(*) > 1) sub",
        "F131-04",
        "Subquery with GROUP BY and HAVING"
    );
}

/// F131-04: Correlated subquery with GROUP BY
#[test]
fn f131_04_correlated_subquery() {
    assert_feature_supported!(
        "SELECT state FROM person p1 \
         WHERE (SELECT COUNT(*) FROM person p2 WHERE p2.state = p1.state GROUP BY state) > 5",
        "F131-04",
        "Correlated subquery with GROUP BY"
    );
}

/// F131-04: GROUP BY with subquery in WHERE
#[test]
fn f131_04_where_subquery() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t \
         WHERE b > (SELECT AVG(b) FROM t) \
         GROUP BY a \
         HAVING COUNT(*) > 1",
        "F131-04",
        "GROUP BY with subquery in WHERE"
    );
}

/// F131-05: Single row SELECT with GROUP BY and HAVING
#[test]
fn f131_05_single_row_group() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
        "F131-05",
        "Single aggregate with GROUP BY and HAVING"
    );
}

/// F131-05: Aggregate only with HAVING
#[test]
fn f131_05_aggregate_having() {
    assert_feature_supported!(
        "SELECT SUM(salary) FROM person GROUP BY state HAVING SUM(salary) > 100000",
        "F131-05",
        "Single aggregate result with HAVING"
    );
}

/// F131: GROUP BY single column
#[test]
fn f131_group_by_single() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "F131",
        "GROUP BY single column"
    );
}

/// F131: GROUP BY multiple columns
#[test]
fn f131_group_by_multiple() {
    assert_feature_supported!(
        "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
        "F131",
        "GROUP BY multiple columns"
    );
}

/// F131: GROUP BY with expressions
#[test]
fn f131_group_by_expression() {
    assert_feature_supported!(
        "SELECT a + b, COUNT(*) FROM t GROUP BY a + b",
        "F131",
        "GROUP BY with expression"
    );
}

/// F131: GROUP BY with multiple expressions
#[test]
fn f131_group_by_multiple_expressions() {
    assert_feature_supported!(
        "SELECT UPPER(first_name), state, COUNT(*) FROM person GROUP BY UPPER(first_name), state",
        "F131",
        "GROUP BY with multiple expressions"
    );
}

/// F131: GROUP BY ordinal - single position
#[test]
fn f131_group_by_ordinal_single() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY 1",
        "F131",
        "GROUP BY ordinal single position"
    );
}

/// F131: GROUP BY ordinal - multiple positions
#[test]
fn f131_group_by_ordinal_multiple() {
    assert_feature_supported!(
        "SELECT a, b, COUNT(*) FROM t GROUP BY 1, 2",
        "F131",
        "GROUP BY ordinal multiple positions"
    );
}

/// F131: GROUP BY ALL (if supported)
#[test]
fn f131_group_by_all() {
    assert_feature_supported!(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ALL",
        "F131",
        "GROUP BY ALL"
    );
}

/// F131: HAVING without aggregate in condition
#[test]
fn f131_having_non_aggregate() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING a > 10",
        "F131",
        "HAVING with non-aggregate condition"
    );
}

/// F131: HAVING with complex condition
#[test]
fn f131_having_complex() {
    assert_feature_supported!(
        "SELECT state, COUNT(*), AVG(age) \
         FROM person \
         GROUP BY state \
         HAVING COUNT(*) > 10 AND AVG(age) > 30 OR state = 'CA'",
        "F131",
        "HAVING with complex condition"
    );
}

/// F131: HAVING with subquery
#[test]
fn f131_having_subquery() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t \
         GROUP BY a \
         HAVING COUNT(*) > (SELECT AVG(cnt) FROM (SELECT COUNT(*) as cnt FROM t GROUP BY a) sub)",
        "F131",
        "HAVING with subquery"
    );
}

/// F131: GROUP BY with DISTINCT in aggregate
#[test]
fn f131_group_distinct_aggregate() {
    assert_feature_supported!(
        "SELECT a, COUNT(DISTINCT b) FROM t GROUP BY a",
        "F131",
        "GROUP BY with DISTINCT in aggregate"
    );
}

/// F131: GROUP BY with multiple DISTINCT aggregates
#[test]
fn f131_multiple_distinct_aggregates() {
    assert_feature_supported!(
        "SELECT state, COUNT(DISTINCT first_name), COUNT(DISTINCT last_name) FROM person GROUP BY state",
        "F131",
        "Multiple DISTINCT aggregates with GROUP BY"
    );
}

/// F131: GROUP BY with ORDER BY
#[test]
fn f131_group_order() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) as cnt FROM t GROUP BY a ORDER BY cnt DESC",
        "F131",
        "GROUP BY with ORDER BY"
    );
}

/// F131: GROUP BY with ORDER BY aggregate
#[test]
fn f131_group_order_aggregate() {
    assert_feature_supported!(
        "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b)",
        "F131",
        "GROUP BY with ORDER BY aggregate"
    );
}

/// F131: Nested GROUP BY in subquery
#[test]
fn f131_nested_group_by() {
    assert_feature_supported!(
        "SELECT * FROM \
         (SELECT a, COUNT(*) as cnt FROM t GROUP BY a) t1 \
         JOIN \
         (SELECT b, SUM(c) as total FROM t GROUP BY b) t2 \
         ON t1.cnt > t2.total",
        "F131",
        "Nested GROUP BY in subqueries"
    );
}

/// F131: GROUP BY with CASE expression
#[test]
fn f131_group_by_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END as category, COUNT(*) \
         FROM person \
         GROUP BY CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END",
        "F131",
        "GROUP BY with CASE expression"
    );
}

/// F131: GROUP BY with COALESCE
#[test]
fn f131_group_by_coalesce() {
    assert_feature_supported!(
        "SELECT COALESCE(state, 'Unknown'), COUNT(*) FROM person GROUP BY COALESCE(state, 'Unknown')",
        "F131",
        "GROUP BY with COALESCE"
    );
}

/// F131: GROUP BY with NULL values
#[test]
fn f131_group_by_nulls() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "F131",
        "GROUP BY handles NULL values"
    );
}

/// F131: HAVING with CASE expression
#[test]
fn f131_having_case() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t \
         GROUP BY a \
         HAVING CASE WHEN COUNT(*) > 10 THEN 1 ELSE 0 END = 1",
        "F131",
        "HAVING with CASE expression"
    );
}

/// F131: GROUP BY with all aggregate types
#[test]
fn f131_all_aggregate_types() {
    assert_feature_supported!(
        "SELECT \
         a, \
         COUNT(*) as cnt, \
         COUNT(b) as cnt_b, \
         COUNT(DISTINCT b) as cnt_distinct_b, \
         SUM(b) as sum_b, \
         AVG(b) as avg_b, \
         MIN(b) as min_b, \
         MAX(b) as max_b \
         FROM t \
         GROUP BY a",
        "F131",
        "GROUP BY with all aggregate types"
    );
}

/// F131: Complex grouped query
#[test]
fn f131_complex_grouped_query() {
    assert_feature_supported!(
        "SELECT \
         p.state, \
         COUNT(*) as person_count, \
         COUNT(DISTINCT o.id) as order_count, \
         AVG(p.age) as avg_age, \
         SUM(p.salary) as total_salary \
         FROM person p \
         LEFT JOIN orders o ON p.id = o.person_id \
         WHERE p.age >= 18 \
         GROUP BY p.state \
         HAVING COUNT(*) > 5 AND AVG(p.age) > 25 \
         ORDER BY total_salary DESC",
        "F131",
        "Complex grouped query with JOIN, WHERE, HAVING, ORDER BY"
    );
}

/// F131: GROUP BY with UNION
#[test]
fn f131_group_by_union() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a \
         UNION \
         SELECT b, COUNT(*) FROM t GROUP BY b",
        "F131",
        "GROUP BY with UNION"
    );
}

/// F131: GROUP BY in CTE
#[test]
fn f131_group_by_cte() {
    assert_feature_supported!(
        "WITH grouped AS (SELECT a, COUNT(*) as cnt FROM t GROUP BY a) \
         SELECT * FROM grouped WHERE cnt > 5",
        "F131",
        "GROUP BY in CTE"
    );
}

/// F131: Multiple CTEs with GROUP BY
#[test]
fn f131_multiple_cte_group_by() {
    assert_feature_supported!(
        "WITH \
         by_state AS (SELECT state, COUNT(*) as cnt FROM person GROUP BY state), \
         by_age AS (SELECT age, AVG(salary) as avg_sal FROM person GROUP BY age) \
         SELECT * FROM by_state JOIN by_age ON by_state.cnt > by_age.avg_sal",
        "F131",
        "Multiple CTEs with GROUP BY"
    );
}
