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

//! SQL:2016 Feature E051 - Basic query specification
//!
//! ISO/IEC 9075-2:2016 Section 7.6
//!
//! This feature covers the basic SELECT query specification, including:
//! - SELECT DISTINCT
//! - GROUP BY clause
//! - Column renaming (AS clause)
//! - HAVING clause
//! - Qualified asterisk (table.*)
//! - Correlation names
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E051-01 | SELECT DISTINCT | Supported |
//! | E051-02 | GROUP BY clause | Supported |
//! | E051-04 | GROUP BY can include columns not in select list | Supported |
//! | E051-05 | Select list items can be renamed | Supported |
//! | E051-06 | HAVING clause | Supported |
//! | E051-07 | Qualified * in select list | Supported |
//! | E051-08 | Correlation names in FROM clause | Supported |
//! | E051-09 | Rename columns in FROM clause | Supported |
//!
//! All E051 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::assert_feature_supported;

// ============================================================================
// E051-01: SELECT DISTINCT
// ============================================================================

/// E051-01: Basic SELECT DISTINCT
#[test]
fn e051_01_select_distinct() {
    assert_feature_supported!(
        "SELECT DISTINCT a FROM t",
        "E051-01",
        "SELECT DISTINCT"
    );
}

/// E051-01: SELECT DISTINCT with multiple columns
#[test]
fn e051_01_distinct_multiple_columns() {
    assert_feature_supported!(
        "SELECT DISTINCT a, b FROM t",
        "E051-01",
        "DISTINCT with multiple columns"
    );
}

/// E051-01: SELECT DISTINCT with expressions
#[test]
fn e051_01_distinct_expressions() {
    assert_feature_supported!(
        "SELECT DISTINCT a + b, c FROM t",
        "E051-01",
        "DISTINCT with expressions"
    );
}

/// E051-01: SELECT DISTINCT *
#[test]
fn e051_01_distinct_star() {
    assert_feature_supported!(
        "SELECT DISTINCT * FROM t",
        "E051-01",
        "DISTINCT with asterisk"
    );
}

// ============================================================================
// E051-02: GROUP BY clause
// ============================================================================

/// E051-02: Basic GROUP BY with single column
#[test]
fn e051_02_group_by_single() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a",
        "E051-02",
        "GROUP BY single column"
    );
}

/// E051-02: GROUP BY with multiple columns
#[test]
fn e051_02_group_by_multiple() {
    assert_feature_supported!(
        "SELECT a, b, COUNT(*) FROM t GROUP BY a, b",
        "E051-02",
        "GROUP BY multiple columns"
    );
}

/// E051-02: GROUP BY with aggregate functions
#[test]
fn e051_02_group_by_aggregates() {
    assert_feature_supported!(
        "SELECT state, COUNT(*), AVG(age), MAX(salary) FROM person GROUP BY state",
        "E051-02",
        "GROUP BY with aggregates"
    );
}

/// E051-02: GROUP BY with column reference by position (extension)
#[test]
fn e051_02_group_by_ordinal() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY 1",
        "E051-02",
        "GROUP BY with ordinal"
    );
}

// ============================================================================
// E051-04: GROUP BY can include columns not in select list
// ============================================================================

/// E051-04: GROUP BY column not in select list
#[test]
fn e051_04_group_by_not_in_select() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM t GROUP BY a",
        "E051-04",
        "GROUP BY column not in select list"
    );
}

/// E051-04: GROUP BY multiple columns, some not in select list
#[test]
fn e051_04_group_by_partial_select() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a, b",
        "E051-04",
        "GROUP BY with extra columns"
    );
}

/// E051-04: GROUP BY with only aggregate in select list
#[test]
fn e051_04_group_by_aggregates_only() {
    assert_feature_supported!(
        "SELECT SUM(salary), AVG(age) FROM person GROUP BY state",
        "E051-04",
        "GROUP BY with only aggregates in select"
    );
}

// ============================================================================
// E051-05: Select list items can be renamed (AS clause)
// ============================================================================

/// E051-05: Rename column with AS
#[test]
fn e051_05_rename_with_as() {
    assert_feature_supported!(
        "SELECT a AS column_a FROM t",
        "E051-05",
        "Rename column with AS"
    );
}

/// E051-05: Rename multiple columns
#[test]
fn e051_05_rename_multiple() {
    assert_feature_supported!(
        "SELECT a AS x, b AS y, c AS z FROM t",
        "E051-05",
        "Rename multiple columns"
    );
}

/// E051-05: Rename expression result
#[test]
fn e051_05_rename_expression() {
    assert_feature_supported!(
        "SELECT a + b AS sum_result FROM t",
        "E051-05",
        "Rename expression"
    );
}

/// E051-05: Rename aggregate function result
#[test]
fn e051_05_rename_aggregate() {
    assert_feature_supported!(
        "SELECT COUNT(*) AS total FROM t",
        "E051-05",
        "Rename aggregate"
    );
}

/// E051-05: Rename without AS keyword (implicit)
#[test]
fn e051_05_rename_implicit() {
    assert_feature_supported!(
        "SELECT a column_a FROM t",
        "E051-05",
        "Implicit column rename"
    );
}

/// E051-05: Rename with quoted identifier
#[test]
fn e051_05_rename_quoted() {
    assert_feature_supported!(
        "SELECT a AS \"Column A\" FROM t",
        "E051-05",
        "Rename with quoted identifier"
    );
}

// ============================================================================
// E051-06: HAVING clause
// ============================================================================

/// E051-06: Basic HAVING clause
#[test]
fn e051_06_having_basic() {
    assert_feature_supported!(
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
        "E051-06",
        "Basic HAVING clause"
    );
}

/// E051-06: HAVING with multiple conditions
#[test]
fn e051_06_having_multiple_conditions() {
    assert_feature_supported!(
        "SELECT state, AVG(salary) FROM person GROUP BY state HAVING AVG(salary) > 50000 AND COUNT(*) > 10",
        "E051-06",
        "HAVING with multiple conditions"
    );
}

/// E051-06: HAVING with aggregate function
#[test]
fn e051_06_having_aggregate() {
    assert_feature_supported!(
        "SELECT a, SUM(b) FROM t GROUP BY a HAVING SUM(b) > 100",
        "E051-06",
        "HAVING with SUM"
    );
}

/// E051-06: HAVING with MAX/MIN
#[test]
fn e051_06_having_max_min() {
    assert_feature_supported!(
        "SELECT state, MAX(age) FROM person GROUP BY state HAVING MAX(age) > 65",
        "E051-06",
        "HAVING with MAX"
    );
}

/// E051-06: HAVING with OR condition
#[test]
fn e051_06_having_or() {
    assert_feature_supported!(
        "SELECT a FROM t GROUP BY a HAVING COUNT(*) > 5 OR SUM(b) > 100",
        "E051-06",
        "HAVING with OR"
    );
}

// ============================================================================
// E051-07: Qualified * in select list
// ============================================================================

/// E051-07: Table qualified asterisk
#[test]
fn e051_07_qualified_asterisk() {
    assert_feature_supported!(
        "SELECT t.* FROM t",
        "E051-07",
        "Qualified asterisk"
    );
}

/// E051-07: Qualified asterisk in join
#[test]
fn e051_07_qualified_asterisk_join() {
    assert_feature_supported!(
        "SELECT t1.*, t2.a FROM t1 JOIN t2 ON t1.a = t2.a",
        "E051-07",
        "Qualified asterisk in join"
    );
}

/// E051-07: Multiple qualified asterisks
#[test]
fn e051_07_multiple_qualified_asterisks() {
    assert_feature_supported!(
        "SELECT t1.*, t2.* FROM t1 JOIN t2 ON t1.a = t2.a",
        "E051-07",
        "Multiple qualified asterisks"
    );
}

/// E051-07: Mix qualified asterisk with specific columns
#[test]
fn e051_07_mixed_asterisk_columns() {
    assert_feature_supported!(
        "SELECT t1.*, t2.b, t2.c FROM t1 JOIN t2 ON t1.a = t2.a",
        "E051-07",
        "Mix asterisk with columns"
    );
}

// ============================================================================
// E051-08: Correlation names in FROM clause
// ============================================================================

/// E051-08: Basic table alias
#[test]
fn e051_08_table_alias() {
    assert_feature_supported!(
        "SELECT x.a FROM t AS x",
        "E051-08",
        "Table alias with AS"
    );
}

/// E051-08: Table alias without AS
#[test]
fn e051_08_table_alias_implicit() {
    assert_feature_supported!(
        "SELECT x.a FROM t x",
        "E051-08",
        "Table alias without AS"
    );
}

/// E051-08: Multiple table aliases in join
#[test]
fn e051_08_multiple_aliases() {
    assert_feature_supported!(
        "SELECT x.a, y.b FROM t1 AS x JOIN t2 AS y ON x.a = y.a",
        "E051-08",
        "Multiple table aliases"
    );
}

/// E051-08: Self-join with aliases
#[test]
fn e051_08_self_join_aliases() {
    assert_feature_supported!(
        "SELECT a.first_name, b.first_name FROM person a JOIN person b ON a.id = b.id + 1",
        "E051-08",
        "Self-join with aliases"
    );
}

/// E051-08: Correlation name in WHERE clause
#[test]
fn e051_08_correlation_where() {
    assert_feature_supported!(
        "SELECT p.first_name FROM person p WHERE p.age > 21",
        "E051-08",
        "Correlation name in WHERE"
    );
}

/// E051-08: Correlation name in GROUP BY
#[test]
fn e051_08_correlation_group_by() {
    assert_feature_supported!(
        "SELECT p.state, COUNT(*) FROM person p GROUP BY p.state",
        "E051-08",
        "Correlation name in GROUP BY"
    );
}

// ============================================================================
// E051-09: Rename columns in FROM clause
// ============================================================================

/// E051-09: Rename columns in table alias
#[test]
fn e051_09_rename_columns_basic() {
    assert_feature_supported!(
        "SELECT col1, col2 FROM t AS x(col1, col2, col3)",
        "E051-09",
        "Rename columns in FROM"
    );
}

/// E051-09: Rename columns without AS
#[test]
fn e051_09_rename_columns_implicit() {
    assert_feature_supported!(
        "SELECT col1 FROM t x(col1, col2, col3)",
        "E051-09",
        "Rename columns without AS"
    );
}

/// E051-09: Rename columns in join
#[test]
fn e051_09_rename_columns_join() {
    assert_feature_supported!(
        "SELECT x.id, y.id FROM t1 AS x(id, val) JOIN t2 AS y(id, val) ON x.id = y.id",
        "E051-09",
        "Rename columns in join"
    );
}

/// E051-09: Rename subset of columns
#[test]
fn e051_09_rename_partial() {
    assert_feature_supported!(
        "SELECT new_a FROM t AS x(new_a)",
        "E051-09",
        "Rename first column only"
    );
}

// ============================================================================
// E051 Combined Tests
// ============================================================================

/// E051: Complex query combining multiple subfeatures
#[test]
fn e051_combined_query() {
    assert_feature_supported!(
        "SELECT DISTINCT p.state AS region, COUNT(*) AS total, AVG(p.salary) AS avg_sal \
         FROM person AS p \
         WHERE p.age >= 18 \
         GROUP BY p.state \
         HAVING COUNT(*) > 5 \
         ORDER BY total DESC",
        "E051",
        "Combined E051 features"
    );
}

/// E051: Query with table and column aliases
#[test]
fn e051_combined_aliases() {
    assert_feature_supported!(
        "SELECT emp.name, emp.pay \
         FROM person AS emp(id, fname, lname, name, years, loc, pay, dob, first_delim, last_delim) \
         WHERE emp.years > 30",
        "E051",
        "Table and column aliases"
    );
}

/// E051: Complex join with qualified asterisks and aggregation
#[test]
fn e051_combined_join_group() {
    assert_feature_supported!(
        "SELECT p.*, COUNT(o.order_id) AS order_count \
         FROM person p \
         LEFT JOIN orders o ON p.id = o.customer_id \
         GROUP BY p.a, p.id, p.first_name, p.last_name, p.name, p.age, p.state, p.city, p.salary, p.birth_date, p.timestamp, p.\"First Name\", p.\"Last Name\", p.middle_name, p.maiden_name, p.spouse_name, p.status, p.action \
         HAVING COUNT(o.order_id) > 0",
        "E051",
        "Join with qualified asterisk and GROUP BY"
    );
}

/// E051: DISTINCT with GROUP BY and HAVING
#[test]
fn e051_combined_distinct_group_having() {
    assert_feature_supported!(
        "SELECT DISTINCT state FROM person GROUP BY state HAVING AVG(age) > 35",
        "E051",
        "DISTINCT with GROUP BY and HAVING"
    );
}

/// E051: Self-join with column renaming
#[test]
fn e051_combined_self_join_rename() {
    assert_feature_supported!(
        "SELECT mgr.fname AS manager_name, emp.first_name AS employee_name \
         FROM person mgr(eid, fname, lname, fullname, yrs, st, sal, bd, first_delim, last_delim) \
         JOIN person emp ON mgr.eid = emp.id - 1",
        "E051",
        "Self-join with column renaming"
    );
}
