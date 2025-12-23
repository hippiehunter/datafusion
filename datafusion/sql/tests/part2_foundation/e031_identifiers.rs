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

//! SQL:2016 Feature E031 - Identifiers
//!
//! ISO/IEC 9075-2:2016 Section 5.2
//!
//! This feature covers the rules for identifiers (names of tables, columns, etc.):
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E031-01 | Delimited identifiers | Supported |
//! | E031-02 | Lower case identifiers | Supported |
//! | E031-03 | Trailing underscore | Supported |
//!
//! All E031 subfeatures are CORE features (mandatory for SQL:2016 conformance).
//!
//! # Identifier Rules (SQL:2016)
//!
//! Regular identifiers:
//! - Start with a letter (A-Z, a-z)
//! - Contain letters, digits (0-9), and underscores (_)
//! - Case-insensitive by default (folded to uppercase)
//!
//! Delimited identifiers:
//! - Enclosed in double quotes ("identifier")
//! - Case-sensitive
//! - Can contain spaces and special characters
//! - Can be reserved words

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// E031-01: Delimited identifiers
// ============================================================================

/// E031-01: Delimited identifier for table name
#[test]
fn e031_01_delimited_table_name() {
    assert_feature_supported!(
        r#"CREATE TABLE "MyTable" (x INT)"#,
        "E031-01",
        "Delimited table name"
    );
}

/// E031-01: Delimited identifier for column name
#[test]
fn e031_01_delimited_column_name() {
    assert_feature_supported!(
        r#"CREATE TABLE t ("MyColumn" INT)"#,
        "E031-01",
        "Delimited column name"
    );
}

/// E031-01: Delimited identifier with spaces
#[test]
fn e031_01_delimited_with_spaces() {
    assert_feature_supported!(
        r#"CREATE TABLE t ("First Name" VARCHAR(50))"#,
        "E031-01",
        "Delimited identifier with spaces"
    );
}

/// E031-01: Delimited identifier with special characters
#[test]
fn e031_01_delimited_special_chars() {
    assert_feature_supported!(
        r#"CREATE TABLE t ("column@name" INT, "column-name" INT, "column.name" INT)"#,
        "E031-01",
        "Delimited identifier with special characters"
    );
}

/// E031-01: Delimited identifier that is a reserved word
#[test]
fn e031_01_delimited_reserved_word() {
    assert_feature_supported!(
        r#"CREATE TABLE "select" ("from" INT, "where" VARCHAR(10))"#,
        "E031-01",
        "Delimited reserved word identifier"
    );
}

/// E031-01: Case-sensitive delimited identifiers
#[test]
fn e031_01_case_sensitive() {
    // Delimited identifiers preserve case
    assert_feature_supported!(
        r#"CREATE TABLE t ("Column" INT, "column" INT, "COLUMN" INT)"#,
        "E031-01",
        "Case-sensitive delimited identifiers"
    );
}

/// E031-01: Delimited identifier in SELECT
#[test]
fn e031_01_delimited_in_select() {
    assert_feature_supported!(
        r#"SELECT "First Name" FROM person"#,
        "E031-01",
        "Delimited identifier in SELECT"
    );
}

/// E031-01: Delimited identifier in WHERE clause
#[test]
fn e031_01_delimited_in_where() {
    assert_feature_supported!(
        r#"SELECT * FROM person WHERE "Last Name" = 'Doe'"#,
        "E031-01",
        "Delimited identifier in WHERE"
    );
}

/// E031-01: Mixed delimited and regular identifiers
#[test]
fn e031_01_mixed_identifiers() {
    assert_feature_supported!(
        r#"CREATE TABLE t (regular_col INT, "Delimited Col" VARCHAR(50))"#,
        "E031-01",
        "Mixed delimited and regular identifiers"
    );
}

/// E031-01: Delimited identifier in JOIN
#[test]
fn e031_01_delimited_in_join() {
    assert_feature_supported!(
        r#"SELECT * FROM t1 JOIN t2 ON t1."User ID" = t2."User ID""#,
        "E031-01",
        "Delimited identifier in JOIN"
    );
}

/// E031-01: Delimited identifier with numbers at start
#[test]
fn e031_01_delimited_starts_with_number() {
    assert_feature_supported!(
        r#"CREATE TABLE t ("1st_column" INT, "2nd_column" INT)"#,
        "E031-01",
        "Delimited identifier starting with number"
    );
}

/// E031-01: Empty delimited identifier (edge case)
#[test]
fn e031_01_empty_delimited() {
    // GAP: SQL standard allows empty delimited identifiers, but this is rarely supported
    // This test may fail, which is acceptable
    assert_feature_supported!(
        r#"CREATE TABLE t ("" INT)"#,
        "E031-01",
        "Empty delimited identifier"
    );
}

/// E031-01: Delimited identifier with escaped quotes
#[test]
fn e031_01_delimited_escaped_quotes() {
    // Double quotes inside delimited identifier are escaped by doubling them
    assert_feature_supported!(
        r#"CREATE TABLE t ("column""name" INT)"#,
        "E031-01",
        "Delimited identifier with escaped quotes"
    );
}

// ============================================================================
// E031-02: Lower case identifiers
// ============================================================================

/// E031-02: Lower case table name
#[test]
fn e031_02_lowercase_table() {
    assert_feature_supported!(
        "CREATE TABLE mytable (x INT)",
        "E031-02",
        "Lower case table name"
    );
}

/// E031-02: Lower case column name
#[test]
fn e031_02_lowercase_column() {
    assert_feature_supported!(
        "CREATE TABLE t (mycolumn INT)",
        "E031-02",
        "Lower case column name"
    );
}

/// E031-02: All lower case identifiers
#[test]
fn e031_02_all_lowercase() {
    assert_feature_supported!(
        "CREATE TABLE users (id INT, firstname VARCHAR(50), lastname VARCHAR(50))",
        "E031-02",
        "All lower case identifiers"
    );
}

/// E031-02: Lower case in SELECT
#[test]
fn e031_02_lowercase_select() {
    assert_feature_supported!(
        "SELECT first_name, last_name FROM person",
        "E031-02",
        "Lower case in SELECT"
    );
}

/// E031-02: Lower case in WHERE clause
#[test]
fn e031_02_lowercase_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE age > 21",
        "E031-02",
        "Lower case in WHERE"
    );
}

/// E031-02: Case insensitivity of regular identifiers
#[test]
fn e031_02_case_insensitive() {
    // Regular identifiers should be case-insensitive
    // These should all refer to the same table/column
    assert_feature_supported!(
        "SELECT First_Name, LAST_NAME, AgE FROM Person WHERE STATE = 'CA'",
        "E031-02",
        "Case insensitive regular identifiers"
    );
}

/// E031-02: Mixed case identifiers
#[test]
fn e031_02_mixed_case() {
    assert_feature_supported!(
        "CREATE TABLE MyTable (MyColumn INT, anotherColumn VARCHAR(50))",
        "E031-02",
        "Mixed case identifiers"
    );
}

// ============================================================================
// E031-03: Trailing underscore
// ============================================================================

/// E031-03: Trailing underscore in table name
#[test]
fn e031_03_trailing_underscore_table() {
    assert_feature_supported!(
        "CREATE TABLE my_table_ (x INT)",
        "E031-03",
        "Trailing underscore in table name"
    );
}

/// E031-03: Trailing underscore in column name
#[test]
fn e031_03_trailing_underscore_column() {
    assert_feature_supported!(
        "CREATE TABLE t (column_ INT)",
        "E031-03",
        "Trailing underscore in column name"
    );
}

/// E031-03: Multiple trailing underscores
#[test]
fn e031_03_multiple_trailing_underscores() {
    assert_feature_supported!(
        "CREATE TABLE t (column__ INT, another___ VARCHAR(50))",
        "E031-03",
        "Multiple trailing underscores"
    );
}

/// E031-03: Trailing underscore in SELECT
#[test]
fn e031_03_trailing_underscore_select() {
    assert_feature_supported!(
        "SELECT column_ FROM t",
        "E031-03",
        "Trailing underscore in SELECT"
    );
}

/// E031-03: Identifier with only underscores after letter
#[test]
fn e031_03_letter_then_underscores() {
    assert_feature_supported!(
        "CREATE TABLE t (a_ INT, b__ INT, c___ VARCHAR(10))",
        "E031-03",
        "Letter followed by underscores"
    );
}

/// E031-03: Leading, middle, and trailing underscores
#[test]
fn e031_03_underscores_everywhere() {
    assert_feature_supported!(
        "CREATE TABLE _table_ (_col_name_ INT)",
        "E031-03",
        "Underscores in various positions"
    );
}

// ============================================================================
// Additional identifier tests (common patterns)
// ============================================================================

/// Common pattern: Snake case identifiers
#[test]
fn e031_snake_case_identifiers() {
    assert_feature_supported!(
        "CREATE TABLE user_profile (user_id INT, first_name VARCHAR(50), last_name VARCHAR(50), created_at TIMESTAMP)",
        "E031",
        "Snake case identifiers"
    );
}

/// Common pattern: CamelCase identifiers
#[test]
fn e031_camel_case_identifiers() {
    assert_feature_supported!(
        "CREATE TABLE UserProfile (UserId INT, FirstName VARCHAR(50), LastName VARCHAR(50))",
        "E031",
        "CamelCase identifiers"
    );
}

/// Common pattern: Identifier with numbers
#[test]
fn e031_identifiers_with_numbers() {
    assert_feature_supported!(
        "CREATE TABLE table1 (column1 INT, column2 VARCHAR(50), value123 DECIMAL(10, 2))",
        "E031",
        "Identifiers with numbers"
    );
}

/// Common pattern: Long identifiers
#[test]
fn e031_long_identifiers() {
    assert_feature_supported!(
        "CREATE TABLE very_long_table_name_that_describes_something (very_long_column_name_that_is_descriptive INT)",
        "E031",
        "Long identifiers"
    );
}

/// Common pattern: Single letter identifiers
#[test]
fn e031_single_letter_identifiers() {
    assert_feature_supported!(
        "CREATE TABLE t (a INT, b INT, c VARCHAR(10))",
        "E031",
        "Single letter identifiers"
    );
}

/// Edge case: Identifier starting with underscore
#[test]
fn e031_leading_underscore() {
    assert_feature_supported!(
        "CREATE TABLE _internal_table (_id INT, _data VARCHAR(100))",
        "E031",
        "Leading underscore"
    );
}

/// Edge case: Unicode identifiers in delimited form
#[test]
fn e031_unicode_delimited() {
    // GAP: Unicode support in identifiers varies by implementation
    // SQL standard allows Unicode in delimited identifiers
    assert_feature_supported!(
        r#"CREATE TABLE "用户表" ("名字" VARCHAR(50))"#,
        "E031-01",
        "Unicode delimited identifiers"
    );
}

// ============================================================================
// Summary Tests - Verify overall E031 support
// ============================================================================

#[test]
fn e031_summary_all_subfeatures() {
    // This test verifies that all E031 subfeatures work together

    // Create table with various identifier styles
    assert_plans!(r#"CREATE TABLE user_data_ (
        id INT,
        "User Name" VARCHAR(50),
        email_address VARCHAR(100),
        created_at_ TIMESTAMP,
        "Status" VARCHAR(20)
    )"#);

    // Query using mixed identifier styles
    assert_plans!(
        r#"SELECT
            id,
            "User Name",
            email_address,
            created_at_
         FROM user_data_
         WHERE "Status" = 'active'
           AND email_address LIKE '%@example.com'"#
    );
}

#[test]
fn e031_summary_complex_identifiers() {
    // Test complex scenarios with identifiers

    // Case insensitivity vs delimited identifiers
    assert_plans!(
        r#"SELECT
            person.first_name,
            person.LAST_NAME,
            person."id"
         FROM person
         WHERE Person.Age > 21
           AND PERSON.state = 'CA'"#
    );
}

#[test]
fn e031_summary_join_with_identifiers() {
    // Test identifiers in JOIN context
    assert_plans!(
        r#"SELECT
            t1.a,
            t1.b,
            t2.a AS "T2 A",
            t2.c AS t2_c_value
         FROM t AS t1
         JOIN t AS t2 ON t1.a = t2.a
         WHERE t1.b > 0"#
    );
}

#[test]
fn e031_summary_aliases() {
    // Test identifier rules with aliases
    assert_plans!(
        r#"SELECT
            first_name AS "First Name",
            last_name AS lastname_,
            age AS Age_Value
         FROM person AS p
         WHERE p.salary > 50000"#
    );
}
