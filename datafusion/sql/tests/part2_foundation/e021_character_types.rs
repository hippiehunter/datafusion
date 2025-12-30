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

//! SQL:2016 Feature E021 - Character data types
//!
//! ISO/IEC 9075-2:2016 Section 4.5
//!
//! This feature covers the character data types and operations required by Core SQL:
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E021-01 | CHARACTER data type | Partial |
//! | E021-02 | CHARACTER VARYING data type | Supported |
//! | E021-03 | Character literals | Supported |
//! | E021-04 | CHARACTER_LENGTH function | Supported |
//! | E021-05 | OCTET_LENGTH function | Supported |
//! | E021-06 | SUBSTRING function | Supported |
//! | E021-07 | Character concatenation (||) | Supported |
//! | E021-08 | UPPER and LOWER functions | Supported |
//! | E021-09 | TRIM function | Supported |
//! | E021-10 | Implicit casting between character types | Supported |
//! | E021-11 | POSITION function | Supported |
//! | E021-12 | Character comparison | Supported |
//!
//! All E021 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::{assert_plans, assert_feature_supported};

// ============================================================================
// E021-01: CHARACTER data type
// ============================================================================

/// E021-01: CHARACTER data type in column definition
#[test]
fn e021_01_character_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x CHARACTER(10))",
        "E021-01",
        "CHARACTER data type"
    );
}

/// E021-01: CHAR abbreviation for CHARACTER
#[test]
fn e021_01_char_abbreviation() {
    assert_feature_supported!(
        "CREATE TABLE t (x CHAR(10))",
        "E021-01",
        "CHAR abbreviation"
    );
}

/// E021-01: CHARACTER without length specification
#[test]
fn e021_01_character_no_length() {
    // GAP: SQL standard requires CHARACTER without length to default to CHARACTER(1)
    // DataFusion may handle this differently
    assert_feature_supported!(
        "CREATE TABLE t (x CHARACTER)",
        "E021-01",
        "CHARACTER without length"
    );
}

/// E021-01: CHARACTER with length 1 (minimum)
#[test]
fn e021_01_character_length_one() {
    assert_feature_supported!(
        "CREATE TABLE t (x CHARACTER(1))",
        "E021-01",
        "CHARACTER(1)"
    );
}

/// E021-01: CHARACTER with large length
#[test]
fn e021_01_character_large_length() {
    assert_feature_supported!(
        "CREATE TABLE t (x CHARACTER(1000))",
        "E021-01",
        "CHARACTER with large length"
    );
}

// ============================================================================
// E021-02: CHARACTER VARYING data type
// ============================================================================

/// E021-02: CHARACTER VARYING data type in column definition
#[test]
fn e021_02_character_varying_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x CHARACTER VARYING(100))",
        "E021-02",
        "CHARACTER VARYING data type"
    );
}

/// E021-02: VARCHAR abbreviation for CHARACTER VARYING
#[test]
fn e021_02_varchar_abbreviation() {
    assert_feature_supported!(
        "CREATE TABLE t (x VARCHAR(100))",
        "E021-02",
        "VARCHAR abbreviation"
    );
}

/// E021-02: VARCHAR without length (should be implementation-defined)
#[test]
fn e021_02_varchar_no_length() {
    assert_feature_supported!(
        "CREATE TABLE t (x VARCHAR)",
        "E021-02",
        "VARCHAR without length"
    );
}

/// E021-02: Multiple character columns in table
#[test]
fn e021_02_multiple_char_columns() {
    assert_feature_supported!(
        "CREATE TABLE t (name VARCHAR(50), code CHAR(10), description VARCHAR(255))",
        "E021-02",
        "Multiple character columns"
    );
}

// ============================================================================
// E021-03: Character literals
// ============================================================================

/// E021-03: Basic character literal in SELECT
#[test]
fn e021_03_character_literal() {
    assert_feature_supported!(
        "SELECT 'hello'",
        "E021-03",
        "Character literal"
    );
}

/// E021-03: Empty string literal
#[test]
fn e021_03_empty_string() {
    assert_feature_supported!(
        "SELECT ''",
        "E021-03",
        "Empty string literal"
    );
}

/// E021-03: Character literal with spaces
#[test]
fn e021_03_literal_with_spaces() {
    assert_feature_supported!(
        "SELECT 'hello world'",
        "E021-03",
        "Literal with spaces"
    );
}

/// E021-03: Character literal with special characters
#[test]
fn e021_03_literal_special_chars() {
    assert_feature_supported!(
        "SELECT 'test@example.com'",
        "E021-03",
        "Literal with special characters"
    );
}

/// E021-03: Escaped single quote in character literal
#[test]
fn e021_03_escaped_quote() {
    assert_feature_supported!(
        "SELECT 'don''t'",
        "E021-03",
        "Escaped single quote"
    );
}

/// E021-03: Character literal in WHERE clause
#[test]
fn e021_03_literal_in_where() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col = 'test'",
        "E021-03",
        "Literal in WHERE clause"
    );
}

// ============================================================================
// E021-04: CHARACTER_LENGTH function
// ============================================================================

/// E021-04: CHARACTER_LENGTH function
#[test]
fn e021_04_character_length_function() {
    assert_feature_supported!(
        "SELECT CHARACTER_LENGTH('hello')",
        "E021-04",
        "CHARACTER_LENGTH function"
    );
}

/// E021-04: CHAR_LENGTH abbreviation
#[test]
fn e021_04_char_length_abbreviation() {
    assert_feature_supported!(
        "SELECT CHAR_LENGTH('hello')",
        "E021-04",
        "CHAR_LENGTH abbreviation"
    );
}

/// E021-04: LENGTH function (common variant)
#[test]
fn e021_04_length_function() {
    assert_feature_supported!(
        "SELECT LENGTH('hello')",
        "E021-04",
        "LENGTH function"
    );
}

/// E021-04: CHARACTER_LENGTH on column
#[test]
fn e021_04_character_length_column() {
    assert_feature_supported!(
        "SELECT CHARACTER_LENGTH(char_col) FROM char_types",
        "E021-04",
        "CHARACTER_LENGTH on column"
    );
}

/// E021-04: CHARACTER_LENGTH in WHERE clause
#[test]
fn e021_04_character_length_where() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE CHARACTER_LENGTH(varchar_col) > 10",
        "E021-04",
        "CHARACTER_LENGTH in WHERE clause"
    );
}

// ============================================================================
// E021-05: OCTET_LENGTH function
// ============================================================================

/// E021-05: OCTET_LENGTH function
#[test]
fn e021_05_octet_length_function() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH('hello')",
        "E021-05",
        "OCTET_LENGTH function"
    );
}

/// E021-05: OCTET_LENGTH on column
#[test]
fn e021_05_octet_length_column() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH(varchar_col) FROM char_types",
        "E021-05",
        "OCTET_LENGTH on column"
    );
}

/// E021-05: OCTET_LENGTH vs CHARACTER_LENGTH
#[test]
fn e021_05_octet_vs_char_length() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH(char_col), CHARACTER_LENGTH(char_col) FROM char_types",
        "E021-05",
        "OCTET_LENGTH vs CHARACTER_LENGTH"
    );
}

// ============================================================================
// E021-06: SUBSTRING function
// ============================================================================

/// E021-06: SUBSTRING with FROM clause
#[test]
fn e021_06_substring_from() {
    assert_feature_supported!(
        "SELECT SUBSTRING('hello' FROM 2)",
        "E021-06",
        "SUBSTRING with FROM"
    );
}

/// E021-06: SUBSTRING with FROM and FOR clauses
#[test]
fn e021_06_substring_from_for() {
    assert_feature_supported!(
        "SELECT SUBSTRING('hello world' FROM 7 FOR 5)",
        "E021-06",
        "SUBSTRING with FROM and FOR"
    );
}

/// E021-06: SUBSTRING on column
#[test]
fn e021_06_substring_column() {
    assert_feature_supported!(
        "SELECT SUBSTRING(varchar_col FROM 1 FOR 10) FROM char_types",
        "E021-06",
        "SUBSTRING on column"
    );
}

/// E021-06: SUBSTR abbreviation (common variant)
#[test]
fn e021_06_substr_function() {
    assert_feature_supported!(
        "SELECT SUBSTR(varchar_col, 1, 10) FROM char_types",
        "E021-06",
        "SUBSTR function"
    );
}

// ============================================================================
// E021-07: Character concatenation
// ============================================================================

/// E021-07: String concatenation with || operator
#[test]
fn e021_07_concatenation_operator() {
    assert_feature_supported!(
        "SELECT 'hello' || ' ' || 'world'",
        "E021-07",
        "Concatenation operator"
    );
}

/// E021-07: Concatenation of columns
#[test]
fn e021_07_concatenation_columns() {
    assert_feature_supported!(
        "SELECT first_name || ' ' || last_name FROM person",
        "E021-07",
        "Concatenation of columns"
    );
}

/// E021-07: Concatenation in WHERE clause
#[test]
fn e021_07_concatenation_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name || last_name = 'JohnDoe'",
        "E021-07",
        "Concatenation in WHERE clause"
    );
}

/// E021-07: Complex concatenation expression
#[test]
fn e021_07_complex_concatenation() {
    assert_feature_supported!(
        "SELECT char_col || varchar_col || text_col FROM char_types",
        "E021-07",
        "Complex concatenation"
    );
}

// ============================================================================
// E021-08: UPPER and LOWER functions
// ============================================================================

/// E021-08: UPPER function
#[test]
fn e021_08_upper_function() {
    assert_feature_supported!(
        "SELECT UPPER('hello')",
        "E021-08",
        "UPPER function"
    );
}

/// E021-08: LOWER function
#[test]
fn e021_08_lower_function() {
    assert_feature_supported!(
        "SELECT LOWER('HELLO')",
        "E021-08",
        "LOWER function"
    );
}

/// E021-08: UPPER on column
#[test]
fn e021_08_upper_column() {
    assert_feature_supported!(
        "SELECT UPPER(first_name) FROM person",
        "E021-08",
        "UPPER on column"
    );
}

/// E021-08: LOWER on column
#[test]
fn e021_08_lower_column() {
    assert_feature_supported!(
        "SELECT LOWER(last_name) FROM person",
        "E021-08",
        "LOWER on column"
    );
}

/// E021-08: Case conversion in WHERE clause
#[test]
fn e021_08_case_conversion_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE UPPER(first_name) = 'JOHN'",
        "E021-08",
        "Case conversion in WHERE"
    );
}

/// E021-08: Combined UPPER and LOWER
#[test]
fn e021_08_combined_case_conversion() {
    assert_feature_supported!(
        "SELECT UPPER(first_name), LOWER(last_name) FROM person",
        "E021-08",
        "Combined case conversion"
    );
}

// ============================================================================
// E021-09: TRIM function
// ============================================================================

/// E021-09: TRIM both sides (default)
#[test]
fn e021_09_trim_both() {
    assert_feature_supported!(
        "SELECT TRIM('  hello  ')",
        "E021-09",
        "TRIM both sides"
    );
}

/// E021-09: TRIM LEADING
#[test]
fn e021_09_trim_leading() {
    assert_feature_supported!(
        "SELECT TRIM(LEADING ' ' FROM '  hello  ')",
        "E021-09",
        "TRIM LEADING"
    );
}

/// E021-09: TRIM TRAILING
#[test]
fn e021_09_trim_trailing() {
    assert_feature_supported!(
        "SELECT TRIM(TRAILING ' ' FROM '  hello  ')",
        "E021-09",
        "TRIM TRAILING"
    );
}

/// E021-09: TRIM BOTH explicitly
#[test]
fn e021_09_trim_both_explicit() {
    assert_feature_supported!(
        "SELECT TRIM(BOTH ' ' FROM '  hello  ')",
        "E021-09",
        "TRIM BOTH explicit"
    );
}

/// E021-09: TRIM on column
#[test]
fn e021_09_trim_column() {
    assert_feature_supported!(
        "SELECT TRIM(char_col) FROM char_types",
        "E021-09",
        "TRIM on column"
    );
}

/// E021-09: TRIM with custom character
#[test]
fn e021_09_trim_custom_char() {
    assert_feature_supported!(
        "SELECT TRIM('x' FROM 'xxxhelloxxx')",
        "E021-09",
        "TRIM with custom character"
    );
}

/// E021-09: LTRIM function (common variant)
#[test]
fn e021_09_ltrim_function() {
    assert_feature_supported!(
        "SELECT LTRIM('  hello')",
        "E021-09",
        "LTRIM function"
    );
}

/// E021-09: RTRIM function (common variant)
#[test]
fn e021_09_rtrim_function() {
    assert_feature_supported!(
        "SELECT RTRIM('hello  ')",
        "E021-09",
        "RTRIM function"
    );
}

// ============================================================================
// E021-10: Implicit casting between character types
// ============================================================================

/// E021-10: CHARACTER and VARCHAR comparison
#[test]
fn e021_10_char_varchar_comparison() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col = varchar_col",
        "E021-10",
        "CHAR and VARCHAR comparison"
    );
}

/// E021-10: String literal compared to VARCHAR column
#[test]
fn e021_10_literal_varchar_comparison() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE varchar_col = 'test'",
        "E021-10",
        "Literal to VARCHAR comparison"
    );
}

/// E021-10: Mixed character type concatenation
#[test]
fn e021_10_mixed_concatenation() {
    assert_feature_supported!(
        "SELECT char_col || varchar_col FROM char_types",
        "E021-10",
        "Mixed character type concatenation"
    );
}

/// E021-10: Character type in arithmetic-like context
#[test]
fn e021_10_char_in_function() {
    assert_feature_supported!(
        "SELECT UPPER(char_col), LOWER(varchar_col) FROM char_types",
        "E021-10",
        "Character types in functions"
    );
}

// ============================================================================
// E021-11: POSITION function
// ============================================================================

/// E021-11: POSITION function basic
#[test]
fn e021_11_position_function() {
    assert_feature_supported!(
        "SELECT POSITION('world' IN 'hello world')",
        "E021-11",
        "POSITION function"
    );
}

/// E021-11: POSITION with column
#[test]
fn e021_11_position_column() {
    assert_feature_supported!(
        "SELECT POSITION('test' IN varchar_col) FROM char_types",
        "E021-11",
        "POSITION with column"
    );
}

/// E021-11: POSITION in WHERE clause
#[test]
fn e021_11_position_where() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE POSITION('test' IN varchar_col) > 0",
        "E021-11",
        "POSITION in WHERE clause"
    );
}

/// E021-11: STRPOS function (PostgreSQL variant)
#[test]
fn e021_11_strpos_function() {
    assert_feature_supported!(
        "SELECT STRPOS('hello world', 'world')",
        "E021-11",
        "STRPOS function"
    );
}

// ============================================================================
// E021-12: Character comparison
// ============================================================================

/// E021-12: Equality comparison
#[test]
fn e021_12_equality() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col = 'test'",
        "E021-12",
        "Equality comparison"
    );
}

/// E021-12: Inequality comparison
#[test]
fn e021_12_inequality() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col <> 'test'",
        "E021-12",
        "Inequality comparison"
    );
}

/// E021-12: Less than comparison
#[test]
fn e021_12_less_than() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name < 'M'",
        "E021-12",
        "Less than comparison"
    );
}

/// E021-12: Greater than comparison
#[test]
fn e021_12_greater_than() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE last_name > 'M'",
        "E021-12",
        "Greater than comparison"
    );
}

/// E021-12: Less than or equal comparison
#[test]
fn e021_12_less_than_or_equal() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name <= 'Z'",
        "E021-12",
        "Less than or equal comparison"
    );
}

/// E021-12: Greater than or equal comparison
#[test]
fn e021_12_greater_than_or_equal() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE last_name >= 'A'",
        "E021-12",
        "Greater than or equal comparison"
    );
}

/// E021-12: BETWEEN for character comparison
#[test]
fn e021_12_between() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name BETWEEN 'A' AND 'M'",
        "E021-12",
        "BETWEEN comparison"
    );
}

/// E021-12: Character column comparison
#[test]
fn e021_12_column_comparison() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col > varchar_col",
        "E021-12",
        "Column comparison"
    );
}

// ============================================================================
// Summary Tests - Verify overall E021 support
// ============================================================================

#[test]
fn e021_summary_all_subfeatures() {
    // This test verifies that all E021 subfeatures work together
    // in a realistic scenario

    // Create table with character types
    assert_plans!("CREATE TABLE users (
        id INT,
        username VARCHAR(50),
        email VARCHAR(100),
        first_name CHAR(30),
        last_name CHAR(30)
    )");

    // Query using various character operations
    assert_plans!(
        "SELECT
            UPPER(username) AS upper_user,
            LOWER(email) AS lower_email,
            first_name || ' ' || last_name AS full_name,
            CHARACTER_LENGTH(username) AS name_length,
            SUBSTRING(email FROM 1 FOR 20) AS email_prefix,
            TRIM(first_name) AS trimmed_name
         FROM users
         WHERE POSITION('@' IN email) > 0
           AND first_name >= 'A'
           AND LOWER(username) <> 'admin'"
    );
}

#[test]
fn e021_summary_complex_string_operations() {
    // Test complex combinations of string operations
    assert_plans!(
        "SELECT
            first_name,
            last_name,
            TRIM(UPPER(first_name)) || ' ' || TRIM(LOWER(last_name)) AS formatted_name,
            CHARACTER_LENGTH(first_name || last_name) AS total_length,
            SUBSTRING(first_name FROM 1 FOR 1) || '.' AS initial
         FROM person
         WHERE POSITION('a' IN LOWER(first_name)) > 0
         ORDER BY UPPER(last_name)"
    );
}
