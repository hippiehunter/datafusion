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

//! SQL:2016 String Functions Conformance Tests
//!
//! This module contains comprehensive tests for string functions defined
//! in SQL:2016 Part 2 - SQL/Foundation.
//!
//! Many of these functions are part of Core SQL features (E021) and optional
//! features (T-series). This test suite identifies gaps in DataFusion's
//! implementation of SQL:2016 string functions.
//!
//! # Coverage
//!
//! This module tests:
//! - CHARACTER_LENGTH / CHAR_LENGTH / LENGTH
//! - OCTET_LENGTH
//! - BIT_LENGTH
//! - POSITION(substring IN string)
//! - SUBSTRING(string FROM start FOR length)
//! - UPPER(string) / LOWER(string)
//! - TRIM functions (LEADING/TRAILING/BOTH)
//! - String concatenation (||)
//! - CONCAT function
//! - OVERLAY function
//! - Extended string functions (LEFT, RIGHT, LPAD, RPAD, etc.)
//! - String manipulation (REPLACE, REVERSE, REPEAT, etc.)
//! - Character conversion (ASCII, CHR, INITCAP, etc.)
//! - Advanced functions (TRANSLATE, SPLIT_PART, STRING_AGG)
//!
//! Each test uses `assert_feature_supported!` macro which fails if the
//! feature is not implemented - thus identifying conformance gaps.

use crate::assert_feature_supported;

// ============================================================================
// CHARACTER_LENGTH / CHAR_LENGTH / LENGTH
// Related to: E021-04
// ============================================================================

#[test]
fn character_length_basic() {
    assert_feature_supported!(
        "SELECT CHARACTER_LENGTH('hello')",
        "E021-04",
        "CHARACTER_LENGTH basic usage"
    );
}

#[test]
fn character_length_null() {
    assert_feature_supported!(
        "SELECT CHARACTER_LENGTH(NULL)",
        "E021-04",
        "CHARACTER_LENGTH with NULL"
    );
}

#[test]
fn character_length_column() {
    assert_feature_supported!(
        "SELECT CHARACTER_LENGTH(first_name) FROM person",
        "E021-04",
        "CHARACTER_LENGTH on column"
    );
}

#[test]
fn character_length_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE CHARACTER_LENGTH(first_name) > 5",
        "E021-04",
        "CHARACTER_LENGTH in WHERE clause"
    );
}

#[test]
fn character_length_expression() {
    assert_feature_supported!(
        "SELECT CHARACTER_LENGTH(first_name || ' ' || last_name) FROM person",
        "E021-04",
        "CHARACTER_LENGTH on expression"
    );
}

#[test]
fn char_length_basic() {
    assert_feature_supported!(
        "SELECT CHAR_LENGTH('test')",
        "E021-04",
        "CHAR_LENGTH basic usage"
    );
}

#[test]
fn char_length_empty_string() {
    assert_feature_supported!(
        "SELECT CHAR_LENGTH('')",
        "E021-04",
        "CHAR_LENGTH empty string"
    );
}

#[test]
fn length_function() {
    assert_feature_supported!(
        "SELECT LENGTH('hello world')",
        "E021-04",
        "LENGTH function"
    );
}

#[test]
fn length_multibyte_chars() {
    assert_feature_supported!(
        "SELECT LENGTH('héllo')",
        "E021-04",
        "LENGTH with multibyte characters"
    );
}

// ============================================================================
// OCTET_LENGTH
// Related to: E021-05
// ============================================================================

#[test]
fn octet_length_basic() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH('hello')",
        "E021-05",
        "OCTET_LENGTH basic usage"
    );
}

#[test]
fn octet_length_null() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH(NULL)",
        "E021-05",
        "OCTET_LENGTH with NULL"
    );
}

#[test]
fn octet_length_column() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH(varchar_col) FROM char_types",
        "E021-05",
        "OCTET_LENGTH on column"
    );
}

#[test]
fn octet_length_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE OCTET_LENGTH(varchar_col) < 100",
        "E021-05",
        "OCTET_LENGTH in WHERE clause"
    );
}

#[test]
fn octet_length_multibyte() {
    assert_feature_supported!(
        "SELECT OCTET_LENGTH('café')",
        "E021-05",
        "OCTET_LENGTH with multibyte chars"
    );
}

// ============================================================================
// BIT_LENGTH
// Related to: SQL:2016 string functions
// ============================================================================

#[test]
fn bit_length_basic() {
    assert_feature_supported!(
        "SELECT BIT_LENGTH('hello')",
        "STRING_FUNC",
        "BIT_LENGTH basic usage"
    );
}

#[test]
fn bit_length_null() {
    assert_feature_supported!(
        "SELECT BIT_LENGTH(NULL)",
        "STRING_FUNC",
        "BIT_LENGTH with NULL"
    );
}

#[test]
fn bit_length_column() {
    assert_feature_supported!(
        "SELECT BIT_LENGTH(char_col) FROM char_types",
        "STRING_FUNC",
        "BIT_LENGTH on column"
    );
}

#[test]
fn bit_length_empty_string() {
    assert_feature_supported!(
        "SELECT BIT_LENGTH('')",
        "STRING_FUNC",
        "BIT_LENGTH empty string"
    );
}

// ============================================================================
// POSITION(substring IN string)
// Related to: E021-11
// ============================================================================

#[test]
fn position_basic() {
    assert_feature_supported!(
        "SELECT POSITION('world' IN 'hello world')",
        "E021-11",
        "POSITION basic usage"
    );
}

#[test]
fn position_not_found() {
    assert_feature_supported!(
        "SELECT POSITION('xyz' IN 'hello world')",
        "E021-11",
        "POSITION substring not found"
    );
}

#[test]
fn position_null_string() {
    assert_feature_supported!(
        "SELECT POSITION('test' IN NULL)",
        "E021-11",
        "POSITION with NULL string"
    );
}

#[test]
fn position_null_substring() {
    assert_feature_supported!(
        "SELECT POSITION(NULL IN 'hello')",
        "E021-11",
        "POSITION with NULL substring"
    );
}

#[test]
fn position_column() {
    assert_feature_supported!(
        "SELECT POSITION('@' IN varchar_col) FROM char_types",
        "E021-11",
        "POSITION on column"
    );
}

#[test]
fn position_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE POSITION('test' IN varchar_col) > 0",
        "E021-11",
        "POSITION in WHERE clause"
    );
}

#[test]
fn position_case_sensitive() {
    assert_feature_supported!(
        "SELECT POSITION('WORLD' IN 'hello world')",
        "E021-11",
        "POSITION case sensitivity"
    );
}

// ============================================================================
// SUBSTRING(string FROM start FOR length)
// Related to: E021-06
// ============================================================================

#[test]
fn substring_from_basic() {
    assert_feature_supported!(
        "SELECT SUBSTRING('hello world' FROM 7)",
        "E021-06",
        "SUBSTRING FROM basic"
    );
}

#[test]
fn substring_from_for() {
    assert_feature_supported!(
        "SELECT SUBSTRING('hello world' FROM 1 FOR 5)",
        "E021-06",
        "SUBSTRING FROM FOR"
    );
}

#[test]
fn substring_from_column() {
    assert_feature_supported!(
        "SELECT SUBSTRING(first_name FROM 1 FOR 3) FROM person",
        "E021-06",
        "SUBSTRING on column"
    );
}

#[test]
fn substring_null_string() {
    assert_feature_supported!(
        "SELECT SUBSTRING(NULL FROM 1 FOR 5)",
        "E021-06",
        "SUBSTRING with NULL string"
    );
}

#[test]
fn substring_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE SUBSTRING(state FROM 1 FOR 1) = 'C'",
        "E021-06",
        "SUBSTRING in WHERE clause"
    );
}

#[test]
fn substring_zero_length() {
    assert_feature_supported!(
        "SELECT SUBSTRING('hello' FROM 1 FOR 0)",
        "E021-06",
        "SUBSTRING with zero length"
    );
}

#[test]
fn substring_expression() {
    assert_feature_supported!(
        "SELECT SUBSTRING(first_name || ' ' || last_name FROM 1 FOR 10) FROM person",
        "E021-06",
        "SUBSTRING on expression"
    );
}

// ============================================================================
// UPPER and LOWER
// Related to: E021-08
// ============================================================================

#[test]
fn upper_basic() {
    assert_feature_supported!(
        "SELECT UPPER('hello world')",
        "E021-08",
        "UPPER basic usage"
    );
}

#[test]
fn upper_null() {
    assert_feature_supported!(
        "SELECT UPPER(NULL)",
        "E021-08",
        "UPPER with NULL"
    );
}

#[test]
fn upper_column() {
    assert_feature_supported!(
        "SELECT UPPER(first_name) FROM person",
        "E021-08",
        "UPPER on column"
    );
}

#[test]
fn upper_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE UPPER(state) = 'CA'",
        "E021-08",
        "UPPER in WHERE clause"
    );
}

#[test]
fn lower_basic() {
    assert_feature_supported!(
        "SELECT LOWER('HELLO WORLD')",
        "E021-08",
        "LOWER basic usage"
    );
}

#[test]
fn lower_null() {
    assert_feature_supported!(
        "SELECT LOWER(NULL)",
        "E021-08",
        "LOWER with NULL"
    );
}

#[test]
fn lower_column() {
    assert_feature_supported!(
        "SELECT LOWER(last_name) FROM person",
        "E021-08",
        "LOWER on column"
    );
}

#[test]
fn lower_expression() {
    assert_feature_supported!(
        "SELECT LOWER(first_name || ' ' || last_name) FROM person",
        "E021-08",
        "LOWER on expression"
    );
}

// ============================================================================
// TRIM functions
// Related to: E021-09
// ============================================================================

#[test]
fn trim_both_default() {
    assert_feature_supported!(
        "SELECT TRIM('  hello  ')",
        "E021-09",
        "TRIM default (both sides)"
    );
}

#[test]
fn trim_leading() {
    assert_feature_supported!(
        "SELECT TRIM(LEADING ' ' FROM '  hello  ')",
        "E021-09",
        "TRIM LEADING"
    );
}

#[test]
fn trim_trailing() {
    assert_feature_supported!(
        "SELECT TRIM(TRAILING ' ' FROM '  hello  ')",
        "E021-09",
        "TRIM TRAILING"
    );
}

#[test]
fn trim_both_explicit() {
    assert_feature_supported!(
        "SELECT TRIM(BOTH ' ' FROM '  hello  ')",
        "E021-09",
        "TRIM BOTH explicit"
    );
}

#[test]
fn trim_custom_char() {
    assert_feature_supported!(
        "SELECT TRIM('x' FROM 'xxxhelloxxx')",
        "E021-09",
        "TRIM custom character"
    );
}

#[test]
fn trim_null() {
    assert_feature_supported!(
        "SELECT TRIM(NULL)",
        "E021-09",
        "TRIM with NULL"
    );
}

#[test]
fn trim_column() {
    assert_feature_supported!(
        "SELECT TRIM(char_col) FROM char_types",
        "E021-09",
        "TRIM on column"
    );
}

#[test]
fn trim_leading_custom() {
    assert_feature_supported!(
        "SELECT TRIM(LEADING '0' FROM '000123')",
        "E021-09",
        "TRIM LEADING custom char"
    );
}

// ============================================================================
// String Concatenation Operator ||
// Related to: E021-07
// ============================================================================

#[test]
fn concat_operator_basic() {
    assert_feature_supported!(
        "SELECT 'hello' || ' ' || 'world'",
        "E021-07",
        "Concatenation operator basic"
    );
}

#[test]
fn concat_operator_null() {
    assert_feature_supported!(
        "SELECT 'hello' || NULL",
        "E021-07",
        "Concatenation with NULL"
    );
}

#[test]
fn concat_operator_columns() {
    assert_feature_supported!(
        "SELECT first_name || ' ' || last_name FROM person",
        "E021-07",
        "Concatenation of columns"
    );
}

#[test]
fn concat_operator_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name || last_name = 'JohnDoe'",
        "E021-07",
        "Concatenation in WHERE clause"
    );
}

#[test]
fn concat_operator_multiple() {
    assert_feature_supported!(
        "SELECT char_col || varchar_col || text_col FROM char_types",
        "E021-07",
        "Multiple concatenations"
    );
}

// ============================================================================
// CONCAT function
// Common extension, not strictly SQL:2016 core but widely supported
// ============================================================================

#[test]
fn concat_function_basic() {
    assert_feature_supported!(
        "SELECT CONCAT('hello', ' ', 'world')",
        "STRING_FUNC",
        "CONCAT function basic"
    );
}

#[test]
fn concat_function_two_args() {
    assert_feature_supported!(
        "SELECT CONCAT('hello', 'world')",
        "STRING_FUNC",
        "CONCAT with two args"
    );
}

#[test]
fn concat_function_many_args() {
    assert_feature_supported!(
        "SELECT CONCAT('a', 'b', 'c', 'd', 'e')",
        "STRING_FUNC",
        "CONCAT with many args"
    );
}

#[test]
fn concat_function_null() {
    assert_feature_supported!(
        "SELECT CONCAT('hello', NULL, 'world')",
        "STRING_FUNC",
        "CONCAT with NULL"
    );
}

#[test]
fn concat_function_columns() {
    assert_feature_supported!(
        "SELECT CONCAT(first_name, ' ', last_name) FROM person",
        "STRING_FUNC",
        "CONCAT on columns"
    );
}

#[test]
fn concat_function_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE CONCAT(first_name, last_name) = 'JohnDoe'",
        "STRING_FUNC",
        "CONCAT in WHERE clause"
    );
}

// ============================================================================
// OVERLAY function
// SQL:2016 string overlay function
// ============================================================================

#[test]
fn overlay_basic() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING 'a' FROM 2 FOR 2)",
        "STRING_FUNC",
        "OVERLAY basic usage"
    );
}

#[test]
fn overlay_without_for() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING 'world' FROM 3)",
        "STRING_FUNC",
        "OVERLAY without FOR clause"
    );
}

#[test]
fn overlay_null_string() {
    assert_feature_supported!(
        "SELECT OVERLAY(NULL PLACING 'test' FROM 1 FOR 2)",
        "STRING_FUNC",
        "OVERLAY with NULL string"
    );
}

#[test]
fn overlay_column() {
    assert_feature_supported!(
        "SELECT OVERLAY(varchar_col PLACING 'XX' FROM 1 FOR 2) FROM char_types",
        "STRING_FUNC",
        "OVERLAY on column"
    );
}

// ============================================================================
// LEFT function
// Common string function extension
// ============================================================================

#[test]
fn left_basic() {
    assert_feature_supported!(
        "SELECT LEFT('hello world', 5)",
        "STRING_FUNC",
        "LEFT basic usage"
    );
}

#[test]
fn left_null_string() {
    assert_feature_supported!(
        "SELECT LEFT(NULL, 5)",
        "STRING_FUNC",
        "LEFT with NULL string"
    );
}

#[test]
fn left_column() {
    assert_feature_supported!(
        "SELECT LEFT(first_name, 3) FROM person",
        "STRING_FUNC",
        "LEFT on column"
    );
}

#[test]
fn left_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE LEFT(state, 1) = 'C'",
        "STRING_FUNC",
        "LEFT in WHERE clause"
    );
}

#[test]
fn left_zero_length() {
    assert_feature_supported!(
        "SELECT LEFT('hello', 0)",
        "STRING_FUNC",
        "LEFT with zero length"
    );
}

// ============================================================================
// RIGHT function
// Common string function extension
// ============================================================================

#[test]
fn right_basic() {
    assert_feature_supported!(
        "SELECT RIGHT('hello world', 5)",
        "STRING_FUNC",
        "RIGHT basic usage"
    );
}

#[test]
fn right_null_string() {
    assert_feature_supported!(
        "SELECT RIGHT(NULL, 5)",
        "STRING_FUNC",
        "RIGHT with NULL string"
    );
}

#[test]
fn right_column() {
    assert_feature_supported!(
        "SELECT RIGHT(last_name, 3) FROM person",
        "STRING_FUNC",
        "RIGHT on column"
    );
}

#[test]
fn right_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE RIGHT(state, 1) = 'A'",
        "STRING_FUNC",
        "RIGHT in WHERE clause"
    );
}

#[test]
fn right_expression() {
    assert_feature_supported!(
        "SELECT RIGHT(first_name || last_name, 5) FROM person",
        "STRING_FUNC",
        "RIGHT on expression"
    );
}

// ============================================================================
// LPAD function
// Left pad string to specified length
// ============================================================================

#[test]
fn lpad_basic() {
    assert_feature_supported!(
        "SELECT LPAD('hello', 10, ' ')",
        "STRING_FUNC",
        "LPAD basic usage"
    );
}

#[test]
fn lpad_two_args() {
    assert_feature_supported!(
        "SELECT LPAD('hello', 10)",
        "STRING_FUNC",
        "LPAD with default pad"
    );
}

#[test]
fn lpad_custom_char() {
    assert_feature_supported!(
        "SELECT LPAD('123', 6, '0')",
        "STRING_FUNC",
        "LPAD with custom pad char"
    );
}

#[test]
fn lpad_null_string() {
    assert_feature_supported!(
        "SELECT LPAD(NULL, 10, ' ')",
        "STRING_FUNC",
        "LPAD with NULL string"
    );
}

#[test]
fn lpad_column() {
    assert_feature_supported!(
        "SELECT LPAD(char_col, 20, '-') FROM char_types",
        "STRING_FUNC",
        "LPAD on column"
    );
}

#[test]
fn lpad_truncate() {
    assert_feature_supported!(
        "SELECT LPAD('hello world', 5, ' ')",
        "STRING_FUNC",
        "LPAD with truncation"
    );
}

// ============================================================================
// RPAD function
// Right pad string to specified length
// ============================================================================

#[test]
fn rpad_basic() {
    assert_feature_supported!(
        "SELECT RPAD('hello', 10, ' ')",
        "STRING_FUNC",
        "RPAD basic usage"
    );
}

#[test]
fn rpad_two_args() {
    assert_feature_supported!(
        "SELECT RPAD('hello', 10)",
        "STRING_FUNC",
        "RPAD with default pad"
    );
}

#[test]
fn rpad_custom_char() {
    assert_feature_supported!(
        "SELECT RPAD('test', 10, 'x')",
        "STRING_FUNC",
        "RPAD with custom pad char"
    );
}

#[test]
fn rpad_null_string() {
    assert_feature_supported!(
        "SELECT RPAD(NULL, 10, ' ')",
        "STRING_FUNC",
        "RPAD with NULL string"
    );
}

#[test]
fn rpad_column() {
    assert_feature_supported!(
        "SELECT RPAD(varchar_col, 30, '.') FROM char_types",
        "STRING_FUNC",
        "RPAD on column"
    );
}

#[test]
fn rpad_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE LENGTH(RPAD(char_col, 20)) = 20",
        "STRING_FUNC",
        "RPAD in WHERE clause"
    );
}

// ============================================================================
// LTRIM and RTRIM functions
// Alternative forms of TRIM
// ============================================================================

#[test]
fn ltrim_basic() {
    assert_feature_supported!(
        "SELECT LTRIM('  hello')",
        "E021-09",
        "LTRIM basic usage"
    );
}

#[test]
fn ltrim_custom_char() {
    assert_feature_supported!(
        "SELECT LTRIM('000123', '0')",
        "E021-09",
        "LTRIM with custom char"
    );
}

#[test]
fn ltrim_null() {
    assert_feature_supported!(
        "SELECT LTRIM(NULL)",
        "E021-09",
        "LTRIM with NULL"
    );
}

#[test]
fn ltrim_column() {
    assert_feature_supported!(
        "SELECT LTRIM(char_col) FROM char_types",
        "E021-09",
        "LTRIM on column"
    );
}

#[test]
fn rtrim_basic() {
    assert_feature_supported!(
        "SELECT RTRIM('hello  ')",
        "E021-09",
        "RTRIM basic usage"
    );
}

#[test]
fn rtrim_custom_char() {
    assert_feature_supported!(
        "SELECT RTRIM('test...', '.')",
        "E021-09",
        "RTRIM with custom char"
    );
}

#[test]
fn rtrim_null() {
    assert_feature_supported!(
        "SELECT RTRIM(NULL)",
        "E021-09",
        "RTRIM with NULL"
    );
}

#[test]
fn rtrim_column() {
    assert_feature_supported!(
        "SELECT RTRIM(varchar_col) FROM char_types",
        "E021-09",
        "RTRIM on column"
    );
}

// ============================================================================
// REPLACE function
// Replace occurrences of substring
// ============================================================================

#[test]
fn replace_basic() {
    assert_feature_supported!(
        "SELECT REPLACE('hello world', 'world', 'there')",
        "STRING_FUNC",
        "REPLACE basic usage"
    );
}

#[test]
fn replace_empty_string() {
    assert_feature_supported!(
        "SELECT REPLACE('hello', 'l', '')",
        "STRING_FUNC",
        "REPLACE with empty replacement"
    );
}

#[test]
fn replace_null_string() {
    assert_feature_supported!(
        "SELECT REPLACE(NULL, 'test', 'new')",
        "STRING_FUNC",
        "REPLACE with NULL string"
    );
}

#[test]
fn replace_column() {
    assert_feature_supported!(
        "SELECT REPLACE(first_name, 'a', 'A') FROM person",
        "STRING_FUNC",
        "REPLACE on column"
    );
}

#[test]
fn replace_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE REPLACE(state, 'CA', 'California') = 'California'",
        "STRING_FUNC",
        "REPLACE in WHERE clause"
    );
}

#[test]
fn replace_multiple_occurrences() {
    assert_feature_supported!(
        "SELECT REPLACE('Mississippi', 's', 'S')",
        "STRING_FUNC",
        "REPLACE multiple occurrences"
    );
}

// ============================================================================
// REVERSE function
// Reverse string characters
// ============================================================================

#[test]
fn reverse_basic() {
    assert_feature_supported!(
        "SELECT REVERSE('hello')",
        "STRING_FUNC",
        "REVERSE basic usage"
    );
}

#[test]
fn reverse_null() {
    assert_feature_supported!(
        "SELECT REVERSE(NULL)",
        "STRING_FUNC",
        "REVERSE with NULL"
    );
}

#[test]
fn reverse_column() {
    assert_feature_supported!(
        "SELECT REVERSE(first_name) FROM person",
        "STRING_FUNC",
        "REVERSE on column"
    );
}

#[test]
fn reverse_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE REVERSE(first_name) = 'nhoJ'",
        "STRING_FUNC",
        "REVERSE in WHERE clause"
    );
}

#[test]
fn reverse_empty_string() {
    assert_feature_supported!(
        "SELECT REVERSE('')",
        "STRING_FUNC",
        "REVERSE empty string"
    );
}

// ============================================================================
// REPEAT function
// Repeat string N times
// ============================================================================

#[test]
fn repeat_basic() {
    assert_feature_supported!(
        "SELECT REPEAT('abc', 3)",
        "STRING_FUNC",
        "REPEAT basic usage"
    );
}

#[test]
fn repeat_zero() {
    assert_feature_supported!(
        "SELECT REPEAT('test', 0)",
        "STRING_FUNC",
        "REPEAT zero times"
    );
}

#[test]
fn repeat_one() {
    assert_feature_supported!(
        "SELECT REPEAT('hello', 1)",
        "STRING_FUNC",
        "REPEAT once"
    );
}

#[test]
fn repeat_null_string() {
    assert_feature_supported!(
        "SELECT REPEAT(NULL, 5)",
        "STRING_FUNC",
        "REPEAT with NULL string"
    );
}

#[test]
fn repeat_column() {
    assert_feature_supported!(
        "SELECT REPEAT(char_col, 2) FROM char_types",
        "STRING_FUNC",
        "REPEAT on column"
    );
}

#[test]
fn repeat_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE LENGTH(REPEAT(c, 3)) > 10",
        "STRING_FUNC",
        "REPEAT in WHERE clause"
    );
}

// ============================================================================
// SPACE function
// Generate string of N spaces
// ============================================================================

#[test]
fn space_basic() {
    assert_feature_supported!(
        "SELECT SPACE(5)",
        "STRING_FUNC",
        "SPACE basic usage"
    );
}

#[test]
fn space_zero() {
    assert_feature_supported!(
        "SELECT SPACE(0)",
        "STRING_FUNC",
        "SPACE zero length"
    );
}

#[test]
fn space_concat() {
    assert_feature_supported!(
        "SELECT 'hello' || SPACE(3) || 'world'",
        "STRING_FUNC",
        "SPACE in concatenation"
    );
}

#[test]
fn space_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c = SPACE(10)",
        "STRING_FUNC",
        "SPACE in WHERE clause"
    );
}

// ============================================================================
// ASCII function
// Get ASCII code of first character
// ============================================================================

#[test]
fn ascii_basic() {
    assert_feature_supported!(
        "SELECT ASCII('A')",
        "STRING_FUNC",
        "ASCII basic usage"
    );
}

#[test]
fn ascii_lowercase() {
    assert_feature_supported!(
        "SELECT ASCII('a')",
        "STRING_FUNC",
        "ASCII lowercase char"
    );
}

#[test]
fn ascii_string() {
    assert_feature_supported!(
        "SELECT ASCII('hello')",
        "STRING_FUNC",
        "ASCII on string"
    );
}

#[test]
fn ascii_null() {
    assert_feature_supported!(
        "SELECT ASCII(NULL)",
        "STRING_FUNC",
        "ASCII with NULL"
    );
}

#[test]
fn ascii_column() {
    assert_feature_supported!(
        "SELECT ASCII(first_name) FROM person",
        "STRING_FUNC",
        "ASCII on column"
    );
}

#[test]
fn ascii_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE ASCII(first_name) < 80",
        "STRING_FUNC",
        "ASCII in WHERE clause"
    );
}

// ============================================================================
// CHR / CHAR function
// Convert ASCII code to character
// ============================================================================

#[test]
fn chr_basic() {
    assert_feature_supported!(
        "SELECT CHR(65)",
        "STRING_FUNC",
        "CHR basic usage"
    );
}

#[test]
fn chr_lowercase() {
    assert_feature_supported!(
        "SELECT CHR(97)",
        "STRING_FUNC",
        "CHR lowercase char"
    );
}

#[test]
fn chr_null() {
    assert_feature_supported!(
        "SELECT CHR(NULL)",
        "STRING_FUNC",
        "CHR with NULL"
    );
}

#[test]
fn chr_expression() {
    assert_feature_supported!(
        "SELECT CHR(ASCII('A') + 1)",
        "STRING_FUNC",
        "CHR with expression"
    );
}

#[test]
fn char_function() {
    assert_feature_supported!(
        "SELECT CHAR(65)",
        "STRING_FUNC",
        "CHAR function"
    );
}

// ============================================================================
// INITCAP function
// Capitalize first letter of each word
// ============================================================================

#[test]
fn initcap_basic() {
    assert_feature_supported!(
        "SELECT INITCAP('hello world')",
        "STRING_FUNC",
        "INITCAP basic usage"
    );
}

#[test]
fn initcap_lowercase() {
    assert_feature_supported!(
        "SELECT INITCAP('test string')",
        "STRING_FUNC",
        "INITCAP lowercase string"
    );
}

#[test]
fn initcap_uppercase() {
    assert_feature_supported!(
        "SELECT INITCAP('HELLO WORLD')",
        "STRING_FUNC",
        "INITCAP uppercase string"
    );
}

#[test]
fn initcap_null() {
    assert_feature_supported!(
        "SELECT INITCAP(NULL)",
        "STRING_FUNC",
        "INITCAP with NULL"
    );
}

#[test]
fn initcap_column() {
    assert_feature_supported!(
        "SELECT INITCAP(first_name) FROM person",
        "STRING_FUNC",
        "INITCAP on column"
    );
}

#[test]
fn initcap_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE INITCAP(first_name) = 'John'",
        "STRING_FUNC",
        "INITCAP in WHERE clause"
    );
}

// ============================================================================
// TRANSLATE function
// Replace characters based on mapping
// ============================================================================

#[test]
fn translate_basic() {
    assert_feature_supported!(
        "SELECT TRANSLATE('hello', 'el', 'ip')",
        "STRING_FUNC",
        "TRANSLATE basic usage"
    );
}

#[test]
fn translate_digits() {
    assert_feature_supported!(
        "SELECT TRANSLATE('2*[3+4]/{7-2}', '[]{}', '()()')",
        "STRING_FUNC",
        "TRANSLATE brackets"
    );
}

#[test]
fn translate_null_string() {
    assert_feature_supported!(
        "SELECT TRANSLATE(NULL, 'abc', 'xyz')",
        "STRING_FUNC",
        "TRANSLATE with NULL string"
    );
}

#[test]
fn translate_column() {
    assert_feature_supported!(
        "SELECT TRANSLATE(char_col, 'aeiou', '12345') FROM char_types",
        "STRING_FUNC",
        "TRANSLATE on column"
    );
}

#[test]
fn translate_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE TRANSLATE(varchar_col, 'a', 'b') = 'test'",
        "STRING_FUNC",
        "TRANSLATE in WHERE clause"
    );
}

// ============================================================================
// SPLIT_PART function
// Split string and return Nth part
// ============================================================================

#[test]
fn split_part_basic() {
    assert_feature_supported!(
        "SELECT SPLIT_PART('a,b,c', ',', 1)",
        "STRING_FUNC",
        "SPLIT_PART basic usage"
    );
}

#[test]
fn split_part_second() {
    assert_feature_supported!(
        "SELECT SPLIT_PART('a,b,c', ',', 2)",
        "STRING_FUNC",
        "SPLIT_PART second field"
    );
}

#[test]
fn split_part_last() {
    assert_feature_supported!(
        "SELECT SPLIT_PART('a,b,c', ',', 3)",
        "STRING_FUNC",
        "SPLIT_PART last field"
    );
}

#[test]
fn split_part_out_of_bounds() {
    assert_feature_supported!(
        "SELECT SPLIT_PART('a,b,c', ',', 5)",
        "STRING_FUNC",
        "SPLIT_PART out of bounds"
    );
}

#[test]
fn split_part_null_string() {
    assert_feature_supported!(
        "SELECT SPLIT_PART(NULL, ',', 1)",
        "STRING_FUNC",
        "SPLIT_PART with NULL string"
    );
}

#[test]
fn split_part_column() {
    assert_feature_supported!(
        "SELECT SPLIT_PART(varchar_col, '-', 1) FROM char_types",
        "STRING_FUNC",
        "SPLIT_PART on column"
    );
}

#[test]
fn split_part_where_clause() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE SPLIT_PART(varchar_col, '@', 2) = 'example.com'",
        "STRING_FUNC",
        "SPLIT_PART in WHERE clause"
    );
}

#[test]
fn split_part_space_delimiter() {
    assert_feature_supported!(
        "SELECT SPLIT_PART('hello world test', ' ', 2)",
        "STRING_FUNC",
        "SPLIT_PART with space delimiter"
    );
}

// ============================================================================
// STRING_AGG aggregate function
// Concatenate strings from multiple rows
// ============================================================================

#[test]
fn string_agg_basic() {
    assert_feature_supported!(
        "SELECT STRING_AGG(first_name, ', ') FROM person",
        "STRING_FUNC",
        "STRING_AGG basic usage"
    );
}

#[test]
fn string_agg_with_group_by() {
    assert_feature_supported!(
        "SELECT state, STRING_AGG(first_name, '; ') FROM person GROUP BY state",
        "STRING_FUNC",
        "STRING_AGG with GROUP BY"
    );
}

#[test]
fn string_agg_no_delimiter() {
    assert_feature_supported!(
        "SELECT STRING_AGG(first_name, '') FROM person",
        "STRING_FUNC",
        "STRING_AGG no delimiter"
    );
}

#[test]
fn string_agg_order_by() {
    assert_feature_supported!(
        "SELECT STRING_AGG(first_name, ', ' ORDER BY first_name) FROM person",
        "STRING_FUNC",
        "STRING_AGG with ORDER BY"
    );
}

// ============================================================================
// LISTAGG aggregate function (alternative to STRING_AGG)
// SQL:2016 standard aggregate for string concatenation
// ============================================================================

#[test]
fn listagg_basic() {
    assert_feature_supported!(
        "SELECT LISTAGG(first_name, ', ') FROM person",
        "STRING_FUNC",
        "LISTAGG basic usage"
    );
}

#[test]
fn listagg_within_group() {
    assert_feature_supported!(
        "SELECT LISTAGG(first_name, ', ') WITHIN GROUP (ORDER BY first_name) FROM person",
        "STRING_FUNC",
        "LISTAGG WITHIN GROUP"
    );
}

#[test]
fn listagg_with_group_by() {
    assert_feature_supported!(
        "SELECT state, LISTAGG(first_name, '; ') WITHIN GROUP (ORDER BY age) FROM person GROUP BY state",
        "STRING_FUNC",
        "LISTAGG with GROUP BY"
    );
}

// ============================================================================
// Combined/Complex String Operations
// Test realistic scenarios with multiple string functions
// ============================================================================

#[test]
fn complex_string_manipulation() {
    assert_feature_supported!(
        "SELECT
            UPPER(SUBSTRING(first_name FROM 1 FOR 1)) ||
            LOWER(SUBSTRING(first_name FROM 2)) ||
            ' ' ||
            UPPER(last_name)
         FROM person",
        "STRING_FUNC",
        "Complex string manipulation"
    );
}

#[test]
fn string_functions_in_case() {
    assert_feature_supported!(
        "SELECT
            CASE
                WHEN LENGTH(first_name) > 5 THEN SUBSTRING(first_name FROM 1 FOR 5) || '...'
                ELSE first_name
            END
         FROM person",
        "STRING_FUNC",
        "String functions in CASE"
    );
}

#[test]
fn nested_string_functions() {
    assert_feature_supported!(
        "SELECT TRIM(UPPER(REPLACE(first_name, 'a', 'A'))) FROM person",
        "STRING_FUNC",
        "Nested string functions"
    );
}

#[test]
fn string_length_comparisons() {
    assert_feature_supported!(
        "SELECT * FROM person
         WHERE CHARACTER_LENGTH(first_name) = OCTET_LENGTH(first_name)
           AND LENGTH(last_name) > 5",
        "STRING_FUNC",
        "Multiple length functions"
    );
}

#[test]
fn string_position_and_substring() {
    assert_feature_supported!(
        "SELECT
            first_name,
            POSITION(' ' IN first_name) AS space_pos,
            SUBSTRING(first_name FROM 1 FOR POSITION(' ' IN first_name) - 1) AS first_word
         FROM person
         WHERE POSITION(' ' IN first_name) > 0",
        "STRING_FUNC",
        "POSITION with SUBSTRING"
    );
}

#[test]
fn padding_and_trimming() {
    assert_feature_supported!(
        "SELECT
            LPAD(first_name, 20, '-'),
            RPAD(last_name, 20, '.'),
            TRIM(LPAD(RTRIM(first_name), 15))
         FROM person",
        "STRING_FUNC",
        "Padding and trimming combined"
    );
}

#[test]
fn string_concat_variations() {
    assert_feature_supported!(
        "SELECT
            first_name || ' ' || last_name AS concat1,
            CONCAT(first_name, ' ', last_name) AS concat2,
            CONCAT(UPPER(LEFT(first_name, 1)), '. ', last_name) AS concat3
         FROM person",
        "STRING_FUNC",
        "Various concatenation methods"
    );
}

#[test]
fn ascii_chr_roundtrip() {
    assert_feature_supported!(
        "SELECT CHR(ASCII('A')) = 'A' AS roundtrip",
        "STRING_FUNC",
        "ASCII and CHR roundtrip"
    );
}

#[test]
fn case_conversion_comparison() {
    assert_feature_supported!(
        "SELECT * FROM person
         WHERE UPPER(first_name) = UPPER('john')
            OR LOWER(last_name) = LOWER('DOE')
            OR INITCAP(state) = 'California'",
        "STRING_FUNC",
        "Case conversion in comparisons"
    );
}
