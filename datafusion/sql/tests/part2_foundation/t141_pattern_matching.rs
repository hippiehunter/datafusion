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

//! SQL:2016 String Pattern Matching Features
//!
//! This module tests SQL:2016 pattern matching features:
//!
//! # T141: SIMILAR predicate
//! ISO/IEC 9075-2:2016 Section 8.6
//!
//! The SIMILAR TO predicate provides SQL regular expression matching using
//! a pattern language that combines LIKE wildcards with regex features:
//! - LIKE wildcards: % (any string), _ (any character)
//! - Regex operators: | (alternation), * (zero or more), + (one or more)
//! - Quantifiers: ? (zero or one), {n}, {n,}, {n,m}
//! - Character classes: [abc], [a-z], [^abc]
//! - Grouping: (pattern)
//! - Escape character support
//!
//! # T581: Regular expression substring function
//! ISO/IEC 9075-2:2016 Section 6.31
//!
//! SUBSTRING with SIMILAR pattern extracts matching portions from a string:
//! - SUBSTRING(string SIMILAR pattern ESCAPE escape)
//! - Uses the same pattern language as SIMILAR TO
//!
//! # F281: LIKE enhancements
//! ISO/IEC 9075-2:2016 Section 8.5
//!
//! Enhanced LIKE predicate features:
//! - Standard LIKE with % and _ wildcards (covered in E061-04)
//! - LIKE with ESCAPE clause (covered in E061-05)
//! - Additional enhancements and edge cases
//!
//! # Additional Pattern Matching (Non-standard but widely supported)
//! - REGEXP / RLIKE operators (MySQL-style)
//! - ~ operator (PostgreSQL-style regex matching)
//! - REGEXP_LIKE, REGEXP_REPLACE, REGEXP_SUBSTR functions
//! - ILIKE (case-insensitive LIKE, PostgreSQL-style)
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | T141    | SIMILAR predicate | Not Implemented |
//! | T581    | Regular expression substring | Not Implemented |
//! | F281    | LIKE enhancements | Partial (basic LIKE supported) |

use crate::assert_feature_supported;

// ============================================================================
// T141: SIMILAR TO predicate - Basic syntax
// ============================================================================

/// T141: Basic SIMILAR TO with simple pattern
#[test]
fn t141_similar_to_basic() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO 'John'",
        "T141",
        "Basic SIMILAR TO"
    );
}

/// T141: SIMILAR TO with % wildcard (any string)
#[test]
fn t141_similar_to_percent() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO 'J%'",
        "T141",
        "SIMILAR TO with % wildcard"
    );
}

/// T141: SIMILAR TO with _ wildcard (any single character)
#[test]
fn t141_similar_to_underscore() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO 'J_hn'",
        "T141",
        "SIMILAR TO with _ wildcard"
    );
}

/// T141: NOT SIMILAR TO
#[test]
fn t141_not_similar_to() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name NOT SIMILAR TO 'J%'",
        "T141",
        "NOT SIMILAR TO"
    );
}

// ============================================================================
// T141: SIMILAR TO with ESCAPE clause
// ============================================================================

/// T141: SIMILAR TO with ESCAPE for percent
#[test]
fn t141_similar_to_escape_percent() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '50!%' ESCAPE '!'",
        "T141",
        "SIMILAR TO with ESCAPE for %"
    );
}

/// T141: SIMILAR TO with ESCAPE for underscore
#[test]
fn t141_similar_to_escape_underscore() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'file!_name' ESCAPE '!'",
        "T141",
        "SIMILAR TO with ESCAPE for _"
    );
}

/// T141: SIMILAR TO with backslash ESCAPE
#[test]
fn t141_similar_to_escape_backslash() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '100\\%' ESCAPE '\\'",
        "T141",
        "SIMILAR TO with backslash ESCAPE"
    );
}

// ============================================================================
// T141: SIMILAR TO with regex operators
// ============================================================================

/// T141: SIMILAR TO with alternation (|)
#[test]
fn t141_similar_to_alternation() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO 'John|Jane|Jack'",
        "T141",
        "SIMILAR TO with alternation"
    );
}

/// T141: SIMILAR TO with asterisk (zero or more)
#[test]
fn t141_similar_to_asterisk() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'ab*c'",
        "T141",
        "SIMILAR TO with * quantifier"
    );
}

/// T141: SIMILAR TO with plus (one or more)
#[test]
fn t141_similar_to_plus() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'ab+c'",
        "T141",
        "SIMILAR TO with + quantifier"
    );
}

/// T141: SIMILAR TO with question mark (zero or one)
#[test]
fn t141_similar_to_question() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'colou?r'",
        "T141",
        "SIMILAR TO with ? quantifier"
    );
}

/// T141: SIMILAR TO with exact count {n}
#[test]
fn t141_similar_to_exact_count() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'a{3}'",
        "T141",
        "SIMILAR TO with {n} quantifier"
    );
}

/// T141: SIMILAR TO with minimum count {n,}
#[test]
fn t141_similar_to_min_count() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'a{2,}'",
        "T141",
        "SIMILAR TO with {n,} quantifier"
    );
}

/// T141: SIMILAR TO with range {n,m}
#[test]
fn t141_similar_to_range_count() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO 'a{2,4}'",
        "T141",
        "SIMILAR TO with {n,m} quantifier"
    );
}

// ============================================================================
// T141: SIMILAR TO with character classes
// ============================================================================

/// T141: SIMILAR TO with character class [abc]
#[test]
fn t141_similar_to_char_class() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '[abc]%'",
        "T141",
        "SIMILAR TO with character class"
    );
}

/// T141: SIMILAR TO with character range [a-z]
#[test]
fn t141_similar_to_char_range() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO '[A-Z]%'",
        "T141",
        "SIMILAR TO with character range"
    );
}

/// T141: SIMILAR TO with digit range [0-9]
#[test]
fn t141_similar_to_digit_range() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '%[0-9]+%'",
        "T141",
        "SIMILAR TO with digit range"
    );
}

/// T141: SIMILAR TO with negated character class [^abc]
#[test]
fn t141_similar_to_negated_class() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO '[^J]%'",
        "T141",
        "SIMILAR TO with negated character class"
    );
}

/// T141: SIMILAR TO with multiple ranges [a-zA-Z0-9]
#[test]
fn t141_similar_to_multiple_ranges() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '[a-zA-Z0-9]+'",
        "T141",
        "SIMILAR TO with multiple ranges"
    );
}

// ============================================================================
// T141: SIMILAR TO with grouping and complex patterns
// ============================================================================

/// T141: SIMILAR TO with grouping parentheses
#[test]
fn t141_similar_to_grouping() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '(ab|cd)+e'",
        "T141",
        "SIMILAR TO with grouping"
    );
}

/// T141: SIMILAR TO with nested grouping
#[test]
fn t141_similar_to_nested_grouping() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '((a|b)(c|d))+'",
        "T141",
        "SIMILAR TO with nested grouping"
    );
}

/// T141: SIMILAR TO combining wildcards and regex
#[test]
fn t141_similar_to_combined() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name SIMILAR TO 'J(oh|a)n%'",
        "T141",
        "SIMILAR TO combining wildcards and regex"
    );
}

/// T141: SIMILAR TO complex email pattern
#[test]
fn t141_similar_to_email_pattern() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '[a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}' ESCAPE '\\'",
        "T141",
        "SIMILAR TO email pattern"
    );
}

/// T141: SIMILAR TO phone number pattern
#[test]
fn t141_similar_to_phone_pattern() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col SIMILAR TO '[0-9]{3}-[0-9]{3}-[0-9]{4}'",
        "T141",
        "SIMILAR TO phone number pattern"
    );
}

// ============================================================================
// T581: SUBSTRING with SIMILAR pattern
// ============================================================================

/// T581: SUBSTRING with SIMILAR basic
#[test]
fn t581_substring_similar_basic() {
    assert_feature_supported!(
        "SELECT SUBSTRING('Hello World' SIMILAR 'W%' ESCAPE '\\')",
        "T581",
        "SUBSTRING with SIMILAR pattern"
    );
}

/// T581: SUBSTRING with SIMILAR from column
#[test]
fn t581_substring_similar_column() {
    assert_feature_supported!(
        "SELECT SUBSTRING(char_col SIMILAR '[0-9]+' ESCAPE '\\') FROM char_types",
        "T581",
        "SUBSTRING SIMILAR from column"
    );
}

/// T581: SUBSTRING with SIMILAR and character class
#[test]
fn t581_substring_similar_char_class() {
    assert_feature_supported!(
        "SELECT SUBSTRING('abc123def' SIMILAR '[0-9]+' ESCAPE '\\')",
        "T581",
        "SUBSTRING SIMILAR with character class"
    );
}

/// T581: SUBSTRING with SIMILAR complex pattern
#[test]
fn t581_substring_similar_complex() {
    assert_feature_supported!(
        "SELECT SUBSTRING(char_col SIMILAR '[a-zA-Z]+@[a-zA-Z]+\\.[a-z]{2,}' ESCAPE '\\') FROM char_types",
        "T581",
        "SUBSTRING SIMILAR complex pattern"
    );
}

// ============================================================================
// F281: LIKE enhancements - Edge cases and complex patterns
// ============================================================================

/// F281: LIKE with multiple % wildcards
#[test]
fn f281_like_multiple_percent() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE '%a%e%'",
        "F281",
        "LIKE with multiple % wildcards"
    );
}

/// F281: LIKE with multiple _ wildcards
#[test]
fn f281_like_multiple_underscore() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '___'",
        "F281",
        "LIKE with multiple _ wildcards"
    );
}

/// F281: LIKE with mixed wildcards
#[test]
fn f281_like_mixed_wildcards() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE 'J_h%'",
        "F281",
        "LIKE with mixed wildcards"
    );
}

/// F281: LIKE with empty pattern
#[test]
fn f281_like_empty_pattern() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE ''",
        "F281",
        "LIKE with empty pattern"
    );
}

/// F281: LIKE with only % wildcard
#[test]
fn f281_like_percent_only() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '%'",
        "F281",
        "LIKE with only % wildcard"
    );
}

/// F281: LIKE matching NULL (should return NULL)
#[test]
fn f281_like_null_handling() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c LIKE '%' AND c IS NULL",
        "F281",
        "LIKE NULL handling"
    );
}

/// F281: LIKE with special characters
#[test]
fn f281_like_special_chars() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '%$%'",
        "F281",
        "LIKE with special characters"
    );
}

/// F281: LIKE case sensitivity
#[test]
fn f281_like_case_sensitive() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE 'john'",
        "F281",
        "LIKE case sensitivity"
    );
}

/// F281: LIKE with ESCAPE and multiple escaped characters
#[test]
fn f281_like_escape_multiple() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '%!!%!_%' ESCAPE '!'",
        "F281",
        "LIKE ESCAPE multiple characters"
    );
}

// ============================================================================
// ILIKE - Case-insensitive LIKE (PostgreSQL extension)
// ============================================================================

/// ILIKE: Basic case-insensitive match
#[test]
fn ilike_basic() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name ILIKE 'john'",
        "ILIKE",
        "Basic ILIKE"
    );
}

/// ILIKE: Case-insensitive with wildcards
#[test]
fn ilike_with_wildcards() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name ILIKE 'J%N'",
        "ILIKE",
        "ILIKE with wildcards"
    );
}

/// ILIKE: NOT ILIKE
#[test]
fn not_ilike() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name NOT ILIKE 'j%'",
        "ILIKE",
        "NOT ILIKE"
    );
}

/// ILIKE: With ESCAPE clause
#[test]
fn ilike_escape() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col ILIKE '50!%' ESCAPE '!'",
        "ILIKE",
        "ILIKE with ESCAPE"
    );
}

// ============================================================================
// REGEXP / RLIKE operators (MySQL-style)
// ============================================================================

/// REGEXP: Basic regex match
#[test]
fn regexp_basic() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name REGEXP '^J'",
        "REGEXP",
        "Basic REGEXP operator"
    );
}

/// REGEXP: With character class
#[test]
fn regexp_char_class() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col REGEXP '[0-9]+'",
        "REGEXP",
        "REGEXP with character class"
    );
}

/// REGEXP: With alternation
#[test]
fn regexp_alternation() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name REGEXP 'John|Jane|Jack'",
        "REGEXP",
        "REGEXP with alternation"
    );
}

/// REGEXP: NOT REGEXP
#[test]
fn not_regexp() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name NOT REGEXP '^J'",
        "REGEXP",
        "NOT REGEXP"
    );
}

/// RLIKE: Synonym for REGEXP
#[test]
fn rlike_basic() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name RLIKE '^J.*n$'",
        "RLIKE",
        "RLIKE operator"
    );
}

/// RLIKE: NOT RLIKE
#[test]
fn not_rlike() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name NOT RLIKE '^A'",
        "RLIKE",
        "NOT RLIKE"
    );
}

// ============================================================================
// ~ operator (PostgreSQL-style regex matching)
// ============================================================================

/// ~: Basic regex match operator
#[test]
fn tilde_regex_basic() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name ~ '^J'",
        "~",
        "~ regex operator"
    );
}

/// ~: Case-insensitive regex match (~*)
#[test]
fn tilde_regex_icase() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name ~* '^j'",
        "~*",
        "~* case-insensitive regex"
    );
}

/// !~: Negated regex match
#[test]
fn tilde_regex_negated() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name !~ '^A'",
        "!~",
        "!~ negated regex"
    );
}

/// !~*: Negated case-insensitive regex match
#[test]
fn tilde_regex_negated_icase() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name !~* '^a'",
        "!~*",
        "!~* negated case-insensitive regex"
    );
}

// ============================================================================
// REGEXP_LIKE function
// ============================================================================

/// REGEXP_LIKE: Basic usage
#[test]
fn regexp_like_basic() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE REGEXP_LIKE(first_name, '^J')",
        "REGEXP_LIKE",
        "Basic REGEXP_LIKE"
    );
}

/// REGEXP_LIKE: With flags (case-insensitive)
#[test]
fn regexp_like_flags() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE REGEXP_LIKE(first_name, '^j', 'i')",
        "REGEXP_LIKE",
        "REGEXP_LIKE with flags"
    );
}

/// REGEXP_LIKE: Complex pattern
#[test]
fn regexp_like_complex() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE REGEXP_LIKE(char_col, '[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}')",
        "REGEXP_LIKE",
        "REGEXP_LIKE complex pattern"
    );
}

// ============================================================================
// REGEXP_REPLACE function
// ============================================================================

/// REGEXP_REPLACE: Basic replacement
#[test]
fn regexp_replace_basic() {
    assert_feature_supported!(
        "SELECT REGEXP_REPLACE('Hello World', 'World', 'Universe')",
        "REGEXP_REPLACE",
        "Basic REGEXP_REPLACE"
    );
}

/// REGEXP_REPLACE: From column
#[test]
fn regexp_replace_column() {
    assert_feature_supported!(
        "SELECT REGEXP_REPLACE(first_name, '^J', 'Z') FROM person",
        "REGEXP_REPLACE",
        "REGEXP_REPLACE from column"
    );
}

/// REGEXP_REPLACE: With regex pattern
#[test]
fn regexp_replace_pattern() {
    assert_feature_supported!(
        "SELECT REGEXP_REPLACE(char_col, '[0-9]+', 'NUM') FROM char_types",
        "REGEXP_REPLACE",
        "REGEXP_REPLACE with regex pattern"
    );
}

/// REGEXP_REPLACE: With flags
#[test]
fn regexp_replace_flags() {
    assert_feature_supported!(
        "SELECT REGEXP_REPLACE('Hello hello', 'hello', 'hi', 'gi')",
        "REGEXP_REPLACE",
        "REGEXP_REPLACE with flags"
    );
}

/// REGEXP_REPLACE: Global replacement
#[test]
fn regexp_replace_global() {
    assert_feature_supported!(
        "SELECT REGEXP_REPLACE('aaa bbb aaa', 'aaa', 'xxx', 'g')",
        "REGEXP_REPLACE",
        "REGEXP_REPLACE global"
    );
}

// ============================================================================
// REGEXP_SUBSTR function
// ============================================================================

/// REGEXP_SUBSTR: Extract matching substring
#[test]
fn regexp_substr_basic() {
    assert_feature_supported!(
        "SELECT REGEXP_SUBSTR('abc123def', '[0-9]+')",
        "REGEXP_SUBSTR",
        "Basic REGEXP_SUBSTR"
    );
}

/// REGEXP_SUBSTR: From column
#[test]
fn regexp_substr_column() {
    assert_feature_supported!(
        "SELECT REGEXP_SUBSTR(char_col, '[A-Z]+') FROM char_types",
        "REGEXP_SUBSTR",
        "REGEXP_SUBSTR from column"
    );
}

/// REGEXP_SUBSTR: Extract email domain
#[test]
fn regexp_substr_email() {
    assert_feature_supported!(
        "SELECT REGEXP_SUBSTR('user@example.com', '@[a-z0-9.-]+\\.[a-z]{2,}')",
        "REGEXP_SUBSTR",
        "REGEXP_SUBSTR extract domain"
    );
}

/// REGEXP_SUBSTR: With occurrence parameter
#[test]
fn regexp_substr_occurrence() {
    assert_feature_supported!(
        "SELECT REGEXP_SUBSTR('aa bb cc', '[a-z]+', 1, 2)",
        "REGEXP_SUBSTR",
        "REGEXP_SUBSTR with occurrence"
    );
}

// ============================================================================
// Combined and complex pattern matching tests
// ============================================================================

/// Complex: Multiple pattern types in WHERE clause
#[test]
fn combined_multiple_patterns() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE first_name LIKE 'J%' AND last_name SIMILAR TO '[A-Z]%' AND state NOT LIKE 'C_'",
        "Combined",
        "Multiple pattern types"
    );
}

/// Complex: Pattern in CASE expression
#[test]
fn pattern_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN first_name LIKE 'J%' THEN 'J-name' ELSE 'Other' END FROM person",
        "Combined",
        "Pattern in CASE"
    );
}

/// Complex: Pattern with JOIN condition
#[test]
fn pattern_in_join() {
    assert_feature_supported!(
        "SELECT * FROM person p JOIN orders o ON p.first_name LIKE o.item",
        "Combined",
        "Pattern in JOIN"
    );
}

/// Complex: Pattern in subquery
#[test]
fn pattern_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE id IN (SELECT customer_id FROM orders WHERE item LIKE '%book%')",
        "Combined",
        "Pattern in subquery"
    );
}

/// Complex: Multiple LIKE with OR/AND
#[test]
fn multiple_like_boolean() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE (first_name LIKE 'J%' OR first_name LIKE 'M%') AND last_name NOT LIKE '%son'",
        "Combined",
        "Multiple LIKE with boolean logic"
    );
}

/// Complex: REGEXP functions in SELECT
#[test]
fn regexp_functions_select() {
    assert_feature_supported!(
        "SELECT first_name, REGEXP_REPLACE(first_name, '[aeiou]', '*') as censored, REGEXP_SUBSTR(first_name, '^.') as initial FROM person",
        "Combined",
        "REGEXP functions in SELECT"
    );
}

/// Complex: Nested pattern matching
#[test]
fn nested_patterns() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE REGEXP_LIKE(REGEXP_REPLACE(first_name, '[0-9]', ''), '^[A-Z]')",
        "Combined",
        "Nested pattern functions"
    );
}

/// Complex: Pattern with aggregate
#[test]
fn pattern_with_aggregate() {
    assert_feature_supported!(
        "SELECT COUNT(*) FROM person WHERE first_name LIKE 'J%' GROUP BY state HAVING COUNT(*) > 10",
        "Combined",
        "Pattern with aggregate"
    );
}

// ============================================================================
// Edge cases and special scenarios
// ============================================================================

/// Edge: Empty string matching
#[test]
fn edge_empty_string() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE ''",
        "Edge",
        "Empty string pattern"
    );
}

/// Edge: NULL in pattern
#[test]
fn edge_null_pattern() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c LIKE NULL",
        "Edge",
        "NULL pattern"
    );
}

/// Edge: Pattern with escape at end
#[test]
fn edge_escape_at_end() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '%!!' ESCAPE '!'",
        "Edge",
        "Escape character at end"
    );
}

/// Edge: Unicode in pattern
#[test]
fn edge_unicode_pattern() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '%café%'",
        "Edge",
        "Unicode in pattern"
    );
}

/// Edge: Very long pattern
#[test]
fn edge_long_pattern() {
    assert_feature_supported!(
        "SELECT * FROM char_types WHERE char_col LIKE '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'",
        "Edge",
        "Very long pattern"
    );
}

/// Edge: Pattern in UPDATE statement
#[test]
fn edge_pattern_in_update() {
    assert_feature_supported!(
        "UPDATE person SET first_name = REGEXP_REPLACE(first_name, '[0-9]', '') WHERE first_name LIKE '%[0-9]%'",
        "Edge",
        "Pattern in UPDATE"
    );
}

/// Edge: Pattern in DELETE statement
#[test]
fn edge_pattern_in_delete() {
    assert_feature_supported!(
        "DELETE FROM person WHERE first_name SIMILAR TO '[0-9]%'",
        "Edge",
        "Pattern in DELETE"
    );
}
