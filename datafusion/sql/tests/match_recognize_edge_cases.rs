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

//! Edge case tests for MATCH_RECOGNIZE implementation

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse SQL and check if it parses successfully
fn parse_sql(sql: &str) -> Result<(), String> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql)
        .map(|_| ())
        .map_err(|e| format!("Parse error: {}", e))
}

#[test]
fn test_empty_pattern() {
    // Empty PATTERN should be a parse error (handled by sqlparser)
    let sql = "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id PATTERN () DEFINE A AS value > 0)";
    let result = parse_sql(sql);
    // This may or may not fail depending on sqlparser implementation
    println!("Empty pattern result: {:?}", result);
}

#[test]
fn test_complex_nested_pattern() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (((A | B)* C)+) \
                   DEFINE A AS value = 1, B AS value = 2, C AS value = 3 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Complex nested pattern should parse: {:?}", result);
}

#[test]
fn test_permute_pattern() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (PERMUTE(A, B, C)) \
                   DEFINE A AS value = 1, B AS value = 2, C AS value = 3 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "PERMUTE pattern should parse: {:?}", result);
}

#[test]
fn test_deeply_nested_alternation() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN ((((A | B) | (C | D)) | ((E | F) | (G | H)))+) \
                   DEFINE \
                       A AS value = 1, B AS value = 2, C AS value = 3, D AS value = 4, \
                       E AS value = 5, F AS value = 6, G AS value = 7, H AS value = 8 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Deeply nested alternation should parse: {:?}", result);
}

#[test]
fn test_pattern_without_define_for_all_symbols() {
    // STRT has no DEFINE clause - this should be valid (treated as always true)
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES STRT.value AS start_value \
                   PATTERN (STRT DOWN+ UP+) \
                   DEFINE DOWN AS value < 100, UP AS value > 100 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Pattern without DEFINE for all symbols should parse: {:?}", result);
}

#[test]
fn test_very_large_quantifier() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (A{1000,5000}) \
                   DEFINE A AS value > 0 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Very large quantifier should parse: {:?}", result);
}

#[test]
fn test_zero_range_quantifier() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (A{0,0}) \
                   DEFINE A AS value > 0 \
               )";
    let result = parse_sql(sql);
    // This might parse but should be semantically invalid
    println!("Zero range quantifier result: {:?}", result);
}

#[test]
fn test_invalid_range_quantifier() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (A{5,2}) \
                   DEFINE A AS value > 0 \
               )";
    let result = parse_sql(sql);
    // This might parse but should be semantically invalid (min > max)
    println!("Invalid range quantifier result: {:?}", result);
}

#[test]
fn test_quoted_identifiers() {
    let sql = r#"SELECT * FROM t
                 MATCH_RECOGNIZE (
                     ORDER BY id
                     MEASURES "weird name".value AS val
                     PATTERN ("weird name"+)
                     DEFINE "weird name" AS value > 0
                 )"#;
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Quoted identifiers should parse: {:?}", result);
}

#[test]
fn test_deep_nesting() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (((((((((A))))))))) \
                   DEFINE A AS value > 0 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Deep nesting should parse: {:?}", result);
}

#[test]
fn test_multiple_quantifiers() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (A* B+ C? D{3} E{2,5} F{6,}) \
                   DEFINE \
                       A AS value = 1, B AS value = 2, C AS value = 3, \
                       D AS value = 4, E AS value = 5, F AS value = 6 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Multiple different quantifiers should parse: {:?}", result);
}

#[test]
fn test_exclude_pattern() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN ({- A -} B+) \
                   DEFINE A AS value = 1, B AS value = 2 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "EXCLUDE pattern should parse: {:?}", result);
}

#[test]
fn test_start_end_anchors() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES COUNT(*) AS cnt \
                   PATTERN (^ A+ $) \
                   DEFINE A AS value > 0 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Start/end anchors should parse: {:?}", result);
}

#[test]
fn test_complex_measures() {
    let sql = "SELECT * FROM t \
               MATCH_RECOGNIZE ( \
                   ORDER BY id \
                   MEASURES \
                       FIRST(A.value) AS first_val, \
                       LAST(A.value) AS last_val, \
                       COUNT(*) AS cnt, \
                       SUM(A.value) AS total, \
                       AVG(A.value) AS avg_val \
                   PATTERN (A+) \
                   DEFINE A AS value > 0 \
               )";
    let result = parse_sql(sql);
    assert!(result.is_ok(), "Complex measures should parse: {:?}", result);
}

#[test]
fn test_all_after_match_skip_options() {
    // Test different AFTER MATCH SKIP options
    let sqls = vec![
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id AFTER MATCH SKIP TO NEXT ROW PATTERN (A) DEFINE A AS value > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id AFTER MATCH SKIP PAST LAST ROW PATTERN (A) DEFINE A AS value > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id AFTER MATCH SKIP TO FIRST A PATTERN (A B) DEFINE A AS value = 1, B AS value = 2)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id AFTER MATCH SKIP TO LAST B PATTERN (A B) DEFINE A AS value = 1, B AS value = 2)",
    ];

    for sql in sqls {
        let result = parse_sql(sql);
        assert!(result.is_ok(), "AFTER MATCH SKIP option should parse: {:?} for SQL: {}", result, sql);
    }
}

#[test]
fn test_all_rows_per_match_options() {
    let sqls = vec![
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id ONE ROW PER MATCH PATTERN (A) DEFINE A AS value > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id ALL ROWS PER MATCH PATTERN (A) DEFINE A AS value > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id ALL ROWS PER MATCH SHOW EMPTY MATCHES PATTERN (A*) DEFINE A AS value > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id ALL ROWS PER MATCH OMIT EMPTY MATCHES PATTERN (A*) DEFINE A AS value > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY id ALL ROWS PER MATCH WITH UNMATCHED ROWS PATTERN (A*) DEFINE A AS value > 0)",
    ];

    for sql in sqls {
        let result = parse_sql(sql);
        println!("ROWS PER MATCH test: {:?} for SQL: {}", result, sql);
    }
}
