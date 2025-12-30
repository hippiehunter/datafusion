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

//! SQL:2016 Feature R010 - Row Pattern Recognition (MATCH_RECOGNIZE)
//!
//! ISO/IEC 9075-2:2016 Part 2, Section 7.6 (Table Reference)
//!
//! Row Pattern Recognition is a major SQL:2016 feature that enables pattern matching
//! within ordered sets of rows. It's particularly useful for:
//! - Time-series analysis (stock price patterns, sensor data)
//! - Sequence detection (user behavior flows, event sequences)
//! - Anomaly detection (detecting unusual patterns in data)
//! - Business process mining (identifying workflow patterns)
//!
//! The MATCH_RECOGNIZE clause provides:
//! - Pattern variables defined with boolean conditions (DEFINE)
//! - Pattern matching with regex-like syntax (PATTERN)
//! - Computed values from matched rows (MEASURES)
//! - Partitioning and ordering of input data
//! - Control over output rows (ONE ROW PER MATCH, ALL ROWS PER MATCH)
//! - Navigation functions (PREV, NEXT, FIRST, LAST)
//! - Pattern quantifiers (*, +, ?, {n}, {n,m})
//! - Pattern alternation and grouping
//!
//! Example:
//! ```sql
//! SELECT *
//! FROM stock_prices
//! MATCH_RECOGNIZE (
//!     PARTITION BY symbol
//!     ORDER BY trade_date
//!     MEASURES
//!         STRT.price AS start_price,
//!         LAST(DOWN.price) AS bottom_price,
//!         LAST(UP.price) AS end_price
//!     ONE ROW PER MATCH
//!     PATTERN (STRT DOWN+ UP+)
//!     DEFINE
//!         DOWN AS price < PREV(price),
//!         UP AS price > PREV(price)
//! ) AS patterns
//! ```
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | R010 | Row Pattern Recognition | Not Implemented |
//!
//! All tests in this module are expected to FAIL as DataFusion does not currently
//! implement MATCH_RECOGNIZE. These tests document the conformance gap.

use crate::assert_feature_supported;

// ============================================================================
// R010: Basic MATCH_RECOGNIZE structure
// ============================================================================

/// R010: Minimal MATCH_RECOGNIZE clause
#[test]
fn r010_basic_match_recognize() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS a_value \
             ONE ROW PER MATCH \
             PATTERN (A) \
             DEFINE A AS value > 0 \
         ) AS mr",
        "R010",
        "Basic MATCH_RECOGNIZE"
    );
}

/// R010: MATCH_RECOGNIZE with simple pattern
#[test]
fn r010_simple_pattern() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY timestamp \
             MEASURES A.id AS start_id \
             PATTERN (A B C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "Simple ABC pattern"
    );
}

// ============================================================================
// R010: PARTITION BY and ORDER BY
// ============================================================================

/// R010: MATCH_RECOGNIZE with PARTITION BY
#[test]
fn r010_partition_by() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             PARTITION BY category \
             ORDER BY timestamp \
             MEASURES A.value AS first_value \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "MATCH_RECOGNIZE with PARTITION BY"
    );
}

/// R010: MATCH_RECOGNIZE with multiple partition columns
#[test]
fn r010_multiple_partition() {
    assert_feature_supported!(
        "SELECT * FROM person \
         MATCH_RECOGNIZE ( \
             PARTITION BY state, city \
             ORDER BY id \
             MEASURES COUNT(*) AS match_count \
             PATTERN (A+) \
             DEFINE A AS age > 18 \
         )",
        "R010",
        "Multiple partition columns"
    );
}

/// R010: MATCH_RECOGNIZE with ORDER BY multiple columns
#[test]
fn r010_multiple_order() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY timestamp DESC, id ASC \
             MEASURES FIRST(A.value) AS first_val \
             PATTERN (A+) \
             DEFINE A AS value IS NOT NULL \
         )",
        "R010",
        "ORDER BY multiple columns"
    );
}

// ============================================================================
// R010: MEASURES clause
// ============================================================================

/// R010: MEASURES with column references
#[test]
fn r010_measures_columns() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES \
                 A.value AS start_value, \
                 B.value AS end_value \
             PATTERN (A B) \
             DEFINE \
                 A AS value < 100, \
                 B AS value >= 100 \
         )",
        "R010",
        "MEASURES with column references"
    );
}

/// R010: MEASURES with aggregate functions
#[test]
fn r010_measures_aggregates() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY timestamp \
             MEASURES \
                 COUNT(*) AS row_count, \
                 SUM(A.value) AS total_value, \
                 AVG(A.value) AS avg_value \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "MEASURES with aggregates"
    );
}

/// R010: MEASURES with navigation functions
#[test]
fn r010_measures_navigation() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES \
                 FIRST(A.value) AS first_val, \
                 LAST(A.value) AS last_val, \
                 PREV(A.value) AS prev_val, \
                 NEXT(A.value) AS next_val \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "MEASURES with navigation functions"
    );
}

// ============================================================================
// R010: ONE ROW PER MATCH vs ALL ROWS PER MATCH
// ============================================================================

/// R010: ONE ROW PER MATCH (default)
#[test]
fn r010_one_row_per_match() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS val \
             ONE ROW PER MATCH \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "ONE ROW PER MATCH"
    );
}

/// R010: ALL ROWS PER MATCH
#[test]
fn r010_all_rows_per_match() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS val \
             ALL ROWS PER MATCH \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "ALL ROWS PER MATCH"
    );
}

/// R010: ALL ROWS PER MATCH with SHOW EMPTY MATCHES
#[test]
fn r010_show_empty_matches() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS val \
             ALL ROWS PER MATCH SHOW EMPTY MATCHES \
             PATTERN (A*) \
             DEFINE A AS value > 100 \
         )",
        "R010",
        "SHOW EMPTY MATCHES"
    );
}

/// R010: ALL ROWS PER MATCH with OMIT EMPTY MATCHES
#[test]
fn r010_omit_empty_matches() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS val \
             ALL ROWS PER MATCH OMIT EMPTY MATCHES \
             PATTERN (A*) \
             DEFINE A AS value > 100 \
         )",
        "R010",
        "OMIT EMPTY MATCHES"
    );
}

// ============================================================================
// R010: AFTER MATCH SKIP clause
// ============================================================================

/// R010: AFTER MATCH SKIP TO NEXT ROW
#[test]
fn r010_skip_to_next_row() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS val \
             ONE ROW PER MATCH \
             AFTER MATCH SKIP TO NEXT ROW \
             PATTERN (A B) \
             DEFINE \
                 A AS value < 10, \
                 B AS value >= 10 \
         )",
        "R010",
        "AFTER MATCH SKIP TO NEXT ROW"
    );
}

/// R010: AFTER MATCH SKIP PAST LAST ROW
#[test]
fn r010_skip_past_last_row() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             AFTER MATCH SKIP PAST LAST ROW \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "AFTER MATCH SKIP PAST LAST ROW"
    );
}

/// R010: AFTER MATCH SKIP TO variable
#[test]
fn r010_skip_to_variable() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS start \
             AFTER MATCH SKIP TO FIRST B \
             PATTERN (A B+ C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "AFTER MATCH SKIP TO variable"
    );
}

/// R010: AFTER MATCH SKIP TO FIRST variable
#[test]
fn r010_skip_to_first() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             AFTER MATCH SKIP TO FIRST A \
             PATTERN (A B+ A) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2 \
         )",
        "R010",
        "AFTER MATCH SKIP TO FIRST"
    );
}

/// R010: AFTER MATCH SKIP TO LAST variable
#[test]
fn r010_skip_to_last() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             AFTER MATCH SKIP TO LAST B \
             PATTERN (A B+) \
             DEFINE \
                 A AS value < 10, \
                 B AS value >= 10 \
         )",
        "R010",
        "AFTER MATCH SKIP TO LAST"
    );
}

// ============================================================================
// R010: PATTERN quantifiers
// ============================================================================

/// R010: Zero or more quantifier (*)
#[test]
fn r010_quantifier_star() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B* C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "Zero or more (*) quantifier"
    );
}

/// R010: One or more quantifier (+)
#[test]
fn r010_quantifier_plus() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B+ C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "One or more (+) quantifier"
    );
}

/// R010: Zero or one quantifier (?)
#[test]
fn r010_quantifier_question() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B? C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "Zero or one (?) quantifier"
    );
}

/// R010: Exact count quantifier {n}
#[test]
fn r010_quantifier_exact() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B{3} C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "Exact count {n} quantifier"
    );
}

/// R010: Range quantifier {n,m}
#[test]
fn r010_quantifier_range() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B{2,5} C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "Range {n,m} quantifier"
    );
}

/// R010: Minimum quantifier {n,}
#[test]
fn r010_quantifier_minimum() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B{3,} C) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3 \
         )",
        "R010",
        "Minimum {n,} quantifier"
    );
}

// ============================================================================
// R010: Pattern alternation and grouping
// ============================================================================

/// R010: Pattern alternation (|)
#[test]
fn r010_pattern_alternation() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES A.value AS val \
             PATTERN (A | B) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2 \
         )",
        "R010",
        "Pattern alternation (|)"
    );
}

/// R010: Pattern grouping with parentheses
#[test]
fn r010_pattern_grouping() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN ((A B)+) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2 \
         )",
        "R010",
        "Pattern grouping"
    );
}

/// R010: Complex pattern with alternation and quantifiers
#[test]
fn r010_pattern_complex_alternation() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A (B | C)+ D) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3, \
                 D AS value = 4 \
         )",
        "R010",
        "Complex alternation with quantifiers"
    );
}

// ============================================================================
// R010: DEFINE clause with PREV/NEXT
// ============================================================================

/// R010: DEFINE with PREV function
#[test]
fn r010_define_prev() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY timestamp \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A+) \
             DEFINE A AS value > PREV(value) \
         )",
        "R010",
        "DEFINE with PREV"
    );
}

/// R010: DEFINE with NEXT function
#[test]
fn r010_define_next() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY timestamp \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A B) \
             DEFINE A AS value < NEXT(value) \
         )",
        "R010",
        "DEFINE with NEXT"
    );
}

/// R010: DEFINE with PREV offset
#[test]
fn r010_define_prev_offset() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A+) \
             DEFINE A AS value > PREV(value, 2) \
         )",
        "R010",
        "DEFINE with PREV offset"
    );
}

// ============================================================================
// R010: Real-world pattern examples
// ============================================================================

/// R010: Stock price V-pattern (down then up)
#[test]
fn r010_stock_v_pattern() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             PARTITION BY symbol \
             ORDER BY trade_date \
             MEASURES \
                 STRT.price AS start_price, \
                 LAST(DOWN.price) AS bottom_price, \
                 LAST(UP.price) AS end_price \
             ONE ROW PER MATCH \
             PATTERN (STRT DOWN+ UP+) \
             DEFINE \
                 DOWN AS price < PREV(price), \
                 UP AS price > PREV(price) \
         ) AS patterns",
        "R010",
        "Stock V-pattern detection"
    );
}

/// R010: Sequential events pattern
#[test]
fn r010_sequential_events() {
    assert_feature_supported!(
        "SELECT * FROM orders \
         MATCH_RECOGNIZE ( \
             PARTITION BY customer_id \
             ORDER BY order_date \
             MEASURES \
                 FIRST(A.order_date) AS first_order, \
                 LAST(C.order_date) AS last_order, \
                 COUNT(*) AS total_orders \
             PATTERN (A B+ C) \
             DEFINE \
                 A AS amount > 100, \
                 B AS amount BETWEEN 50 AND 100, \
                 C AS amount < 50 \
         )",
        "R010",
        "Sequential event pattern"
    );
}

/// R010: Anomaly detection pattern
#[test]
fn r010_anomaly_detection() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY timestamp \
             MEASURES \
                 SPIKE.value AS spike_value, \
                 SPIKE.timestamp AS spike_time \
             PATTERN (NORMAL+ SPIKE NORMAL+) \
             DEFINE \
                 NORMAL AS value BETWEEN 0 AND 100, \
                 SPIKE AS value > 100 \
         )",
        "R010",
        "Anomaly detection"
    );
}

/// R010: User session pattern
#[test]
fn r010_user_session() {
    assert_feature_supported!(
        "SELECT * FROM person \
         MATCH_RECOGNIZE ( \
             PARTITION BY id \
             ORDER BY timestamp \
             MEASURES \
                 COUNT(ACTIVE.*) AS active_count, \
                 FIRST(ACTIVE.timestamp) AS session_start, \
                 LAST(ACTIVE.timestamp) AS session_end \
             PATTERN (ACTIVE+ IDLE) \
             DEFINE \
                 ACTIVE AS action IS NOT NULL, \
                 IDLE AS action IS NULL \
         )",
        "R010",
        "User session detection"
    );
}

// ============================================================================
// R010: Navigation functions
// ============================================================================

/// R010: FIRST function in MEASURES
#[test]
fn r010_first_function() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES FIRST(A.value) AS first_val \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "FIRST navigation function"
    );
}

/// R010: LAST function in MEASURES
#[test]
fn r010_last_function() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES LAST(A.value) AS last_val \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "LAST navigation function"
    );
}

/// R010: Multiple navigation functions
#[test]
fn r010_multiple_navigation() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES \
                 FIRST(A.value) AS first_val, \
                 LAST(A.value) AS last_val, \
                 FIRST(A.id) AS first_id, \
                 LAST(A.id) AS last_id \
             PATTERN (A+) \
             DEFINE A AS value > 0 \
         )",
        "R010",
        "Multiple navigation functions"
    );
}

// ============================================================================
// R010: Complex real-world scenarios
// ============================================================================

/// R010: Multi-stage pattern with different quantifiers
#[test]
fn r010_multi_stage_pattern() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             PARTITION BY category \
             ORDER BY timestamp \
             MEASURES \
                 COUNT(GROWTH.*) AS growth_periods, \
                 COUNT(STABLE.*) AS stable_periods, \
                 COUNT(DECLINE.*) AS decline_periods \
             ONE ROW PER MATCH \
             PATTERN (GROWTH{2,} STABLE? DECLINE*) \
             DEFINE \
                 GROWTH AS value > PREV(value), \
                 STABLE AS value = PREV(value), \
                 DECLINE AS value < PREV(value) \
         )",
        "R010",
        "Multi-stage pattern"
    );
}

/// R010: Nested pattern groups with alternation
#[test]
fn r010_nested_pattern_groups() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS match_count \
             PATTERN ((A B | C D)+) \
             DEFINE \
                 A AS value = 1, \
                 B AS value = 2, \
                 C AS value = 3, \
                 D AS value = 4 \
         )",
        "R010",
        "Nested pattern groups"
    );
}

/// R010: Pattern with exclusion (reluctant quantifier)
#[test]
fn r010_reluctant_quantifier() {
    assert_feature_supported!(
        "SELECT * FROM t \
         MATCH_RECOGNIZE ( \
             ORDER BY id \
             MEASURES COUNT(*) AS cnt \
             PATTERN (A+? B) \
             DEFINE \
                 A AS value < 10, \
                 B AS value >= 10 \
         )",
        "R010",
        "Reluctant quantifier"
    );
}

/// R010: Complete MATCH_RECOGNIZE with all clauses
#[test]
fn r010_complete_match_recognize() {
    assert_feature_supported!(
        "SELECT mr.* FROM t \
         MATCH_RECOGNIZE ( \
             PARTITION BY category, region \
             ORDER BY timestamp DESC, id ASC \
             MEASURES \
                 FIRST(UP.value) AS pattern_start, \
                 LAST(DOWN.value) AS pattern_end, \
                 COUNT(UP.*) AS up_count, \
                 COUNT(DOWN.*) AS down_count, \
                 SUM(UP.value) AS up_total, \
                 AVG(DOWN.value) AS down_avg \
             ONE ROW PER MATCH \
             AFTER MATCH SKIP TO LAST DOWN \
             PATTERN (UP{2,} FLAT? DOWN+) \
             DEFINE \
                 UP AS value > PREV(value), \
                 FLAT AS value = PREV(value), \
                 DOWN AS value < PREV(value) \
         ) AS mr",
        "R010",
        "Complete MATCH_RECOGNIZE with all clauses"
    );
}

/// R010: MATCH_RECOGNIZE in subquery
#[test]
fn r010_match_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM ( \
             SELECT * FROM t \
             MATCH_RECOGNIZE ( \
                 ORDER BY id \
                 MEASURES A.value AS matched_value \
                 PATTERN (A+) \
                 DEFINE A AS value > 10 \
             ) \
         ) AS matched_results \
         WHERE matched_value > 50",
        "R010",
        "MATCH_RECOGNIZE in subquery"
    );
}

/// R010: MATCH_RECOGNIZE with JOIN
#[test]
fn r010_match_with_join() {
    assert_feature_supported!(
        "SELECT * FROM t1 \
         JOIN ( \
             SELECT * FROM t \
             MATCH_RECOGNIZE ( \
                 ORDER BY timestamp \
                 MEASURES FIRST(A.id) AS pattern_id \
                 PATTERN (A B C) \
                 DEFINE \
                     A AS value = 1, \
                     B AS value = 2, \
                     C AS value = 3 \
             ) \
         ) patterns ON t1.id = patterns.pattern_id",
        "R010",
        "MATCH_RECOGNIZE with JOIN"
    );
}
