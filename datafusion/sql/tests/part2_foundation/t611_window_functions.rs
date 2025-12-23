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

//! SQL:2016 Feature T611 - Elementary OLAP operations
//! SQL:2016 Feature T612 - Advanced OLAP operations
//!
//! ISO/IEC 9075-2:2016 Section 6.10 (Window functions)
//!
//! These features cover window functions (also called analytic functions or OLAP functions)
//! which allow calculations across a set of table rows that are related to the current row.
//!
//! # T611: Elementary OLAP operations
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | T611-01 | ROW_NUMBER() | Supported |
//! | T611-02 | RANK() | Supported |
//! | T611-03 | DENSE_RANK() | Supported |
//! | T611-04 | OVER clause with PARTITION BY | Supported |
//! | T611-05 | OVER clause with ORDER BY | Supported |
//! | T611-06 | Window frame: ROWS BETWEEN | Supported |
//! | T611-07 | UNBOUNDED PRECEDING | Supported |
//! | T611-08 | CURRENT ROW | Supported |
//! | T611-09 | UNBOUNDED FOLLOWING | Supported |
//!
//! # T612: Advanced OLAP operations
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | T612-01 | NTILE(n) | Supported |
//! | T612-02 | LEAD(col, offset, default) | Supported |
//! | T612-03 | LAG(col, offset, default) | Supported |
//! | T612-04 | FIRST_VALUE(col) | Supported |
//! | T612-05 | LAST_VALUE(col) | Supported |
//! | T612-06 | NTH_VALUE(col, n) | Supported |
//! | T612-07 | PERCENT_RANK() | Supported |
//! | T612-08 | CUME_DIST() | Supported |
//! | T612-09 | Window frame: RANGE BETWEEN | Supported |
//! | T612-10 | Window frame: GROUPS BETWEEN | Partial |
//! | T612-11 | Named window definitions (WINDOW clause) | Supported |
//! | T612-12 | Aggregate functions as window functions | Supported |
//! | T612-13 | FILTER clause with window functions | Supported |
//! | T612-14 | RESPECT NULLS / IGNORE NULLS | Partial |
//! | T612-15 | EXCLUDE clause | Not Implemented |
//!
//! Window functions are not Core SQL features but are widely used for analytical queries.

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// T611: Elementary OLAP operations
// ============================================================================

// ----------------------------------------------------------------------------
// T611-01: ROW_NUMBER() window function
// ----------------------------------------------------------------------------

/// T611-01: Basic ROW_NUMBER() function
#[test]
fn t611_01_row_number_basic() {
    assert_feature_supported!(
        "SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t",
        "T611-01",
        "ROW_NUMBER() function"
    );
}

/// T611-01: ROW_NUMBER() with partition
#[test]
fn t611_01_row_number_with_partition() {
    assert_feature_supported!(
        "SELECT ROW_NUMBER() OVER (PARTITION BY c ORDER BY a) FROM t",
        "T611-01",
        "ROW_NUMBER() with PARTITION BY"
    );
}

/// T611-01: ROW_NUMBER() with multiple order columns
#[test]
fn t611_01_row_number_multiple_order() {
    assert_feature_supported!(
        "SELECT ROW_NUMBER() OVER (ORDER BY a DESC, b ASC) FROM t",
        "T611-01",
        "ROW_NUMBER() with multiple ORDER BY"
    );
}

/// T611-01: ROW_NUMBER() in WHERE clause (via subquery)
#[test]
fn t611_01_row_number_in_where() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM t) WHERE rn = 1",
        "T611-01",
        "ROW_NUMBER() filtered in WHERE"
    );
}

/// T611-01: Multiple ROW_NUMBER() with different windows
#[test]
fn t611_01_multiple_row_number() {
    assert_feature_supported!(
        "SELECT ROW_NUMBER() OVER (ORDER BY a), ROW_NUMBER() OVER (ORDER BY b) FROM t",
        "T611-01",
        "Multiple ROW_NUMBER() functions"
    );
}

// ----------------------------------------------------------------------------
// T611-02: RANK() window function
// ----------------------------------------------------------------------------

/// T611-02: Basic RANK() function
#[test]
fn t611_02_rank_basic() {
    assert_feature_supported!(
        "SELECT RANK() OVER (ORDER BY a) FROM t",
        "T611-02",
        "RANK() function"
    );
}

/// T611-02: RANK() with partition
#[test]
fn t611_02_rank_with_partition() {
    assert_feature_supported!(
        "SELECT RANK() OVER (PARTITION BY c ORDER BY a) FROM t",
        "T611-02",
        "RANK() with PARTITION BY"
    );
}

/// T611-02: RANK() with ties (multiple order columns)
#[test]
fn t611_02_rank_with_ties() {
    assert_feature_supported!(
        "SELECT RANK() OVER (ORDER BY a DESC, b ASC) FROM t",
        "T611-02",
        "RANK() handling ties"
    );
}

/// T611-02: RANK() for top-N query
#[test]
fn t611_02_rank_top_n() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b, RANK() OVER (ORDER BY a DESC) AS rnk FROM t) WHERE rnk <= 10",
        "T611-02",
        "RANK() for top-N query"
    );
}

/// T611-02: RANK() partitioned top-N
#[test]
fn t611_02_rank_partitioned_top_n() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT c, a, RANK() OVER (PARTITION BY c ORDER BY a DESC) AS rnk FROM t) WHERE rnk = 1",
        "T611-02",
        "RANK() partitioned top-N"
    );
}

// ----------------------------------------------------------------------------
// T611-03: DENSE_RANK() window function
// ----------------------------------------------------------------------------

/// T611-03: Basic DENSE_RANK() function
#[test]
fn t611_03_dense_rank_basic() {
    assert_feature_supported!(
        "SELECT DENSE_RANK() OVER (ORDER BY a) FROM t",
        "T611-03",
        "DENSE_RANK() function"
    );
}

/// T611-03: DENSE_RANK() with partition
#[test]
fn t611_03_dense_rank_with_partition() {
    assert_feature_supported!(
        "SELECT DENSE_RANK() OVER (PARTITION BY c ORDER BY a) FROM t",
        "T611-03",
        "DENSE_RANK() with PARTITION BY"
    );
}

/// T611-03: DENSE_RANK() vs RANK() comparison
#[test]
fn t611_03_dense_rank_vs_rank() {
    assert_feature_supported!(
        "SELECT RANK() OVER (ORDER BY a), DENSE_RANK() OVER (ORDER BY a) FROM t",
        "T611-03",
        "RANK() vs DENSE_RANK()"
    );
}

/// T611-03: DENSE_RANK() with multiple order columns
#[test]
fn t611_03_dense_rank_multiple_order() {
    assert_feature_supported!(
        "SELECT DENSE_RANK() OVER (ORDER BY a DESC, b ASC) FROM t",
        "T611-03",
        "DENSE_RANK() with multiple ORDER BY"
    );
}

// ----------------------------------------------------------------------------
// T611-04: OVER clause with PARTITION BY
// ----------------------------------------------------------------------------

/// T611-04: PARTITION BY single column
#[test]
fn t611_04_partition_by_single() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c) FROM t",
        "T611-04",
        "PARTITION BY single column"
    );
}

/// T611-04: PARTITION BY multiple columns
#[test]
fn t611_04_partition_by_multiple() {
    assert_feature_supported!(
        "SELECT SUM(qty) OVER (PARTITION BY customer_id, item) FROM orders",
        "T611-04",
        "PARTITION BY multiple columns"
    );
}

/// T611-04: PARTITION BY with expression
#[test]
fn t611_04_partition_by_expression() {
    assert_feature_supported!(
        "SELECT COUNT(*) OVER (PARTITION BY a + b) FROM t",
        "T611-04",
        "PARTITION BY with expression"
    );
}

/// T611-04: Multiple window functions with same partition
#[test]
fn t611_04_multiple_functions_same_partition() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c), AVG(a) OVER (PARTITION BY c) FROM t",
        "T611-04",
        "Multiple functions with same partition"
    );
}

/// T611-04: Window function with PARTITION BY on person table
#[test]
fn t611_04_partition_by_on_person() {
    assert_feature_supported!(
        "SELECT first_name, state, AVG(salary) OVER (PARTITION BY state) FROM person",
        "T611-04",
        "PARTITION BY on person table"
    );
}

// ----------------------------------------------------------------------------
// T611-05: OVER clause with ORDER BY
// ----------------------------------------------------------------------------

/// T611-05: ORDER BY single column ascending
#[test]
fn t611_05_order_by_asc() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ASC) FROM t",
        "T611-05",
        "ORDER BY ascending"
    );
}

/// T611-05: ORDER BY single column descending
#[test]
fn t611_05_order_by_desc() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a DESC) FROM t",
        "T611-05",
        "ORDER BY descending"
    );
}

/// T611-05: ORDER BY multiple columns
#[test]
fn t611_05_order_by_multiple() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a DESC, b ASC) FROM t",
        "T611-05",
        "ORDER BY multiple columns"
    );
}

/// T611-05: ORDER BY with NULLS FIRST
#[test]
fn t611_05_order_by_nulls_first() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a NULLS FIRST) FROM t",
        "T611-05",
        "ORDER BY with NULLS FIRST"
    );
}

/// T611-05: ORDER BY with NULLS LAST
#[test]
fn t611_05_order_by_nulls_last() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a NULLS LAST) FROM t",
        "T611-05",
        "ORDER BY with NULLS LAST"
    );
}

/// T611-05: PARTITION BY and ORDER BY combined
#[test]
fn t611_05_partition_and_order() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T611-05",
        "PARTITION BY and ORDER BY combined"
    );
}

// ----------------------------------------------------------------------------
// T611-06: Window frame: ROWS BETWEEN
// ----------------------------------------------------------------------------

/// T611-06: ROWS BETWEEN with bounded range
#[test]
fn t611_06_rows_between_bounded() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        "T611-06",
        "ROWS BETWEEN bounded range"
    );
}

/// T611-06: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
#[test]
fn t611_06_rows_unbounded_preceding_current() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "T611-06",
        "ROWS UNBOUNDED PRECEDING to CURRENT ROW"
    );
}

/// T611-06: ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
#[test]
fn t611_06_rows_current_unbounded_following() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        "T611-06",
        "ROWS CURRENT ROW to UNBOUNDED FOLLOWING"
    );
}

/// T611-06: ROWS BETWEEN with asymmetric bounds
#[test]
fn t611_06_rows_asymmetric() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) FROM t",
        "T611-06",
        "ROWS BETWEEN asymmetric bounds"
    );
}

/// T611-06: ROWS N PRECEDING (shorthand)
#[test]
fn t611_06_rows_n_preceding() {
    assert_feature_supported!(
        "SELECT AVG(a) OVER (ORDER BY a ROWS 3 PRECEDING) FROM t",
        "T611-06",
        "ROWS N PRECEDING shorthand"
    );
}

/// T611-06: ROWS CURRENT ROW (single row frame)
#[test]
fn t611_06_rows_current_row() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS CURRENT ROW) FROM t",
        "T611-06",
        "ROWS CURRENT ROW"
    );
}

// ----------------------------------------------------------------------------
// T611-07: UNBOUNDED PRECEDING
// ----------------------------------------------------------------------------

/// T611-07: UNBOUNDED PRECEDING in ROWS frame
#[test]
fn t611_07_unbounded_preceding_rows() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS UNBOUNDED PRECEDING) FROM t",
        "T611-07",
        "UNBOUNDED PRECEDING in ROWS frame"
    );
}

/// T611-07: UNBOUNDED PRECEDING with partition
#[test]
fn t611_07_unbounded_preceding_partition() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c ORDER BY a ROWS UNBOUNDED PRECEDING) FROM t",
        "T611-07",
        "UNBOUNDED PRECEDING with partition"
    );
}

/// T611-07: Running total using UNBOUNDED PRECEDING
#[test]
fn t611_07_running_total() {
    assert_feature_supported!(
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM t",
        "T611-07",
        "Running total with UNBOUNDED PRECEDING"
    );
}

// ----------------------------------------------------------------------------
// T611-08: CURRENT ROW
// ----------------------------------------------------------------------------

/// T611-08: CURRENT ROW as frame start
#[test]
fn t611_08_current_row_start() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM t",
        "T611-08",
        "CURRENT ROW as frame start"
    );
}

/// T611-08: CURRENT ROW as frame end
#[test]
fn t611_08_current_row_end() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
        "T611-08",
        "CURRENT ROW as frame end"
    );
}

/// T611-08: CURRENT ROW as both start and end
#[test]
fn t611_08_current_row_both() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM t",
        "T611-08",
        "CURRENT ROW as both bounds"
    );
}

// ----------------------------------------------------------------------------
// T611-09: UNBOUNDED FOLLOWING
// ----------------------------------------------------------------------------

/// T611-09: UNBOUNDED FOLLOWING in ROWS frame
#[test]
fn t611_09_unbounded_following_rows() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        "T611-09",
        "UNBOUNDED FOLLOWING in ROWS frame"
    );
}

/// T611-09: UNBOUNDED FOLLOWING with partition
#[test]
fn t611_09_unbounded_following_partition() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        "T611-09",
        "UNBOUNDED FOLLOWING with partition"
    );
}

/// T611-09: Reverse running total using UNBOUNDED FOLLOWING
#[test]
fn t611_09_reverse_running_total() {
    assert_feature_supported!(
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS reverse_total FROM t",
        "T611-09",
        "Reverse running total"
    );
}

// ============================================================================
// T612: Advanced OLAP operations
// ============================================================================

// ----------------------------------------------------------------------------
// T612-01: NTILE(n) function
// ----------------------------------------------------------------------------

/// T612-01: Basic NTILE() function
#[test]
fn t612_01_ntile_basic() {
    assert_feature_supported!(
        "SELECT NTILE(4) OVER (ORDER BY a) FROM t",
        "T612-01",
        "NTILE() function"
    );
}

/// T612-01: NTILE() with partition
#[test]
fn t612_01_ntile_with_partition() {
    assert_feature_supported!(
        "SELECT NTILE(3) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-01",
        "NTILE() with PARTITION BY"
    );
}

/// T612-01: NTILE() for quartiles
#[test]
fn t612_01_ntile_quartiles() {
    assert_feature_supported!(
        "SELECT salary, NTILE(4) OVER (ORDER BY salary) AS quartile FROM person",
        "T612-01",
        "NTILE() for quartiles"
    );
}

/// T612-01: NTILE() for deciles
#[test]
fn t612_01_ntile_deciles() {
    assert_feature_supported!(
        "SELECT a, NTILE(10) OVER (ORDER BY a) AS decile FROM t",
        "T612-01",
        "NTILE() for deciles"
    );
}

// ----------------------------------------------------------------------------
// T612-02: LEAD(col, offset, default) function
// ----------------------------------------------------------------------------

/// T612-02: Basic LEAD() function
#[test]
fn t612_02_lead_basic() {
    assert_feature_supported!(
        "SELECT LEAD(a) OVER (ORDER BY a) FROM t",
        "T612-02",
        "LEAD() function"
    );
}

/// T612-02: LEAD() with offset
#[test]
fn t612_02_lead_with_offset() {
    assert_feature_supported!(
        "SELECT LEAD(a, 2) OVER (ORDER BY a) FROM t",
        "T612-02",
        "LEAD() with offset"
    );
}

/// T612-02: LEAD() with offset and default
#[test]
fn t612_02_lead_with_default() {
    assert_feature_supported!(
        "SELECT LEAD(a, 1, 0) OVER (ORDER BY a) FROM t",
        "T612-02",
        "LEAD() with default value"
    );
}

/// T612-02: LEAD() with partition
#[test]
fn t612_02_lead_with_partition() {
    assert_feature_supported!(
        "SELECT LEAD(a) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-02",
        "LEAD() with PARTITION BY"
    );
}

/// T612-02: LEAD() for comparing to next row
#[test]
fn t612_02_lead_comparison() {
    assert_feature_supported!(
        "SELECT a, LEAD(a) OVER (ORDER BY a) AS next_a, LEAD(a) OVER (ORDER BY a) - a AS diff FROM t",
        "T612-02",
        "LEAD() for row comparison"
    );
}

// ----------------------------------------------------------------------------
// T612-03: LAG(col, offset, default) function
// ----------------------------------------------------------------------------

/// T612-03: Basic LAG() function
#[test]
fn t612_03_lag_basic() {
    assert_feature_supported!(
        "SELECT LAG(a) OVER (ORDER BY a) FROM t",
        "T612-03",
        "LAG() function"
    );
}

/// T612-03: LAG() with offset
#[test]
fn t612_03_lag_with_offset() {
    assert_feature_supported!(
        "SELECT LAG(a, 2) OVER (ORDER BY a) FROM t",
        "T612-03",
        "LAG() with offset"
    );
}

/// T612-03: LAG() with offset and default
#[test]
fn t612_03_lag_with_default() {
    assert_feature_supported!(
        "SELECT LAG(a, 1, 0) OVER (ORDER BY a) FROM t",
        "T612-03",
        "LAG() with default value"
    );
}

/// T612-03: LAG() with partition
#[test]
fn t612_03_lag_with_partition() {
    assert_feature_supported!(
        "SELECT LAG(a) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-03",
        "LAG() with PARTITION BY"
    );
}

/// T612-03: LAG() for comparing to previous row
#[test]
fn t612_03_lag_comparison() {
    assert_feature_supported!(
        "SELECT a, LAG(a) OVER (ORDER BY a) AS prev_a, a - LAG(a) OVER (ORDER BY a) AS diff FROM t",
        "T612-03",
        "LAG() for row comparison"
    );
}

/// T612-03: LAG() and LEAD() together
#[test]
fn t612_03_lag_and_lead() {
    assert_feature_supported!(
        "SELECT LAG(a) OVER (ORDER BY a) AS prev, a, LEAD(a) OVER (ORDER BY a) AS next FROM t",
        "T612-03",
        "LAG() and LEAD() together"
    );
}

// ----------------------------------------------------------------------------
// T612-04: FIRST_VALUE(col) function
// ----------------------------------------------------------------------------

/// T612-04: Basic FIRST_VALUE() function
#[test]
fn t612_04_first_value_basic() {
    assert_feature_supported!(
        "SELECT FIRST_VALUE(a) OVER (ORDER BY a) FROM t",
        "T612-04",
        "FIRST_VALUE() function"
    );
}

/// T612-04: FIRST_VALUE() with partition
#[test]
fn t612_04_first_value_with_partition() {
    assert_feature_supported!(
        "SELECT FIRST_VALUE(a) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-04",
        "FIRST_VALUE() with PARTITION BY"
    );
}

/// T612-04: FIRST_VALUE() with frame
#[test]
fn t612_04_first_value_with_frame() {
    assert_feature_supported!(
        "SELECT FIRST_VALUE(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t",
        "T612-04",
        "FIRST_VALUE() with frame"
    );
}

/// T612-04: FIRST_VALUE() for comparing to partition first
#[test]
fn t612_04_first_value_comparison() {
    assert_feature_supported!(
        "SELECT a, FIRST_VALUE(a) OVER (PARTITION BY c ORDER BY a) AS first_in_partition FROM t",
        "T612-04",
        "FIRST_VALUE() comparison"
    );
}

// ----------------------------------------------------------------------------
// T612-05: LAST_VALUE(col) function
// ----------------------------------------------------------------------------

/// T612-05: Basic LAST_VALUE() function
#[test]
fn t612_05_last_value_basic() {
    assert_feature_supported!(
        "SELECT LAST_VALUE(a) OVER (ORDER BY a) FROM t",
        "T612-05",
        "LAST_VALUE() function"
    );
}

/// T612-05: LAST_VALUE() with partition
#[test]
fn t612_05_last_value_with_partition() {
    assert_feature_supported!(
        "SELECT LAST_VALUE(a) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-05",
        "LAST_VALUE() with PARTITION BY"
    );
}

/// T612-05: LAST_VALUE() with full partition frame
#[test]
fn t612_05_last_value_full_frame() {
    assert_feature_supported!(
        "SELECT LAST_VALUE(a) OVER (PARTITION BY c ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        "T612-05",
        "LAST_VALUE() with full frame"
    );
}

/// T612-05: LAST_VALUE() with bounded frame
#[test]
fn t612_05_last_value_with_frame() {
    assert_feature_supported!(
        "SELECT LAST_VALUE(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t",
        "T612-05",
        "LAST_VALUE() with bounded frame"
    );
}

/// T612-05: FIRST_VALUE() and LAST_VALUE() together
#[test]
fn t612_05_first_and_last_value() {
    assert_feature_supported!(
        "SELECT FIRST_VALUE(a) OVER w AS first, LAST_VALUE(a) OVER w AS last FROM t WINDOW w AS (PARTITION BY c ORDER BY a)",
        "T612-05",
        "FIRST_VALUE() and LAST_VALUE() together"
    );
}

// ----------------------------------------------------------------------------
// T612-06: NTH_VALUE(col, n) function
// ----------------------------------------------------------------------------

/// T612-06: Basic NTH_VALUE() function
#[test]
fn t612_06_nth_value_basic() {
    assert_feature_supported!(
        "SELECT NTH_VALUE(a, 2) OVER (ORDER BY a) FROM t",
        "T612-06",
        "NTH_VALUE() function"
    );
}

/// T612-06: NTH_VALUE() with partition
#[test]
fn t612_06_nth_value_with_partition() {
    assert_feature_supported!(
        "SELECT NTH_VALUE(a, 3) OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-06",
        "NTH_VALUE() with PARTITION BY"
    );
}

/// T612-06: NTH_VALUE() with frame
#[test]
fn t612_06_nth_value_with_frame() {
    assert_feature_supported!(
        "SELECT NTH_VALUE(a, 2) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        "T612-06",
        "NTH_VALUE() with frame"
    );
}

/// T612-06: NTH_VALUE() for median approximation
#[test]
fn t612_06_nth_value_median() {
    assert_feature_supported!(
        "SELECT NTH_VALUE(salary, 50) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM person",
        "T612-06",
        "NTH_VALUE() for median"
    );
}

// ----------------------------------------------------------------------------
// T612-07: PERCENT_RANK() function
// ----------------------------------------------------------------------------

/// T612-07: Basic PERCENT_RANK() function
#[test]
fn t612_07_percent_rank_basic() {
    assert_feature_supported!(
        "SELECT PERCENT_RANK() OVER (ORDER BY a) FROM t",
        "T612-07",
        "PERCENT_RANK() function"
    );
}

/// T612-07: PERCENT_RANK() with partition
#[test]
fn t612_07_percent_rank_with_partition() {
    assert_feature_supported!(
        "SELECT PERCENT_RANK() OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-07",
        "PERCENT_RANK() with PARTITION BY"
    );
}

/// T612-07: PERCENT_RANK() for percentile calculation
#[test]
fn t612_07_percent_rank_percentile() {
    assert_feature_supported!(
        "SELECT salary, PERCENT_RANK() OVER (ORDER BY salary) AS percentile FROM person",
        "T612-07",
        "PERCENT_RANK() for percentiles"
    );
}

/// T612-07: PERCENT_RANK() with multiple order columns
#[test]
fn t612_07_percent_rank_multiple_order() {
    assert_feature_supported!(
        "SELECT PERCENT_RANK() OVER (ORDER BY a DESC, b ASC) FROM t",
        "T612-07",
        "PERCENT_RANK() with multiple ORDER BY"
    );
}

// ----------------------------------------------------------------------------
// T612-08: CUME_DIST() function
// ----------------------------------------------------------------------------

/// T612-08: Basic CUME_DIST() function
#[test]
fn t612_08_cume_dist_basic() {
    assert_feature_supported!(
        "SELECT CUME_DIST() OVER (ORDER BY a) FROM t",
        "T612-08",
        "CUME_DIST() function"
    );
}

/// T612-08: CUME_DIST() with partition
#[test]
fn t612_08_cume_dist_with_partition() {
    assert_feature_supported!(
        "SELECT CUME_DIST() OVER (PARTITION BY c ORDER BY a) FROM t",
        "T612-08",
        "CUME_DIST() with PARTITION BY"
    );
}

/// T612-08: CUME_DIST() vs PERCENT_RANK()
#[test]
fn t612_08_cume_dist_vs_percent_rank() {
    assert_feature_supported!(
        "SELECT CUME_DIST() OVER (ORDER BY a) AS cume, PERCENT_RANK() OVER (ORDER BY a) AS pct FROM t",
        "T612-08",
        "CUME_DIST() vs PERCENT_RANK()"
    );
}

/// T612-08: CUME_DIST() for cumulative distribution
#[test]
fn t612_08_cume_dist_distribution() {
    assert_feature_supported!(
        "SELECT salary, CUME_DIST() OVER (ORDER BY salary) AS cumulative_dist FROM person",
        "T612-08",
        "CUME_DIST() for distribution"
    );
}

// ----------------------------------------------------------------------------
// T612-09: Window frame: RANGE BETWEEN
// ----------------------------------------------------------------------------

/// T612-09: RANGE BETWEEN with UNBOUNDED PRECEDING
#[test]
fn t612_09_range_unbounded_preceding() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "T612-09",
        "RANGE UNBOUNDED PRECEDING"
    );
}

/// T612-09: RANGE BETWEEN with UNBOUNDED FOLLOWING
#[test]
fn t612_09_range_unbounded_following() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        "T612-09",
        "RANGE UNBOUNDED FOLLOWING"
    );
}

/// T612-09: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
#[test]
fn t612_09_range_unbounded_both() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        "T612-09",
        "RANGE full frame"
    );
}

/// T612-09: RANGE CURRENT ROW
#[test]
fn t612_09_range_current_row() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a RANGE CURRENT ROW) FROM t",
        "T612-09",
        "RANGE CURRENT ROW"
    );
}

/// T612-09: RANGE with partition
#[test]
fn t612_09_range_with_partition() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "T612-09",
        "RANGE with PARTITION BY"
    );
}

// ----------------------------------------------------------------------------
// T612-10: Window frame: GROUPS BETWEEN (SQL:2011)
// ----------------------------------------------------------------------------

/// T612-10: GROUPS BETWEEN with UNBOUNDED PRECEDING
#[test]
fn t612_10_groups_unbounded_preceding() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "T612-10",
        "GROUPS UNBOUNDED PRECEDING"
    );
}

/// T612-10: GROUPS BETWEEN bounded
#[test]
fn t612_10_groups_bounded() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        "T612-10",
        "GROUPS BETWEEN bounded"
    );
}

/// T612-10: GROUPS CURRENT ROW
#[test]
fn t612_10_groups_current_row() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a GROUPS CURRENT ROW) FROM t",
        "T612-10",
        "GROUPS CURRENT ROW"
    );
}

/// T612-10: GROUPS with partition
#[test]
fn t612_10_groups_with_partition() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (PARTITION BY c ORDER BY a GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t",
        "T612-10",
        "GROUPS with PARTITION BY"
    );
}

// ----------------------------------------------------------------------------
// T612-11: Named window definitions (WINDOW clause)
// ----------------------------------------------------------------------------

/// T612-11: Basic WINDOW clause
#[test]
fn t612_11_window_clause_basic() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER w FROM t WINDOW w AS (ORDER BY a)",
        "T612-11",
        "Named window definition"
    );
}

/// T612-11: Multiple named windows
#[test]
fn t612_11_multiple_named_windows() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER w1, AVG(a) OVER w2 FROM t WINDOW w1 AS (ORDER BY a), w2 AS (PARTITION BY c)",
        "T612-11",
        "Multiple named windows"
    );
}

/// T612-11: Named window with partition and order
#[test]
fn t612_11_window_partition_order() {
    assert_feature_supported!(
        "SELECT RANK() OVER w FROM t WINDOW w AS (PARTITION BY c ORDER BY a)",
        "T612-11",
        "Named window with PARTITION and ORDER"
    );
}

/// T612-11: Named window with frame
#[test]
fn t612_11_window_with_frame() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER w FROM t WINDOW w AS (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)",
        "T612-11",
        "Named window with frame"
    );
}

/// T612-11: Reusing named window
#[test]
fn t612_11_reuse_named_window() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER w, COUNT(*) OVER w, AVG(a) OVER w FROM t WINDOW w AS (PARTITION BY c ORDER BY a)",
        "T612-11",
        "Reusing named window"
    );
}

// ----------------------------------------------------------------------------
// T612-12: Aggregate functions as window functions
// ----------------------------------------------------------------------------

/// T612-12: SUM() as window function
#[test]
fn t612_12_sum_over() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a) FROM t",
        "T612-12",
        "SUM() OVER"
    );
}

/// T612-12: COUNT() as window function
#[test]
fn t612_12_count_over() {
    assert_feature_supported!(
        "SELECT COUNT(*) OVER (ORDER BY a) FROM t",
        "T612-12",
        "COUNT() OVER"
    );
}

/// T612-12: AVG() as window function
#[test]
fn t612_12_avg_over() {
    assert_feature_supported!(
        "SELECT AVG(a) OVER (ORDER BY a) FROM t",
        "T612-12",
        "AVG() OVER"
    );
}

/// T612-12: MIN() as window function
#[test]
fn t612_12_min_over() {
    assert_feature_supported!(
        "SELECT MIN(a) OVER (ORDER BY a) FROM t",
        "T612-12",
        "MIN() OVER"
    );
}

/// T612-12: MAX() as window function
#[test]
fn t612_12_max_over() {
    assert_feature_supported!(
        "SELECT MAX(a) OVER (ORDER BY a) FROM t",
        "T612-12",
        "MAX() OVER"
    );
}

/// T612-12: Multiple aggregate window functions
#[test]
fn t612_12_multiple_aggregates() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a), AVG(a) OVER (ORDER BY a), COUNT(*) OVER (ORDER BY a) FROM t",
        "T612-12",
        "Multiple aggregate window functions"
    );
}

/// T612-12: Aggregate with DISTINCT as window function
#[test]
fn t612_12_aggregate_distinct() {
    assert_feature_supported!(
        "SELECT COUNT(DISTINCT a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "T612-12",
        "Aggregate with DISTINCT OVER"
    );
}

// ----------------------------------------------------------------------------
// T612-13: FILTER clause with window functions
// ----------------------------------------------------------------------------

/// T612-13: COUNT() with FILTER clause
#[test]
fn t612_13_count_filter() {
    assert_feature_supported!(
        "SELECT COUNT(*) FILTER (WHERE a > 10) OVER (ORDER BY a) FROM t",
        "T612-13",
        "COUNT() with FILTER in window"
    );
}

/// T612-13: SUM() with FILTER clause
#[test]
fn t612_13_sum_filter() {
    assert_feature_supported!(
        "SELECT SUM(a) FILTER (WHERE a > 0) OVER (ORDER BY a) FROM t",
        "T612-13",
        "SUM() with FILTER in window"
    );
}

/// T612-13: AVG() with FILTER clause
#[test]
fn t612_13_avg_filter() {
    assert_feature_supported!(
        "SELECT AVG(price) FILTER (WHERE price > 100) OVER (PARTITION BY customer_id ORDER BY order_id) FROM orders",
        "T612-13",
        "AVG() with FILTER in window"
    );
}

/// T612-13: Multiple filters in window functions
#[test]
fn t612_13_multiple_filters() {
    assert_feature_supported!(
        "SELECT COUNT(*) FILTER (WHERE a > 10) OVER (ORDER BY a), SUM(b) FILTER (WHERE b < 100) OVER (ORDER BY a) FROM t",
        "T612-13",
        "Multiple FILTER clauses"
    );
}

// ----------------------------------------------------------------------------
// T612-14: RESPECT NULLS / IGNORE NULLS
// ----------------------------------------------------------------------------

/// T612-14: FIRST_VALUE() with IGNORE NULLS
#[test]
fn t612_14_first_value_ignore_nulls() {
    assert_feature_supported!(
        "SELECT FIRST_VALUE(a IGNORE NULLS) OVER (ORDER BY a) FROM t",
        "T612-14",
        "FIRST_VALUE() IGNORE NULLS"
    );
}

/// T612-14: LAST_VALUE() with IGNORE NULLS
#[test]
fn t612_14_last_value_ignore_nulls() {
    assert_feature_supported!(
        "SELECT LAST_VALUE(a IGNORE NULLS) OVER (ORDER BY a) FROM t",
        "T612-14",
        "LAST_VALUE() IGNORE NULLS"
    );
}

/// T612-14: NTH_VALUE() with IGNORE NULLS
#[test]
fn t612_14_nth_value_ignore_nulls() {
    assert_feature_supported!(
        "SELECT NTH_VALUE(a IGNORE NULLS, 2) OVER (ORDER BY a) FROM t",
        "T612-14",
        "NTH_VALUE() IGNORE NULLS"
    );
}

/// T612-14: LEAD() with IGNORE NULLS
#[test]
fn t612_14_lead_ignore_nulls() {
    assert_feature_supported!(
        "SELECT LEAD(a IGNORE NULLS) OVER (ORDER BY a) FROM t",
        "T612-14",
        "LEAD() IGNORE NULLS"
    );
}

/// T612-14: LAG() with IGNORE NULLS
#[test]
fn t612_14_lag_ignore_nulls() {
    assert_feature_supported!(
        "SELECT LAG(a IGNORE NULLS) OVER (ORDER BY a) FROM t",
        "T612-14",
        "LAG() IGNORE NULLS"
    );
}

/// T612-14: FIRST_VALUE() with RESPECT NULLS
#[test]
fn t612_14_first_value_respect_nulls() {
    assert_feature_supported!(
        "SELECT FIRST_VALUE(a RESPECT NULLS) OVER (ORDER BY a) FROM t",
        "T612-14",
        "FIRST_VALUE() RESPECT NULLS"
    );
}

// ----------------------------------------------------------------------------
// T612-15: EXCLUDE clause
// ----------------------------------------------------------------------------

/// T612-15: EXCLUDE CURRENT ROW
#[test]
fn t612_15_exclude_current_row() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        "T612-15",
        "EXCLUDE CURRENT ROW"
    );
}

/// T612-15: EXCLUDE GROUP
#[test]
fn t612_15_exclude_group() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM t",
        "T612-15",
        "EXCLUDE GROUP"
    );
}

/// T612-15: EXCLUDE TIES
#[test]
fn t612_15_exclude_ties() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
        "T612-15",
        "EXCLUDE TIES"
    );
}

/// T612-15: EXCLUDE NO OTHERS
#[test]
fn t612_15_exclude_no_others() {
    assert_feature_supported!(
        "SELECT SUM(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE NO OTHERS) FROM t",
        "T612-15",
        "EXCLUDE NO OTHERS"
    );
}

// ============================================================================
// Complex scenarios and real-world use cases
// ============================================================================

/// Scenario: Running total by partition
#[test]
fn scenario_running_total_by_partition() {
    assert_feature_supported!(
        "SELECT c, a, SUM(a) OVER (PARTITION BY c ORDER BY a ROWS UNBOUNDED PRECEDING) AS running_total FROM t",
        "T611",
        "Running total by partition"
    );
}

/// Scenario: Moving average (3-period)
#[test]
fn scenario_moving_average_3period() {
    assert_feature_supported!(
        "SELECT a, AVG(a) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3 FROM t",
        "T611",
        "3-period moving average"
    );
}

/// Scenario: Ranking with ties handling
#[test]
fn scenario_ranking_with_ties() {
    assert_feature_supported!(
        "SELECT a, RANK() OVER (ORDER BY a) AS rnk, DENSE_RANK() OVER (ORDER BY a) AS dense_rnk, ROW_NUMBER() OVER (ORDER BY a) AS row_num FROM t",
        "T611",
        "Ranking with ties"
    );
}

/// Scenario: Partitioned top-N per group
#[test]
fn scenario_partitioned_top_n() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT c, a, ROW_NUMBER() OVER (PARTITION BY c ORDER BY a DESC) AS rn FROM t) WHERE rn <= 3",
        "T611",
        "Top 3 per partition"
    );
}

/// Scenario: Year-over-year comparison using LAG
#[test]
fn scenario_year_over_year() {
    assert_feature_supported!(
        "SELECT order_id, price, LAG(price, 12) OVER (PARTITION BY customer_id ORDER BY order_id) AS prev_year_price FROM orders",
        "T612",
        "Year-over-year comparison"
    );
}

/// Scenario: Cumulative distribution by category
#[test]
fn scenario_cumulative_distribution() {
    assert_feature_supported!(
        "SELECT salary, state, CUME_DIST() OVER (PARTITION BY state ORDER BY salary) AS salary_percentile FROM person",
        "T612",
        "Cumulative distribution by state"
    );
}

/// Scenario: Gap analysis with LEAD and LAG
#[test]
fn scenario_gap_analysis() {
    assert_feature_supported!(
        "SELECT a, LAG(a) OVER (ORDER BY a) AS prev, LEAD(a) OVER (ORDER BY a) AS next, LEAD(a) OVER (ORDER BY a) - LAG(a) OVER (ORDER BY a) AS gap FROM t",
        "T612",
        "Gap analysis"
    );
}

/// Scenario: Percentile buckets with NTILE
#[test]
fn scenario_percentile_buckets() {
    assert_feature_supported!(
        "SELECT salary, NTILE(100) OVER (ORDER BY salary) AS percentile, CASE WHEN NTILE(4) OVER (ORDER BY salary) = 1 THEN 'Q1' WHEN NTILE(4) OVER (ORDER BY salary) = 2 THEN 'Q2' WHEN NTILE(4) OVER (ORDER BY salary) = 3 THEN 'Q3' ELSE 'Q4' END AS quartile FROM person",
        "T612",
        "Percentile buckets"
    );
}

/// Scenario: Rolling window with different aggregates
#[test]
fn scenario_rolling_window_multi_aggregate() {
    assert_feature_supported!(
        "SELECT a, SUM(a) OVER w AS sum, AVG(a) OVER w AS avg, MIN(a) OVER w AS min, MAX(a) OVER w AS max, COUNT(*) OVER w AS cnt FROM t WINDOW w AS (ORDER BY a ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING)",
        "T612",
        "Rolling window multi-aggregate"
    );
}

/// Scenario: First and last in partition
#[test]
fn scenario_first_last_in_partition() {
    assert_feature_supported!(
        "SELECT c, a, FIRST_VALUE(a) OVER w AS first, LAST_VALUE(a) OVER w AS last FROM t WINDOW w AS (PARTITION BY c ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        "T612",
        "First and last in partition"
    );
}

/// Scenario: Complex partitioning and ordering
#[test]
fn scenario_complex_partition_order() {
    assert_feature_supported!(
        "SELECT state, first_name, salary, RANK() OVER (PARTITION BY state ORDER BY salary DESC, first_name ASC) AS salary_rank FROM person",
        "T611",
        "Complex partition and order"
    );
}

/// Scenario: Multiple window functions with different specifications
#[test]
fn scenario_multiple_different_windows() {
    assert_feature_supported!(
        "SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS global_rn, ROW_NUMBER() OVER (PARTITION BY c ORDER BY a) AS partition_rn, SUM(a) OVER (ORDER BY a) AS running_sum, AVG(a) OVER (PARTITION BY c) AS partition_avg FROM t",
        "T611",
        "Multiple different windows"
    );
}

/// Scenario: Window function in WHERE via subquery
#[test]
fn scenario_window_in_where_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT a, b, c, ROW_NUMBER() OVER (PARTITION BY c ORDER BY a DESC) AS rn FROM t) subq WHERE rn = 1 AND a > 10",
        "T611",
        "Window function filtered in WHERE"
    );
}

/// Scenario: Running totals with RANGE frame
#[test]
fn scenario_running_total_range() {
    assert_feature_supported!(
        "SELECT a, SUM(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_sum FROM t",
        "T612",
        "Running total with RANGE"
    );
}

/// Scenario: Comparison to window aggregate
#[test]
fn scenario_compare_to_aggregate() {
    assert_feature_supported!(
        "SELECT a, c, AVG(a) OVER (PARTITION BY c) AS category_avg, a - AVG(a) OVER (PARTITION BY c) AS deviation FROM t",
        "T612",
        "Deviation from category average"
    );
}

/// Scenario: Conditional aggregation with FILTER in window
#[test]
fn scenario_conditional_window_aggregate() {
    assert_feature_supported!(
        "SELECT order_id, customer_id, SUM(price) FILTER (WHERE qty > 1) OVER (PARTITION BY customer_id ORDER BY order_id) AS bulk_order_total FROM orders",
        "T612",
        "Conditional window aggregation"
    );
}

/// Scenario: Using NTILE for equal distribution
#[test]
fn scenario_ntile_distribution() {
    assert_feature_supported!(
        "SELECT first_name, salary, NTILE(5) OVER (ORDER BY salary) AS quintile FROM person",
        "T612",
        "Salary quintiles"
    );
}

/// Scenario: Window function with NULL handling
#[test]
fn scenario_window_null_handling() {
    assert_feature_supported!(
        "SELECT a, FIRST_VALUE(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first, LAST_VALUE(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last FROM t",
        "T612",
        "Window function with NULLs"
    );
}

/// Scenario: Rank dense rank and row number comparison
#[test]
fn scenario_rank_comparison() {
    assert_feature_supported!(
        "SELECT salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num, RANK() OVER (ORDER BY salary DESC) AS rank, DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank FROM person",
        "T611",
        "Ranking function comparison"
    );
}

/// Scenario: Weighted moving average
#[test]
fn scenario_weighted_moving_average() {
    assert_feature_supported!(
        "SELECT a, SUM(a * b) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) / SUM(b) OVER (ORDER BY a ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS weighted_avg FROM t",
        "T612",
        "Weighted moving average"
    );
}
