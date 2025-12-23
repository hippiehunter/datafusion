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

//! SQL:2016 Feature F261 - CASE expression
//!
//! ISO/IEC 9075-2:2016 Section 6.12
//!
//! This feature covers the CASE expression and related conditional functions:
//! - Simple CASE (CASE expr WHEN value THEN result...)
//! - Searched CASE (CASE WHEN condition THEN result...)
//! - NULLIF function
//! - COALESCE function
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F261-01 | Simple CASE | Supported |
//! | F261-02 | Searched CASE | Supported |
//! | F261-03 | NULLIF function | Supported |
//! | F261-04 | COALESCE function | Supported |
//!
//! All F261 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// F261-01: Simple CASE
// ============================================================================

/// F261-01: Basic simple CASE expression
#[test]
fn f261_01_simple_case_basic() {
    assert_feature_supported!(
        "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t",
        "F261-01",
        "Simple CASE expression"
    );
}

/// F261-01: Simple CASE with numeric result
#[test]
fn f261_01_simple_case_numeric() {
    assert_feature_supported!(
        "SELECT CASE state WHEN 'CA' THEN 100 WHEN 'NY' THEN 200 WHEN 'TX' THEN 300 ELSE 0 END FROM person",
        "F261-01",
        "Simple CASE with numeric result"
    );
}

/// F261-01: Simple CASE without ELSE clause
#[test]
fn f261_01_simple_case_no_else() {
    assert_feature_supported!(
        "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t",
        "F261-01",
        "Simple CASE without ELSE"
    );
}

/// F261-01: Simple CASE with single WHEN
#[test]
fn f261_01_simple_case_single_when() {
    assert_feature_supported!(
        "SELECT CASE state WHEN 'CA' THEN 'California' ELSE state END FROM person",
        "F261-01",
        "Simple CASE with single WHEN"
    );
}

/// F261-01: Simple CASE in WHERE clause
#[test]
fn f261_01_simple_case_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE CASE a WHEN 1 THEN b WHEN 2 THEN c ELSE a END > 10",
        "F261-01",
        "Simple CASE in WHERE clause"
    );
}

/// F261-01: Simple CASE with column expression
#[test]
fn f261_01_simple_case_column_expr() {
    assert_feature_supported!(
        "SELECT CASE a + b WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END FROM t",
        "F261-01",
        "Simple CASE with expression"
    );
}

/// F261-01: Simple CASE with NULL in WHEN
#[test]
fn f261_01_simple_case_null_when() {
    assert_feature_supported!(
        "SELECT CASE a WHEN NULL THEN 'is null' WHEN 1 THEN 'one' ELSE 'other' END FROM t",
        "F261-01",
        "Simple CASE with NULL in WHEN"
    );
}

/// F261-01: Simple CASE with multiple columns in result
#[test]
fn f261_01_simple_case_multiple_results() {
    assert_feature_supported!(
        "SELECT CASE state WHEN 'CA' THEN first_name WHEN 'NY' THEN last_name ELSE 'Unknown' END FROM person",
        "F261-01",
        "Simple CASE with column results"
    );
}

// ============================================================================
// F261-02: Searched CASE
// ============================================================================

/// F261-02: Basic searched CASE expression
#[test]
fn f261_02_searched_case_basic() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN 'high' WHEN a > 5 THEN 'medium' ELSE 'low' END FROM t",
        "F261-02",
        "Searched CASE expression"
    );
}

/// F261-02: Searched CASE with complex conditions
#[test]
fn f261_02_searched_case_complex() {
    assert_feature_supported!(
        "SELECT CASE WHEN age < 18 THEN 'minor' WHEN age >= 18 AND age < 65 THEN 'adult' WHEN age >= 65 THEN 'senior' END FROM person",
        "F261-02",
        "Searched CASE with complex conditions"
    );
}

/// F261-02: Searched CASE without ELSE
#[test]
fn f261_02_searched_case_no_else() {
    assert_feature_supported!(
        "SELECT CASE WHEN salary > 100000 THEN 'high earner' WHEN salary > 50000 THEN 'medium earner' END FROM person",
        "F261-02",
        "Searched CASE without ELSE"
    );
}

/// F261-02: Searched CASE with NULL conditions
#[test]
fn f261_02_searched_case_null() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS NULL THEN 'null' WHEN a > 0 THEN 'positive' ELSE 'negative or zero' END FROM t",
        "F261-02",
        "Searched CASE with NULL condition"
    );
}

/// F261-02: Searched CASE in SELECT list with multiple cases
#[test]
fn f261_02_searched_case_multiple() {
    assert_feature_supported!(
        "SELECT CASE WHEN age < 18 THEN 'Y' ELSE 'N' END AS is_minor, CASE WHEN salary > 50000 THEN 'Y' ELSE 'N' END AS high_earner FROM person",
        "F261-02",
        "Multiple searched CASE expressions"
    );
}

/// F261-02: Searched CASE in WHERE clause
#[test]
fn f261_02_searched_case_in_where() {
    assert_feature_supported!(
        "SELECT * FROM person WHERE CASE WHEN state = 'CA' THEN salary > 80000 WHEN state = 'NY' THEN salary > 90000 ELSE salary > 60000 END",
        "F261-02",
        "Searched CASE in WHERE clause"
    );
}

/// F261-02: Searched CASE with BETWEEN
#[test]
fn f261_02_searched_case_between() {
    assert_feature_supported!(
        "SELECT CASE WHEN age BETWEEN 0 AND 12 THEN 'child' WHEN age BETWEEN 13 AND 19 THEN 'teen' ELSE 'adult' END FROM person",
        "F261-02",
        "Searched CASE with BETWEEN"
    );
}

/// F261-02: Searched CASE with IN
#[test]
fn f261_02_searched_case_in() {
    assert_feature_supported!(
        "SELECT CASE WHEN state IN ('CA', 'OR', 'WA') THEN 'West Coast' WHEN state IN ('NY', 'NJ', 'MA') THEN 'East Coast' ELSE 'Other' END FROM person",
        "F261-02",
        "Searched CASE with IN"
    );
}

/// F261-02: Searched CASE with LIKE
#[test]
fn f261_02_searched_case_like() {
    assert_feature_supported!(
        "SELECT CASE WHEN first_name LIKE 'J%' THEN 'Starts with J' WHEN first_name LIKE '%n' THEN 'Ends with n' ELSE 'Other' END FROM person",
        "F261-02",
        "Searched CASE with LIKE"
    );
}

/// F261-02: Searched CASE with subquery
#[test]
fn f261_02_searched_case_subquery() {
    assert_feature_supported!(
        "SELECT CASE WHEN salary > (SELECT AVG(salary) FROM person) THEN 'above average' ELSE 'below average' END FROM person",
        "F261-02",
        "Searched CASE with subquery"
    );
}

// ============================================================================
// F261-03: NULLIF function
// ============================================================================

/// F261-03: Basic NULLIF
#[test]
fn f261_03_nullif_basic() {
    assert_feature_supported!(
        "SELECT NULLIF(a, b) FROM t",
        "F261-03",
        "Basic NULLIF function"
    );
}

/// F261-03: NULLIF with constants
#[test]
fn f261_03_nullif_constants() {
    assert_feature_supported!(
        "SELECT NULLIF(a, 0) FROM t",
        "F261-03",
        "NULLIF with constant"
    );
}

/// F261-03: NULLIF with strings
#[test]
fn f261_03_nullif_strings() {
    assert_feature_supported!(
        "SELECT NULLIF(first_name, '') FROM person",
        "F261-03",
        "NULLIF with strings"
    );
}

/// F261-03: NULLIF in expression
#[test]
fn f261_03_nullif_in_expression() {
    assert_feature_supported!(
        "SELECT 100 / NULLIF(a, 0) FROM t",
        "F261-03",
        "NULLIF to avoid division by zero"
    );
}

/// F261-03: NULLIF with NULL literal
#[test]
fn f261_03_nullif_with_null() {
    assert_feature_supported!(
        "SELECT NULLIF(a, NULL) FROM t",
        "F261-03",
        "NULLIF with NULL literal"
    );
}

/// F261-03: Multiple NULLIF in SELECT
#[test]
fn f261_03_nullif_multiple() {
    assert_feature_supported!(
        "SELECT NULLIF(a, 0), NULLIF(b, -1), NULLIF(c, '') FROM t",
        "F261-03",
        "Multiple NULLIF functions"
    );
}

/// F261-03: NULLIF in WHERE clause
#[test]
fn f261_03_nullif_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE NULLIF(a, b) IS NOT NULL",
        "F261-03",
        "NULLIF in WHERE clause"
    );
}

/// F261-03: NULLIF with expressions
#[test]
fn f261_03_nullif_expressions() {
    assert_feature_supported!(
        "SELECT NULLIF(a + b, 10) FROM t",
        "F261-03",
        "NULLIF with expression arguments"
    );
}

// ============================================================================
// F261-04: COALESCE function
// ============================================================================

/// F261-04: Basic COALESCE with two arguments
#[test]
fn f261_04_coalesce_two_args() {
    assert_feature_supported!(
        "SELECT COALESCE(a, 0) FROM t",
        "F261-04",
        "COALESCE with two arguments"
    );
}

/// F261-04: COALESCE with three arguments
#[test]
fn f261_04_coalesce_three_args() {
    assert_feature_supported!(
        "SELECT COALESCE(a, b, 0) FROM t",
        "F261-04",
        "COALESCE with three arguments"
    );
}

/// F261-04: COALESCE with many arguments
#[test]
fn f261_04_coalesce_many_args() {
    assert_feature_supported!(
        "SELECT COALESCE(a, b, c, 0) FROM t",
        "F261-04",
        "COALESCE with multiple arguments"
    );
}

/// F261-04: COALESCE with strings
#[test]
fn f261_04_coalesce_strings() {
    assert_feature_supported!(
        "SELECT COALESCE(first_name, last_name, 'Unknown') FROM person",
        "F261-04",
        "COALESCE with strings"
    );
}

/// F261-04: COALESCE in WHERE clause
#[test]
fn f261_04_coalesce_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE COALESCE(a, b, 0) > 10",
        "F261-04",
        "COALESCE in WHERE clause"
    );
}

/// F261-04: COALESCE with expressions
#[test]
fn f261_04_coalesce_expressions() {
    assert_feature_supported!(
        "SELECT COALESCE(a + b, a, b, 0) FROM t",
        "F261-04",
        "COALESCE with expressions"
    );
}

/// F261-04: Multiple COALESCE in SELECT
#[test]
fn f261_04_coalesce_multiple() {
    assert_feature_supported!(
        "SELECT COALESCE(a, 0) AS col1, COALESCE(b, c, 1) AS col2 FROM t",
        "F261-04",
        "Multiple COALESCE functions"
    );
}

/// F261-04: COALESCE with subquery
#[test]
fn f261_04_coalesce_subquery() {
    assert_feature_supported!(
        "SELECT COALESCE((SELECT MAX(a) FROM t), 0) FROM t",
        "F261-04",
        "COALESCE with scalar subquery"
    );
}

/// F261-04: COALESCE with NULL literal
#[test]
fn f261_04_coalesce_null_literal() {
    assert_feature_supported!(
        "SELECT COALESCE(NULL, a, NULL, b, 0) FROM t",
        "F261-04",
        "COALESCE with NULL literals"
    );
}

// ============================================================================
// F261 Advanced Tests: Nested CASE
// ============================================================================

/// F261: Nested CASE expressions
#[test]
fn f261_nested_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN CASE WHEN b > 5 THEN 'A' ELSE 'B' END ELSE CASE WHEN b > 5 THEN 'C' ELSE 'D' END END FROM t",
        "F261",
        "Nested CASE expressions"
    );
}

/// F261: CASE within CASE WHEN condition
#[test]
fn f261_case_in_when() {
    assert_feature_supported!(
        "SELECT CASE WHEN CASE a WHEN 1 THEN b ELSE c END > 10 THEN 'yes' ELSE 'no' END FROM t",
        "F261",
        "CASE in WHEN condition"
    );
}

/// F261: CASE within CASE THEN result
#[test]
fn f261_case_in_then() {
    assert_feature_supported!(
        "SELECT CASE WHEN age < 18 THEN CASE state WHEN 'CA' THEN 'CA minor' ELSE 'other minor' END ELSE 'adult' END FROM person",
        "F261",
        "CASE in THEN result"
    );
}

/// F261: Simple CASE nested in searched CASE
#[test]
fn f261_simple_in_searched() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN CASE b WHEN 1 THEN 'one' WHEN 2 THEN 'two' END ELSE 'low' END FROM t",
        "F261",
        "Simple CASE nested in searched CASE"
    );
}

/// F261: Searched CASE nested in simple CASE
#[test]
fn f261_searched_in_simple() {
    assert_feature_supported!(
        "SELECT CASE a WHEN 1 THEN CASE WHEN b > 5 THEN 'high' ELSE 'low' END WHEN 2 THEN 'two' END FROM t",
        "F261",
        "Searched CASE nested in simple CASE"
    );
}

// ============================================================================
// F261 Advanced Tests: CASE with NULL handling
// ============================================================================

/// F261: CASE with NULL in ELSE
#[test]
fn f261_case_null_else() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN 'high' ELSE NULL END FROM t",
        "F261",
        "CASE with NULL in ELSE"
    );
}

/// F261: CASE with all NULL results
#[test]
fn f261_case_all_null() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN NULL WHEN a < 5 THEN NULL ELSE NULL END FROM t",
        "F261",
        "CASE with all NULL results"
    );
}

/// F261: CASE testing IS NULL
#[test]
fn f261_case_is_null_test() {
    assert_feature_supported!(
        "SELECT CASE WHEN a IS NULL THEN 0 WHEN b IS NULL THEN 1 ELSE 2 END FROM t",
        "F261",
        "CASE with IS NULL tests"
    );
}

/// F261: CASE with NULL result for missing value
#[test]
fn f261_case_null_default() {
    assert_feature_supported!(
        "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t",
        "F261",
        "CASE with implicit NULL for no match"
    );
}

// ============================================================================
// F261 Advanced Tests: CASE in ORDER BY
// ============================================================================

/// F261: CASE in ORDER BY
#[test]
fn f261_case_in_order_by() {
    assert_feature_supported!(
        "SELECT * FROM person ORDER BY CASE state WHEN 'CA' THEN 1 WHEN 'NY' THEN 2 ELSE 3 END",
        "F261",
        "CASE in ORDER BY"
    );
}

/// F261: Searched CASE in ORDER BY
#[test]
fn f261_searched_case_in_order_by() {
    assert_feature_supported!(
        "SELECT * FROM person ORDER BY CASE WHEN age < 30 THEN age ELSE 100 - age END",
        "F261",
        "Searched CASE in ORDER BY"
    );
}

/// F261: Multiple CASE in ORDER BY
#[test]
fn f261_multiple_case_order_by() {
    assert_feature_supported!(
        "SELECT * FROM person ORDER BY CASE WHEN state = 'CA' THEN 1 ELSE 2 END, CASE WHEN age < 30 THEN 1 ELSE 2 END",
        "F261",
        "Multiple CASE in ORDER BY"
    );
}

// ============================================================================
// F261 Advanced Tests: CASE in GROUP BY
// ============================================================================

/// F261: CASE in GROUP BY
#[test]
fn f261_case_in_group_by() {
    assert_feature_supported!(
        "SELECT CASE WHEN age < 30 THEN 'young' ELSE 'old' END AS age_group, COUNT(*) FROM person GROUP BY CASE WHEN age < 30 THEN 'young' ELSE 'old' END",
        "F261",
        "CASE in GROUP BY"
    );
}

/// F261: Simple CASE in GROUP BY
#[test]
fn f261_simple_case_group_by() {
    assert_feature_supported!(
        "SELECT CASE state WHEN 'CA' THEN 'West' WHEN 'NY' THEN 'East' ELSE 'Other' END AS region, COUNT(*) FROM person GROUP BY CASE state WHEN 'CA' THEN 'West' WHEN 'NY' THEN 'East' ELSE 'Other' END",
        "F261",
        "Simple CASE in GROUP BY"
    );
}

/// F261: CASE in GROUP BY with HAVING
#[test]
fn f261_case_group_by_having() {
    assert_feature_supported!(
        "SELECT CASE WHEN salary > 50000 THEN 'high' ELSE 'low' END AS salary_group, COUNT(*) FROM person GROUP BY CASE WHEN salary > 50000 THEN 'high' ELSE 'low' END HAVING COUNT(*) > 5",
        "F261",
        "CASE in GROUP BY with HAVING"
    );
}

// ============================================================================
// F261 Advanced Tests: CASE in aggregate functions
// ============================================================================

/// F261: CASE inside COUNT
#[test]
fn f261_case_in_count() {
    assert_feature_supported!(
        "SELECT COUNT(CASE WHEN age > 21 THEN 1 END) FROM person",
        "F261",
        "CASE inside COUNT"
    );
}

/// F261: CASE inside SUM
#[test]
fn f261_case_in_sum() {
    assert_feature_supported!(
        "SELECT SUM(CASE WHEN state = 'CA' THEN salary ELSE 0 END) FROM person",
        "F261",
        "CASE inside SUM"
    );
}

/// F261: CASE inside AVG
#[test]
fn f261_case_in_avg() {
    assert_feature_supported!(
        "SELECT AVG(CASE WHEN age >= 18 THEN salary END) FROM person",
        "F261",
        "CASE inside AVG"
    );
}

/// F261: Multiple CASE in aggregates
#[test]
fn f261_multiple_case_aggregates() {
    assert_feature_supported!(
        "SELECT SUM(CASE WHEN state = 'CA' THEN 1 ELSE 0 END) AS ca_count, SUM(CASE WHEN state = 'NY' THEN 1 ELSE 0 END) AS ny_count FROM person",
        "F261",
        "Multiple CASE in aggregates (pivot pattern)"
    );
}

// ============================================================================
// F261 Advanced Tests: Combining NULLIF and COALESCE
// ============================================================================

/// F261: NULLIF inside COALESCE
#[test]
fn f261_nullif_in_coalesce() {
    assert_feature_supported!(
        "SELECT COALESCE(NULLIF(a, 0), 1) FROM t",
        "F261",
        "NULLIF inside COALESCE"
    );
}

/// F261: COALESCE inside NULLIF
#[test]
fn f261_coalesce_in_nullif() {
    assert_feature_supported!(
        "SELECT NULLIF(COALESCE(a, 0), -1) FROM t",
        "F261",
        "COALESCE inside NULLIF"
    );
}

/// F261: CASE with COALESCE
#[test]
fn f261_case_with_coalesce() {
    assert_feature_supported!(
        "SELECT CASE WHEN COALESCE(a, 0) > 10 THEN 'high' ELSE 'low' END FROM t",
        "F261",
        "CASE with COALESCE in condition"
    );
}

/// F261: CASE with NULLIF
#[test]
fn f261_case_with_nullif() {
    assert_feature_supported!(
        "SELECT CASE WHEN NULLIF(a, b) IS NULL THEN 'equal' ELSE 'different' END FROM t",
        "F261",
        "CASE with NULLIF in condition"
    );
}

/// F261: Complex combination of conditional functions
#[test]
fn f261_complex_conditional_combo() {
    assert_feature_supported!(
        "SELECT COALESCE(NULLIF(CASE WHEN a > 10 THEN b ELSE c END, 0), 1) FROM t",
        "F261",
        "Complex combination of CASE, NULLIF, and COALESCE"
    );
}

// ============================================================================
// F261 Advanced Tests: CASE with arithmetic
// ============================================================================

/// F261: CASE result in arithmetic
#[test]
fn f261_case_in_arithmetic() {
    assert_feature_supported!(
        "SELECT (CASE WHEN a > 0 THEN a ELSE 1 END) * 10 FROM t",
        "F261",
        "CASE result in arithmetic"
    );
}

/// F261: Arithmetic in CASE condition
#[test]
fn f261_arithmetic_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a + b > 100 THEN a * 2 WHEN a - b < 0 THEN b / 2 ELSE a + b END FROM t",
        "F261",
        "Arithmetic in CASE conditions and results"
    );
}

/// F261: CASE dividing NULLIF for safe division
#[test]
fn f261_safe_division() {
    assert_feature_supported!(
        "SELECT CASE WHEN b = 0 THEN NULL ELSE a / b END FROM t",
        "F261",
        "CASE for safe division"
    );
}

// ============================================================================
// F261 Advanced Tests: CASE with JOIN
// ============================================================================

/// F261: CASE in JOIN condition
#[test]
fn f261_case_in_join() {
    assert_feature_supported!(
        "SELECT * FROM t1 JOIN t2 ON CASE WHEN t1.a > 10 THEN t1.a ELSE t1.b END = t2.a",
        "F261",
        "CASE in JOIN condition"
    );
}

/// F261: CASE selecting join column
#[test]
fn f261_case_select_join_column() {
    assert_feature_supported!(
        "SELECT CASE WHEN t1.a > t2.a THEN t1.a ELSE t2.a END FROM t1 JOIN t2 ON t1.b = t2.b",
        "F261",
        "CASE selecting from joined columns"
    );
}

// ============================================================================
// F261 Combined Tests
// ============================================================================

/// F261: All CASE variants in single query
#[test]
fn f261_combined_all_variants() {
    assert_feature_supported!(
        "SELECT \
         CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END AS simple_case, \
         CASE WHEN b > 10 THEN 'high' WHEN b > 5 THEN 'med' ELSE 'low' END AS searched_case, \
         NULLIF(a, 0) AS nullif_result, \
         COALESCE(a, b, 0) AS coalesce_result \
         FROM t",
        "F261",
        "All F261 CASE variants in single query"
    );
}

/// F261: Deeply nested conditional logic
#[test]
fn f261_combined_deep_nesting() {
    assert_feature_supported!(
        "SELECT CASE \
         WHEN COALESCE(a, 0) > 10 THEN \
           CASE WHEN NULLIF(b, 0) IS NULL THEN 'B is zero' \
           ELSE CASE b WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END \
           END \
         ELSE 'A is low' \
         END \
         FROM t",
        "F261",
        "Deeply nested CASE with COALESCE and NULLIF"
    );
}

/// F261: CASE in multiple clauses
#[test]
fn f261_combined_multiple_clauses() {
    assert_feature_supported!(
        "SELECT CASE WHEN age < 30 THEN 'young' ELSE 'old' END AS age_group, \
         COUNT(*) \
         FROM person \
         WHERE CASE state WHEN 'CA' THEN salary > 60000 ELSE salary > 40000 END \
         GROUP BY CASE WHEN age < 30 THEN 'young' ELSE 'old' END \
         HAVING COUNT(*) > CASE WHEN AVG(salary) > 50000 THEN 5 ELSE 10 END \
         ORDER BY CASE WHEN COUNT(*) > 100 THEN 1 ELSE 2 END",
        "F261",
        "CASE in SELECT, WHERE, GROUP BY, HAVING, ORDER BY"
    );
}

/// F261: Complex real-world scenario
#[test]
fn f261_combined_real_world() {
    assert_feature_supported!(
        "SELECT \
         first_name, \
         last_name, \
         CASE \
           WHEN age < 18 THEN 'Minor' \
           WHEN age BETWEEN 18 AND 64 THEN 'Adult' \
           ELSE 'Senior' \
         END AS age_category, \
         CASE state \
           WHEN 'CA' THEN 'California' \
           WHEN 'NY' THEN 'New York' \
           WHEN 'TX' THEN 'Texas' \
           ELSE state \
         END AS state_name, \
         COALESCE(NULLIF(salary, 0), 30000) AS adjusted_salary, \
         CASE \
           WHEN salary > (SELECT AVG(salary) FROM person) THEN 'Above Average' \
           WHEN salary < (SELECT AVG(salary) FROM person) THEN 'Below Average' \
           ELSE 'Average' \
         END AS salary_position \
         FROM person \
         WHERE NULLIF(first_name, '') IS NOT NULL \
         ORDER BY CASE WHEN state IN ('CA', 'NY') THEN 1 ELSE 2 END, salary DESC",
        "F261",
        "Complex real-world CASE scenario"
    );
}
