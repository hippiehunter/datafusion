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

//! SQL:2023 Feature F292 - UNIQUE NULLS handling
//!
//! ISO/IEC 9075-2:2023 Section 11.6
//!
//! This feature clarifies how NULL values are handled in UNIQUE constraints:
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F292 | UNIQUE NULLS DISTINCT | Supported |
//! | F292 | UNIQUE NULLS NOT DISTINCT | Supported |
//!
//! SQL:2023 introduces explicit control over how NULL values are treated in UNIQUE constraints:
//! - `UNIQUE NULLS DISTINCT` (default): NULLs are considered distinct, multiple NULLs allowed
//! - `UNIQUE NULLS NOT DISTINCT`: NULLs are considered equal, only one NULL allowed

use crate::assert_feature_supported;

// ============================================================================
// F292: UNIQUE NULLS DISTINCT (default behavior)
// ============================================================================

/// F292: Column-level UNIQUE with NULLS DISTINCT (explicit)
#[test]
fn f292_unique_nulls_distinct_column() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT UNIQUE NULLS DISTINCT)",
        "F292",
        "UNIQUE NULLS DISTINCT column constraint"
    );
}

/// F292: Table-level UNIQUE with NULLS DISTINCT
#[test]
fn f292_unique_nulls_distinct_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT, UNIQUE NULLS DISTINCT (val))",
        "F292",
        "UNIQUE NULLS DISTINCT table-level constraint"
    );
}

/// F292: Composite UNIQUE with NULLS DISTINCT
#[test]
fn f292_unique_nulls_distinct_composite() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, a INT, b INT, UNIQUE NULLS DISTINCT (a, b))",
        "F292",
        "Composite UNIQUE NULLS DISTINCT constraint"
    );
}

/// F292: Named UNIQUE with NULLS DISTINCT
#[test]
fn f292_unique_nulls_distinct_named() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT, CONSTRAINT uq_val UNIQUE NULLS DISTINCT (val))",
        "F292",
        "Named UNIQUE NULLS DISTINCT constraint"
    );
}

/// F292: UNIQUE without explicit NULLS clause (defaults to NULLS DISTINCT)
#[test]
fn f292_unique_default_nulls_distinct() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT UNIQUE)",
        "F292",
        "UNIQUE defaults to NULLS DISTINCT"
    );
}

// ============================================================================
// F292: UNIQUE NULLS NOT DISTINCT
// ============================================================================

/// F292: Column-level UNIQUE with NULLS NOT DISTINCT
#[test]
fn f292_unique_nulls_not_distinct_column() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT UNIQUE NULLS NOT DISTINCT)",
        "F292",
        "UNIQUE NULLS NOT DISTINCT column constraint"
    );
}

/// F292: Table-level UNIQUE with NULLS NOT DISTINCT
#[test]
fn f292_unique_nulls_not_distinct_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT, UNIQUE NULLS NOT DISTINCT (val))",
        "F292",
        "UNIQUE NULLS NOT DISTINCT table-level constraint"
    );
}

/// F292: Composite UNIQUE with NULLS NOT DISTINCT
#[test]
fn f292_unique_nulls_not_distinct_composite() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, a INT, b INT, UNIQUE NULLS NOT DISTINCT (a, b))",
        "F292",
        "Composite UNIQUE NULLS NOT DISTINCT constraint"
    );
}

/// F292: Named UNIQUE with NULLS NOT DISTINCT
#[test]
fn f292_unique_nulls_not_distinct_named() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT, CONSTRAINT uq_val UNIQUE NULLS NOT DISTINCT (val))",
        "F292",
        "Named UNIQUE NULLS NOT DISTINCT constraint"
    );
}

// ============================================================================
// F292: Mixed constraints
// ============================================================================

/// F292: Multiple UNIQUE constraints with different NULLS handling
#[test]
fn f292_mixed_nulls_handling() {
    assert_feature_supported!(
        "CREATE TABLE t (
            id INT PRIMARY KEY,
            email VARCHAR(100) UNIQUE NULLS DISTINCT,
            phone VARCHAR(20) UNIQUE NULLS NOT DISTINCT
        )",
        "F292",
        "Mixed NULLS DISTINCT and NOT DISTINCT constraints"
    );
}

/// F292: UNIQUE with NOT NULL and NULLS DISTINCT
#[test]
fn f292_not_null_with_nulls_distinct() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT NOT NULL UNIQUE NULLS DISTINCT)",
        "F292",
        "NOT NULL with UNIQUE NULLS DISTINCT"
    );
}

/// F292: UNIQUE with NOT NULL and NULLS NOT DISTINCT
#[test]
fn f292_not_null_with_nulls_not_distinct() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, val INT NOT NULL UNIQUE NULLS NOT DISTINCT)",
        "F292",
        "NOT NULL with UNIQUE NULLS NOT DISTINCT"
    );
}
