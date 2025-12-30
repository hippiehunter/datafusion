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

//! SQL:2016 Feature E141 - Basic integrity constraints
//!
//! ISO/IEC 9075-2:2016 Section 11.6
//!
//! This feature covers basic integrity constraints required by Core SQL:
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | E141-01 | NOT NULL constraints | Partial |
//! | E141-02 | UNIQUE constraints of NOT NULL columns | Partial |
//! | E141-03 | PRIMARY KEY constraints | Partial |
//! | E141-04 | Basic FOREIGN KEY constraint with NO ACTION default | Partial |
//! | E141-06 | CHECK constraints | Partial |
//! | E141-07 | Column defaults | Partial |
//! | E141-08 | NOT NULL inferred on PRIMARY KEY | Partial |
//! | E141-10 | Names in foreign key can be specified in any order | Partial |
//! | E141-11 | Foreign key columns match referenced columns by position | Partial |
//!
//! All E141 subfeatures are CORE features (mandatory for SQL:2016 conformance).

use crate::{assert_plans, assert_feature_supported};

// ============================================================================
// E141-01: NOT NULL constraints
// ============================================================================

/// E141-01: Column-level NOT NULL constraint
#[test]
fn e141_01_not_null_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT NOT NULL)",
        "E141-01",
        "NOT NULL column constraint"
    );
}

/// E141-01: Multiple NOT NULL columns
#[test]
fn e141_01_multiple_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT NOT NULL, y VARCHAR(50) NOT NULL)",
        "E141-01",
        "Multiple NOT NULL columns"
    );
}

/// E141-01: NOT NULL with other constraints
#[test]
fn e141_01_not_null_with_default() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT NOT NULL DEFAULT 0)",
        "E141-01",
        "NOT NULL with DEFAULT"
    );
}

/// E141-01: Named NOT NULL constraint
#[test]
fn e141_01_named_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT CONSTRAINT nn_x NOT NULL)",
        "E141-01",
        "Named NOT NULL constraint"
    );
}

// ============================================================================
// E141-02: UNIQUE constraints of NOT NULL columns
// ============================================================================

/// E141-02: Column-level UNIQUE constraint
#[test]
fn e141_02_unique_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT UNIQUE)",
        "E141-02",
        "UNIQUE column constraint"
    );
}

/// E141-02: UNIQUE on NOT NULL column
#[test]
fn e141_02_unique_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT NOT NULL UNIQUE)",
        "E141-02",
        "UNIQUE NOT NULL column"
    );
}

/// E141-02: Table-level UNIQUE constraint
#[test]
fn e141_02_unique_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, UNIQUE(x))",
        "E141-02",
        "Table-level UNIQUE constraint"
    );
}

/// E141-02: Composite UNIQUE constraint
#[test]
fn e141_02_unique_composite() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, UNIQUE(x, y))",
        "E141-02",
        "Composite UNIQUE constraint"
    );
}

/// E141-02: Named UNIQUE constraint
#[test]
fn e141_02_named_unique() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT CONSTRAINT uq_x UNIQUE)",
        "E141-02",
        "Named UNIQUE constraint"
    );
}

/// E141-02: Named table-level UNIQUE constraint
#[test]
fn e141_02_named_unique_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, CONSTRAINT uq_xy UNIQUE(x, y))",
        "E141-02",
        "Named table-level UNIQUE constraint"
    );
}

// ============================================================================
// E141-03: PRIMARY KEY constraints
// ============================================================================

/// E141-03: Column-level PRIMARY KEY constraint
#[test]
fn e141_03_primary_key_column() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT PRIMARY KEY)",
        "E141-03",
        "Column-level PRIMARY KEY"
    );
}

/// E141-03: Table-level PRIMARY KEY constraint
#[test]
fn e141_03_primary_key_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, PRIMARY KEY(id))",
        "E141-03",
        "Table-level PRIMARY KEY"
    );
}

/// E141-03: Composite PRIMARY KEY constraint
#[test]
fn e141_03_primary_key_composite() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, PRIMARY KEY(x, y))",
        "E141-03",
        "Composite PRIMARY KEY"
    );
}

/// E141-03: Named PRIMARY KEY constraint
#[test]
fn e141_03_named_primary_key() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT CONSTRAINT pk_t PRIMARY KEY)",
        "E141-03",
        "Named PRIMARY KEY constraint"
    );
}

/// E141-03: Named table-level PRIMARY KEY constraint
#[test]
fn e141_03_named_primary_key_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, CONSTRAINT pk_t PRIMARY KEY(id))",
        "E141-03",
        "Named table-level PRIMARY KEY"
    );
}

/// E141-03: PRIMARY KEY with other column constraints
#[test]
fn e141_03_primary_key_with_default() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT DEFAULT 1 PRIMARY KEY)",
        "E141-03",
        "PRIMARY KEY with DEFAULT"
    );
}

// ============================================================================
// E141-04: Basic FOREIGN KEY constraint with NO ACTION default
// ============================================================================

/// E141-04: Column-level FOREIGN KEY constraint
#[test]
fn e141_04_foreign_key_column() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id))",
        "E141-04",
        "Column-level FOREIGN KEY"
    );
}

/// E141-04: Table-level FOREIGN KEY constraint
#[test]
fn e141_04_foreign_key_table_level() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT, FOREIGN KEY(customer_id) REFERENCES person(id))",
        "E141-04",
        "Table-level FOREIGN KEY"
    );
}

/// E141-04: Composite FOREIGN KEY constraint
#[test]
fn e141_04_foreign_key_composite() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, FOREIGN KEY(x, y) REFERENCES person(id, age))",
        "E141-04",
        "Composite FOREIGN KEY"
    );
}

/// E141-04: Named FOREIGN KEY constraint
#[test]
fn e141_04_named_foreign_key() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT CONSTRAINT fk_customer REFERENCES person(id))",
        "E141-04",
        "Named FOREIGN KEY constraint"
    );
}

/// E141-04: Named table-level FOREIGN KEY constraint
#[test]
fn e141_04_named_foreign_key_table_level() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT, CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES person(id))",
        "E141-04",
        "Named table-level FOREIGN KEY"
    );
}

/// E141-04: FOREIGN KEY with explicit NO ACTION
#[test]
fn e141_04_foreign_key_no_action() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id) ON DELETE NO ACTION)",
        "E141-04",
        "FOREIGN KEY with NO ACTION"
    );
}

/// E141-04: FOREIGN KEY with ON UPDATE NO ACTION
#[test]
fn e141_04_foreign_key_on_update_no_action() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id) ON UPDATE NO ACTION)",
        "E141-04",
        "FOREIGN KEY with ON UPDATE NO ACTION"
    );
}

/// E141-04: FOREIGN KEY with both ON DELETE and ON UPDATE
#[test]
fn e141_04_foreign_key_delete_update() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id) ON DELETE NO ACTION ON UPDATE NO ACTION)",
        "E141-04",
        "FOREIGN KEY with ON DELETE and ON UPDATE"
    );
}

// ============================================================================
// E141-06: CHECK constraints
// ============================================================================

/// E141-06: Column-level CHECK constraint
#[test]
fn e141_06_check_column() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT CHECK (x > 0))",
        "E141-06",
        "Column-level CHECK constraint"
    );
}

/// E141-06: Table-level CHECK constraint
#[test]
fn e141_06_check_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, CHECK (x > y))",
        "E141-06",
        "Table-level CHECK constraint"
    );
}

/// E141-06: Named CHECK constraint
#[test]
fn e141_06_named_check() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT CONSTRAINT chk_positive CHECK (x > 0))",
        "E141-06",
        "Named CHECK constraint"
    );
}

/// E141-06: Named table-level CHECK constraint
#[test]
fn e141_06_named_check_table_level() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, CONSTRAINT chk_xy CHECK (x > y))",
        "E141-06",
        "Named table-level CHECK constraint"
    );
}

/// E141-06: CHECK constraint with multiple conditions
#[test]
fn e141_06_check_multiple_conditions() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT CHECK (x >= 0 AND x <= 100))",
        "E141-06",
        "CHECK with multiple conditions"
    );
}

/// E141-06: CHECK constraint with IN predicate
#[test]
fn e141_06_check_in_predicate() {
    assert_feature_supported!(
        "CREATE TABLE t (status VARCHAR(20) CHECK (status IN ('active', 'inactive', 'pending')))",
        "E141-06",
        "CHECK with IN predicate"
    );
}

/// E141-06: Multiple CHECK constraints on same column
#[test]
fn e141_06_multiple_checks() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT CHECK (x > 0) CHECK (x < 100))",
        "E141-06",
        "Multiple CHECK constraints"
    );
}

// ============================================================================
// E141-07: Column defaults
// ============================================================================

/// E141-07: DEFAULT with literal integer
#[test]
fn e141_07_default_integer() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT DEFAULT 0)",
        "E141-07",
        "DEFAULT with integer"
    );
}

/// E141-07: DEFAULT with literal string
#[test]
fn e141_07_default_string() {
    assert_feature_supported!(
        "CREATE TABLE t (status VARCHAR(20) DEFAULT 'active')",
        "E141-07",
        "DEFAULT with string"
    );
}

/// E141-07: DEFAULT with NULL
#[test]
fn e141_07_default_null() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT DEFAULT NULL)",
        "E141-07",
        "DEFAULT with NULL"
    );
}

/// E141-07: DEFAULT with expression
#[test]
fn e141_07_default_expression() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT DEFAULT (10 + 5))",
        "E141-07",
        "DEFAULT with expression"
    );
}

/// E141-07: Multiple DEFAULT constraints
#[test]
fn e141_07_multiple_defaults() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT DEFAULT 0, y VARCHAR(20) DEFAULT 'none')",
        "E141-07",
        "Multiple DEFAULT constraints"
    );
}

/// E141-07: DEFAULT with NOT NULL
#[test]
fn e141_07_default_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT NOT NULL DEFAULT 0)",
        "E141-07",
        "DEFAULT with NOT NULL"
    );
}

// ============================================================================
// E141-08: NOT NULL inferred on PRIMARY KEY
// ============================================================================

/// E141-08: PRIMARY KEY implies NOT NULL (column-level)
#[test]
fn e141_08_primary_key_implies_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT PRIMARY KEY)",
        "E141-08",
        "PRIMARY KEY implies NOT NULL"
    );
}

/// E141-08: PRIMARY KEY implies NOT NULL (table-level)
#[test]
fn e141_08_primary_key_table_level_implies_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, PRIMARY KEY(id))",
        "E141-08",
        "Table-level PRIMARY KEY implies NOT NULL"
    );
}

/// E141-08: Composite PRIMARY KEY implies NOT NULL on all columns
#[test]
fn e141_08_composite_primary_key_implies_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, PRIMARY KEY(x, y))",
        "E141-08",
        "Composite PRIMARY KEY implies NOT NULL"
    );
}

// ============================================================================
// E141-10: Names in foreign key can be specified in any order
// ============================================================================

/// E141-10: FOREIGN KEY with reordered columns
#[test]
fn e141_10_foreign_key_reordered() {
    assert_feature_supported!(
        "CREATE TABLE t (a INT, b INT, FOREIGN KEY(b, a) REFERENCES person(age, id))",
        "E141-10",
        "FOREIGN KEY with reordered columns"
    );
}

/// E141-10: FOREIGN KEY referencing columns in different order than table definition
#[test]
fn e141_10_foreign_key_different_order() {
    assert_feature_supported!(
        "CREATE TABLE orders (item_id INT, customer_id INT, FOREIGN KEY(customer_id, item_id) REFERENCES t(b, a))",
        "E141-10",
        "FOREIGN KEY with different column order"
    );
}

// ============================================================================
// E141-11: Foreign key columns match referenced columns by position
// ============================================================================

/// E141-11: FOREIGN KEY matches by position (single column)
#[test]
fn e141_11_foreign_key_position_single() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT, FOREIGN KEY(customer_id) REFERENCES person(id))",
        "E141-11",
        "FOREIGN KEY matches by position (single)"
    );
}

/// E141-11: FOREIGN KEY matches by position (composite)
#[test]
fn e141_11_foreign_key_position_composite() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT, y INT, FOREIGN KEY(x, y) REFERENCES person(id, age))",
        "E141-11",
        "FOREIGN KEY matches by position (composite)"
    );
}

// ============================================================================
// Additional constraint tests
// ============================================================================

/// Table with multiple constraint types combined
#[test]
fn e141_multiple_constraint_types() {
    assert_feature_supported!(
        "CREATE TABLE person (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE,
            age INT CHECK (age >= 0 AND age <= 150),
            status VARCHAR(20) DEFAULT 'active'
        )",
        "E141",
        "Multiple constraint types"
    );
}

/// All constraints named
#[test]
fn e141_all_named_constraints() {
    assert_feature_supported!(
        "CREATE TABLE person (
            id INT CONSTRAINT pk_person PRIMARY KEY,
            name VARCHAR(100) CONSTRAINT nn_name NOT NULL,
            email VARCHAR(100) CONSTRAINT uq_email UNIQUE,
            age INT CONSTRAINT chk_age CHECK (age >= 0)
        )",
        "E141",
        "All named constraints"
    );
}

/// Table-level constraints for all types
#[test]
fn e141_table_level_all_constraints() {
    assert_feature_supported!(
        "CREATE TABLE orders (
            id INT,
            customer_id INT,
            total DECIMAL(10,2),
            status VARCHAR(20),
            CONSTRAINT pk_orders PRIMARY KEY(id),
            CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES person(id),
            CONSTRAINT uq_order UNIQUE(id, customer_id),
            CONSTRAINT chk_total CHECK (total >= 0),
            CONSTRAINT chk_status CHECK (status IN ('pending', 'completed', 'cancelled'))
        )",
        "E141",
        "Table-level all constraint types"
    );
}

/// Mixed column-level and table-level constraints
#[test]
fn e141_mixed_constraint_levels() {
    assert_feature_supported!(
        "CREATE TABLE orders (
            id INT PRIMARY KEY,
            customer_id INT NOT NULL,
            order_date DATE DEFAULT CURRENT_DATE,
            total DECIMAL(10,2) CHECK (total > 0),
            FOREIGN KEY(customer_id) REFERENCES person(id),
            CHECK (total < 1000000)
        )",
        "E141",
        "Mixed constraint levels"
    );
}

/// Multiple UNIQUE constraints on different columns
#[test]
fn e141_multiple_unique_constraints() {
    assert_feature_supported!(
        "CREATE TABLE person (
            id INT PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            ssn VARCHAR(11) UNIQUE,
            username VARCHAR(50) UNIQUE
        )",
        "E141",
        "Multiple UNIQUE constraints"
    );
}

/// FOREIGN KEY with CASCADE actions (beyond E141-04, but related)
#[test]
fn e141_foreign_key_cascade() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id) ON DELETE CASCADE)",
        "E141",
        "FOREIGN KEY with CASCADE"
    );
}

/// FOREIGN KEY with SET NULL action
#[test]
fn e141_foreign_key_set_null() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id) ON DELETE SET NULL)",
        "E141",
        "FOREIGN KEY with SET NULL"
    );
}

/// FOREIGN KEY with SET DEFAULT action
#[test]
fn e141_foreign_key_set_default() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id) ON DELETE SET DEFAULT)",
        "E141",
        "FOREIGN KEY with SET DEFAULT"
    );
}

/// Constraints with various data types
#[test]
fn e141_constraints_various_types() {
    assert_feature_supported!(
        "CREATE TABLE t (
            int_col INT PRIMARY KEY,
            varchar_col VARCHAR(100) NOT NULL UNIQUE,
            decimal_col DECIMAL(10,2) CHECK (decimal_col >= 0),
            date_col DATE DEFAULT CURRENT_DATE,
            bool_col BOOLEAN DEFAULT FALSE
        )",
        "E141",
        "Constraints on various data types"
    );
}

// ============================================================================
// Summary Tests - Verify overall E141 support
// ============================================================================

#[test]
fn e141_summary_all_subfeatures() {
    // This test verifies that all E141 subfeatures work together
    // in a realistic scenario with related tables

    // Parent table with PRIMARY KEY
    assert_plans!("CREATE TABLE person (
        id INT CONSTRAINT pk_person PRIMARY KEY,
        first_name VARCHAR(50) CONSTRAINT nn_first_name NOT NULL,
        last_name VARCHAR(50) CONSTRAINT nn_last_name NOT NULL,
        email VARCHAR(100) CONSTRAINT uq_email UNIQUE,
        age INT CONSTRAINT chk_age CHECK (age >= 0 AND age <= 150),
        status VARCHAR(20) DEFAULT 'active',
        CONSTRAINT chk_status CHECK (status IN ('active', 'inactive', 'pending'))
    )");

    // Child table with FOREIGN KEY
    assert_plans!("CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE DEFAULT CURRENT_DATE,
        total DECIMAL(10,2) DEFAULT 0.00,
        CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES person(id) ON DELETE NO ACTION,
        CONSTRAINT chk_total CHECK (total >= 0)
    )");
}
