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

//! SQL:2016 Features E151/E152 - Transaction support
//!
//! ISO/IEC 9075-2:2016 Section 17
//!
//! These features cover transaction management required by Core SQL:
//!
//! | Feature | Subfeature | Description | Status |
//! |---------|------------|-------------|--------|
//! | E151 | E151-01 | COMMIT statement | Partial |
//! | E151 | E151-02 | ROLLBACK statement | Partial |
//! | E152 | E152-01 | SET TRANSACTION ISOLATION LEVEL SERIALIZABLE | Partial |
//! | E152 | E152-02 | SET TRANSACTION READ ONLY and READ WRITE | Partial |
//!
//! Related optional features also tested:
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | T241 | START TRANSACTION statement | Partial |
//! | T271 | Savepoints | Partial |
//! | T261 | Chained transactions | Partial |
//!
//! E151 and E152 are CORE features (mandatory for SQL:2016 conformance).

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// E151-01: COMMIT statement
// ============================================================================

/// E151-01: Basic COMMIT statement
#[test]
fn e151_01_commit_basic() {
    assert_feature_supported!(
        "COMMIT",
        "E151-01",
        "COMMIT statement"
    );
}

/// E151-01: COMMIT WORK (optional WORK keyword)
#[test]
fn e151_01_commit_work() {
    assert_feature_supported!(
        "COMMIT WORK",
        "E151-01",
        "COMMIT WORK statement"
    );
}

/// E151-01: COMMIT with explicit AND CHAIN
#[test]
fn e151_01_commit_and_chain() {
    assert_feature_supported!(
        "COMMIT AND CHAIN",
        "E151-01",
        "COMMIT AND CHAIN"
    );
}

/// E151-01: COMMIT with explicit AND NO CHAIN
#[test]
fn e151_01_commit_no_chain() {
    assert_feature_supported!(
        "COMMIT AND NO CHAIN",
        "E151-01",
        "COMMIT AND NO CHAIN"
    );
}

/// E151-01: COMMIT WORK AND CHAIN
#[test]
fn e151_01_commit_work_and_chain() {
    assert_feature_supported!(
        "COMMIT WORK AND CHAIN",
        "E151-01",
        "COMMIT WORK AND CHAIN"
    );
}

// ============================================================================
// E151-02: ROLLBACK statement
// ============================================================================

/// E151-02: Basic ROLLBACK statement
#[test]
fn e151_02_rollback_basic() {
    assert_feature_supported!(
        "ROLLBACK",
        "E151-02",
        "ROLLBACK statement"
    );
}

/// E151-02: ROLLBACK WORK (optional WORK keyword)
#[test]
fn e151_02_rollback_work() {
    assert_feature_supported!(
        "ROLLBACK WORK",
        "E151-02",
        "ROLLBACK WORK statement"
    );
}

/// E151-02: ROLLBACK with explicit AND CHAIN
#[test]
fn e151_02_rollback_and_chain() {
    assert_feature_supported!(
        "ROLLBACK AND CHAIN",
        "E151-02",
        "ROLLBACK AND CHAIN"
    );
}

/// E151-02: ROLLBACK with explicit AND NO CHAIN
#[test]
fn e151_02_rollback_no_chain() {
    assert_feature_supported!(
        "ROLLBACK AND NO CHAIN",
        "E151-02",
        "ROLLBACK AND NO CHAIN"
    );
}

/// E151-02: ROLLBACK WORK AND CHAIN
#[test]
fn e151_02_rollback_work_and_chain() {
    assert_feature_supported!(
        "ROLLBACK WORK AND CHAIN",
        "E151-02",
        "ROLLBACK WORK AND CHAIN"
    );
}

// ============================================================================
// E152-01: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
// ============================================================================

/// E152-01: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
#[test]
fn e152_01_set_transaction_serializable() {
    assert_feature_supported!(
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "E152-01",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    );
}

/// E152-01: SET TRANSACTION with SESSION scope
#[test]
fn e152_01_set_session_transaction_serializable() {
    assert_feature_supported!(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "E152-01",
        "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    );
}

// ============================================================================
// E152-02: SET TRANSACTION READ ONLY and READ WRITE
// ============================================================================

/// E152-02: SET TRANSACTION READ ONLY
#[test]
fn e152_02_set_transaction_read_only() {
    assert_feature_supported!(
        "SET TRANSACTION READ ONLY",
        "E152-02",
        "SET TRANSACTION READ ONLY"
    );
}

/// E152-02: SET TRANSACTION READ WRITE
#[test]
fn e152_02_set_transaction_read_write() {
    assert_feature_supported!(
        "SET TRANSACTION READ WRITE",
        "E152-02",
        "SET TRANSACTION READ WRITE"
    );
}

/// E152-02: SET TRANSACTION with both isolation level and access mode
#[test]
fn e152_02_set_transaction_combined() {
    assert_feature_supported!(
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
        "E152-02",
        "SET TRANSACTION with isolation and access mode"
    );
}

/// E152-02: SET TRANSACTION READ WRITE with isolation level
#[test]
fn e152_02_set_transaction_read_write_with_isolation() {
    assert_feature_supported!(
        "SET TRANSACTION READ WRITE ISOLATION LEVEL SERIALIZABLE",
        "E152-02",
        "SET TRANSACTION READ WRITE with isolation level"
    );
}

// ============================================================================
// T241: START TRANSACTION statement
// ============================================================================

/// T241: Basic START TRANSACTION statement
#[test]
fn t241_start_transaction_basic() {
    assert_feature_supported!(
        "START TRANSACTION",
        "T241",
        "START TRANSACTION statement"
    );
}

/// T241: BEGIN (synonym for START TRANSACTION)
#[test]
fn t241_begin_transaction() {
    assert_feature_supported!(
        "BEGIN",
        "T241",
        "BEGIN statement"
    );
}

/// T241: BEGIN TRANSACTION
#[test]
fn t241_begin_transaction_explicit() {
    assert_feature_supported!(
        "BEGIN TRANSACTION",
        "T241",
        "BEGIN TRANSACTION statement"
    );
}

/// T241: BEGIN WORK
#[test]
fn t241_begin_work() {
    assert_feature_supported!(
        "BEGIN WORK",
        "T241",
        "BEGIN WORK statement"
    );
}

/// T241: START TRANSACTION with READ ONLY
#[test]
fn t241_start_transaction_read_only() {
    assert_feature_supported!(
        "START TRANSACTION READ ONLY",
        "T241",
        "START TRANSACTION READ ONLY"
    );
}

/// T241: START TRANSACTION with READ WRITE
#[test]
fn t241_start_transaction_read_write() {
    assert_feature_supported!(
        "START TRANSACTION READ WRITE",
        "T241",
        "START TRANSACTION READ WRITE"
    );
}

/// T241: START TRANSACTION with isolation level
#[test]
fn t241_start_transaction_isolation() {
    assert_feature_supported!(
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "T241",
        "START TRANSACTION with isolation level"
    );
}

/// T241: START TRANSACTION with multiple options
#[test]
fn t241_start_transaction_multiple_options() {
    assert_feature_supported!(
        "START TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE",
        "T241",
        "START TRANSACTION with multiple options"
    );
}

// ============================================================================
// T271: Savepoints
// ============================================================================

/// T271: SAVEPOINT statement
#[test]
fn t271_savepoint() {
    assert_feature_supported!(
        "SAVEPOINT sp1",
        "T271",
        "SAVEPOINT statement"
    );
}

/// T271: SAVEPOINT with quoted identifier
#[test]
fn t271_savepoint_quoted() {
    assert_feature_supported!(
        "SAVEPOINT \"my_savepoint\"",
        "T271",
        "SAVEPOINT with quoted identifier"
    );
}

/// T271: ROLLBACK TO SAVEPOINT
#[test]
fn t271_rollback_to_savepoint() {
    assert_feature_supported!(
        "ROLLBACK TO SAVEPOINT sp1",
        "T271",
        "ROLLBACK TO SAVEPOINT"
    );
}

/// T271: ROLLBACK TO (without SAVEPOINT keyword)
#[test]
fn t271_rollback_to() {
    assert_feature_supported!(
        "ROLLBACK TO sp1",
        "T271",
        "ROLLBACK TO"
    );
}

/// T271: RELEASE SAVEPOINT
#[test]
fn t271_release_savepoint() {
    assert_feature_supported!(
        "RELEASE SAVEPOINT sp1",
        "T271",
        "RELEASE SAVEPOINT"
    );
}

/// T271: RELEASE (without SAVEPOINT keyword)
#[test]
fn t271_release() {
    assert_feature_supported!(
        "RELEASE sp1",
        "T271",
        "RELEASE"
    );
}

// ============================================================================
// Isolation Levels - Beyond SERIALIZABLE
// ============================================================================

/// SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
#[test]
fn isolation_level_read_uncommitted() {
    assert_feature_supported!(
        "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "E152",
        "ISOLATION LEVEL READ UNCOMMITTED"
    );
}

/// SET TRANSACTION ISOLATION LEVEL READ COMMITTED
#[test]
fn isolation_level_read_committed() {
    assert_feature_supported!(
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "E152",
        "ISOLATION LEVEL READ COMMITTED"
    );
}

/// SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
#[test]
fn isolation_level_repeatable_read() {
    assert_feature_supported!(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "E152",
        "ISOLATION LEVEL REPEATABLE READ"
    );
}

/// START TRANSACTION with READ UNCOMMITTED
#[test]
fn start_transaction_read_uncommitted() {
    assert_feature_supported!(
        "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        "T241",
        "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
    );
}

/// START TRANSACTION with READ COMMITTED
#[test]
fn start_transaction_read_committed() {
    assert_feature_supported!(
        "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "T241",
        "START TRANSACTION ISOLATION LEVEL READ COMMITTED"
    );
}

/// START TRANSACTION with REPEATABLE READ
#[test]
fn start_transaction_repeatable_read() {
    assert_feature_supported!(
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "T241",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    );
}

// ============================================================================
// Session-level transaction settings
// ============================================================================

/// SET SESSION CHARACTERISTICS for READ ONLY
#[test]
fn set_session_read_only() {
    assert_feature_supported!(
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "E152",
        "SET SESSION READ ONLY"
    );
}

/// SET SESSION CHARACTERISTICS for READ WRITE
#[test]
fn set_session_read_write() {
    assert_feature_supported!(
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE",
        "E152",
        "SET SESSION READ WRITE"
    );
}

/// SET SESSION CHARACTERISTICS with multiple options
#[test]
fn set_session_multiple_options() {
    assert_feature_supported!(
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY",
        "E152",
        "SET SESSION with multiple options"
    );
}

// ============================================================================
// END statement (synonym for COMMIT in some systems)
// ============================================================================

/// END statement (PostgreSQL-style)
#[test]
fn end_statement() {
    assert_feature_supported!(
        "END",
        "E151",
        "END statement"
    );
}

/// END TRANSACTION
#[test]
fn end_transaction() {
    assert_feature_supported!(
        "END TRANSACTION",
        "E151",
        "END TRANSACTION statement"
    );
}

/// END WORK
#[test]
fn end_work() {
    assert_feature_supported!(
        "END WORK",
        "E151",
        "END WORK statement"
    );
}

// ============================================================================
// Abort statement (synonym for ROLLBACK in some systems)
// ============================================================================

/// ABORT statement
#[test]
fn abort_statement() {
    assert_feature_supported!(
        "ABORT",
        "E151",
        "ABORT statement"
    );
}

/// ABORT TRANSACTION
#[test]
fn abort_transaction() {
    assert_feature_supported!(
        "ABORT TRANSACTION",
        "E151",
        "ABORT TRANSACTION statement"
    );
}

/// ABORT WORK
#[test]
fn abort_work() {
    assert_feature_supported!(
        "ABORT WORK",
        "E151",
        "ABORT WORK statement"
    );
}

// ============================================================================
// Transaction blocks with data manipulation
// ============================================================================

/// Transaction block scenario (parsing only)
#[test]
fn transaction_block_insert() {
    assert_parses!("START TRANSACTION");
    assert_parses!("INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date) VALUES (1, 'John', 'Doe', 30, 'CA', 50000, TIMESTAMP '2023-01-01 00:00:00')");
    assert_parses!("COMMIT");
}

/// Transaction block with rollback scenario
#[test]
fn transaction_block_rollback() {
    assert_parses!("START TRANSACTION");
    assert_parses!("UPDATE person SET salary = salary * 1.1");
    assert_parses!("ROLLBACK");
}

/// Transaction block with savepoints
#[test]
fn transaction_block_savepoints() {
    assert_parses!("BEGIN");
    assert_parses!("INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date) VALUES (1, 'Alice', 'Smith', 25, 'NY', 60000, TIMESTAMP '2023-01-01 00:00:00')");
    assert_parses!("SAVEPOINT sp1");
    assert_parses!("UPDATE person SET salary = 70000 WHERE id = 1");
    assert_parses!("ROLLBACK TO SAVEPOINT sp1");
    assert_parses!("COMMIT");
}

/// Nested savepoints
#[test]
fn nested_savepoints() {
    assert_parses!("BEGIN");
    assert_parses!("SAVEPOINT sp1");
    assert_parses!("INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date) VALUES (1, 'Bob', 'Johnson', 35, 'TX', 55000, TIMESTAMP '2023-01-01 00:00:00')");
    assert_parses!("SAVEPOINT sp2");
    assert_parses!("UPDATE person SET age = 36 WHERE id = 1");
    assert_parses!("ROLLBACK TO SAVEPOINT sp2");
    assert_parses!("RELEASE SAVEPOINT sp1");
    assert_parses!("COMMIT");
}

/// Transaction with multiple isolation levels set
#[test]
fn transaction_isolation_levels_sequence() {
    assert_parses!("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    assert_parses!("BEGIN");
    assert_parses!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    assert_parses!("COMMIT");
}

// ============================================================================
// Chained transactions (T261)
// ============================================================================

/// COMMIT AND CHAIN creates new transaction
#[test]
fn t261_commit_and_chain() {
    assert_feature_supported!(
        "COMMIT AND CHAIN",
        "T261",
        "COMMIT AND CHAIN"
    );
}

/// ROLLBACK AND CHAIN creates new transaction
#[test]
fn t261_rollback_and_chain() {
    assert_feature_supported!(
        "ROLLBACK AND CHAIN",
        "T261",
        "ROLLBACK AND CHAIN"
    );
}

/// COMMIT AND NO CHAIN explicitly terminates
#[test]
fn t261_commit_no_chain() {
    assert_feature_supported!(
        "COMMIT AND NO CHAIN",
        "T261",
        "COMMIT AND NO CHAIN"
    );
}

/// ROLLBACK AND NO CHAIN explicitly terminates
#[test]
fn t261_rollback_no_chain() {
    assert_feature_supported!(
        "ROLLBACK AND NO CHAIN",
        "T261",
        "ROLLBACK AND NO CHAIN"
    );
}

// ============================================================================
// Complex transaction scenarios
// ============================================================================

/// Read-only transaction with SELECT
#[test]
fn read_only_transaction_select() {
    assert_parses!("START TRANSACTION READ ONLY");
    assert_parses!("SELECT * FROM person WHERE age > 25");
    assert_parses!("COMMIT");
}

/// Multiple statements in transaction
#[test]
fn multiple_statements_in_transaction() {
    assert_parses!("BEGIN TRANSACTION");
    assert_parses!("INSERT INTO person (id, first_name, last_name, age, state, salary, birth_date) VALUES (1, 'Charlie', 'Brown', 40, 'FL', 65000, TIMESTAMP '2023-01-01 00:00:00')");
    assert_parses!("UPDATE orders SET total = total * 1.05 WHERE customer_id = 1");
    assert_parses!("DELETE FROM orders WHERE total < 10");
    assert_parses!("COMMIT");
}

/// Transaction with all isolation levels tested
#[test]
fn all_isolation_levels() {
    assert_parses!("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    assert_parses!("ROLLBACK");

    assert_parses!("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
    assert_parses!("ROLLBACK");

    assert_parses!("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    assert_parses!("ROLLBACK");

    assert_parses!("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    assert_parses!("ROLLBACK");
}

// ============================================================================
// Summary Tests - Verify overall E151/E152 support
// ============================================================================

#[test]
fn e151_e152_summary_basic_transaction_flow() {
    // Basic transaction lifecycle
    assert_plans!("BEGIN");
    assert_plans!("COMMIT");
    assert_plans!("ROLLBACK");
}

#[test]
fn e151_e152_summary_transaction_settings() {
    // Transaction configuration
    assert_plans!("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    assert_plans!("SET TRANSACTION READ ONLY");
    assert_plans!("SET TRANSACTION READ WRITE");
    assert_plans!("START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");
}

#[test]
fn e151_e152_summary_savepoints() {
    // Savepoint management
    assert_plans!("SAVEPOINT sp1");
    assert_plans!("ROLLBACK TO SAVEPOINT sp1");
    assert_plans!("RELEASE SAVEPOINT sp1");
}

#[test]
fn e151_e152_summary_all_features() {
    // Comprehensive transaction test combining all features
    assert_plans!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    assert_plans!("START TRANSACTION READ WRITE");
    assert_plans!("SAVEPOINT before_changes");
    assert_plans!("ROLLBACK TO SAVEPOINT before_changes");
    assert_plans!("RELEASE SAVEPOINT before_changes");
    assert_plans!("COMMIT AND CHAIN");
    assert_plans!("ROLLBACK");
}
