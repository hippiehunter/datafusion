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

//! SQL:2016 Feature B021 - Direct SQL (Utility Statements)
//!
//! ISO/IEC 9075-2:2016 Part 2 - Foundation
//!
//! This feature covers utility statements for direct SQL execution:
//!
//! | Statement | Description | Status |
//! |-----------|-------------|--------|
//! | COPY | Copy data to/from files | Partial |
//! | EXPLAIN | Show query execution plan | Supported |
//! | PREPARE | Prepare a SQL statement | Partial |
//! | EXECUTE | Execute a prepared statement | Partial |
//! | DEALLOCATE | Deallocate prepared statement | Partial |
//! | SHOW | Show configuration variables | Partial |
//! | SET | Set configuration variables | Partial |
//! | DESCRIBE | Describe table structure | Partial |
//! | TRUNCATE | Truncate table | Partial |
//! | ANALYZE | Analyze table statistics | Partial |
//! | VACUUM | Reclaim storage space | Not Implemented |
//! | USE | Set current schema/database | Not Implemented |
//!
//! B021 is part of Direct SQL support for interactive and batch SQL execution.
//!
//! Tests that fail indicate gaps in DataFusion's utility statement support.

use crate::{assert_plans, assert_feature_supported};

// ============================================================================
// COPY Statement
// ============================================================================

/// COPY: Basic COPY TO with table source
#[test]
fn b021_copy_to_basic() {
    assert_feature_supported!(
        "COPY t TO 'output.csv'",
        "B021",
        "COPY table TO file"
    );
}

/// COPY: COPY TO with format options
#[test]
fn b021_copy_to_format_csv() {
    assert_feature_supported!(
        "COPY t TO 'output.csv' (FORMAT CSV)",
        "B021",
        "COPY TO with FORMAT CSV"
    );
}

/// COPY: COPY TO with format parquet
#[test]
fn b021_copy_to_format_parquet() {
    assert_feature_supported!(
        "COPY t TO 'output.parquet' (FORMAT PARQUET)",
        "B021",
        "COPY TO with FORMAT PARQUET"
    );
}

/// COPY: COPY TO with format json
#[test]
fn b021_copy_to_format_json() {
    assert_feature_supported!(
        "COPY t TO 'output.json' (FORMAT JSON)",
        "B021",
        "COPY TO with FORMAT JSON"
    );
}

/// COPY: COPY TO with CSV header option
#[test]
fn b021_copy_to_csv_header() {
    assert_feature_supported!(
        "COPY t TO 'output.csv' (FORMAT CSV, HEADER true)",
        "B021",
        "COPY TO with CSV header option"
    );
}

/// COPY: COPY TO with delimiter option
#[test]
fn b021_copy_to_csv_delimiter() {
    assert_feature_supported!(
        "COPY t TO 'output.csv' (FORMAT CSV, DELIMITER '|')",
        "B021",
        "COPY TO with delimiter option"
    );
}

/// COPY: COPY TO with column list
#[test]
fn b021_copy_to_with_columns() {
    assert_feature_supported!(
        "COPY t (a, b) TO 'output.csv'",
        "B021",
        "COPY TO with column list"
    );
}

/// COPY: COPY query result to file
#[test]
fn b021_copy_query_to() {
    assert_feature_supported!(
        "COPY (SELECT a, b FROM t WHERE a > 10) TO 'output.csv'",
        "B021",
        "COPY query result TO file"
    );
}

/// COPY: COPY complex query to file
#[test]
fn b021_copy_complex_query() {
    assert_feature_supported!(
        "COPY (
            SELECT p.id, p.first_name, COUNT(o.order_id) as order_count
            FROM person p
            LEFT JOIN orders o ON p.id = o.customer_id
            GROUP BY p.id, p.first_name
        ) TO 'summary.csv' (FORMAT CSV, HEADER true)",
        "B021",
        "COPY complex query with JOIN"
    );
}

/// COPY: COPY FROM basic
#[test]
fn b021_copy_from_basic() {
    assert_feature_supported!(
        "COPY t FROM 'input.csv'",
        "B021",
        "COPY FROM file to table"
    );
}

/// COPY: COPY FROM with format
#[test]
fn b021_copy_from_format_csv() {
    assert_feature_supported!(
        "COPY t FROM 'input.csv' (FORMAT CSV)",
        "B021",
        "COPY FROM with FORMAT CSV"
    );
}

/// COPY: COPY FROM with column list
#[test]
fn b021_copy_from_with_columns() {
    assert_feature_supported!(
        "COPY t (a, b, c) FROM 'input.csv'",
        "B021",
        "COPY FROM with column list"
    );
}

/// COPY: COPY FROM with header option
#[test]
fn b021_copy_from_csv_header() {
    assert_feature_supported!(
        "COPY t FROM 'input.csv' (FORMAT CSV, HEADER true)",
        "B021",
        "COPY FROM with header option"
    );
}

/// COPY: COPY FROM with delimiter
#[test]
fn b021_copy_from_csv_delimiter() {
    assert_feature_supported!(
        "COPY t FROM 'input.csv' (FORMAT CSV, DELIMITER '|')",
        "B021",
        "COPY FROM with delimiter"
    );
}

/// COPY: COPY FROM parquet file
#[test]
fn b021_copy_from_parquet() {
    assert_feature_supported!(
        "COPY person FROM 'data.parquet' (FORMAT PARQUET)",
        "B021",
        "COPY FROM parquet file"
    );
}

// ============================================================================
// EXPLAIN Statement
// ============================================================================

/// EXPLAIN: Basic EXPLAIN SELECT
#[test]
fn b021_explain_select() {
    assert_feature_supported!(
        "EXPLAIN SELECT * FROM t",
        "B021",
        "EXPLAIN SELECT"
    );
}

/// EXPLAIN: EXPLAIN with WHERE clause
#[test]
fn b021_explain_select_where() {
    assert_feature_supported!(
        "EXPLAIN SELECT * FROM person WHERE age > 21",
        "B021",
        "EXPLAIN SELECT with WHERE"
    );
}

/// EXPLAIN: EXPLAIN with JOIN
#[test]
fn b021_explain_join() {
    assert_feature_supported!(
        "EXPLAIN SELECT p.first_name, o.item
         FROM person p
         JOIN orders o ON p.id = o.customer_id",
        "B021",
        "EXPLAIN JOIN query"
    );
}

/// EXPLAIN: EXPLAIN with aggregation
#[test]
fn b021_explain_aggregation() {
    assert_feature_supported!(
        "EXPLAIN SELECT state, COUNT(*), AVG(salary)
         FROM person
         GROUP BY state",
        "B021",
        "EXPLAIN aggregation query"
    );
}

/// EXPLAIN: EXPLAIN with subquery
#[test]
fn b021_explain_subquery() {
    assert_feature_supported!(
        "EXPLAIN SELECT * FROM person
         WHERE age > (SELECT AVG(age) FROM person)",
        "B021",
        "EXPLAIN with subquery"
    );
}

/// EXPLAIN: EXPLAIN VERBOSE (if supported)
#[test]
fn b021_explain_verbose() {
    assert_feature_supported!(
        "EXPLAIN VERBOSE SELECT * FROM t",
        "B021",
        "EXPLAIN VERBOSE"
    );
}

/// EXPLAIN: EXPLAIN ANALYZE (if supported)
#[test]
fn b021_explain_analyze() {
    assert_feature_supported!(
        "EXPLAIN ANALYZE SELECT * FROM t",
        "B021",
        "EXPLAIN ANALYZE"
    );
}

/// EXPLAIN: EXPLAIN INSERT
#[test]
fn b021_explain_insert() {
    assert_feature_supported!(
        "EXPLAIN INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
        "B021",
        "EXPLAIN INSERT"
    );
}

/// EXPLAIN: EXPLAIN UPDATE
#[test]
fn b021_explain_update() {
    assert_feature_supported!(
        "EXPLAIN UPDATE person SET salary = salary * 1.1 WHERE age > 30",
        "B021",
        "EXPLAIN UPDATE"
    );
}

/// EXPLAIN: EXPLAIN DELETE
#[test]
fn b021_explain_delete() {
    assert_feature_supported!(
        "EXPLAIN DELETE FROM orders WHERE qty = 0",
        "B021",
        "EXPLAIN DELETE"
    );
}

/// EXPLAIN: EXPLAIN CREATE TABLE AS
#[test]
fn b021_explain_create_table_as() {
    assert_feature_supported!(
        "EXPLAIN CREATE TABLE new_table AS SELECT * FROM t WHERE a > 10",
        "B021",
        "EXPLAIN CREATE TABLE AS"
    );
}

// ============================================================================
// PREPARE/EXECUTE Statements (Prepared Statements)
// ============================================================================

/// PREPARE: Basic PREPARE statement
#[test]
fn b021_prepare_basic() {
    assert_feature_supported!(
        "PREPARE my_query AS SELECT * FROM t",
        "B021",
        "PREPARE statement"
    );
}

/// PREPARE: PREPARE with WHERE clause
#[test]
fn b021_prepare_where() {
    assert_feature_supported!(
        "PREPARE filtered_query AS SELECT * FROM person WHERE age > 21",
        "B021",
        "PREPARE with WHERE"
    );
}

/// PREPARE: PREPARE with parameters ($1 style)
#[test]
fn b021_prepare_parameters_dollar() {
    assert_feature_supported!(
        "PREPARE param_query AS SELECT * FROM person WHERE age > $1 AND state = $2",
        "B021",
        "PREPARE with $N parameters"
    );
}

/// PREPARE: PREPARE with question mark parameters
#[test]
fn b021_prepare_parameters_question() {
    assert_feature_supported!(
        "PREPARE param_query AS SELECT * FROM person WHERE age > ? AND state = ?",
        "B021",
        "PREPARE with ? parameters"
    );
}

/// PREPARE: PREPARE INSERT statement
#[test]
fn b021_prepare_insert() {
    assert_feature_supported!(
        "PREPARE insert_stmt AS INSERT INTO t (a, b, c) VALUES ($1, $2, $3)",
        "B021",
        "PREPARE INSERT"
    );
}

/// PREPARE: PREPARE UPDATE statement
#[test]
fn b021_prepare_update() {
    assert_feature_supported!(
        "PREPARE update_stmt AS UPDATE person SET salary = $1 WHERE id = $2",
        "B021",
        "PREPARE UPDATE"
    );
}

/// PREPARE: PREPARE DELETE statement
#[test]
fn b021_prepare_delete() {
    assert_feature_supported!(
        "PREPARE delete_stmt AS DELETE FROM orders WHERE order_id = $1",
        "B021",
        "PREPARE DELETE"
    );
}

/// EXECUTE: Basic EXECUTE statement
#[test]
fn b021_execute_basic() {
    assert_feature_supported!(
        "EXECUTE my_query",
        "B021",
        "EXECUTE prepared statement"
    );
}

/// EXECUTE: EXECUTE with USING clause
#[test]
fn b021_execute_using() {
    assert_feature_supported!(
        "EXECUTE param_query USING 30, 'CA'",
        "B021",
        "EXECUTE with USING values"
    );
}

/// EXECUTE: EXECUTE with parenthesized values
#[test]
fn b021_execute_values() {
    assert_feature_supported!(
        "EXECUTE param_query(30, 'CA')",
        "B021",
        "EXECUTE with parameter values"
    );
}

/// DEALLOCATE: DEALLOCATE specific statement
#[test]
fn b021_deallocate_specific() {
    assert_feature_supported!(
        "DEALLOCATE my_query",
        "B021",
        "DEALLOCATE prepared statement"
    );
}

/// DEALLOCATE: DEALLOCATE PREPARE variant
#[test]
fn b021_deallocate_prepare() {
    assert_feature_supported!(
        "DEALLOCATE PREPARE my_query",
        "B021",
        "DEALLOCATE PREPARE"
    );
}

/// DEALLOCATE: DEALLOCATE ALL
#[test]
fn b021_deallocate_all() {
    assert_feature_supported!(
        "DEALLOCATE ALL",
        "B021",
        "DEALLOCATE ALL"
    );
}

/// DEALLOCATE: DEALLOCATE PREPARE ALL
#[test]
fn b021_deallocate_prepare_all() {
    assert_feature_supported!(
        "DEALLOCATE PREPARE ALL",
        "B021",
        "DEALLOCATE PREPARE ALL"
    );
}

// ============================================================================
// SHOW/SET Statements (Configuration)
// ============================================================================

/// SHOW: SHOW single variable
#[test]
fn b021_show_variable() {
    assert_feature_supported!(
        "SHOW datafusion.catalog.information_schema",
        "B021",
        "SHOW variable"
    );
}

/// SHOW: SHOW with variable name containing dots
#[test]
fn b021_show_quoted_variable() {
    assert_feature_supported!(
        "SHOW datafusion.execution.batch_size",
        "B021",
        "SHOW variable with dots"
    );
}

/// SHOW: SHOW ALL variables
#[test]
fn b021_show_all() {
    assert_feature_supported!(
        "SHOW ALL",
        "B021",
        "SHOW ALL variables"
    );
}

/// SHOW: SHOW TABLES (if supported)
#[test]
fn b021_show_tables() {
    assert_feature_supported!(
        "SHOW TABLES",
        "B021",
        "SHOW TABLES"
    );
}

/// SHOW: SHOW COLUMNS (if supported)
#[test]
fn b021_show_columns() {
    assert_feature_supported!(
        "SHOW COLUMNS FROM person",
        "B021",
        "SHOW COLUMNS"
    );
}

/// SET: SET variable to value
#[test]
fn b021_set_variable() {
    assert_feature_supported!(
        "SET max_parallel_workers = 8",
        "B021",
        "SET variable"
    );
}

/// SET: SET variable to string value
#[test]
fn b021_set_string_value() {
    assert_feature_supported!(
        "SET timezone = 'UTC'",
        "B021",
        "SET string variable"
    );
}

/// SET: SET with dotted variable name
#[test]
fn b021_set_dotted_variable() {
    assert_feature_supported!(
        "SET datafusion.execution.batch_size = 8192",
        "B021",
        "SET dotted variable name"
    );
}

/// SET: SET SESSION variable
#[test]
fn b021_set_session() {
    assert_feature_supported!(
        "SET SESSION max_parallel_workers = 8",
        "B021",
        "SET SESSION variable"
    );
}

/// SET: SET LOCAL variable (transaction scope)
#[test]
fn b021_set_local() {
    assert_feature_supported!(
        "SET LOCAL timezone = 'America/New_York'",
        "B021",
        "SET LOCAL variable"
    );
}

/// SET: SET variable to DEFAULT
#[test]
fn b021_set_default() {
    assert_feature_supported!(
        "SET max_parallel_workers = DEFAULT",
        "B021",
        "SET variable to DEFAULT"
    );
}

/// SET: SET TIME ZONE
#[test]
fn b021_set_time_zone() {
    assert_feature_supported!(
        "SET TIME ZONE 'UTC'",
        "B021",
        "SET TIME ZONE"
    );
}

/// RESET: RESET variable to default
#[test]
fn b021_reset_variable() {
    assert_feature_supported!(
        "RESET max_parallel_workers",
        "B021",
        "RESET variable"
    );
}

/// RESET: RESET ALL variables
#[test]
fn b021_reset_all() {
    assert_feature_supported!(
        "RESET ALL",
        "B021",
        "RESET ALL"
    );
}

// ============================================================================
// DESCRIBE/DESC Statements
// ============================================================================

/// DESCRIBE: DESCRIBE table
#[test]
fn b021_describe_table() {
    assert_feature_supported!(
        "DESCRIBE person",
        "B021",
        "DESCRIBE table"
    );
}

/// DESCRIBE: DESC abbreviation
#[test]
fn b021_desc_table() {
    assert_feature_supported!(
        "DESC person",
        "B021",
        "DESC table"
    );
}

/// DESCRIBE: DESCRIBE with schema qualifier
#[test]
fn b021_describe_qualified() {
    assert_feature_supported!(
        "DESCRIBE public.person",
        "B021",
        "DESCRIBE qualified table"
    );
}

/// DESCRIBE: DESCRIBE query result (if supported)
#[test]
fn b021_describe_query() {
    assert_feature_supported!(
        "DESCRIBE SELECT * FROM person WHERE age > 21",
        "B021",
        "DESCRIBE query"
    );
}

/// DESCRIBE: DESC query with aggregation
#[test]
fn b021_describe_aggregation_query() {
    assert_feature_supported!(
        "DESC SELECT state, COUNT(*) as cnt FROM person GROUP BY state",
        "B021",
        "DESCRIBE aggregation query"
    );
}

// ============================================================================
// TRUNCATE Statement
// ============================================================================

/// TRUNCATE: Basic TRUNCATE TABLE
#[test]
fn b021_truncate_basic() {
    assert_feature_supported!(
        "TRUNCATE TABLE t",
        "B021",
        "TRUNCATE TABLE"
    );
}

/// TRUNCATE: TRUNCATE without TABLE keyword
#[test]
fn b021_truncate_no_table_keyword() {
    assert_feature_supported!(
        "TRUNCATE person",
        "B021",
        "TRUNCATE without TABLE"
    );
}

/// TRUNCATE: TRUNCATE with qualified name
#[test]
fn b021_truncate_qualified() {
    assert_feature_supported!(
        "TRUNCATE TABLE public.orders",
        "B021",
        "TRUNCATE qualified table"
    );
}

// ============================================================================
// ANALYZE Statement
// ============================================================================

/// ANALYZE: ANALYZE table
#[test]
fn b021_analyze_table() {
    assert_feature_supported!(
        "ANALYZE TABLE person",
        "B021",
        "ANALYZE TABLE"
    );
}

/// ANALYZE: ANALYZE without TABLE keyword
#[test]
fn b021_analyze_no_table_keyword() {
    assert_feature_supported!(
        "ANALYZE person",
        "B021",
        "ANALYZE without TABLE"
    );
}

/// ANALYZE: ANALYZE with qualified name
#[test]
fn b021_analyze_qualified() {
    assert_feature_supported!(
        "ANALYZE TABLE public.orders",
        "B021",
        "ANALYZE qualified table"
    );
}

/// ANALYZE: ANALYZE with COMPUTE STATISTICS
#[test]
fn b021_analyze_compute_statistics() {
    assert_feature_supported!(
        "ANALYZE TABLE person COMPUTE STATISTICS",
        "B021",
        "ANALYZE COMPUTE STATISTICS"
    );
}

// ============================================================================
// VACUUM Statement (PostgreSQL-style maintenance)
// ============================================================================

/// VACUUM: Basic VACUUM
#[test]
fn b021_vacuum_basic() {
    assert_feature_supported!(
        "VACUUM",
        "B021",
        "VACUUM"
    );
}

/// VACUUM: VACUUM specific table
#[test]
fn b021_vacuum_table() {
    assert_feature_supported!(
        "VACUUM person",
        "B021",
        "VACUUM table"
    );
}

/// VACUUM: VACUUM FULL
#[test]
fn b021_vacuum_full() {
    assert_feature_supported!(
        "VACUUM FULL person",
        "B021",
        "VACUUM FULL"
    );
}

/// VACUUM: VACUUM ANALYZE
#[test]
fn b021_vacuum_analyze() {
    assert_feature_supported!(
        "VACUUM ANALYZE person",
        "B021",
        "VACUUM ANALYZE"
    );
}

// ============================================================================
// USE Statement (Database/Schema selection)
// ============================================================================

/// USE: USE database
#[test]
fn b021_use_database() {
    assert_feature_supported!(
        "USE mydb",
        "B021",
        "USE database"
    );
}

/// USE: USE schema
#[test]
fn b021_use_schema() {
    assert_feature_supported!(
        "USE SCHEMA public",
        "B021",
        "USE SCHEMA"
    );
}

/// USE: USE DATABASE explicit
#[test]
fn b021_use_database_explicit() {
    assert_feature_supported!(
        "USE DATABASE mydb",
        "B021",
        "USE DATABASE"
    );
}

// ============================================================================
// Additional Utility Statements
// ============================================================================

/// BEGIN TRANSACTION
#[test]
fn b021_begin_transaction() {
    assert_feature_supported!(
        "BEGIN",
        "B021",
        "BEGIN transaction"
    );
}

/// BEGIN TRANSACTION explicit
#[test]
fn b021_begin_transaction_explicit() {
    assert_feature_supported!(
        "BEGIN TRANSACTION",
        "B021",
        "BEGIN TRANSACTION"
    );
}

/// START TRANSACTION
#[test]
fn b021_start_transaction() {
    assert_feature_supported!(
        "START TRANSACTION",
        "B021",
        "START TRANSACTION"
    );
}

/// COMMIT
#[test]
fn b021_commit() {
    assert_feature_supported!(
        "COMMIT",
        "B021",
        "COMMIT transaction"
    );
}

/// COMMIT TRANSACTION
#[test]
fn b021_commit_transaction() {
    assert_feature_supported!(
        "COMMIT TRANSACTION",
        "B021",
        "COMMIT TRANSACTION"
    );
}

/// ROLLBACK
#[test]
fn b021_rollback() {
    assert_feature_supported!(
        "ROLLBACK",
        "B021",
        "ROLLBACK transaction"
    );
}

/// ROLLBACK TRANSACTION
#[test]
fn b021_rollback_transaction() {
    assert_feature_supported!(
        "ROLLBACK TRANSACTION",
        "B021",
        "ROLLBACK TRANSACTION"
    );
}

// ============================================================================
// Combined/Complex Utility Scenarios
// ============================================================================

/// Complex scenario: EXPLAIN with COPY
#[test]
fn b021_explain_copy() {
    assert_feature_supported!(
        "EXPLAIN COPY (SELECT * FROM person WHERE age > 30) TO 'output.csv'",
        "B021",
        "EXPLAIN COPY statement"
    );
}

/// Complex scenario: Multiple SET statements
#[test]
fn b021_multiple_set_operations() {
    // Individual SET operations should work
    assert_plans!("SET max_parallel_workers = 8");
    assert_plans!("SET datafusion.execution.batch_size = 8192");
    assert_plans!("SET timezone = 'UTC'");
}

/// Summary: Verify EXPLAIN works with all DML statements
#[test]
fn b021_explain_all_dml() {
    assert_plans!("EXPLAIN SELECT * FROM t");
    assert_plans!("EXPLAIN INSERT INTO t (a, b, c) VALUES (1, 2, 3)");
    assert_plans!("EXPLAIN UPDATE t SET a = 10 WHERE b > 5");
    assert_plans!("EXPLAIN DELETE FROM t WHERE a < 0");
}
