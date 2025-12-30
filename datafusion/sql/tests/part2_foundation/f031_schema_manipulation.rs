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

//! SQL:2016 Feature F031 - Basic schema manipulation
//!
//! ISO/IEC 9075-2:2016 Section 11
//!
//! This feature covers basic schema manipulation operations required by Core SQL:
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F031-01 | CREATE TABLE statement to create persistent base tables | Partial |
//! | F031-02 | CREATE VIEW statement | Partial |
//! | F031-03 | GRANT statement | Not Implemented |
//! | F031-04 | ALTER TABLE statement: ADD COLUMN clause | Partial |
//! | F031-13 | DROP TABLE statement: RESTRICT clause | Partial |
//! | F031-16 | DROP VIEW statement: RESTRICT clause | Partial |
//! | F031-19 | REVOKE statement: RESTRICT clause | Not Implemented |
//!
//! Related features:
//! - F311: Schema definition statement
//! - F033: ALTER TABLE statement: DROP COLUMN clause
//! - F381: Extended schema manipulation (ADD/DROP CONSTRAINT)
//!
//! F031 is a CORE feature (mandatory for SQL:2016 conformance).

use crate::{assert_plans, assert_feature_supported};

// ============================================================================
// F031-01: CREATE TABLE statement to create persistent base tables
// ============================================================================

/// F031-01: Basic CREATE TABLE with single column
#[test]
fn f031_01_create_table_basic() {
    assert_feature_supported!(
        "CREATE TABLE t (x INT)",
        "F031-01",
        "Basic CREATE TABLE"
    );
}

/// F031-01: CREATE TABLE with multiple columns
#[test]
fn f031_01_create_table_multiple_columns() {
    assert_feature_supported!(
        "CREATE TABLE person_test (id INT, name VARCHAR(100), age INT)",
        "F031-01",
        "CREATE TABLE with multiple columns"
    );
}

/// F031-01: CREATE TABLE with various data types
#[test]
fn f031_01_create_table_various_types() {
    assert_feature_supported!(
        "CREATE TABLE t (
            int_col INT,
            varchar_col VARCHAR(50),
            decimal_col DECIMAL(10,2),
            date_col DATE,
            bool_col BOOLEAN
        )",
        "F031-01",
        "CREATE TABLE with various data types"
    );
}

/// F031-01: CREATE TABLE with NOT NULL constraints
#[test]
fn f031_01_create_table_not_null() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT NOT NULL, name VARCHAR(100) NOT NULL)",
        "F031-01",
        "CREATE TABLE with NOT NULL"
    );
}

/// F031-01: CREATE TABLE with PRIMARY KEY
#[test]
fn f031_01_create_table_primary_key() {
    assert_feature_supported!(
        "CREATE TABLE person_pk (id INT PRIMARY KEY, name VARCHAR(100))",
        "F031-01",
        "CREATE TABLE with PRIMARY KEY"
    );
}

/// F031-01: CREATE TABLE with DEFAULT values
#[test]
fn f031_01_create_table_defaults() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT DEFAULT 0, status VARCHAR(20) DEFAULT 'active')",
        "F031-01",
        "CREATE TABLE with DEFAULT values"
    );
}

/// F031-01: CREATE TABLE with CHECK constraints
#[test]
fn f031_01_create_table_check() {
    assert_feature_supported!(
        "CREATE TABLE t (age INT CHECK (age >= 0))",
        "F031-01",
        "CREATE TABLE with CHECK constraint"
    );
}

/// F031-01: CREATE TABLE with UNIQUE constraints
#[test]
fn f031_01_create_table_unique() {
    assert_feature_supported!(
        "CREATE TABLE person_unique (email VARCHAR(100) UNIQUE)",
        "F031-01",
        "CREATE TABLE with UNIQUE constraint"
    );
}

/// F031-01: CREATE TABLE with FOREIGN KEY
#[test]
fn f031_01_create_table_foreign_key() {
    assert_feature_supported!(
        "CREATE TABLE orders (customer_id INT REFERENCES person(id))",
        "F031-01",
        "CREATE TABLE with FOREIGN KEY"
    );
}

/// F031-01: CREATE TABLE IF NOT EXISTS
#[test]
fn f031_01_create_table_if_not_exists() {
    assert_feature_supported!(
        "CREATE TABLE IF NOT EXISTS t (x INT)",
        "F031-01",
        "CREATE TABLE IF NOT EXISTS"
    );
}

/// F031-01: CREATE TABLE with qualified name
#[test]
fn f031_01_create_table_qualified() {
    assert_feature_supported!(
        "CREATE TABLE schema_name.t (x INT)",
        "F031-01",
        "CREATE TABLE with qualified name"
    );
}

// ============================================================================
// F031-02: CREATE VIEW statement
// ============================================================================

/// F031-02: Basic CREATE VIEW
#[test]
fn f031_02_create_view_basic() {
    assert_feature_supported!(
        "CREATE VIEW v AS SELECT * FROM t",
        "F031-02",
        "Basic CREATE VIEW"
    );
}

/// F031-02: CREATE VIEW with column list
#[test]
fn f031_02_create_view_column_list() {
    assert_feature_supported!(
        "CREATE VIEW v (person_id, full_name) AS SELECT id, first_name FROM person",
        "F031-02",
        "CREATE VIEW with column list"
    );
}

/// F031-02: CREATE VIEW with WHERE clause
#[test]
fn f031_02_create_view_where() {
    assert_feature_supported!(
        "CREATE VIEW active_persons AS SELECT * FROM person WHERE age > 18",
        "F031-02",
        "CREATE VIEW with WHERE clause"
    );
}

/// F031-02: CREATE VIEW with JOIN
#[test]
fn f031_02_create_view_join() {
    assert_feature_supported!(
        "CREATE VIEW customer_orders AS
         SELECT p.first_name, o.order_id
         FROM person p JOIN orders o ON p.id = o.customer_id",
        "F031-02",
        "CREATE VIEW with JOIN"
    );
}

/// F031-02: CREATE VIEW with aggregation
#[test]
fn f031_02_create_view_aggregation() {
    assert_feature_supported!(
        "CREATE VIEW order_summary AS
         SELECT customer_id, COUNT(*) as order_count
         FROM orders
         GROUP BY customer_id",
        "F031-02",
        "CREATE VIEW with aggregation"
    );
}

/// F031-02: CREATE VIEW IF NOT EXISTS
#[test]
fn f031_02_create_view_if_not_exists() {
    assert_feature_supported!(
        "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t",
        "F031-02",
        "CREATE VIEW IF NOT EXISTS"
    );
}

/// F031-02: CREATE OR REPLACE VIEW
#[test]
fn f031_02_create_or_replace_view() {
    assert_feature_supported!(
        "CREATE OR REPLACE VIEW v AS SELECT * FROM t",
        "F031-02",
        "CREATE OR REPLACE VIEW"
    );
}

/// F031-02: CREATE VIEW with qualified name
#[test]
fn f031_02_create_view_qualified() {
    assert_feature_supported!(
        "CREATE VIEW schema_name.v AS SELECT * FROM t",
        "F031-02",
        "CREATE VIEW with qualified name"
    );
}

/// F031-02: CREATE VIEW on another view
#[test]
fn f031_02_create_view_on_view() {
    assert_feature_supported!(
        "CREATE VIEW v2 AS SELECT * FROM v",
        "F031-02",
        "CREATE VIEW on another view"
    );
}

// ============================================================================
// F031-03: GRANT statement
// ============================================================================

/// F031-03: GRANT SELECT privilege
#[test]
fn f031_03_grant_select() {
    // GAP: DataFusion does not currently support GRANT statements
    assert_feature_supported!(
        "GRANT SELECT ON t TO user1",
        "F031-03",
        "GRANT SELECT privilege"
    );
}

/// F031-03: GRANT multiple privileges
#[test]
fn f031_03_grant_multiple() {
    // GAP: DataFusion does not currently support GRANT statements
    assert_feature_supported!(
        "GRANT SELECT, INSERT, UPDATE ON person TO user1",
        "F031-03",
        "GRANT multiple privileges"
    );
}

/// F031-03: GRANT ALL PRIVILEGES
#[test]
fn f031_03_grant_all() {
    // GAP: DataFusion does not currently support GRANT statements
    assert_feature_supported!(
        "GRANT ALL PRIVILEGES ON t TO user1",
        "F031-03",
        "GRANT ALL PRIVILEGES"
    );
}

/// F031-03: GRANT with WITH GRANT OPTION
#[test]
fn f031_03_grant_with_grant_option() {
    // GAP: DataFusion does not currently support GRANT statements
    assert_feature_supported!(
        "GRANT SELECT ON t TO user1 WITH GRANT OPTION",
        "F031-03",
        "GRANT with WITH GRANT OPTION"
    );
}

/// F031-03: GRANT to PUBLIC
#[test]
fn f031_03_grant_to_public() {
    // GAP: DataFusion does not currently support GRANT statements
    assert_feature_supported!(
        "GRANT SELECT ON t TO PUBLIC",
        "F031-03",
        "GRANT to PUBLIC"
    );
}

// ============================================================================
// F031-04: ALTER TABLE statement: ADD COLUMN clause
// ============================================================================

/// F031-04: ALTER TABLE ADD COLUMN basic
#[test]
fn f031_04_alter_table_add_column_basic() {
    assert_feature_supported!(
        "ALTER TABLE t ADD COLUMN new_col INT",
        "F031-04",
        "ALTER TABLE ADD COLUMN basic"
    );
}

/// F031-04: ALTER TABLE ADD COLUMN with data type
#[test]
fn f031_04_alter_table_add_column_varchar() {
    assert_feature_supported!(
        "ALTER TABLE person ADD COLUMN email VARCHAR(100)",
        "F031-04",
        "ALTER TABLE ADD COLUMN VARCHAR"
    );
}

/// F031-04: ALTER TABLE ADD COLUMN with NOT NULL
#[test]
fn f031_04_alter_table_add_column_not_null() {
    assert_feature_supported!(
        "ALTER TABLE t ADD COLUMN new_col INT NOT NULL",
        "F031-04",
        "ALTER TABLE ADD COLUMN with NOT NULL"
    );
}

/// F031-04: ALTER TABLE ADD COLUMN with DEFAULT
#[test]
fn f031_04_alter_table_add_column_default() {
    assert_feature_supported!(
        "ALTER TABLE t ADD COLUMN status VARCHAR(20) DEFAULT 'active'",
        "F031-04",
        "ALTER TABLE ADD COLUMN with DEFAULT"
    );
}

/// F031-04: ALTER TABLE ADD COLUMN with CHECK
#[test]
fn f031_04_alter_table_add_column_check() {
    assert_feature_supported!(
        "ALTER TABLE t ADD COLUMN age INT CHECK (age >= 0)",
        "F031-04",
        "ALTER TABLE ADD COLUMN with CHECK"
    );
}

/// F031-04: ALTER TABLE ADD COLUMN with UNIQUE
#[test]
fn f031_04_alter_table_add_column_unique() {
    assert_feature_supported!(
        "ALTER TABLE t ADD COLUMN email VARCHAR(100) UNIQUE",
        "F031-04",
        "ALTER TABLE ADD COLUMN with UNIQUE"
    );
}

/// F031-04: ALTER TABLE ADD COLUMN without COLUMN keyword
#[test]
fn f031_04_alter_table_add_without_column() {
    assert_feature_supported!(
        "ALTER TABLE t ADD new_col INT",
        "F031-04",
        "ALTER TABLE ADD without COLUMN keyword"
    );
}

/// F031-04: ALTER TABLE ADD IF NOT EXISTS
#[test]
fn f031_04_alter_table_add_if_not_exists() {
    assert_feature_supported!(
        "ALTER TABLE t ADD COLUMN IF NOT EXISTS new_col INT",
        "F031-04",
        "ALTER TABLE ADD COLUMN IF NOT EXISTS"
    );
}

// ============================================================================
// F031-13: DROP TABLE statement: RESTRICT clause
// ============================================================================

/// F031-13: DROP TABLE basic
#[test]
fn f031_13_drop_table_basic() {
    assert_feature_supported!(
        "DROP TABLE t",
        "F031-13",
        "Basic DROP TABLE"
    );
}

/// F031-13: DROP TABLE with RESTRICT
#[test]
fn f031_13_drop_table_restrict() {
    assert_feature_supported!(
        "DROP TABLE t RESTRICT",
        "F031-13",
        "DROP TABLE RESTRICT"
    );
}

/// F031-13: DROP TABLE with CASCADE
#[test]
fn f031_13_drop_table_cascade() {
    assert_feature_supported!(
        "DROP TABLE t CASCADE",
        "F031-13",
        "DROP TABLE CASCADE"
    );
}

/// F031-13: DROP TABLE IF EXISTS
#[test]
fn f031_13_drop_table_if_exists() {
    assert_feature_supported!(
        "DROP TABLE IF EXISTS t",
        "F031-13",
        "DROP TABLE IF EXISTS"
    );
}

/// F031-13: DROP TABLE with qualified name
#[test]
fn f031_13_drop_table_qualified() {
    assert_feature_supported!(
        "DROP TABLE schema_name.t",
        "F031-13",
        "DROP TABLE with qualified name"
    );
}

/// F031-13: DROP TABLE IF EXISTS with RESTRICT
#[test]
fn f031_13_drop_table_if_exists_restrict() {
    assert_feature_supported!(
        "DROP TABLE IF EXISTS t RESTRICT",
        "F031-13",
        "DROP TABLE IF EXISTS RESTRICT"
    );
}

/// F031-13: DROP TABLE IF EXISTS with CASCADE
#[test]
fn f031_13_drop_table_if_exists_cascade() {
    assert_feature_supported!(
        "DROP TABLE IF EXISTS t CASCADE",
        "F031-13",
        "DROP TABLE IF EXISTS CASCADE"
    );
}

// ============================================================================
// F031-16: DROP VIEW statement: RESTRICT clause
// ============================================================================

/// F031-16: DROP VIEW basic
#[test]
fn f031_16_drop_view_basic() {
    assert_feature_supported!(
        "DROP VIEW v",
        "F031-16",
        "Basic DROP VIEW"
    );
}

/// F031-16: DROP VIEW with RESTRICT
#[test]
fn f031_16_drop_view_restrict() {
    assert_feature_supported!(
        "DROP VIEW v RESTRICT",
        "F031-16",
        "DROP VIEW RESTRICT"
    );
}

/// F031-16: DROP VIEW with CASCADE
#[test]
fn f031_16_drop_view_cascade() {
    assert_feature_supported!(
        "DROP VIEW v CASCADE",
        "F031-16",
        "DROP VIEW CASCADE"
    );
}

/// F031-16: DROP VIEW IF EXISTS
#[test]
fn f031_16_drop_view_if_exists() {
    assert_feature_supported!(
        "DROP VIEW IF EXISTS v",
        "F031-16",
        "DROP VIEW IF EXISTS"
    );
}

/// F031-16: DROP VIEW with qualified name
#[test]
fn f031_16_drop_view_qualified() {
    assert_feature_supported!(
        "DROP VIEW schema_name.v",
        "F031-16",
        "DROP VIEW with qualified name"
    );
}

/// F031-16: DROP VIEW IF EXISTS with RESTRICT
#[test]
fn f031_16_drop_view_if_exists_restrict() {
    assert_feature_supported!(
        "DROP VIEW IF EXISTS v RESTRICT",
        "F031-16",
        "DROP VIEW IF EXISTS RESTRICT"
    );
}

/// F031-16: DROP VIEW IF EXISTS with CASCADE
#[test]
fn f031_16_drop_view_if_exists_cascade() {
    assert_feature_supported!(
        "DROP VIEW IF EXISTS v CASCADE",
        "F031-16",
        "DROP VIEW IF EXISTS CASCADE"
    );
}

// ============================================================================
// F031-19: REVOKE statement: RESTRICT clause
// ============================================================================

/// F031-19: REVOKE basic
#[test]
fn f031_19_revoke_basic() {
    // GAP: DataFusion does not currently support REVOKE statements
    assert_feature_supported!(
        "REVOKE SELECT ON t FROM user1",
        "F031-19",
        "Basic REVOKE"
    );
}

/// F031-19: REVOKE with RESTRICT
#[test]
fn f031_19_revoke_restrict() {
    // GAP: DataFusion does not currently support REVOKE statements
    assert_feature_supported!(
        "REVOKE SELECT ON t FROM user1 RESTRICT",
        "F031-19",
        "REVOKE with RESTRICT"
    );
}

/// F031-19: REVOKE with CASCADE
#[test]
fn f031_19_revoke_cascade() {
    // GAP: DataFusion does not currently support REVOKE statements
    assert_feature_supported!(
        "REVOKE SELECT ON t FROM user1 CASCADE",
        "F031-19",
        "REVOKE with CASCADE"
    );
}

/// F031-19: REVOKE multiple privileges
#[test]
fn f031_19_revoke_multiple() {
    // GAP: DataFusion does not currently support REVOKE statements
    assert_feature_supported!(
        "REVOKE SELECT, INSERT, UPDATE ON person FROM user1",
        "F031-19",
        "REVOKE multiple privileges"
    );
}

/// F031-19: REVOKE ALL PRIVILEGES
#[test]
fn f031_19_revoke_all() {
    // GAP: DataFusion does not currently support REVOKE statements
    assert_feature_supported!(
        "REVOKE ALL PRIVILEGES ON t FROM user1",
        "F031-19",
        "REVOKE ALL PRIVILEGES"
    );
}

/// F031-19: REVOKE GRANT OPTION FOR
#[test]
fn f031_19_revoke_grant_option() {
    // GAP: DataFusion does not currently support REVOKE statements
    assert_feature_supported!(
        "REVOKE GRANT OPTION FOR SELECT ON t FROM user1",
        "F031-19",
        "REVOKE GRANT OPTION FOR"
    );
}

// ============================================================================
// F311: Schema definition statement
// ============================================================================

/// F311-01: CREATE SCHEMA basic
#[test]
fn f311_01_create_schema_basic() {
    assert_feature_supported!(
        "CREATE SCHEMA schema_name",
        "F311-01",
        "Basic CREATE SCHEMA"
    );
}

/// F311-01: CREATE SCHEMA IF NOT EXISTS
#[test]
fn f311_01_create_schema_if_not_exists() {
    assert_feature_supported!(
        "CREATE SCHEMA IF NOT EXISTS schema_name",
        "F311-01",
        "CREATE SCHEMA IF NOT EXISTS"
    );
}

/// F311-01: CREATE SCHEMA with AUTHORIZATION
#[test]
fn f311_01_create_schema_authorization() {
    // GAP: DataFusion may not support AUTHORIZATION clause
    assert_feature_supported!(
        "CREATE SCHEMA schema_name AUTHORIZATION user1",
        "F311-01",
        "CREATE SCHEMA with AUTHORIZATION"
    );
}

/// F311-02: CREATE TABLE in schema context (F311-02)
#[test]
fn f311_02_create_table_persistent() {
    assert_feature_supported!(
        "CREATE TABLE person (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )",
        "F311-02",
        "CREATE TABLE for persistent base tables"
    );
}

/// F311-03: CREATE VIEW in schema context (F311-03)
#[test]
fn f311_03_create_view() {
    assert_feature_supported!(
        "CREATE VIEW person_view AS SELECT * FROM person",
        "F311-03",
        "CREATE VIEW in schema definition"
    );
}

/// F311-04: CREATE VIEW with CHECK OPTION
#[test]
fn f311_04_create_view_check_option() {
    assert_feature_supported!(
        "CREATE VIEW active_persons AS
         SELECT * FROM person WHERE age >= 18
         WITH CHECK OPTION",
        "F311-04",
        "CREATE VIEW with CHECK OPTION"
    );
}

/// F311-04: CREATE VIEW with CASCADED CHECK OPTION
#[test]
fn f311_04_create_view_cascaded_check() {
    assert_feature_supported!(
        "CREATE VIEW active_persons AS
         SELECT * FROM person WHERE age >= 18
         WITH CASCADED CHECK OPTION",
        "F311-04",
        "CREATE VIEW with CASCADED CHECK OPTION"
    );
}

/// F311-04: CREATE VIEW with LOCAL CHECK OPTION
#[test]
fn f311_04_create_view_local_check() {
    assert_feature_supported!(
        "CREATE VIEW active_persons AS
         SELECT * FROM person WHERE age >= 18
         WITH LOCAL CHECK OPTION",
        "F311-04",
        "CREATE VIEW with LOCAL CHECK OPTION"
    );
}

/// F311-05: GRANT in schema context
#[test]
fn f311_05_grant_statement() {
    // GAP: DataFusion does not currently support GRANT statements
    assert_feature_supported!(
        "GRANT SELECT, INSERT ON person TO user1",
        "F311-05",
        "GRANT statement in schema definition"
    );
}

// ============================================================================
// F033: ALTER TABLE statement: DROP COLUMN clause
// ============================================================================

/// F033: ALTER TABLE DROP COLUMN basic
#[test]
fn f033_alter_table_drop_column_basic() {
    assert_feature_supported!(
        "ALTER TABLE t DROP COLUMN old_col",
        "F033",
        "ALTER TABLE DROP COLUMN basic"
    );
}

/// F033: ALTER TABLE DROP without COLUMN keyword
#[test]
fn f033_alter_table_drop_without_column() {
    assert_feature_supported!(
        "ALTER TABLE t DROP old_col",
        "F033",
        "ALTER TABLE DROP without COLUMN keyword"
    );
}

/// F033: ALTER TABLE DROP COLUMN IF EXISTS
#[test]
fn f033_alter_table_drop_if_exists() {
    assert_feature_supported!(
        "ALTER TABLE t DROP COLUMN IF EXISTS old_col",
        "F033",
        "ALTER TABLE DROP COLUMN IF EXISTS"
    );
}

/// F033: ALTER TABLE DROP COLUMN with RESTRICT
#[test]
fn f033_alter_table_drop_restrict() {
    assert_feature_supported!(
        "ALTER TABLE t DROP COLUMN old_col RESTRICT",
        "F033",
        "ALTER TABLE DROP COLUMN RESTRICT"
    );
}

/// F033: ALTER TABLE DROP COLUMN with CASCADE
#[test]
fn f033_alter_table_drop_cascade() {
    assert_feature_supported!(
        "ALTER TABLE t DROP COLUMN old_col CASCADE",
        "F033",
        "ALTER TABLE DROP COLUMN CASCADE"
    );
}

// ============================================================================
// F381: Extended schema manipulation (ALTER TABLE ADD/DROP CONSTRAINT)
// ============================================================================

/// F381: ALTER TABLE ADD CONSTRAINT PRIMARY KEY
#[test]
fn f381_alter_table_add_primary_key() {
    assert_feature_supported!(
        "ALTER TABLE t ADD CONSTRAINT pk_t PRIMARY KEY (id)",
        "F381",
        "ALTER TABLE ADD CONSTRAINT PRIMARY KEY"
    );
}

/// F381: ALTER TABLE ADD CONSTRAINT FOREIGN KEY
#[test]
fn f381_alter_table_add_foreign_key() {
    assert_feature_supported!(
        "ALTER TABLE orders ADD CONSTRAINT fk_customer
         FOREIGN KEY (customer_id) REFERENCES person(id)",
        "F381",
        "ALTER TABLE ADD CONSTRAINT FOREIGN KEY"
    );
}

/// F381: ALTER TABLE ADD CONSTRAINT UNIQUE
#[test]
fn f381_alter_table_add_unique() {
    assert_feature_supported!(
        "ALTER TABLE t ADD CONSTRAINT uq_email UNIQUE (c)",
        "F381",
        "ALTER TABLE ADD CONSTRAINT UNIQUE"
    );
}

/// F381: ALTER TABLE ADD CONSTRAINT CHECK
#[test]
fn f381_alter_table_add_check() {
    assert_feature_supported!(
        "ALTER TABLE t ADD CONSTRAINT chk_age CHECK (age >= 0)",
        "F381",
        "ALTER TABLE ADD CONSTRAINT CHECK"
    );
}

/// F381: ALTER TABLE DROP CONSTRAINT
#[test]
fn f381_alter_table_drop_constraint() {
    assert_feature_supported!(
        "ALTER TABLE t DROP CONSTRAINT chk_age",
        "F381",
        "ALTER TABLE DROP CONSTRAINT"
    );
}

/// F381: ALTER TABLE DROP CONSTRAINT IF EXISTS
#[test]
fn f381_alter_table_drop_constraint_if_exists() {
    assert_feature_supported!(
        "ALTER TABLE t DROP CONSTRAINT IF EXISTS chk_age",
        "F381",
        "ALTER TABLE DROP CONSTRAINT IF EXISTS"
    );
}

/// F381: ALTER TABLE DROP CONSTRAINT with RESTRICT
#[test]
fn f381_alter_table_drop_constraint_restrict() {
    assert_feature_supported!(
        "ALTER TABLE t DROP CONSTRAINT chk_age RESTRICT",
        "F381",
        "ALTER TABLE DROP CONSTRAINT RESTRICT"
    );
}

/// F381: ALTER TABLE DROP CONSTRAINT with CASCADE
#[test]
fn f381_alter_table_drop_constraint_cascade() {
    assert_feature_supported!(
        "ALTER TABLE t DROP CONSTRAINT chk_age CASCADE",
        "F381",
        "ALTER TABLE DROP CONSTRAINT CASCADE"
    );
}

// ============================================================================
// Additional schema manipulation features
// ============================================================================

/// DROP SCHEMA basic
#[test]
fn drop_schema_basic() {
    assert_feature_supported!(
        "DROP SCHEMA schema_name",
        "F031",
        "Basic DROP SCHEMA"
    );
}

/// DROP SCHEMA IF EXISTS
#[test]
fn drop_schema_if_exists() {
    assert_feature_supported!(
        "DROP SCHEMA IF EXISTS schema_name",
        "F031",
        "DROP SCHEMA IF EXISTS"
    );
}

/// DROP SCHEMA with RESTRICT
#[test]
fn drop_schema_restrict() {
    assert_feature_supported!(
        "DROP SCHEMA schema_name RESTRICT",
        "F031",
        "DROP SCHEMA RESTRICT"
    );
}

/// DROP SCHEMA with CASCADE
#[test]
fn drop_schema_cascade() {
    assert_feature_supported!(
        "DROP SCHEMA schema_name CASCADE",
        "F031",
        "DROP SCHEMA CASCADE"
    );
}

/// CREATE INDEX basic
#[test]
fn create_index_basic() {
    assert_feature_supported!(
        "CREATE INDEX idx_name ON t (name)",
        "F031",
        "Basic CREATE INDEX"
    );
}

/// CREATE UNIQUE INDEX
#[test]
fn create_unique_index() {
    assert_feature_supported!(
        "CREATE UNIQUE INDEX idx_email ON person (email)",
        "F031",
        "CREATE UNIQUE INDEX"
    );
}

/// CREATE INDEX with multiple columns
#[test]
fn create_index_multiple_columns() {
    assert_feature_supported!(
        "CREATE INDEX idx_name_age ON person (name, age)",
        "F031",
        "CREATE INDEX with multiple columns"
    );
}

/// CREATE INDEX IF NOT EXISTS
#[test]
fn create_index_if_not_exists() {
    assert_feature_supported!(
        "CREATE INDEX IF NOT EXISTS idx_name ON t (name)",
        "F031",
        "CREATE INDEX IF NOT EXISTS"
    );
}

/// DROP INDEX basic
#[test]
fn drop_index_basic() {
    assert_feature_supported!(
        "DROP INDEX idx_name",
        "F031",
        "Basic DROP INDEX"
    );
}

/// DROP INDEX IF EXISTS
#[test]
fn drop_index_if_exists() {
    assert_feature_supported!(
        "DROP INDEX IF EXISTS idx_name",
        "F031",
        "DROP INDEX IF EXISTS"
    );
}

/// CREATE TABLE AS SELECT (CTAS)
#[test]
fn create_table_as_select() {
    assert_feature_supported!(
        "CREATE TABLE new_table AS SELECT * FROM person",
        "F031",
        "CREATE TABLE AS SELECT"
    );
}

/// CREATE TABLE AS SELECT with WHERE
#[test]
fn create_table_as_select_where() {
    assert_feature_supported!(
        "CREATE TABLE adults AS SELECT * FROM person WHERE age >= 18",
        "F031",
        "CREATE TABLE AS SELECT with WHERE"
    );
}

/// CREATE TABLE AS SELECT with column list
#[test]
fn create_table_as_select_columns() {
    assert_feature_supported!(
        "CREATE TABLE person_summary (person_id, full_name)
         AS SELECT id, name FROM person",
        "F031",
        "CREATE TABLE AS SELECT with columns"
    );
}

/// CREATE TABLE IF NOT EXISTS AS SELECT
#[test]
fn create_table_if_not_exists_as_select() {
    assert_feature_supported!(
        "CREATE TABLE IF NOT EXISTS new_table AS SELECT * FROM person",
        "F031",
        "CREATE TABLE IF NOT EXISTS AS SELECT"
    );
}

/// ALTER TABLE RENAME TO
#[test]
fn alter_table_rename() {
    assert_feature_supported!(
        "ALTER TABLE t RENAME TO new_table",
        "F031",
        "ALTER TABLE RENAME TO"
    );
}

/// ALTER TABLE RENAME COLUMN
#[test]
fn alter_table_rename_column() {
    assert_feature_supported!(
        "ALTER TABLE t RENAME COLUMN old_name TO new_name",
        "F031",
        "ALTER TABLE RENAME COLUMN"
    );
}

/// ALTER TABLE ALTER COLUMN SET DEFAULT
#[test]
fn alter_table_alter_column_set_default() {
    assert_feature_supported!(
        "ALTER TABLE t ALTER COLUMN status SET DEFAULT 'active'",
        "F031",
        "ALTER TABLE ALTER COLUMN SET DEFAULT"
    );
}

/// ALTER TABLE ALTER COLUMN DROP DEFAULT
#[test]
fn alter_table_alter_column_drop_default() {
    assert_feature_supported!(
        "ALTER TABLE t ALTER COLUMN status DROP DEFAULT",
        "F031",
        "ALTER TABLE ALTER COLUMN DROP DEFAULT"
    );
}

/// ALTER TABLE ALTER COLUMN SET NOT NULL
#[test]
fn alter_table_alter_column_set_not_null() {
    assert_feature_supported!(
        "ALTER TABLE t ALTER COLUMN name SET NOT NULL",
        "F031",
        "ALTER TABLE ALTER COLUMN SET NOT NULL"
    );
}

/// ALTER TABLE ALTER COLUMN DROP NOT NULL
#[test]
fn alter_table_alter_column_drop_not_null() {
    assert_feature_supported!(
        "ALTER TABLE t ALTER COLUMN name DROP NOT NULL",
        "F031",
        "ALTER TABLE ALTER COLUMN DROP NOT NULL"
    );
}

// ============================================================================
// Summary Tests - Verify overall F031 and F311 support
// ============================================================================

#[test]
fn f031_f311_summary_ddl_workflow() {
    // This test verifies a complete DDL workflow using F031 and F311 features

    // Create schema
    assert_plans!("CREATE SCHEMA IF NOT EXISTS myschema");

    // Create base tables
    assert_plans!("CREATE TABLE IF NOT EXISTS person (
        id INT PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE,
        age INT CHECK (age >= 0 AND age <= 150),
        status VARCHAR(20) DEFAULT 'active'
    )");

    assert_plans!("CREATE TABLE IF NOT EXISTS orders (
        order_id INT PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE DEFAULT CURRENT_DATE,
        total DECIMAL(10,2) DEFAULT 0.00,
        FOREIGN KEY(customer_id) REFERENCES person(id)
    )");

    // Create views
    assert_plans!("CREATE VIEW active_customers AS
                   SELECT * FROM person WHERE status = 'active'");

    assert_plans!("CREATE VIEW customer_order_summary AS
                   SELECT p.id, p.first_name, p.last_name, COUNT(o.order_id) as order_count
                   FROM person p
                   LEFT JOIN orders o ON p.id = o.customer_id
                   GROUP BY p.id, p.first_name, p.last_name");

    // Alter table - add columns
    assert_plans!("ALTER TABLE person ADD COLUMN phone VARCHAR(20)");
    assert_plans!("ALTER TABLE orders ADD COLUMN notes TEXT");

    // Alter table - drop columns
    assert_plans!("ALTER TABLE person DROP COLUMN phone");

    // Drop objects
    assert_plans!("DROP VIEW customer_order_summary");
    assert_plans!("DROP VIEW active_customers");
    assert_plans!("DROP TABLE orders");
    assert_plans!("DROP TABLE person");
}

#[test]
fn f031_table_lifecycle() {
    // Test complete table lifecycle: CREATE -> ALTER -> DROP

    // Create table
    assert_plans!("CREATE TABLE t (id INT, name VARCHAR(100))");

    // Add columns
    assert_plans!("ALTER TABLE t ADD COLUMN age INT");
    assert_plans!("ALTER TABLE t ADD COLUMN email VARCHAR(100) UNIQUE");

    // Add constraints
    assert_plans!("ALTER TABLE t ADD CONSTRAINT pk_t PRIMARY KEY (id)");
    assert_plans!("ALTER TABLE t ADD CONSTRAINT chk_age CHECK (age >= 0)");

    // Drop constraint
    assert_plans!("ALTER TABLE t DROP CONSTRAINT chk_age");

    // Drop column
    assert_plans!("ALTER TABLE t DROP COLUMN age");

    // Drop table
    assert_plans!("DROP TABLE t");
}

#[test]
fn f031_view_lifecycle() {
    // Test complete view lifecycle: CREATE -> DROP

    // Create view
    assert_plans!("CREATE VIEW v AS SELECT * FROM person");

    // Create view on view
    assert_plans!("CREATE VIEW v2 AS SELECT id, first_name FROM v");

    // Drop views (must drop dependent view first)
    assert_plans!("DROP VIEW v2");
    assert_plans!("DROP VIEW v");
}

#[test]
fn f031_ctas_workflow() {
    // Test CREATE TABLE AS SELECT workflow

    // Simple CTAS
    assert_plans!("CREATE TABLE person_copy AS SELECT * FROM person");

    // CTAS with filtering
    assert_plans!("CREATE TABLE adults AS
                   SELECT * FROM person WHERE age >= 18");

    // CTAS with aggregation
    assert_plans!("CREATE TABLE age_groups AS
                   SELECT
                     CASE
                       WHEN age < 18 THEN 'minor'
                       WHEN age < 65 THEN 'adult'
                       ELSE 'senior'
                     END as age_group,
                     COUNT(*) as count
                   FROM person
                   GROUP BY age_group");

    // Drop created tables
    assert_plans!("DROP TABLE age_groups");
    assert_plans!("DROP TABLE adults");
    assert_plans!("DROP TABLE person_copy");
}
