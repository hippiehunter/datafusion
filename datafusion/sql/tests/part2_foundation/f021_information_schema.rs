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

//! SQL:2016 Feature F021 - Basic information schema
//!
//! ISO/IEC 9075-11:2016 (SQL/Schemata)
//!
//! This feature provides access to database metadata through standard
//! INFORMATION_SCHEMA views, allowing applications to query schema information
//! using SQL rather than proprietary system catalogs.
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | F021-01 | COLUMNS view | Gap |
//! | F021-02 | TABLES view | Gap |
//! | F021-03 | VIEWS view | Gap |
//! | F021-04 | TABLE_CONSTRAINTS view | Gap |
//! | F021-05 | REFERENTIAL_CONSTRAINTS view | Gap |
//! | F021-06 | CHECK_CONSTRAINTS view | Gap |
//!
//! F021 is a CORE feature (mandatory for SQL:2016 conformance).
//!
//! # Information Schema Overview
//!
//! The INFORMATION_SCHEMA is a special schema that contains views providing
//! access to database metadata. All views are read-only and use standard
//! column names defined by the SQL standard.
//!
//! # Standard Views
//!
//! - TABLES: Information about tables and views
//! - COLUMNS: Information about columns in tables and views
//! - VIEWS: Information about views (view definitions)
//! - TABLE_CONSTRAINTS: Information about table constraints
//! - REFERENTIAL_CONSTRAINTS: Information about foreign key constraints
//! - CHECK_CONSTRAINTS: Information about check constraints

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// F021-01: INFORMATION_SCHEMA.COLUMNS view
// ============================================================================

/// F021-01: Basic query on INFORMATION_SCHEMA.COLUMNS
#[test]
fn f021_01_columns_view_basic() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.COLUMNS",
        "F021-01",
        "INFORMATION_SCHEMA.COLUMNS view"
    );
}

/// F021-01: Query COLUMNS view with WHERE clause
#[test]
fn f021_01_columns_view_filtered() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'person'",
        "F021-01",
        "COLUMNS view with WHERE clause"
    );
}

/// F021-01: Query COLUMNS view with specific columns
#[test]
fn f021_01_columns_view_specific_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE \
         FROM INFORMATION_SCHEMA.COLUMNS",
        "F021-01",
        "COLUMNS view specific columns"
    );
}

/// F021-01: Query COLUMNS for specific table
#[test]
fn f021_01_columns_for_table() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_NAME = 'person' \
         ORDER BY ORDINAL_POSITION",
        "F021-01",
        "COLUMNS for specific table"
    );
}

/// F021-01: Query COLUMNS with schema filter
#[test]
fn f021_01_columns_with_schema_filter() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = 'public'",
        "F021-01",
        "COLUMNS with schema filter"
    );
}

/// F021-01: Query COLUMNS with data type filter
#[test]
fn f021_01_columns_by_data_type() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT TABLE_NAME, COLUMN_NAME \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE DATA_TYPE = 'INTEGER'",
        "F021-01",
        "COLUMNS filtered by data type"
    );
}

/// F021-01: Query nullable columns
#[test]
fn f021_01_nullable_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT TABLE_NAME, COLUMN_NAME \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE IS_NULLABLE = 'YES'",
        "F021-01",
        "Query nullable columns"
    );
}

/// F021-01: Count columns per table
#[test]
fn f021_01_count_columns_per_table() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.COLUMNS
    assert_feature_supported!(
        "SELECT TABLE_NAME, COUNT(*) AS column_count \
         FROM INFORMATION_SCHEMA.COLUMNS \
         GROUP BY TABLE_NAME \
         ORDER BY column_count DESC",
        "F021-01",
        "Count columns per table"
    );
}

// ============================================================================
// F021-02: INFORMATION_SCHEMA.TABLES view
// ============================================================================

/// F021-02: Basic query on INFORMATION_SCHEMA.TABLES
#[test]
fn f021_02_tables_view_basic() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.TABLES",
        "F021-02",
        "INFORMATION_SCHEMA.TABLES view"
    );
}

/// F021-02: Query TABLES view with WHERE clause
#[test]
fn f021_02_tables_view_filtered() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
        "F021-02",
        "TABLES view with WHERE clause"
    );
}

/// F021-02: Query TABLES view with specific columns
#[test]
fn f021_02_tables_view_specific_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
         FROM INFORMATION_SCHEMA.TABLES",
        "F021-02",
        "TABLES view specific columns"
    );
}

/// F021-02: List all base tables
#[test]
fn f021_02_list_base_tables() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT TABLE_NAME \
         FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_TYPE = 'BASE TABLE' \
         ORDER BY TABLE_NAME",
        "F021-02",
        "List all base tables"
    );
}

/// F021-02: List all views
#[test]
fn f021_02_list_views() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT TABLE_NAME \
         FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_TYPE = 'VIEW' \
         ORDER BY TABLE_NAME",
        "F021-02",
        "List all views"
    );
}

/// F021-02: Query tables in specific schema
#[test]
fn f021_02_tables_in_schema() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT TABLE_NAME, TABLE_TYPE \
         FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_SCHEMA = 'public'",
        "F021-02",
        "Tables in specific schema"
    );
}

/// F021-02: Count tables by type
#[test]
fn f021_02_count_tables_by_type() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT TABLE_TYPE, COUNT(*) AS table_count \
         FROM INFORMATION_SCHEMA.TABLES \
         GROUP BY TABLE_TYPE",
        "F021-02",
        "Count tables by type"
    );
}

/// F021-02: Find table by name pattern
#[test]
fn f021_02_find_table_by_pattern() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLES
    assert_feature_supported!(
        "SELECT TABLE_NAME \
         FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_NAME LIKE 'person%'",
        "F021-02",
        "Find table by name pattern"
    );
}

// ============================================================================
// F021-03: INFORMATION_SCHEMA.VIEWS view
// ============================================================================

/// F021-03: Basic query on INFORMATION_SCHEMA.VIEWS
#[test]
fn f021_03_views_view_basic() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.VIEWS",
        "F021-03",
        "INFORMATION_SCHEMA.VIEWS view"
    );
}

/// F021-03: Query VIEWS view with specific columns
#[test]
fn f021_03_views_view_specific_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION \
         FROM INFORMATION_SCHEMA.VIEWS",
        "F021-03",
        "VIEWS view specific columns"
    );
}

/// F021-03: List all view names
#[test]
fn f021_03_list_view_names() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT TABLE_NAME \
         FROM INFORMATION_SCHEMA.VIEWS \
         ORDER BY TABLE_NAME",
        "F021-03",
        "List all view names"
    );
}

/// F021-03: Query view definition
#[test]
fn f021_03_query_view_definition() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT VIEW_DEFINITION \
         FROM INFORMATION_SCHEMA.VIEWS \
         WHERE TABLE_NAME = 'person_view'",
        "F021-03",
        "Query view definition"
    );
}

/// F021-03: Views in specific schema
#[test]
fn f021_03_views_in_schema() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT TABLE_NAME, VIEW_DEFINITION \
         FROM INFORMATION_SCHEMA.VIEWS \
         WHERE TABLE_SCHEMA = 'public'",
        "F021-03",
        "Views in specific schema"
    );
}

/// F021-03: Check if view is updatable
#[test]
fn f021_03_check_updatable_view() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT TABLE_NAME, IS_UPDATABLE \
         FROM INFORMATION_SCHEMA.VIEWS",
        "F021-03",
        "Check if view is updatable"
    );
}

/// F021-03: Find views by name pattern
#[test]
fn f021_03_find_views_by_pattern() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.VIEWS
    assert_feature_supported!(
        "SELECT TABLE_NAME \
         FROM INFORMATION_SCHEMA.VIEWS \
         WHERE TABLE_NAME LIKE '%_view'",
        "F021-03",
        "Find views by name pattern"
    );
}

// ============================================================================
// F021-04: INFORMATION_SCHEMA.TABLE_CONSTRAINTS view
// ============================================================================

/// F021-04: Basic query on INFORMATION_SCHEMA.TABLE_CONSTRAINTS
#[test]
fn f021_04_table_constraints_view_basic() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
        "F021-04",
        "INFORMATION_SCHEMA.TABLE_CONSTRAINTS view"
    );
}

/// F021-04: Query TABLE_CONSTRAINTS with specific columns
#[test]
fn f021_04_table_constraints_specific_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS",
        "F021-04",
        "TABLE_CONSTRAINTS specific columns"
    );
}

/// F021-04: List constraints for specific table
#[test]
fn f021_04_constraints_for_table() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         WHERE TABLE_NAME = 'person'",
        "F021-04",
        "Constraints for specific table"
    );
}

/// F021-04: List all primary keys
#[test]
fn f021_04_list_primary_keys() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT TABLE_NAME, CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'",
        "F021-04",
        "List all primary keys"
    );
}

/// F021-04: List all foreign keys
#[test]
fn f021_04_list_foreign_keys() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT TABLE_NAME, CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         WHERE CONSTRAINT_TYPE = 'FOREIGN KEY'",
        "F021-04",
        "List all foreign keys"
    );
}

/// F021-04: List all unique constraints
#[test]
fn f021_04_list_unique_constraints() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT TABLE_NAME, CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         WHERE CONSTRAINT_TYPE = 'UNIQUE'",
        "F021-04",
        "List all unique constraints"
    );
}

/// F021-04: List all check constraints
#[test]
fn f021_04_list_check_constraints() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT TABLE_NAME, CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         WHERE CONSTRAINT_TYPE = 'CHECK'",
        "F021-04",
        "List all check constraints"
    );
}

/// F021-04: Count constraints by type
#[test]
fn f021_04_count_constraints_by_type() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_TYPE, COUNT(*) AS constraint_count \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         GROUP BY CONSTRAINT_TYPE",
        "F021-04",
        "Count constraints by type"
    );
}

// ============================================================================
// F021-05: INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS view
// ============================================================================

/// F021-05: Basic query on INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
#[test]
fn f021_05_referential_constraints_view_basic() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
        "F021-05",
        "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS view"
    );
}

/// F021-05: Query REFERENTIAL_CONSTRAINTS with specific columns
#[test]
fn f021_05_referential_constraints_specific_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME, DELETE_RULE, UPDATE_RULE \
         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
        "F021-05",
        "REFERENTIAL_CONSTRAINTS specific columns"
    );
}

/// F021-05: Query foreign key relationships
#[test]
fn f021_05_foreign_key_relationships() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    assert_feature_supported!(
        "SELECT rc.CONSTRAINT_NAME, rc.UNIQUE_CONSTRAINT_NAME, rc.DELETE_RULE \
         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc",
        "F021-05",
        "Foreign key relationships"
    );
}

/// F021-05: Query CASCADE delete rules
#[test]
fn f021_05_cascade_delete_rules() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS \
         WHERE DELETE_RULE = 'CASCADE'",
        "F021-05",
        "CASCADE delete rules"
    );
}

/// F021-05: Query CASCADE update rules
#[test]
fn f021_05_cascade_update_rules() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS \
         WHERE UPDATE_RULE = 'CASCADE'",
        "F021-05",
        "CASCADE update rules"
    );
}

/// F021-05: Join TABLE_CONSTRAINTS and REFERENTIAL_CONSTRAINTS
#[test]
fn f021_05_join_table_and_referential_constraints() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA views
    assert_feature_supported!(
        "SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, rc.DELETE_RULE, rc.UPDATE_RULE \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
         JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc \
           ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME \
         WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'",
        "F021-05",
        "Join TABLE_CONSTRAINTS and REFERENTIAL_CONSTRAINTS"
    );
}

// ============================================================================
// F021-06: INFORMATION_SCHEMA.CHECK_CONSTRAINTS view
// ============================================================================

/// F021-06: Basic query on INFORMATION_SCHEMA.CHECK_CONSTRAINTS
#[test]
fn f021_06_check_constraints_view_basic() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.CHECK_CONSTRAINTS
    assert_feature_supported!(
        "SELECT * FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS",
        "F021-06",
        "INFORMATION_SCHEMA.CHECK_CONSTRAINTS view"
    );
}

/// F021-06: Query CHECK_CONSTRAINTS with specific columns
#[test]
fn f021_06_check_constraints_specific_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.CHECK_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME, CHECK_CLAUSE \
         FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS",
        "F021-06",
        "CHECK_CONSTRAINTS specific columns"
    );
}

/// F021-06: Query check constraint definitions
#[test]
fn f021_06_check_constraint_definitions() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.CHECK_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME, CHECK_CLAUSE \
         FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS \
         ORDER BY CONSTRAINT_NAME",
        "F021-06",
        "Check constraint definitions"
    );
}

/// F021-06: Find check constraints by name pattern
#[test]
fn f021_06_find_check_by_pattern() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA.CHECK_CONSTRAINTS
    assert_feature_supported!(
        "SELECT CONSTRAINT_NAME, CHECK_CLAUSE \
         FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS \
         WHERE CONSTRAINT_NAME LIKE 'chk_%'",
        "F021-06",
        "Find check constraints by pattern"
    );
}

/// F021-06: Join TABLE_CONSTRAINTS and CHECK_CONSTRAINTS
#[test]
fn f021_06_join_table_and_check_constraints() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA views
    assert_feature_supported!(
        "SELECT tc.TABLE_NAME, cc.CONSTRAINT_NAME, cc.CHECK_CLAUSE \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
         JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc \
           ON tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME \
         WHERE tc.CONSTRAINT_TYPE = 'CHECK'",
        "F021-06",
        "Join TABLE_CONSTRAINTS and CHECK_CONSTRAINTS"
    );
}

// ============================================================================
// F021 Combined Tests - Complex INFORMATION_SCHEMA queries
// ============================================================================

/// F021: Query table and column information together
#[test]
fn f021_combined_tables_and_columns() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA views
    assert_feature_supported!(
        "SELECT t.TABLE_NAME, t.TABLE_TYPE, COUNT(c.COLUMN_NAME) AS column_count \
         FROM INFORMATION_SCHEMA.TABLES t \
         JOIN INFORMATION_SCHEMA.COLUMNS c \
           ON t.TABLE_NAME = c.TABLE_NAME \
         WHERE t.TABLE_TYPE = 'BASE TABLE' \
         GROUP BY t.TABLE_NAME, t.TABLE_TYPE \
         ORDER BY column_count DESC",
        "F021",
        "Tables and columns combined query"
    );
}

/// F021: Complete schema metadata query
#[test]
fn f021_combined_complete_metadata() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA views
    assert_feature_supported!(
        "SELECT \
           t.TABLE_NAME, \
           c.COLUMN_NAME, \
           c.DATA_TYPE, \
           c.IS_NULLABLE, \
           tc.CONSTRAINT_TYPE \
         FROM INFORMATION_SCHEMA.TABLES t \
         JOIN INFORMATION_SCHEMA.COLUMNS c \
           ON t.TABLE_NAME = c.TABLE_NAME \
         LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
           ON t.TABLE_NAME = tc.TABLE_NAME \
         WHERE t.TABLE_NAME = 'person'",
        "F021",
        "Complete schema metadata"
    );
}

/// F021: Find all tables with foreign keys
#[test]
fn f021_combined_tables_with_fk() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA views
    assert_feature_supported!(
        "SELECT DISTINCT t.TABLE_NAME \
         FROM INFORMATION_SCHEMA.TABLES t \
         JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
           ON t.TABLE_NAME = tc.TABLE_NAME \
         WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'",
        "F021",
        "Find tables with foreign keys"
    );
}

/// F021: Schema comparison query
#[test]
fn f021_combined_schema_comparison() {
    // GAP: DataFusion does not currently support INFORMATION_SCHEMA views
    assert_feature_supported!(
        "SELECT TABLE_SCHEMA, COUNT(*) AS table_count \
         FROM INFORMATION_SCHEMA.TABLES \
         GROUP BY TABLE_SCHEMA \
         HAVING COUNT(*) > 5",
        "F021",
        "Schema comparison query"
    );
}
