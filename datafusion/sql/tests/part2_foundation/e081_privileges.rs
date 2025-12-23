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

//! SQL:2016 Feature E081 - Basic privileges
//!
//! ISO/IEC 9075-2:2016 Section 12 (Access control)
//!
//! This feature covers the basic privilege system required by Core SQL:
//!
//! | Feature | Subfeature | Description | Status |
//! |---------|------------|-------------|--------|
//! | E081 | E081-01 | SELECT privilege at table level | Not Implemented |
//! | E081 | E081-02 | DELETE privilege | Not Implemented |
//! | E081 | E081-03 | INSERT privilege at table level | Not Implemented |
//! | E081 | E081-04 | UPDATE privilege at table level | Not Implemented |
//! | E081 | E081-05 | UPDATE privilege at column level | Not Implemented |
//! | E081 | E081-06 | REFERENCES privilege at table level | Not Implemented |
//! | E081 | E081-07 | REFERENCES privilege at column level | Not Implemented |
//! | E081 | E081-08 | WITH GRANT OPTION | Not Implemented |
//! | E081 | E081-09 | USAGE privilege | Not Implemented |
//! | E081 | E081-10 | EXECUTE privilege | Not Implemented |
//!
//! Related optional features also tested:
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | T331 | Basic roles (CREATE ROLE, DROP ROLE) | Not Implemented |
//! | T332 | Extended roles (GRANT role TO user) | Not Implemented |
//!
//! E081 is a CORE feature (mandatory for SQL:2016 conformance).
//!
//! ## SQL Syntax
//!
//! ```sql
//! GRANT privilege_list ON object TO grantee [WITH GRANT OPTION]
//! REVOKE [GRANT OPTION FOR] privilege_list ON object FROM grantee [CASCADE | RESTRICT]
//!
//! privilege_list ::= ALL PRIVILEGES | privilege [, privilege]...
//! privilege ::= SELECT | INSERT [(column_list)]
//!             | UPDATE [(column_list)] | DELETE
//!             | REFERENCES [(column_list)]
//!             | USAGE | EXECUTE
//!
//! object ::= [TABLE] table_name
//!          | DOMAIN domain_name
//!          | COLLATION collation_name
//!          | CHARACTER SET charset_name
//!          | TRANSLATION translation_name
//!          | TYPE type_name
//!          | SEQUENCE sequence_name
//!          | specific_routine_designator
//!
//! grantee ::= PUBLIC | authorization_identifier
//! ```

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// E081-01: SELECT privilege at table level
// ============================================================================

/// E081-01: GRANT SELECT on a table to a user
#[test]
fn e081_01_grant_select_to_user() {
    assert_feature_supported!(
        "GRANT SELECT ON person TO alice",
        "E081-01",
        "GRANT SELECT privilege to user"
    );
}

/// E081-01: GRANT SELECT on a table to a role
#[test]
fn e081_01_grant_select_to_role() {
    assert_feature_supported!(
        "GRANT SELECT ON person TO reporting_role",
        "E081-01",
        "GRANT SELECT privilege to role"
    );
}

/// E081-01: GRANT SELECT with explicit TABLE keyword
#[test]
fn e081_01_grant_select_explicit_table() {
    assert_feature_supported!(
        "GRANT SELECT ON TABLE person TO alice",
        "E081-01",
        "GRANT SELECT with TABLE keyword"
    );
}

/// E081-01: GRANT SELECT to PUBLIC
#[test]
fn e081_01_grant_select_to_public() {
    assert_feature_supported!(
        "GRANT SELECT ON person TO PUBLIC",
        "E081-01",
        "GRANT SELECT to PUBLIC"
    );
}

/// E081-01: GRANT SELECT on qualified table name
#[test]
fn e081_01_grant_select_qualified_table() {
    assert_feature_supported!(
        "GRANT SELECT ON myschema.person TO alice",
        "E081-01",
        "GRANT SELECT on qualified table"
    );
}

/// E081-01: GRANT SELECT to multiple grantees
#[test]
fn e081_01_grant_select_multiple_grantees() {
    assert_feature_supported!(
        "GRANT SELECT ON person TO alice, bob, charlie",
        "E081-01",
        "GRANT SELECT to multiple users"
    );
}

// ============================================================================
// E081-02: DELETE privilege
// ============================================================================

/// E081-02: GRANT DELETE on a table
#[test]
fn e081_02_grant_delete() {
    assert_feature_supported!(
        "GRANT DELETE ON person TO alice",
        "E081-02",
        "GRANT DELETE privilege"
    );
}

/// E081-02: GRANT DELETE with explicit TABLE keyword
#[test]
fn e081_02_grant_delete_explicit_table() {
    assert_feature_supported!(
        "GRANT DELETE ON TABLE orders TO admin_role",
        "E081-02",
        "GRANT DELETE with TABLE keyword"
    );
}

/// E081-02: GRANT DELETE to PUBLIC
#[test]
fn e081_02_grant_delete_to_public() {
    assert_feature_supported!(
        "GRANT DELETE ON orders TO PUBLIC",
        "E081-02",
        "GRANT DELETE to PUBLIC"
    );
}

// ============================================================================
// E081-03: INSERT privilege at table level
// ============================================================================

/// E081-03: GRANT INSERT on a table
#[test]
fn e081_03_grant_insert() {
    assert_feature_supported!(
        "GRANT INSERT ON person TO alice",
        "E081-03",
        "GRANT INSERT privilege"
    );
}

/// E081-03: GRANT INSERT with explicit TABLE keyword
#[test]
fn e081_03_grant_insert_explicit_table() {
    assert_feature_supported!(
        "GRANT INSERT ON TABLE person TO data_entry_role",
        "E081-03",
        "GRANT INSERT with TABLE keyword"
    );
}

/// E081-03: GRANT INSERT on multiple tables
#[test]
fn e081_03_grant_insert_multiple_tables() {
    assert_feature_supported!(
        "GRANT INSERT ON person TO alice",
        "E081-03",
        "GRANT INSERT on table"
    );
}

// ============================================================================
// E081-04: UPDATE privilege at table level
// ============================================================================

/// E081-04: GRANT UPDATE on a table
#[test]
fn e081_04_grant_update() {
    assert_feature_supported!(
        "GRANT UPDATE ON person TO alice",
        "E081-04",
        "GRANT UPDATE privilege"
    );
}

/// E081-04: GRANT UPDATE with explicit TABLE keyword
#[test]
fn e081_04_grant_update_explicit_table() {
    assert_feature_supported!(
        "GRANT UPDATE ON TABLE person TO editor_role",
        "E081-04",
        "GRANT UPDATE with TABLE keyword"
    );
}

/// E081-04: GRANT UPDATE to PUBLIC
#[test]
fn e081_04_grant_update_to_public() {
    assert_feature_supported!(
        "GRANT UPDATE ON person TO PUBLIC",
        "E081-04",
        "GRANT UPDATE to PUBLIC"
    );
}

// ============================================================================
// E081-05: UPDATE privilege at column level
// ============================================================================

/// E081-05: GRANT UPDATE on specific columns
#[test]
fn e081_05_grant_update_columns() {
    assert_feature_supported!(
        "GRANT UPDATE (salary, age) ON person TO alice",
        "E081-05",
        "GRANT UPDATE on columns"
    );
}

/// E081-05: GRANT UPDATE on single column
#[test]
fn e081_05_grant_update_single_column() {
    assert_feature_supported!(
        "GRANT UPDATE (salary) ON person TO hr_role",
        "E081-05",
        "GRANT UPDATE on single column"
    );
}

/// E081-05: GRANT UPDATE on multiple columns
#[test]
fn e081_05_grant_update_multiple_columns() {
    assert_feature_supported!(
        "GRANT UPDATE (first_name, last_name, age, salary) ON person TO admin",
        "E081-05",
        "GRANT UPDATE on multiple columns"
    );
}

/// E081-05: GRANT UPDATE on columns with TABLE keyword
#[test]
fn e081_05_grant_update_columns_explicit_table() {
    assert_feature_supported!(
        "GRANT UPDATE (salary) ON TABLE person TO hr_manager",
        "E081-05",
        "GRANT UPDATE on column with TABLE keyword"
    );
}

// ============================================================================
// E081-06: REFERENCES privilege at table level
// ============================================================================

/// E081-06: GRANT REFERENCES on a table
#[test]
fn e081_06_grant_references() {
    assert_feature_supported!(
        "GRANT REFERENCES ON person TO alice",
        "E081-06",
        "GRANT REFERENCES privilege"
    );
}

/// E081-06: GRANT REFERENCES with explicit TABLE keyword
#[test]
fn e081_06_grant_references_explicit_table() {
    assert_feature_supported!(
        "GRANT REFERENCES ON TABLE person TO schema_designer",
        "E081-06",
        "GRANT REFERENCES with TABLE keyword"
    );
}

/// E081-06: GRANT REFERENCES to PUBLIC
#[test]
fn e081_06_grant_references_to_public() {
    assert_feature_supported!(
        "GRANT REFERENCES ON person TO PUBLIC",
        "E081-06",
        "GRANT REFERENCES to PUBLIC"
    );
}

// ============================================================================
// E081-07: REFERENCES privilege at column level
// ============================================================================

/// E081-07: GRANT REFERENCES on specific columns
#[test]
fn e081_07_grant_references_columns() {
    assert_feature_supported!(
        "GRANT REFERENCES (id) ON person TO alice",
        "E081-07",
        "GRANT REFERENCES on column"
    );
}

/// E081-07: GRANT REFERENCES on multiple columns
#[test]
fn e081_07_grant_references_multiple_columns() {
    assert_feature_supported!(
        "GRANT REFERENCES (id, first_name, last_name) ON person TO fk_creator",
        "E081-07",
        "GRANT REFERENCES on multiple columns"
    );
}

/// E081-07: GRANT REFERENCES on column with TABLE keyword
#[test]
fn e081_07_grant_references_column_explicit_table() {
    assert_feature_supported!(
        "GRANT REFERENCES (id) ON TABLE person TO bob",
        "E081-07",
        "GRANT REFERENCES on column with TABLE"
    );
}

// ============================================================================
// E081-08: WITH GRANT OPTION
// ============================================================================

/// E081-08: GRANT SELECT with GRANT OPTION
#[test]
fn e081_08_grant_select_with_grant_option() {
    assert_feature_supported!(
        "GRANT SELECT ON person TO alice WITH GRANT OPTION",
        "E081-08",
        "GRANT with GRANT OPTION"
    );
}

/// E081-08: GRANT multiple privileges with GRANT OPTION
#[test]
fn e081_08_grant_multiple_with_grant_option() {
    assert_feature_supported!(
        "GRANT SELECT, INSERT, UPDATE ON person TO alice WITH GRANT OPTION",
        "E081-08",
        "GRANT multiple privileges with GRANT OPTION"
    );
}

/// E081-08: GRANT UPDATE on columns with GRANT OPTION
#[test]
fn e081_08_grant_update_columns_with_grant_option() {
    assert_feature_supported!(
        "GRANT UPDATE (salary) ON person TO hr_role WITH GRANT OPTION",
        "E081-08",
        "GRANT UPDATE on column with GRANT OPTION"
    );
}

/// E081-08: GRANT DELETE with GRANT OPTION
#[test]
fn e081_08_grant_delete_with_grant_option() {
    assert_feature_supported!(
        "GRANT DELETE ON orders TO admin WITH GRANT OPTION",
        "E081-08",
        "GRANT DELETE with GRANT OPTION"
    );
}

// ============================================================================
// E081-09: USAGE privilege
// ============================================================================

/// E081-09: GRANT USAGE on DOMAIN
#[test]
fn e081_09_grant_usage_domain() {
    assert_feature_supported!(
        "GRANT USAGE ON DOMAIN email_address TO alice",
        "E081-09",
        "GRANT USAGE on domain"
    );
}

/// E081-09: GRANT USAGE on CHARACTER SET
#[test]
fn e081_09_grant_usage_charset() {
    assert_feature_supported!(
        "GRANT USAGE ON CHARACTER SET utf8 TO alice",
        "E081-09",
        "GRANT USAGE on character set"
    );
}

/// E081-09: GRANT USAGE on COLLATION
#[test]
fn e081_09_grant_usage_collation() {
    assert_feature_supported!(
        "GRANT USAGE ON COLLATION utf8_general_ci TO alice",
        "E081-09",
        "GRANT USAGE on collation"
    );
}

/// E081-09: GRANT USAGE on SEQUENCE
#[test]
fn e081_09_grant_usage_sequence() {
    assert_feature_supported!(
        "GRANT USAGE ON SEQUENCE person_id_seq TO alice",
        "E081-09",
        "GRANT USAGE on sequence"
    );
}

/// E081-09: GRANT USAGE on TYPE
#[test]
fn e081_09_grant_usage_type() {
    assert_feature_supported!(
        "GRANT USAGE ON TYPE address_type TO alice",
        "E081-09",
        "GRANT USAGE on type"
    );
}

/// E081-09: GRANT USAGE to PUBLIC
#[test]
fn e081_09_grant_usage_to_public() {
    assert_feature_supported!(
        "GRANT USAGE ON SEQUENCE person_id_seq TO PUBLIC",
        "E081-09",
        "GRANT USAGE to PUBLIC"
    );
}

// ============================================================================
// E081-10: EXECUTE privilege
// ============================================================================

/// E081-10: GRANT EXECUTE on a function
#[test]
fn e081_10_grant_execute_function() {
    assert_feature_supported!(
        "GRANT EXECUTE ON FUNCTION calculate_bonus TO alice",
        "E081-10",
        "GRANT EXECUTE on function"
    );
}

/// E081-10: GRANT EXECUTE on a procedure
#[test]
fn e081_10_grant_execute_procedure() {
    assert_feature_supported!(
        "GRANT EXECUTE ON PROCEDURE update_salary TO alice",
        "E081-10",
        "GRANT EXECUTE on procedure"
    );
}

/// E081-10: GRANT EXECUTE on specific routine
#[test]
fn e081_10_grant_execute_specific_routine() {
    assert_feature_supported!(
        "GRANT EXECUTE ON SPECIFIC FUNCTION myschema.my_function TO alice",
        "E081-10",
        "GRANT EXECUTE on specific routine"
    );
}

/// E081-10: GRANT EXECUTE with GRANT OPTION
#[test]
fn e081_10_grant_execute_with_grant_option() {
    assert_feature_supported!(
        "GRANT EXECUTE ON FUNCTION calculate_bonus TO alice WITH GRANT OPTION",
        "E081-10",
        "GRANT EXECUTE with GRANT OPTION"
    );
}

/// E081-10: GRANT EXECUTE to PUBLIC
#[test]
fn e081_10_grant_execute_to_public() {
    assert_feature_supported!(
        "GRANT EXECUTE ON FUNCTION public_function TO PUBLIC",
        "E081-10",
        "GRANT EXECUTE to PUBLIC"
    );
}

// ============================================================================
// Multiple Privileges
// ============================================================================

/// GRANT multiple privileges on same object
#[test]
fn grant_multiple_privileges() {
    assert_feature_supported!(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON person TO alice",
        "E081",
        "GRANT multiple privileges"
    );
}

/// GRANT INSERT and UPDATE with column list
#[test]
fn grant_insert_update_columns() {
    assert_feature_supported!(
        "GRANT INSERT, UPDATE (salary, age) ON person TO alice",
        "E081",
        "GRANT INSERT and UPDATE with columns"
    );
}

/// GRANT SELECT and REFERENCES
#[test]
fn grant_select_references() {
    assert_feature_supported!(
        "GRANT SELECT, REFERENCES ON person TO alice",
        "E081",
        "GRANT SELECT and REFERENCES"
    );
}

/// GRANT ALL PRIVILEGES
#[test]
fn grant_all_privileges() {
    assert_feature_supported!(
        "GRANT ALL PRIVILEGES ON person TO alice",
        "E081",
        "GRANT ALL PRIVILEGES"
    );
}

/// GRANT ALL (shorthand)
#[test]
fn grant_all() {
    assert_feature_supported!(
        "GRANT ALL ON person TO alice",
        "E081",
        "GRANT ALL"
    );
}

/// GRANT ALL PRIVILEGES with GRANT OPTION
#[test]
fn grant_all_privileges_with_grant_option() {
    assert_feature_supported!(
        "GRANT ALL PRIVILEGES ON person TO alice WITH GRANT OPTION",
        "E081",
        "GRANT ALL PRIVILEGES with GRANT OPTION"
    );
}

// ============================================================================
// REVOKE Statements
// ============================================================================

/// REVOKE SELECT privilege
#[test]
fn revoke_select() {
    assert_feature_supported!(
        "REVOKE SELECT ON person FROM alice",
        "E081",
        "REVOKE SELECT"
    );
}

/// REVOKE INSERT privilege
#[test]
fn revoke_insert() {
    assert_feature_supported!(
        "REVOKE INSERT ON person FROM alice",
        "E081",
        "REVOKE INSERT"
    );
}

/// REVOKE UPDATE privilege
#[test]
fn revoke_update() {
    assert_feature_supported!(
        "REVOKE UPDATE ON person FROM alice",
        "E081",
        "REVOKE UPDATE"
    );
}

/// REVOKE DELETE privilege
#[test]
fn revoke_delete() {
    assert_feature_supported!(
        "REVOKE DELETE ON person FROM alice",
        "E081",
        "REVOKE DELETE"
    );
}

/// REVOKE UPDATE on specific columns
#[test]
fn revoke_update_columns() {
    assert_feature_supported!(
        "REVOKE UPDATE (salary, age) ON person FROM alice",
        "E081",
        "REVOKE UPDATE on columns"
    );
}

/// REVOKE REFERENCES
#[test]
fn revoke_references() {
    assert_feature_supported!(
        "REVOKE REFERENCES ON person FROM alice",
        "E081",
        "REVOKE REFERENCES"
    );
}

/// REVOKE REFERENCES on columns
#[test]
fn revoke_references_columns() {
    assert_feature_supported!(
        "REVOKE REFERENCES (id) ON person FROM alice",
        "E081",
        "REVOKE REFERENCES on column"
    );
}

/// REVOKE multiple privileges
#[test]
fn revoke_multiple_privileges() {
    assert_feature_supported!(
        "REVOKE SELECT, INSERT, UPDATE, DELETE ON person FROM alice",
        "E081",
        "REVOKE multiple privileges"
    );
}

/// REVOKE ALL PRIVILEGES
#[test]
fn revoke_all_privileges() {
    assert_feature_supported!(
        "REVOKE ALL PRIVILEGES ON person FROM alice",
        "E081",
        "REVOKE ALL PRIVILEGES"
    );
}

/// REVOKE ALL
#[test]
fn revoke_all() {
    assert_feature_supported!(
        "REVOKE ALL ON person FROM alice",
        "E081",
        "REVOKE ALL"
    );
}

/// REVOKE from multiple grantees
#[test]
fn revoke_multiple_grantees() {
    assert_feature_supported!(
        "REVOKE SELECT ON person FROM alice, bob, charlie",
        "E081",
        "REVOKE from multiple users"
    );
}

/// REVOKE from PUBLIC
#[test]
fn revoke_from_public() {
    assert_feature_supported!(
        "REVOKE SELECT ON person FROM PUBLIC",
        "E081",
        "REVOKE from PUBLIC"
    );
}

/// REVOKE with CASCADE
#[test]
fn revoke_cascade() {
    assert_feature_supported!(
        "REVOKE SELECT ON person FROM alice CASCADE",
        "E081",
        "REVOKE with CASCADE"
    );
}

/// REVOKE with RESTRICT
#[test]
fn revoke_restrict() {
    assert_feature_supported!(
        "REVOKE SELECT ON person FROM alice RESTRICT",
        "E081",
        "REVOKE with RESTRICT"
    );
}

/// REVOKE GRANT OPTION FOR
#[test]
fn revoke_grant_option_for() {
    assert_feature_supported!(
        "REVOKE GRANT OPTION FOR SELECT ON person FROM alice",
        "E081-08",
        "REVOKE GRANT OPTION FOR"
    );
}

/// REVOKE GRANT OPTION FOR multiple privileges
#[test]
fn revoke_grant_option_for_multiple() {
    assert_feature_supported!(
        "REVOKE GRANT OPTION FOR SELECT, INSERT, UPDATE ON person FROM alice",
        "E081-08",
        "REVOKE GRANT OPTION FOR multiple privileges"
    );
}

/// REVOKE GRANT OPTION FOR with CASCADE
#[test]
fn revoke_grant_option_cascade() {
    assert_feature_supported!(
        "REVOKE GRANT OPTION FOR SELECT ON person FROM alice CASCADE",
        "E081-08",
        "REVOKE GRANT OPTION with CASCADE"
    );
}

/// REVOKE USAGE privilege
#[test]
fn revoke_usage() {
    assert_feature_supported!(
        "REVOKE USAGE ON SEQUENCE person_id_seq FROM alice",
        "E081-09",
        "REVOKE USAGE"
    );
}

/// REVOKE EXECUTE privilege
#[test]
fn revoke_execute() {
    assert_feature_supported!(
        "REVOKE EXECUTE ON FUNCTION calculate_bonus FROM alice",
        "E081-10",
        "REVOKE EXECUTE"
    );
}

// ============================================================================
// T331: Basic roles
// ============================================================================

/// T331: CREATE ROLE
#[test]
fn t331_create_role() {
    assert_feature_supported!(
        "CREATE ROLE reporting_role",
        "T331",
        "CREATE ROLE"
    );
}

/// T331: CREATE ROLE with quoted identifier
#[test]
fn t331_create_role_quoted() {
    assert_feature_supported!(
        "CREATE ROLE \"Admin Role\"",
        "T331",
        "CREATE ROLE with quoted name"
    );
}

/// T331: DROP ROLE
#[test]
fn t331_drop_role() {
    assert_feature_supported!(
        "DROP ROLE reporting_role",
        "T331",
        "DROP ROLE"
    );
}

/// T331: DROP ROLE with IF EXISTS
#[test]
fn t331_drop_role_if_exists() {
    assert_feature_supported!(
        "DROP ROLE IF EXISTS reporting_role",
        "T331",
        "DROP ROLE IF EXISTS"
    );
}

/// T331: DROP ROLE with CASCADE
#[test]
fn t331_drop_role_cascade() {
    assert_feature_supported!(
        "DROP ROLE reporting_role CASCADE",
        "T331",
        "DROP ROLE CASCADE"
    );
}

/// T331: DROP ROLE with RESTRICT
#[test]
fn t331_drop_role_restrict() {
    assert_feature_supported!(
        "DROP ROLE reporting_role RESTRICT",
        "T331",
        "DROP ROLE RESTRICT"
    );
}

// ============================================================================
// T332: Extended roles
// ============================================================================

/// T332: GRANT role TO user
#[test]
fn t332_grant_role_to_user() {
    assert_feature_supported!(
        "GRANT reporting_role TO alice",
        "T332",
        "GRANT role to user"
    );
}

/// T332: GRANT role TO multiple users
#[test]
fn t332_grant_role_to_multiple_users() {
    assert_feature_supported!(
        "GRANT reporting_role TO alice, bob, charlie",
        "T332",
        "GRANT role to multiple users"
    );
}

/// T332: GRANT role TO role (role hierarchy)
#[test]
fn t332_grant_role_to_role() {
    assert_feature_supported!(
        "GRANT admin_role TO super_admin_role",
        "T332",
        "GRANT role to role"
    );
}

/// T332: GRANT role WITH ADMIN OPTION
#[test]
fn t332_grant_role_with_admin_option() {
    assert_feature_supported!(
        "GRANT reporting_role TO alice WITH ADMIN OPTION",
        "T332",
        "GRANT role WITH ADMIN OPTION"
    );
}

/// T332: GRANT multiple roles
#[test]
fn t332_grant_multiple_roles() {
    assert_feature_supported!(
        "GRANT role1, role2, role3 TO alice",
        "T332",
        "GRANT multiple roles"
    );
}

/// T332: REVOKE role FROM user
#[test]
fn t332_revoke_role_from_user() {
    assert_feature_supported!(
        "REVOKE reporting_role FROM alice",
        "T332",
        "REVOKE role from user"
    );
}

/// T332: REVOKE role FROM multiple users
#[test]
fn t332_revoke_role_from_multiple_users() {
    assert_feature_supported!(
        "REVOKE reporting_role FROM alice, bob, charlie",
        "T332",
        "REVOKE role from multiple users"
    );
}

/// T332: REVOKE multiple roles
#[test]
fn t332_revoke_multiple_roles() {
    assert_feature_supported!(
        "REVOKE role1, role2, role3 FROM alice",
        "T332",
        "REVOKE multiple roles"
    );
}

/// T332: REVOKE role with CASCADE
#[test]
fn t332_revoke_role_cascade() {
    assert_feature_supported!(
        "REVOKE reporting_role FROM alice CASCADE",
        "T332",
        "REVOKE role CASCADE"
    );
}

/// T332: REVOKE role with RESTRICT
#[test]
fn t332_revoke_role_restrict() {
    assert_feature_supported!(
        "REVOKE reporting_role FROM alice RESTRICT",
        "T332",
        "REVOKE role RESTRICT"
    );
}

/// T332: REVOKE ADMIN OPTION FOR
#[test]
fn t332_revoke_admin_option_for() {
    assert_feature_supported!(
        "REVOKE ADMIN OPTION FOR reporting_role FROM alice",
        "T332",
        "REVOKE ADMIN OPTION FOR"
    );
}

// ============================================================================
// Role usage in privileges
// ============================================================================

/// GRANT privilege to role
#[test]
fn grant_privilege_to_role() {
    assert_feature_supported!(
        "GRANT SELECT, INSERT ON person TO reporting_role",
        "T332",
        "GRANT privilege to role"
    );
}

/// GRANT ALL PRIVILEGES to role
#[test]
fn grant_all_to_role() {
    assert_feature_supported!(
        "GRANT ALL PRIVILEGES ON person TO admin_role",
        "T332",
        "GRANT ALL to role"
    );
}

/// REVOKE privilege from role
#[test]
fn revoke_privilege_from_role() {
    assert_feature_supported!(
        "REVOKE SELECT ON person FROM reporting_role",
        "T332",
        "REVOKE privilege from role"
    );
}

// ============================================================================
// Complex scenarios
// ============================================================================

/// Complete role-based access control scenario
#[test]
fn rbac_scenario() {
    // Create roles
    assert_parses!("CREATE ROLE read_only");
    assert_parses!("CREATE ROLE read_write");
    assert_parses!("CREATE ROLE admin");

    // Grant privileges to roles
    assert_parses!("GRANT SELECT ON person TO read_only");
    assert_parses!("GRANT SELECT, INSERT, UPDATE ON person TO read_write");
    assert_parses!("GRANT ALL PRIVILEGES ON person TO admin WITH GRANT OPTION");

    // Grant roles to users
    assert_parses!("GRANT read_only TO alice");
    assert_parses!("GRANT read_write TO bob");
    assert_parses!("GRANT admin TO charlie WITH ADMIN OPTION");

    // Hierarchical roles
    assert_parses!("GRANT read_only TO read_write");
    assert_parses!("GRANT read_write TO admin");
}

/// Column-level privilege scenario
#[test]
fn column_level_privilege_scenario() {
    assert_parses!("GRANT SELECT ON person TO alice");
    assert_parses!("GRANT UPDATE (salary) ON person TO hr_manager");
    assert_parses!("GRANT UPDATE (first_name, last_name) ON person TO receptionist");
    assert_parses!("GRANT REFERENCES (id) ON person TO schema_designer");
}

/// Privilege revocation cascade
#[test]
fn privilege_revocation_cascade() {
    assert_parses!("GRANT SELECT ON person TO alice WITH GRANT OPTION");
    assert_parses!("REVOKE SELECT ON person FROM alice CASCADE");
}

/// Mixed privileges on different objects
#[test]
fn mixed_privileges() {
    assert_parses!("GRANT SELECT ON person TO alice");
    assert_parses!("GRANT INSERT, UPDATE, DELETE ON orders TO alice");
    assert_parses!("GRANT REFERENCES ON products TO alice");
    assert_parses!("GRANT USAGE ON SEQUENCE order_id_seq TO alice");
    assert_parses!("GRANT EXECUTE ON FUNCTION calculate_total TO alice");
}

/// PUBLIC grantee scenarios
#[test]
fn public_grantee_scenarios() {
    assert_parses!("GRANT SELECT ON person TO PUBLIC");
    assert_parses!("GRANT SELECT ON orders TO PUBLIC");
    assert_parses!("GRANT USAGE ON SEQUENCE person_id_seq TO PUBLIC");
    assert_parses!("REVOKE SELECT ON person FROM PUBLIC");
}

/// Schema-qualified object privileges
#[test]
fn schema_qualified_privileges() {
    assert_parses!("GRANT SELECT ON myschema.person TO alice");
    assert_parses!("GRANT INSERT ON myschema.orders TO bob");
    assert_parses!("GRANT ALL PRIVILEGES ON myschema.products TO admin_role");
    assert_parses!("GRANT USAGE ON SEQUENCE myschema.seq1 TO alice");
}

/// Qualified user and role names
#[test]
fn qualified_grantees() {
    assert_parses!("GRANT SELECT ON person TO \"User Name\"");
    assert_parses!("GRANT INSERT ON orders TO \"Admin Role\"");
    assert_parses!("CREATE ROLE \"Complex-Role.Name\"");
}

// ============================================================================
// Summary Tests - Verify overall E081 support
// ============================================================================

#[test]
fn e081_summary_basic_grants() {
    // Core privilege types
    assert_plans!("GRANT SELECT ON person TO alice");
    assert_plans!("GRANT INSERT ON person TO alice");
    assert_plans!("GRANT UPDATE ON person TO alice");
    assert_plans!("GRANT DELETE ON person TO alice");
}

#[test]
fn e081_summary_column_privileges() {
    // Column-level privileges
    assert_plans!("GRANT UPDATE (salary) ON person TO alice");
    assert_plans!("GRANT REFERENCES (id) ON person TO alice");
}

#[test]
fn e081_summary_grant_options() {
    // WITH GRANT OPTION
    assert_plans!("GRANT SELECT ON person TO alice WITH GRANT OPTION");
    assert_plans!("GRANT ALL PRIVILEGES ON person TO alice WITH GRANT OPTION");
}

#[test]
fn e081_summary_special_privileges() {
    // USAGE and EXECUTE
    assert_plans!("GRANT USAGE ON SEQUENCE person_id_seq TO alice");
    assert_plans!("GRANT EXECUTE ON FUNCTION my_function TO alice");
}

#[test]
fn e081_summary_revoke_statements() {
    // Revocation patterns
    assert_plans!("REVOKE SELECT ON person FROM alice");
    assert_plans!("REVOKE ALL PRIVILEGES ON person FROM alice");
    assert_plans!("REVOKE SELECT ON person FROM alice CASCADE");
    assert_plans!("REVOKE GRANT OPTION FOR SELECT ON person FROM alice");
}

#[test]
fn e081_summary_roles() {
    // Role management
    assert_plans!("CREATE ROLE reporting_role");
    assert_plans!("DROP ROLE reporting_role");
    assert_plans!("GRANT reporting_role TO alice");
    assert_plans!("REVOKE reporting_role FROM alice");
}

#[test]
fn e081_summary_all_features() {
    // Comprehensive privilege test combining all E081 features
    assert_plans!("CREATE ROLE admin");
    assert_plans!("CREATE ROLE read_only");
    assert_plans!("GRANT SELECT, INSERT, UPDATE, DELETE ON person TO admin WITH GRANT OPTION");
    assert_plans!("GRANT SELECT ON person TO read_only");
    assert_plans!("GRANT UPDATE (salary) ON person TO admin");
    assert_plans!("GRANT REFERENCES (id) ON person TO admin");
    assert_plans!("GRANT USAGE ON SEQUENCE person_id_seq TO admin");
    assert_plans!("GRANT EXECUTE ON FUNCTION calculate_bonus TO admin");
    assert_plans!("GRANT admin TO alice WITH ADMIN OPTION");
    assert_plans!("GRANT read_only TO PUBLIC");
    assert_plans!("REVOKE SELECT ON person FROM PUBLIC");
    assert_plans!("REVOKE admin FROM alice CASCADE");
}
