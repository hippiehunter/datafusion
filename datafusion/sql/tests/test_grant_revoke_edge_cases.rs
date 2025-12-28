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

//! Edge case tests for GRANT/REVOKE Role statements

use datafusion_sql::parser::DFParser;

/// Test that empty role list is rejected
#[test]
fn test_grant_role_empty_list() {
    let sql = "GRANT TO alice";
    let result = DFParser::parse_sql(sql);

    // Should fail during parsing
    assert!(
        result.is_err(),
        "Expected parse error for empty role list, but parsing succeeded"
    );

    if let Err(e) = result {
        let error_msg = e.to_string();
        println!("Error (expected): {}", error_msg);
        // Error should mention something about unexpected TO keyword or missing identifier
        assert!(
            error_msg.to_lowercase().contains("unexpected")
            || error_msg.to_lowercase().contains("expected"),
            "Error message should indicate parsing issue: {}",
            error_msg
        );
    }
}

/// Test that empty grantee list is rejected
#[test]
fn test_grant_role_empty_grantees() {
    let sql = "GRANT reporting_role TO";
    let result = DFParser::parse_sql(sql);

    // Should fail during parsing
    assert!(
        result.is_err(),
        "Expected parse error for empty grantee list, but parsing succeeded"
    );

    if let Err(e) = result {
        let error_msg = e.to_string();
        println!("Error (expected): {}", error_msg);
        assert!(
            error_msg.to_lowercase().contains("unexpected")
            || error_msg.to_lowercase().contains("expected"),
            "Error message should indicate parsing issue: {}",
            error_msg
        );
    }
}

/// Test that REVOKE with empty role list is rejected
#[test]
fn test_revoke_role_empty_list() {
    let sql = "REVOKE FROM alice";
    let result = DFParser::parse_sql(sql);

    // Should fail during parsing
    assert!(
        result.is_err(),
        "Expected parse error for empty role list, but parsing succeeded"
    );
}

/// Test that REVOKE with empty grantee list is rejected
#[test]
fn test_revoke_role_empty_grantees() {
    let sql = "REVOKE reporting_role FROM";
    let result = DFParser::parse_sql(sql);

    // Should fail during parsing
    assert!(
        result.is_err(),
        "Expected parse error for empty grantee list, but parsing succeeded"
    );
}

/// Test special characters in role names (should succeed)
#[test]
fn test_grant_role_special_chars() {
    let test_cases = vec![
        r#"GRANT "role-with-dashes" TO alice"#,
        r#"GRANT "role.with.dots" TO alice"#,
        r#"GRANT "role with spaces" TO alice"#,
    ];

    for sql in test_cases {
        let result = DFParser::parse_sql(sql);
        assert!(
            result.is_ok(),
            "Failed to parse SQL with special characters in role name: {}\nError: {:?}",
            sql,
            result.err()
        );
    }
}

/// Test special characters in grantee names (should succeed)
#[test]
fn test_grant_role_special_chars_grantees() {
    let test_cases = vec![
        r#"GRANT reporting_role TO "user-with-dashes""#,
        r#"GRANT reporting_role TO "user.with.dots""#,
        r#"GRANT reporting_role TO "user with spaces""#,
    ];

    for sql in test_cases {
        let result = DFParser::parse_sql(sql);
        assert!(
            result.is_ok(),
            "Failed to parse SQL with special characters in grantee name: {}\nError: {:?}",
            sql,
            result.err()
        );
    }
}

/// Test all optional clauses together
#[test]
fn test_grant_role_all_options() {
    let sql = "GRANT role1, role2 TO alice, bob WITH ADMIN OPTION GRANTED BY admin";
    let result = DFParser::parse_sql(sql);

    assert!(
        result.is_ok(),
        "Failed to parse GRANT with all optional clauses: {:?}",
        result.err()
    );
}

/// Test REVOKE with all optional clauses
#[test]
fn test_revoke_role_all_options() {
    let sql = "REVOKE role1, role2 FROM alice, bob GRANTED BY admin CASCADE";
    let result = DFParser::parse_sql(sql);

    assert!(
        result.is_ok(),
        "Failed to parse REVOKE with all optional clauses: {:?}",
        result.err()
    );
}

/// Test REVOKE ADMIN OPTION FOR with CASCADE
#[test]
fn test_revoke_admin_option_cascade() {
    let sql = "REVOKE ADMIN OPTION FOR role1, role2 FROM alice CASCADE";
    let result = DFParser::parse_sql(sql);

    assert!(
        result.is_ok(),
        "Failed to parse REVOKE ADMIN OPTION FOR with CASCADE: {:?}",
        result.err()
    );
}
