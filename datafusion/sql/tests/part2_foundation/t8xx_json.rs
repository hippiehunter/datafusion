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

//! SQL:2016 Feature T8xx - JSON Support
//!
//! ISO/IEC 9075-2:2016 Section 9.42 - JSON functions and operators
//!
//! This feature set covers JSON support added in SQL:2016, including:
//! - JSON data type and storage
//! - JSON constructors (JSON_OBJECT, JSON_ARRAY)
//! - JSON aggregate functions (JSON_OBJECTAGG, JSON_ARRAYAGG)
//! - JSON predicates (IS JSON)
//! - JSON path queries (JSON_EXISTS, JSON_VALUE, JSON_QUERY)
//! - JSON table functions (JSON_TABLE)
//!
//! | Subfeature | Description | Status |
//! |------------|-------------|--------|
//! | T803 | String-based JSON storage | Not Implemented |
//! | T811 | JSON_OBJECT constructor | Not Implemented |
//! | T812 | JSON_OBJECTAGG aggregate | Not Implemented |
//! | T813 | JSON_ARRAYAGG aggregate | Not Implemented |
//! | T814 | JSON_ARRAY constructor | Not Implemented |
//! | T821 | JSON_EXISTS predicate | Not Implemented |
//! | T822 | IS JSON predicate | Not Implemented |
//! | T823 | JSON_VALUE function | Not Implemented |
//! | T824 | JSON_QUERY function | Not Implemented |
//! | T825 | ON EMPTY/ON ERROR clauses | Not Implemented |
//! | T827 | JSON_TABLE function | Not Implemented |
//!
//! All T8xx features are OPTIONAL (not required for SQL:2016 conformance).
//! These tests document the conformance gaps and serve as a roadmap for
//! future JSON support implementation.

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// T803: String-based JSON storage
// ============================================================================

/// T803: CREATE TABLE with JSON data type
#[test]
fn t803_json_column_type() {
    assert_feature_supported!(
        "CREATE TABLE json_data (id INT, data JSON)",
        "T803",
        "JSON column data type"
    );
}

/// T803: JSON type in CREATE TABLE with constraints
#[test]
fn t803_json_column_with_constraint() {
    assert_feature_supported!(
        "CREATE TABLE documents (id INT PRIMARY KEY, content JSON NOT NULL)",
        "T803",
        "JSON column with NOT NULL constraint"
    );
}

/// T803: Multiple JSON columns in table
#[test]
fn t803_multiple_json_columns() {
    assert_feature_supported!(
        "CREATE TABLE multi_json (id INT, config JSON, metadata JSON, tags JSON)",
        "T803",
        "Multiple JSON columns in table"
    );
}

// ============================================================================
// T811: JSON_OBJECT constructor function
// ============================================================================

/// T811: JSON_OBJECT basic constructor with single key-value pair
#[test]
fn t811_json_object_basic() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT('name': 'test')",
        "T811",
        "JSON_OBJECT basic constructor"
    );
}

/// T811: JSON_OBJECT with multiple key-value pairs
#[test]
fn t811_json_object_multiple_pairs() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT('name': 'Alice', 'age': 30, 'active': true)",
        "T811",
        "JSON_OBJECT with multiple pairs"
    );
}

/// T811: JSON_OBJECT with column values
#[test]
fn t811_json_object_from_columns() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT('id': id, 'name': first_name) FROM person",
        "T811",
        "JSON_OBJECT from table columns"
    );
}

/// T811: JSON_OBJECT with nested expressions
#[test]
fn t811_json_object_nested_expression() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT('total': price * qty, 'discount': price * 0.1) FROM orders",
        "T811",
        "JSON_OBJECT with expressions"
    );
}

/// T811: JSON_OBJECT with NULL handling options
#[test]
fn t811_json_object_null_on_null() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT('key': NULL NULL ON NULL)",
        "T811",
        "JSON_OBJECT NULL ON NULL clause"
    );
}

/// T811: JSON_OBJECT with ABSENT ON NULL
#[test]
fn t811_json_object_absent_on_null() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT('key': NULL ABSENT ON NULL)",
        "T811",
        "JSON_OBJECT ABSENT ON NULL clause"
    );
}

// ============================================================================
// T812: JSON_OBJECTAGG aggregate function
// ============================================================================

/// T812: JSON_OBJECTAGG basic aggregation
#[test]
fn t812_json_objectagg_basic() {
    assert_feature_supported!(
        "SELECT JSON_OBJECTAGG(first_name: age) FROM person",
        "T812",
        "JSON_OBJECTAGG basic aggregation"
    );
}

/// T812: JSON_OBJECTAGG with GROUP BY
#[test]
fn t812_json_objectagg_group_by() {
    assert_feature_supported!(
        "SELECT state, JSON_OBJECTAGG(first_name: salary) FROM person GROUP BY state",
        "T812",
        "JSON_OBJECTAGG with GROUP BY"
    );
}

/// T812: JSON_OBJECTAGG with NULL handling
#[test]
fn t812_json_objectagg_null_on_null() {
    assert_feature_supported!(
        "SELECT JSON_OBJECTAGG(first_name: age NULL ON NULL) FROM person",
        "T812",
        "JSON_OBJECTAGG NULL ON NULL"
    );
}

/// T812: JSON_OBJECTAGG with ABSENT ON NULL
#[test]
fn t812_json_objectagg_absent_on_null() {
    assert_feature_supported!(
        "SELECT JSON_OBJECTAGG(first_name: age ABSENT ON NULL) FROM person",
        "T812",
        "JSON_OBJECTAGG ABSENT ON NULL"
    );
}

// ============================================================================
// T813: JSON_ARRAYAGG aggregate function
// ============================================================================

/// T813: JSON_ARRAYAGG basic aggregation
#[test]
fn t813_json_arrayagg_basic() {
    assert_feature_supported!(
        "SELECT JSON_ARRAYAGG(first_name) FROM person",
        "T813",
        "JSON_ARRAYAGG basic aggregation"
    );
}

/// T813: JSON_ARRAYAGG with GROUP BY
#[test]
fn t813_json_arrayagg_group_by() {
    assert_feature_supported!(
        "SELECT state, JSON_ARRAYAGG(first_name) FROM person GROUP BY state",
        "T813",
        "JSON_ARRAYAGG with GROUP BY"
    );
}

/// T813: JSON_ARRAYAGG with ORDER BY clause
#[test]
fn t813_json_arrayagg_order_by() {
    assert_feature_supported!(
        "SELECT JSON_ARRAYAGG(first_name ORDER BY age DESC) FROM person",
        "T813",
        "JSON_ARRAYAGG with ORDER BY"
    );
}

/// T813: JSON_ARRAYAGG with NULL ON NULL
#[test]
fn t813_json_arrayagg_null_on_null() {
    assert_feature_supported!(
        "SELECT JSON_ARRAYAGG(first_name NULL ON NULL) FROM person",
        "T813",
        "JSON_ARRAYAGG NULL ON NULL"
    );
}

/// T813: JSON_ARRAYAGG with ABSENT ON NULL
#[test]
fn t813_json_arrayagg_absent_on_null() {
    assert_feature_supported!(
        "SELECT JSON_ARRAYAGG(first_name ABSENT ON NULL) FROM person",
        "T813",
        "JSON_ARRAYAGG ABSENT ON NULL"
    );
}

// ============================================================================
// T814: JSON_ARRAY constructor function
// ============================================================================

/// T814: JSON_ARRAY basic constructor with literals
#[test]
fn t814_json_array_basic() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY(1, 2, 3)",
        "T814",
        "JSON_ARRAY basic constructor"
    );
}

/// T814: JSON_ARRAY with mixed types
#[test]
fn t814_json_array_mixed_types() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY('text', 42, true, 3.14)",
        "T814",
        "JSON_ARRAY with mixed types"
    );
}

/// T814: JSON_ARRAY with column values
#[test]
fn t814_json_array_from_columns() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY(first_name, last_name, age) FROM person",
        "T814",
        "JSON_ARRAY from columns"
    );
}

/// T814: JSON_ARRAY with expressions
#[test]
fn t814_json_array_expressions() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY(price, qty, price * qty) FROM orders",
        "T814",
        "JSON_ARRAY with expressions"
    );
}

/// T814: JSON_ARRAY with NULL ON NULL
#[test]
fn t814_json_array_null_on_null() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY(1, NULL, 3 NULL ON NULL)",
        "T814",
        "JSON_ARRAY NULL ON NULL"
    );
}

/// T814: JSON_ARRAY with ABSENT ON NULL
#[test]
fn t814_json_array_absent_on_null() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY(1, NULL, 3 ABSENT ON NULL)",
        "T814",
        "JSON_ARRAY ABSENT ON NULL"
    );
}

// ============================================================================
// T821: JSON_EXISTS predicate
// ============================================================================

/// T821: JSON_EXISTS basic path check
#[test]
fn t821_json_exists_basic() {
    assert_feature_supported!(
        "SELECT * FROM json_data WHERE JSON_EXISTS(data, '$.name')",
        "T821",
        "JSON_EXISTS basic path check"
    );
}

/// T821: JSON_EXISTS with nested path
#[test]
fn t821_json_exists_nested_path() {
    assert_feature_supported!(
        "SELECT * FROM json_data WHERE JSON_EXISTS(data, '$.address.city')",
        "T821",
        "JSON_EXISTS with nested path"
    );
}

/// T821: JSON_EXISTS with array index
#[test]
fn t821_json_exists_array_index() {
    assert_feature_supported!(
        "SELECT * FROM json_data WHERE JSON_EXISTS(data, '$.items[0]')",
        "T821",
        "JSON_EXISTS with array index"
    );
}

/// T821: JSON_EXISTS with filter expression
#[test]
fn t821_json_exists_filter() {
    assert_feature_supported!(
        "SELECT * FROM json_data WHERE JSON_EXISTS(data, '$.prices[*] ? (@ > 100)')",
        "T821",
        "JSON_EXISTS with filter expression"
    );
}

/// T821: JSON_EXISTS with ON ERROR clause
#[test]
fn t821_json_exists_on_error() {
    assert_feature_supported!(
        "SELECT * FROM json_data WHERE JSON_EXISTS(data, '$.key' FALSE ON ERROR)",
        "T821",
        "JSON_EXISTS with FALSE ON ERROR"
    );
}

// ============================================================================
// T822: IS JSON predicate
// ============================================================================

/// T822: IS JSON basic predicate
#[test]
fn t822_is_json_basic() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS JSON",
        "T822",
        "IS JSON basic predicate"
    );
}

/// T822: IS NOT JSON predicate
#[test]
fn t822_is_not_json() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS NOT JSON",
        "T822",
        "IS NOT JSON predicate"
    );
}

/// T822: IS JSON VALUE (scalar values only)
#[test]
fn t822_is_json_value() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS JSON VALUE",
        "T822",
        "IS JSON VALUE predicate"
    );
}

/// T822: IS JSON OBJECT
#[test]
fn t822_is_json_object() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS JSON OBJECT",
        "T822",
        "IS JSON OBJECT predicate"
    );
}

/// T822: IS JSON ARRAY
#[test]
fn t822_is_json_array() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS JSON ARRAY",
        "T822",
        "IS JSON ARRAY predicate"
    );
}

/// T822: IS JSON SCALAR (primitive values)
#[test]
fn t822_is_json_scalar() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS JSON SCALAR",
        "T822",
        "IS JSON SCALAR predicate"
    );
}

/// T822: IS JSON with UNIQUE KEYS constraint
#[test]
fn t822_is_json_unique_keys() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE c IS JSON OBJECT WITH UNIQUE KEYS",
        "T822",
        "IS JSON OBJECT WITH UNIQUE KEYS"
    );
}

// ============================================================================
// T823: JSON_VALUE function - extract scalar values
// ============================================================================

/// T823: JSON_VALUE basic extraction
#[test]
fn t823_json_value_basic() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.name') FROM json_data",
        "T823",
        "JSON_VALUE basic extraction"
    );
}

/// T823: JSON_VALUE with nested path
#[test]
fn t823_json_value_nested() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.address.city') FROM json_data",
        "T823",
        "JSON_VALUE nested path"
    );
}

/// T823: JSON_VALUE with RETURNING clause (explicit type)
#[test]
fn t823_json_value_returning() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.age' RETURNING INT) FROM json_data",
        "T823",
        "JSON_VALUE with RETURNING INT"
    );
}

/// T823: JSON_VALUE with DEFAULT ON EMPTY
#[test]
fn t823_json_value_default_on_empty() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.missing' DEFAULT 'N/A' ON EMPTY) FROM json_data",
        "T823",
        "JSON_VALUE DEFAULT ON EMPTY"
    );
}

/// T823: JSON_VALUE with NULL ON EMPTY
#[test]
fn t823_json_value_null_on_empty() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.missing' NULL ON EMPTY) FROM json_data",
        "T823",
        "JSON_VALUE NULL ON EMPTY"
    );
}

/// T823: JSON_VALUE with ERROR ON ERROR
#[test]
fn t823_json_value_error_on_error() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.key' ERROR ON ERROR) FROM json_data",
        "T823",
        "JSON_VALUE ERROR ON ERROR"
    );
}

/// T823: JSON_VALUE with DEFAULT ON ERROR
#[test]
fn t823_json_value_default_on_error() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.key' DEFAULT 'error' ON ERROR) FROM json_data",
        "T823",
        "JSON_VALUE DEFAULT ON ERROR"
    );
}

// ============================================================================
// T824: JSON_QUERY function - extract objects/arrays
// ============================================================================

/// T824: JSON_QUERY basic extraction
#[test]
fn t824_json_query_basic() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.address') FROM json_data",
        "T824",
        "JSON_QUERY basic extraction"
    );
}

/// T824: JSON_QUERY extracting array
#[test]
fn t824_json_query_array() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.items') FROM json_data",
        "T824",
        "JSON_QUERY array extraction"
    );
}

/// T824: JSON_QUERY with array wildcard
#[test]
fn t824_json_query_array_wildcard() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.items[*]') FROM json_data",
        "T824",
        "JSON_QUERY array wildcard"
    );
}

/// T824: JSON_QUERY with WITHOUT ARRAY WRAPPER
#[test]
fn t824_json_query_without_wrapper() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.items' WITHOUT ARRAY WRAPPER) FROM json_data",
        "T824",
        "JSON_QUERY WITHOUT ARRAY WRAPPER"
    );
}

/// T824: JSON_QUERY with WITH ARRAY WRAPPER
#[test]
fn t824_json_query_with_wrapper() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.name' WITH ARRAY WRAPPER) FROM json_data",
        "T824",
        "JSON_QUERY WITH ARRAY WRAPPER"
    );
}

/// T824: JSON_QUERY with EMPTY ARRAY ON EMPTY
#[test]
fn t824_json_query_empty_array_on_empty() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.missing' EMPTY ARRAY ON EMPTY) FROM json_data",
        "T824",
        "JSON_QUERY EMPTY ARRAY ON EMPTY"
    );
}

/// T824: JSON_QUERY with NULL ON EMPTY
#[test]
fn t824_json_query_null_on_empty() {
    assert_feature_supported!(
        "SELECT JSON_QUERY(data, '$.missing' NULL ON EMPTY) FROM json_data",
        "T824",
        "JSON_QUERY NULL ON EMPTY"
    );
}

// ============================================================================
// T825: ON EMPTY and ON ERROR clauses (comprehensive)
// ============================================================================

/// T825: Combination of ON EMPTY and ON ERROR clauses
#[test]
fn t825_on_empty_and_error() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.key' DEFAULT 'none' ON EMPTY ERROR ON ERROR) FROM json_data",
        "T825",
        "ON EMPTY and ON ERROR combination"
    );
}

/// T825: ERROR ON EMPTY clause
#[test]
fn t825_error_on_empty() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.key' ERROR ON EMPTY) FROM json_data",
        "T825",
        "ERROR ON EMPTY clause"
    );
}

/// T825: DEFAULT value with expression ON EMPTY
#[test]
fn t825_default_expression_on_empty() {
    assert_feature_supported!(
        "SELECT JSON_VALUE(data, '$.count' DEFAULT '0' ON EMPTY RETURNING INT) FROM json_data",
        "T825",
        "DEFAULT with expression ON EMPTY"
    );
}

// ============================================================================
// T827: JSON_TABLE function - transform JSON to relational table
// ============================================================================

/// T827: JSON_TABLE basic usage
#[test]
fn t827_json_table_basic() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data, JSON_TABLE(data, '$' COLUMNS(name VARCHAR(100) PATH '$.name')) AS jt",
        "T827",
        "JSON_TABLE basic usage"
    );
}

/// T827: JSON_TABLE with multiple columns
#[test]
fn t827_json_table_multiple_columns() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$' COLUMNS(
             name VARCHAR(100) PATH '$.name',
             age INT PATH '$.age',
             city VARCHAR(100) PATH '$.address.city'
         )) AS jt",
        "T827",
        "JSON_TABLE with multiple columns"
    );
}

/// T827: JSON_TABLE with array expansion
#[test]
fn t827_json_table_array() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$.items[*]' COLUMNS(
             item_name VARCHAR(100) PATH '$.name',
             price DECIMAL(10,2) PATH '$.price'
         )) AS jt",
        "T827",
        "JSON_TABLE array expansion"
    );
}

/// T827: JSON_TABLE with nested paths
#[test]
fn t827_json_table_nested() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$' COLUMNS(
             NESTED PATH '$.addresses[*]' COLUMNS(
                 street VARCHAR(200) PATH '$.street',
                 city VARCHAR(100) PATH '$.city'
             )
         )) AS jt",
        "T827",
        "JSON_TABLE with NESTED PATH"
    );
}

/// T827: JSON_TABLE with FOR ORDINALITY
#[test]
fn t827_json_table_ordinality() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$.items[*]' COLUMNS(
             item_num FOR ORDINALITY,
             item_name VARCHAR(100) PATH '$.name'
         )) AS jt",
        "T827",
        "JSON_TABLE with FOR ORDINALITY"
    );
}

/// T827: JSON_TABLE with EXISTS clause
#[test]
fn t827_json_table_exists() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$' COLUMNS(
             has_address INT EXISTS PATH '$.address'
         )) AS jt",
        "T827",
        "JSON_TABLE with EXISTS clause"
    );
}

/// T827: JSON_TABLE with ON EMPTY clause
#[test]
fn t827_json_table_on_empty() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$' COLUMNS(
             name VARCHAR(100) PATH '$.name' DEFAULT 'Unknown' ON EMPTY
         )) AS jt",
        "T827",
        "JSON_TABLE with ON EMPTY"
    );
}

/// T827: JSON_TABLE with ON ERROR clause
#[test]
fn t827_json_table_on_error() {
    assert_feature_supported!(
        "SELECT jt.* FROM json_data,
         JSON_TABLE(data, '$' COLUMNS(
             age INT PATH '$.age' NULL ON ERROR
         )) AS jt",
        "T827",
        "JSON_TABLE with ON ERROR"
    );
}

// ============================================================================
// Complex JSON scenarios - combining multiple features
// ============================================================================

/// Complex: Nested JSON_OBJECT creation
#[test]
fn t8xx_complex_nested_json_object() {
    assert_feature_supported!(
        "SELECT JSON_OBJECT(
            'person': JSON_OBJECT('name': first_name, 'age': age),
            'location': JSON_OBJECT('state': state)
         ) FROM person",
        "T811",
        "Nested JSON_OBJECT creation"
    );
}

/// Complex: JSON_ARRAY containing JSON_OBJECT
#[test]
fn t8xx_complex_array_of_objects() {
    assert_feature_supported!(
        "SELECT JSON_ARRAY(
            JSON_OBJECT('id': 1, 'name': 'Alice'),
            JSON_OBJECT('id': 2, 'name': 'Bob')
         )",
        "T814",
        "JSON_ARRAY of JSON_OBJECT values"
    );
}

/// Complex: Combining JSON_VALUE with other predicates
#[test]
fn t8xx_complex_json_value_where() {
    assert_feature_supported!(
        "SELECT * FROM json_data
         WHERE JSON_VALUE(data, '$.age' RETURNING INT) > 21
         AND JSON_EXISTS(data, '$.active')",
        "T823",
        "JSON_VALUE in WHERE clause"
    );
}

/// Complex: JSON aggregations with grouping
#[test]
fn t8xx_complex_json_agg_grouping() {
    assert_feature_supported!(
        "SELECT state,
                JSON_OBJECTAGG(first_name: salary) AS salaries,
                JSON_ARRAYAGG(first_name ORDER BY age) AS names
         FROM person
         GROUP BY state",
        "T812",
        "Multiple JSON aggregations with GROUP BY"
    );
}

/// Complex: JSON_TABLE with joins
#[test]
fn t8xx_complex_json_table_join() {
    assert_feature_supported!(
        "SELECT p.*, jt.item_name, jt.quantity
         FROM person p
         JOIN json_data jd ON p.id = jd.person_id
         CROSS JOIN JSON_TABLE(jd.data, '$.orders[*]' COLUMNS(
             item_name VARCHAR(100) PATH '$.item',
             quantity INT PATH '$.qty'
         )) AS jt",
        "T827",
        "JSON_TABLE with table joins"
    );
}

// ============================================================================
// Summary Test - Overall T8xx JSON Support
// ============================================================================

/// Summary: This test demonstrates the expected comprehensive JSON support
/// that would be required for full SQL:2016 JSON conformance
#[test]
fn t8xx_summary_json_features() {
    // Note: This test will FAIL as most JSON features are not yet implemented
    // This is expected and documents the conformance gap

    // Create a table with JSON storage
    assert_plans!("CREATE TABLE events (
        id INT PRIMARY KEY,
        event_data JSON,
        metadata JSON
    )");

    // Construct JSON objects from relational data
    assert_plans!("SELECT JSON_OBJECT(
        'id': id,
        'name': first_name,
        'details': JSON_OBJECT('age': age, 'state': state)
    ) FROM person");

    // Query with JSON predicates and extraction
    assert_plans!("SELECT
        id,
        JSON_VALUE(event_data, '$.type') AS event_type,
        JSON_QUERY(event_data, '$.tags') AS tags
    FROM events
    WHERE JSON_EXISTS(event_data, '$.timestamp')
      AND event_data IS JSON OBJECT");

    // Aggregate JSON data
    assert_plans!("SELECT
        JSON_VALUE(event_data, '$.category') AS category,
        JSON_ARRAYAGG(JSON_VALUE(event_data, '$.name')) AS event_names
    FROM events
    GROUP BY JSON_VALUE(event_data, '$.category')");

    // Transform JSON arrays to tables
    assert_plans!("SELECT jt.product_id, jt.quantity
    FROM events,
    JSON_TABLE(event_data, '$.items[*]' COLUMNS(
        product_id INT PATH '$.id',
        quantity INT PATH '$.qty'
    )) AS jt
    WHERE JSON_VALUE(event_data, '$.type') = 'order'");
}
