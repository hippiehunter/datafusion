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

//! SQL:2016 Feature T051 - Row types
//! SQL:2016 Feature T052 - MAX and MIN for row types
//! SQL:2016 Feature T053 - Explicit aliases for all-fields reference
//! SQL:2016 Feature T312 - OVERLAY function
//!
//! ISO/IEC 9075-2:2016 Section 6.33 (Row value constructor)
//! ISO/IEC 9075-2:2016 Section 9.5 (OVERLAY function)
//!
//! This file tests row types (also known as composite types or structured types)
//! and related SQL:2016 features. In DataFusion, row types are implemented using
//! STRUCT types from Apache Arrow.
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | T051 | Row types | Testing |
//! | T052 | MAX and MIN for row types | Testing |
//! | T053 | Explicit aliases for all-fields reference | Testing |
//! | T312 | OVERLAY function | Testing |
//!
//! # Row Types vs STRUCT Types
//!
//! SQL:2016 defines ROW types as composite types that contain a sequence of fields.
//! DataFusion implements these using Apache Arrow's STRUCT data type, which provides:
//! - Named fields with heterogeneous types
//! - Nested structure support
//! - NULL value support at both row and field level
//!
//! # Test Coverage
//!
//! ## T051 - Row types
//! - ROW constructor syntax: ROW(value1, value2, ...)
//! - Row types in CREATE TABLE
//! - Row comparison operations
//! - Row field access
//! - Nested row structures
//! - STRUCT literal syntax (DataFusion extension)
//! - Named STRUCT syntax
//!
//! ## T052 - MAX and MIN for row types
//! - MAX aggregate on row/struct columns
//! - MIN aggregate on row/struct columns
//!
//! ## T053 - Explicit aliases for all-fields reference
//! - SELECT t.* AS (alias1, alias2, ...) syntax
//!
//! ## T312 - OVERLAY function
//! - OVERLAY(string PLACING replacement FROM start)
//! - OVERLAY(string PLACING replacement FROM start FOR length)
//! - OVERLAY with various data types
//! - OVERLAY with expressions

use crate::{assert_parses, assert_plans, assert_feature_supported};

// ============================================================================
// T051: Row types - Basic ROW constructor
// ============================================================================

/// T051: Basic ROW constructor with literals
#[test]
fn t051_row_constructor_basic() {
    assert_feature_supported!(
        "SELECT ROW(1, 'a', 3.14)",
        "T051",
        "Basic ROW constructor"
    );
}

/// T051: ROW constructor with multiple values
#[test]
fn t051_row_constructor_multiple_values() {
    assert_feature_supported!(
        "SELECT ROW(1, 2, 3, 4, 5)",
        "T051",
        "ROW constructor with multiple values"
    );
}

/// T051: ROW constructor with NULL values
#[test]
fn t051_row_constructor_with_null() {
    assert_feature_supported!(
        "SELECT ROW(1, NULL, 'test')",
        "T051",
        "ROW constructor with NULL"
    );
}

/// T051: ROW constructor with column references
#[test]
fn t051_row_constructor_with_columns() {
    assert_feature_supported!(
        "SELECT ROW(a, b, c) FROM t",
        "T051",
        "ROW constructor with columns"
    );
}

/// T051: ROW constructor with expressions
#[test]
fn t051_row_constructor_with_expressions() {
    assert_feature_supported!(
        "SELECT ROW(a + 1, b * 2, UPPER(c)) FROM t",
        "T051",
        "ROW constructor with expressions"
    );
}

/// T051: ROW constructor with subquery results
#[test]
fn t051_row_constructor_with_subquery() {
    assert_feature_supported!(
        "SELECT ROW((SELECT a FROM t1 LIMIT 1), (SELECT b FROM t2 LIMIT 1))",
        "T051",
        "ROW constructor with subquery"
    );
}

/// T051: Empty ROW constructor
#[test]
fn t051_row_constructor_empty() {
    assert_feature_supported!(
        "SELECT ROW()",
        "T051",
        "Empty ROW constructor"
    );
}

// ============================================================================
// T051: Row types in CREATE TABLE
// ============================================================================

/// T051: CREATE TABLE with ROW type column
#[test]
fn t051_create_table_with_row_type() {
    assert_feature_supported!(
        "CREATE TABLE t (r ROW(x INT, y VARCHAR))",
        "T051",
        "CREATE TABLE with ROW type"
    );
}

/// T051: CREATE TABLE with named ROW fields
#[test]
fn t051_create_table_with_named_row_fields() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, location ROW(lat DOUBLE, lon DOUBLE, alt DOUBLE))",
        "T051",
        "CREATE TABLE with named ROW fields"
    );
}

/// T051: CREATE TABLE with multiple ROW columns
#[test]
fn t051_create_table_multiple_row_columns() {
    assert_feature_supported!(
        "CREATE TABLE t (point1 ROW(x INT, y INT), point2 ROW(x INT, y INT))",
        "T051",
        "CREATE TABLE with multiple ROW columns"
    );
}

// ============================================================================
// T051: STRUCT type (DataFusion's equivalent)
// ============================================================================

/// T051: STRUCT literal basic
#[test]
fn t051_struct_literal_basic() {
    assert_feature_supported!(
        "SELECT STRUCT(1, 'a')",
        "T051",
        "Basic STRUCT literal"
    );
}

/// T051: STRUCT literal with NULL
#[test]
fn t051_struct_literal_with_null() {
    assert_feature_supported!(
        "SELECT STRUCT(1, NULL, 'test')",
        "T051",
        "STRUCT literal with NULL"
    );
}

/// T051: Named STRUCT with field assignment
#[test]
fn t051_named_struct() {
    assert_feature_supported!(
        "SELECT STRUCT(x := 1, y := 'a')",
        "T051",
        "Named STRUCT with field assignment"
    );
}

/// T051: Named STRUCT with column references
#[test]
fn t051_named_struct_with_columns() {
    assert_feature_supported!(
        "SELECT STRUCT(id := a, name := c) FROM t",
        "T051",
        "Named STRUCT with columns"
    );
}

/// T051: Named STRUCT with expressions
#[test]
fn t051_named_struct_with_expressions() {
    assert_feature_supported!(
        "SELECT STRUCT(sum := a + b, product := a * b) FROM t",
        "T051",
        "Named STRUCT with expressions"
    );
}

/// T051: STRUCT from table column
#[test]
fn t051_struct_from_table() {
    assert_feature_supported!(
        "SELECT struct_col FROM struct_types",
        "T051",
        "SELECT STRUCT column from table"
    );
}

// ============================================================================
// T051: Row comparison
// ============================================================================

/// T051: Row equality comparison
#[test]
fn t051_row_equality_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ROW(a, b) = ROW(1, 2)",
        "T051",
        "Row equality comparison"
    );
}

/// T051: Row inequality comparison
#[test]
fn t051_row_inequality_comparison() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ROW(a, b) != ROW(1, 2)",
        "T051",
        "Row inequality comparison"
    );
}

/// T051: Row comparison with columns
#[test]
fn t051_row_comparison_with_columns() {
    assert_feature_supported!(
        "SELECT * FROM t1 WHERE ROW(a, b) = (SELECT ROW(a, b) FROM t2 LIMIT 1)",
        "T051",
        "Row comparison with subquery"
    );
}

/// T051: Row comparison in JOIN
#[test]
fn t051_row_comparison_in_join() {
    assert_feature_supported!(
        "SELECT * FROM t1 JOIN t2 ON ROW(t1.a, t1.b) = ROW(t2.a, t2.b)",
        "T051",
        "Row comparison in JOIN"
    );
}

/// T051: Row less than comparison
#[test]
fn t051_row_less_than() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ROW(a, b) < ROW(10, 20)",
        "T051",
        "Row less than comparison"
    );
}

/// T051: Row greater than comparison
#[test]
fn t051_row_greater_than() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ROW(a, b) > ROW(5, 10)",
        "T051",
        "Row greater than comparison"
    );
}

// ============================================================================
// T051: Row field access
// ============================================================================

/// T051: Row field access with dot notation
#[test]
fn t051_row_field_access_dot() {
    assert_feature_supported!(
        "SELECT struct_col.x FROM struct_types",
        "T051",
        "Row field access with dot notation"
    );
}

/// T051: Row field access with bracket notation
#[test]
fn t051_row_field_access_bracket() {
    assert_feature_supported!(
        "SELECT struct_col['x'] FROM struct_types",
        "T051",
        "Row field access with bracket notation"
    );
}

/// T051: Multiple field access
#[test]
fn t051_multiple_field_access() {
    assert_feature_supported!(
        "SELECT struct_col.x, struct_col.y FROM struct_types",
        "T051",
        "Multiple field access"
    );
}

/// T051: Field access in WHERE clause
#[test]
fn t051_field_access_in_where() {
    assert_feature_supported!(
        "SELECT * FROM struct_types WHERE struct_col.x > 10",
        "T051",
        "Field access in WHERE clause"
    );
}

/// T051: Field access in ORDER BY
#[test]
fn t051_field_access_in_order_by() {
    assert_feature_supported!(
        "SELECT * FROM struct_types ORDER BY struct_col.x",
        "T051",
        "Field access in ORDER BY"
    );
}

/// T051: Field access in expressions
#[test]
fn t051_field_access_in_expression() {
    assert_feature_supported!(
        "SELECT struct_col.x + struct_col.y FROM struct_types",
        "T051",
        "Field access in expressions"
    );
}

// ============================================================================
// T051: Nested rows
// ============================================================================

/// T051: Nested ROW constructor
#[test]
fn t051_nested_row_constructor() {
    assert_feature_supported!(
        "SELECT ROW(1, ROW('a', 'b'), 3)",
        "T051",
        "Nested ROW constructor"
    );
}

/// T051: Deeply nested ROW
#[test]
fn t051_deeply_nested_row() {
    assert_feature_supported!(
        "SELECT ROW(1, ROW(2, ROW(3, ROW(4, 5))))",
        "T051",
        "Deeply nested ROW"
    );
}

/// T051: Nested STRUCT literal
#[test]
fn t051_nested_struct_literal() {
    assert_feature_supported!(
        "SELECT STRUCT(id := 1, inner := STRUCT(x := 2, y := 3))",
        "T051",
        "Nested STRUCT literal"
    );
}

/// T051: CREATE TABLE with nested ROW type
/// Note: Using STRUCT syntax for nested types due to parser limitations
#[test]
fn t051_create_table_nested_row() {
    assert_feature_supported!(
        "CREATE TABLE t (id INT, nested STRUCT(a INT, b STRUCT(x INT, y INT)))",
        "T051",
        "CREATE TABLE with nested ROW"
    );
}

/// T051: Nested field access
#[test]
fn t051_nested_field_access() {
    assert_feature_supported!(
        "SELECT STRUCT(1, STRUCT(2, 3)).c0",
        "T051",
        "Nested field access"
    );
}

// ============================================================================
// T051: Row types in various contexts
// ============================================================================

/// T051: ROW in SELECT list
#[test]
fn t051_row_in_select_list() {
    assert_feature_supported!(
        "SELECT a, ROW(b, c) AS row_val FROM t",
        "T051",
        "ROW in SELECT list"
    );
}

/// T051: ROW in subquery
#[test]
fn t051_row_in_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT ROW(a, b) AS r FROM t) AS sub",
        "T051",
        "ROW in subquery"
    );
}

/// T051: ROW with GROUP BY
#[test]
fn t051_row_with_group_by() {
    assert_feature_supported!(
        "SELECT ROW(a, b), COUNT(*) FROM t GROUP BY ROW(a, b)",
        "T051",
        "ROW with GROUP BY"
    );
}

/// T051: ROW in CASE expression
#[test]
fn t051_row_in_case() {
    assert_feature_supported!(
        "SELECT CASE WHEN a > 10 THEN ROW(a, b) ELSE ROW(0, 0) END FROM t",
        "T051",
        "ROW in CASE expression"
    );
}

/// T051: ROW in UNION
#[test]
fn t051_row_in_union() {
    assert_feature_supported!(
        "SELECT ROW(a, b) FROM t1 UNION SELECT ROW(a, b) FROM t2",
        "T051",
        "ROW in UNION"
    );
}

/// T051: ROW in CTE
#[test]
fn t051_row_in_cte() {
    assert_feature_supported!(
        "WITH cte AS (SELECT ROW(a, b) AS r FROM t) SELECT * FROM cte",
        "T051",
        "ROW in CTE"
    );
}

/// T051: Multiple ROWs in SELECT
#[test]
fn t051_multiple_rows_in_select() {
    assert_feature_supported!(
        "SELECT ROW(a, b) AS r1, ROW(b, c) AS r2 FROM t",
        "T051",
        "Multiple ROWs in SELECT"
    );
}

// ============================================================================
// T051: STRUCT-specific DataFusion features
// ============================================================================

/// T051: STRUCT with various data types
#[test]
fn t051_struct_mixed_types() {
    assert_feature_supported!(
        "SELECT STRUCT(1, 'text', 3.14, true, NULL)",
        "T051",
        "STRUCT with mixed data types"
    );
}

/// T051: STRUCT in WHERE clause
#[test]
fn t051_struct_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE STRUCT(a, b) = STRUCT(1, 2)",
        "T051",
        "STRUCT in WHERE clause"
    );
}

/// T051: STRUCT in ORDER BY
#[test]
fn t051_struct_in_order_by() {
    assert_feature_supported!(
        "SELECT * FROM struct_types ORDER BY struct_col",
        "T051",
        "STRUCT in ORDER BY"
    );
}

/// T051: STRUCT with aggregate functions
#[test]
fn t051_struct_with_aggregates() {
    assert_feature_supported!(
        "SELECT STRUCT(SUM(a), AVG(b)) FROM t",
        "T051",
        "STRUCT with aggregate functions"
    );
}

/// T051: Array of STRUCTs
#[test]
fn t051_array_of_structs() {
    assert_feature_supported!(
        "SELECT ARRAY[STRUCT(1, 'a'), STRUCT(2, 'b')]",
        "T051",
        "Array of STRUCTs"
    );
}

// ============================================================================
// T052: MAX and MIN for row types
// ============================================================================

/// T052: MAX on STRUCT column
#[test]
fn t052_max_on_struct() {
    assert_feature_supported!(
        "SELECT MAX(struct_col) FROM struct_types",
        "T052",
        "MAX on STRUCT column"
    );
}

/// T052: MIN on STRUCT column
#[test]
fn t052_min_on_struct() {
    assert_feature_supported!(
        "SELECT MIN(struct_col) FROM struct_types",
        "T052",
        "MIN on STRUCT column"
    );
}

/// T052: MAX and MIN on STRUCT with GROUP BY
#[test]
fn t052_max_min_struct_group_by() {
    assert_feature_supported!(
        "SELECT a, MAX(ROW(b, c)), MIN(ROW(b, c)) FROM t GROUP BY a",
        "T052",
        "MAX and MIN on ROW with GROUP BY"
    );
}

/// T052: MAX on ROW constructor
#[test]
fn t052_max_on_row_constructor() {
    assert_feature_supported!(
        "SELECT MAX(ROW(a, b)) FROM t",
        "T052",
        "MAX on ROW constructor"
    );
}

/// T052: MIN on ROW constructor
#[test]
fn t052_min_on_row_constructor() {
    assert_feature_supported!(
        "SELECT MIN(ROW(a, b)) FROM t",
        "T052",
        "MIN on ROW constructor"
    );
}

/// T052: MAX with nested STRUCT
#[test]
fn t052_max_nested_struct() {
    assert_feature_supported!(
        "SELECT MAX(STRUCT(a, STRUCT(b, c))) FROM t",
        "T052",
        "MAX with nested STRUCT"
    );
}

/// T052: MIN with nested STRUCT
#[test]
fn t052_min_nested_struct() {
    assert_feature_supported!(
        "SELECT MIN(STRUCT(a, STRUCT(b, c))) FROM t",
        "T052",
        "MIN with nested STRUCT"
    );
}

/// T052: MAX on STRUCT with WHERE
#[test]
fn t052_max_struct_with_where() {
    assert_feature_supported!(
        "SELECT MAX(struct_col) FROM struct_types WHERE struct_col.x > 0",
        "T052",
        "MAX on STRUCT with WHERE"
    );
}

/// T052: MIN on STRUCT with HAVING
#[test]
fn t052_min_struct_with_having() {
    assert_feature_supported!(
        "SELECT a, MIN(ROW(b, c)) FROM t GROUP BY a HAVING MIN(ROW(b, c)) > ROW(0, 0)",
        "T052",
        "MIN on ROW with HAVING"
    );
}

// ============================================================================
// T053: Explicit aliases for all-fields reference
// ============================================================================

/// T053: Basic all-fields alias
#[test]
fn t053_all_fields_alias_basic() {
    assert_feature_supported!(
        "SELECT t.* AS (col1, col2, col3) FROM t",
        "T053",
        "Basic all-fields alias"
    );
}

/// T053: All-fields alias with specific table
#[test]
fn t053_all_fields_alias_specific_table() {
    assert_feature_supported!(
        "SELECT person.* AS (pid, fname, lname, fullname, age_val, state_val, sal, bdate, first_delim, last_delim) FROM person",
        "T053",
        "All-fields alias for specific table"
    );
}

/// T053: All-fields alias in JOIN
#[test]
fn t053_all_fields_alias_join() {
    assert_feature_supported!(
        "SELECT t1.* AS (a1, b1, c1), t2.* AS (a2, b2, c2) FROM t1 JOIN t2 ON t1.a = t2.a",
        "T053",
        "All-fields alias in JOIN"
    );
}

/// T053: All-fields alias with WHERE
#[test]
fn t053_all_fields_alias_with_where() {
    assert_feature_supported!(
        "SELECT t.* AS (x, y, z) FROM t WHERE a > 10",
        "T053",
        "All-fields alias with WHERE"
    );
}

/// T053: All-fields alias in subquery
#[test]
fn t053_all_fields_alias_subquery() {
    assert_feature_supported!(
        "SELECT * FROM (SELECT t.* AS (col1, col2, col3) FROM t) AS sub",
        "T053",
        "All-fields alias in subquery"
    );
}

/// T053: All-fields alias with ORDER BY
#[test]
fn t053_all_fields_alias_order_by() {
    assert_feature_supported!(
        "SELECT t.* AS (x, y, z) FROM t ORDER BY x",
        "T053",
        "All-fields alias with ORDER BY"
    );
}

/// T053: Multiple all-fields aliases
#[test]
fn t053_multiple_all_fields_aliases() {
    assert_feature_supported!(
        "SELECT t1.* AS (a, b, c), t2.* AS (d, e, f) FROM t1, t2",
        "T053",
        "Multiple all-fields aliases"
    );
}

// ============================================================================
// T312: OVERLAY function - Basic usage
// ============================================================================

/// T312: Basic OVERLAY with PLACING and FROM
#[test]
fn t312_overlay_basic() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING 'a' FROM 2)",
        "T312",
        "Basic OVERLAY function"
    );
}

/// T312: OVERLAY with FROM and FOR
#[test]
fn t312_overlay_with_for() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello world' PLACING 'beautiful' FROM 7 FOR 5)",
        "T312",
        "OVERLAY with FROM and FOR"
    );
}

/// T312: OVERLAY on column
#[test]
fn t312_overlay_on_column() {
    assert_feature_supported!(
        "SELECT OVERLAY(c PLACING 'X' FROM 1) FROM t",
        "T312",
        "OVERLAY on column"
    );
}

/// T312: OVERLAY with expressions
#[test]
fn t312_overlay_with_expressions() {
    assert_feature_supported!(
        "SELECT OVERLAY('test' PLACING UPPER('abc') FROM 1 + 1)",
        "T312",
        "OVERLAY with expressions"
    );
}

/// T312: OVERLAY replacing single character
#[test]
fn t312_overlay_single_char() {
    assert_feature_supported!(
        "SELECT OVERLAY('abcdef' PLACING 'X' FROM 3 FOR 1)",
        "T312",
        "OVERLAY replacing single character"
    );
}

/// T312: OVERLAY replacing multiple characters
#[test]
fn t312_overlay_multiple_chars() {
    assert_feature_supported!(
        "SELECT OVERLAY('abcdefgh' PLACING 'XYZ' FROM 3 FOR 4)",
        "T312",
        "OVERLAY replacing multiple characters"
    );
}

/// T312: OVERLAY with zero length FOR
#[test]
fn t312_overlay_zero_length() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING 'INSERT' FROM 3 FOR 0)",
        "T312",
        "OVERLAY with zero length FOR (insertion)"
    );
}

/// T312: OVERLAY at beginning of string
#[test]
fn t312_overlay_at_beginning() {
    assert_feature_supported!(
        "SELECT OVERLAY('world' PLACING 'Hello ' FROM 1 FOR 0)",
        "T312",
        "OVERLAY at beginning of string"
    );
}

/// T312: OVERLAY at end of string
#[test]
fn t312_overlay_at_end() {
    assert_feature_supported!(
        "SELECT OVERLAY('Hello' PLACING ' world' FROM 6)",
        "T312",
        "OVERLAY at end of string"
    );
}

// ============================================================================
// T312: OVERLAY function - Advanced usage
// ============================================================================

/// T312: OVERLAY in WHERE clause
#[test]
fn t312_overlay_in_where() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE OVERLAY(c PLACING 'test' FROM 1) = 'test'",
        "T312",
        "OVERLAY in WHERE clause"
    );
}

/// T312: OVERLAY with subquery
#[test]
fn t312_overlay_with_subquery() {
    assert_feature_supported!(
        "SELECT OVERLAY(c PLACING (SELECT c FROM t1 LIMIT 1) FROM 1) FROM t",
        "T312",
        "OVERLAY with subquery"
    );
}

/// T312: OVERLAY with CASE expression
#[test]
fn t312_overlay_with_case() {
    assert_feature_supported!(
        "SELECT OVERLAY(c PLACING CASE WHEN a > 10 THEN 'HIGH' ELSE 'LOW' END FROM 1) FROM t",
        "T312",
        "OVERLAY with CASE"
    );
}

/// T312: OVERLAY with NULL values
#[test]
fn t312_overlay_with_null() {
    assert_feature_supported!(
        "SELECT OVERLAY(NULL PLACING 'test' FROM 1)",
        "T312",
        "OVERLAY with NULL source"
    );
}

/// T312: OVERLAY with NULL replacement
#[test]
fn t312_overlay_null_replacement() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING NULL FROM 1)",
        "T312",
        "OVERLAY with NULL replacement"
    );
}

/// T312: OVERLAY in SELECT list with other columns
#[test]
fn t312_overlay_in_select_list() {
    assert_feature_supported!(
        "SELECT a, OVERLAY(c PLACING 'XXX' FROM 1 FOR 3) AS masked FROM t",
        "T312",
        "OVERLAY in SELECT list"
    );
}

/// T312: OVERLAY with JOIN
#[test]
fn t312_overlay_with_join() {
    assert_feature_supported!(
        "SELECT OVERLAY(t1.c PLACING t2.c FROM 1) FROM t1 JOIN t2 ON t1.a = t2.a",
        "T312",
        "OVERLAY with JOIN"
    );
}

/// T312: OVERLAY in ORDER BY
#[test]
fn t312_overlay_in_order_by() {
    assert_feature_supported!(
        "SELECT c FROM t ORDER BY OVERLAY(c PLACING 'Z' FROM 1)",
        "T312",
        "OVERLAY in ORDER BY"
    );
}

/// T312: OVERLAY in GROUP BY
#[test]
fn t312_overlay_in_group_by() {
    assert_feature_supported!(
        "SELECT OVERLAY(c PLACING 'X' FROM 1 FOR 1), COUNT(*) FROM t GROUP BY OVERLAY(c PLACING 'X' FROM 1 FOR 1)",
        "T312",
        "OVERLAY in GROUP BY"
    );
}

/// T312: Nested OVERLAY functions
#[test]
fn t312_nested_overlay() {
    assert_feature_supported!(
        "SELECT OVERLAY(OVERLAY('hello' PLACING 'a' FROM 2) PLACING 'b' FROM 3)",
        "T312",
        "Nested OVERLAY functions"
    );
}

/// T312: OVERLAY with CONCAT
#[test]
fn t312_overlay_with_concat() {
    assert_feature_supported!(
        "SELECT OVERLAY(CONCAT('hello', ' ', 'world') PLACING 'beautiful' FROM 7)",
        "T312",
        "OVERLAY with CONCAT"
    );
}

/// T312: OVERLAY with string functions
#[test]
fn t312_overlay_with_string_functions() {
    assert_feature_supported!(
        "SELECT OVERLAY(UPPER(c) PLACING LOWER('ABC') FROM 1) FROM t",
        "T312",
        "OVERLAY with string functions"
    );
}

/// T312: OVERLAY with large FROM position
#[test]
fn t312_overlay_large_position() {
    assert_feature_supported!(
        "SELECT OVERLAY('short' PLACING 'extension' FROM 100)",
        "T312",
        "OVERLAY with large FROM position"
    );
}

/// T312: OVERLAY with negative or zero FROM position
#[test]
fn t312_overlay_zero_position() {
    assert_feature_supported!(
        "SELECT OVERLAY('test' PLACING 'X' FROM 0)",
        "T312",
        "OVERLAY with zero FROM position"
    );
}

/// T312: OVERLAY replacing entire string
#[test]
fn t312_overlay_entire_string() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING 'goodbye' FROM 1 FOR 5)",
        "T312",
        "OVERLAY replacing entire string"
    );
}

// ============================================================================
// T312: OVERLAY function - Data masking use cases
// ============================================================================

/// T312: Credit card masking with OVERLAY
#[test]
fn t312_overlay_credit_card_masking() {
    assert_feature_supported!(
        "SELECT OVERLAY('1234-5678-9012-3456' PLACING '****-****-****-' FROM 1 FOR 15) AS masked_card",
        "T312",
        "Credit card masking with OVERLAY"
    );
}

/// T312: Email masking with OVERLAY
#[test]
fn t312_overlay_email_masking() {
    assert_feature_supported!(
        "SELECT OVERLAY('user@example.com' PLACING '****' FROM 1 FOR 4) AS masked_email",
        "T312",
        "Email masking with OVERLAY"
    );
}

/// T312: Phone number masking
#[test]
fn t312_overlay_phone_masking() {
    assert_feature_supported!(
        "SELECT OVERLAY('555-123-4567' PLACING 'XXX-XXX' FROM 1 FOR 7) AS masked_phone",
        "T312",
        "Phone number masking with OVERLAY"
    );
}

// ============================================================================
// Mixed features: Combining row types with other SQL features
// ============================================================================

/// Mixed: ROW with window functions
#[test]
fn mixed_row_with_window() {
    assert_feature_supported!(
        "SELECT ROW(a, b), ROW_NUMBER() OVER (ORDER BY a) FROM t",
        "T051",
        "ROW with window functions"
    );
}

/// Mixed: STRUCT in aggregate with FILTER
#[test]
fn mixed_struct_aggregate_filter() {
    assert_feature_supported!(
        "SELECT MAX(ROW(a, b)) FILTER (WHERE a > 10) FROM t",
        "T051/T052",
        "STRUCT aggregate with FILTER"
    );
}

/// Mixed: ROW comparison with IS NULL
#[test]
fn mixed_row_is_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ROW(a, b) IS NULL",
        "T051",
        "ROW with IS NULL"
    );
}

/// Mixed: ROW comparison with IS NOT NULL
#[test]
fn mixed_row_is_not_null() {
    assert_feature_supported!(
        "SELECT * FROM t WHERE ROW(a, b) IS NOT NULL",
        "T051",
        "ROW with IS NOT NULL"
    );
}

/// Mixed: OVERLAY on STRUCT field
#[test]
fn mixed_overlay_on_struct_field() {
    assert_feature_supported!(
        "SELECT OVERLAY(STRUCT(name := 'John', age := 30).name PLACING 'Jane' FROM 1)",
        "T051/T312",
        "OVERLAY on STRUCT field"
    );
}

/// Mixed: ROW in DISTINCT
#[test]
fn mixed_row_in_distinct() {
    assert_feature_supported!(
        "SELECT DISTINCT ROW(a, b) FROM t",
        "T051",
        "ROW in DISTINCT"
    );
}

/// Mixed: STRUCT with CAST
#[test]
fn mixed_struct_with_cast() {
    assert_feature_supported!(
        "SELECT STRUCT(a := CAST(a AS BIGINT), b := CAST(b AS VARCHAR)) FROM t",
        "T051",
        "STRUCT with CAST"
    );
}

/// Mixed: ROW in VALUES clause
#[test]
fn mixed_row_in_values() {
    assert_feature_supported!(
        "SELECT * FROM (VALUES (ROW(1, 'a'), 100), (ROW(2, 'b'), 200)) AS v(r, n)",
        "T051",
        "ROW in VALUES"
    );
}

/// Mixed: OVERLAY with aggregates
#[test]
fn mixed_overlay_with_aggregates() {
    assert_feature_supported!(
        "SELECT OVERLAY(MAX(c) PLACING MIN(c) FROM 1) FROM t",
        "T312",
        "OVERLAY with aggregate functions"
    );
}

// ============================================================================
// Edge cases and error scenarios
// ============================================================================

/// Edge: Empty field name in STRUCT
#[test]
fn edge_empty_field_name_struct() {
    assert_feature_supported!(
        "SELECT STRUCT(1, 2, 3)",
        "T051",
        "STRUCT with positional fields (no names)"
    );
}

/// Edge: Single element ROW
#[test]
fn edge_single_element_row() {
    assert_feature_supported!(
        "SELECT ROW(42)",
        "T051",
        "Single element ROW"
    );
}

/// Edge: ROW with all NULLs
#[test]
fn edge_row_all_nulls() {
    assert_feature_supported!(
        "SELECT ROW(NULL, NULL, NULL)",
        "T051",
        "ROW with all NULLs"
    );
}

/// Edge: OVERLAY with empty string
#[test]
fn edge_overlay_empty_string() {
    assert_feature_supported!(
        "SELECT OVERLAY('hello' PLACING '' FROM 1 FOR 2)",
        "T312",
        "OVERLAY with empty replacement"
    );
}

/// Edge: OVERLAY on empty string
#[test]
fn edge_overlay_on_empty() {
    assert_feature_supported!(
        "SELECT OVERLAY('' PLACING 'test' FROM 1)",
        "T312",
        "OVERLAY on empty string"
    );
}

/// Edge: Very long STRUCT
#[test]
fn edge_very_long_struct() {
    assert_feature_supported!(
        "SELECT STRUCT(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)",
        "T051",
        "STRUCT with many fields"
    );
}

/// Edge: STRUCT field name with special characters
#[test]
fn edge_struct_field_special_chars() {
    assert_feature_supported!(
        "SELECT STRUCT(\"field-name\" := 1, \"field.name\" := 2)",
        "T051",
        "STRUCT field with special characters"
    );
}

/// Edge: ROW comparison with different lengths
#[test]
fn edge_row_comparison_different_lengths() {
    assert_feature_supported!(
        "SELECT ROW(1, 2) = ROW(1, 2, 3)",
        "T051",
        "ROW comparison with different lengths"
    );
}

// ============================================================================
// Error cases and edge case validation tests
// ============================================================================

/// Edge: Mixed named and positional fields should fail
#[test]
#[should_panic(expected = "Cannot mix named and unnamed fields in STRUCT")]
fn edge_mixed_named_and_positional() {
    use crate::logical_plan;
    let _ = logical_plan("SELECT STRUCT(x := 1, 2)").unwrap();
}

/// Edge: Duplicate field names should fail
#[test]
#[should_panic(expected = "Duplicate field name")]
fn edge_duplicate_field_names() {
    use crate::logical_plan;
    let _ = logical_plan("SELECT STRUCT(x := 1, x := 2)").unwrap();
}

/// Edge: Deeply nested named and positional structs
#[test]
fn edge_deeply_nested_mixed_structs() {
    assert_feature_supported!(
        "SELECT STRUCT(a := ROW(1, 2), b := STRUCT(x := ROW(3, 4), y := STRUCT(z := 5)))",
        "T051",
        "Deeply nested mixed ROW and STRUCT"
    );
}
