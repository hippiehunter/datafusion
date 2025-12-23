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

//! SQL:2016 Conformance Test Framework
//!
//! This module provides test infrastructure for tracking DataFusion's conformance
//! to the ISO/IEC 9075:2016 SQL standard (SQL:2016).
//!
//! # Organization
//!
//! Tests are organized by SQL standard parts:
//! - Part 2: SQL/Foundation (Core SQL)
//! - Part 3: SQL/CLI (Call-Level Interface)
//! - Part 4: SQL/PSM (Persistent Stored Modules)
//! - Part 9: SQL/MED (Management of External Data)
//! - Part 11: SQL/Schemata (Information Schema)
//! - Part 14: SQL/XML (XML-related specifications)
//!
//! # Feature IDs
//!
//! Each test maps to a SQL standard feature ID:
//! - E-series: Core features (E011, E021, etc.)
//! - F-series: Extended features (F031, F041, etc.)
//! - S-series: Object-related features
//! - T-series: Temporal and other features
//!
//! # Test Macros
//!
//! - `assert_parses!` - Verify SQL parses without error
//! - `assert_plans!` - Verify SQL converts to logical plan
//! - `assert_not_implemented!` - Mark feature as not yet implemented
//! - `assert_feature_supported!` - Verify a feature works end-to-end

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::*;
use datafusion_common::config::ConfigOptions;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{GetExt, Result, TableReference, plan_err};
use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_expr::planner::ExprPlanner;
use datafusion_sql::parser::DFParser;
use datafusion_sql::planner::{ContextProvider, ParserOptions, SqlToRel};
use sqlparser::dialect::{Dialect, GenericDialect};

// Re-export submodules for each standard part
pub mod part2_foundation;
pub mod part4_psm;

// ============================================================================
// Test Macros
// ============================================================================

/// Assert that SQL text parses without error using DFParser.
///
/// This tests the parsing layer only (SQL text -> AST).
///
/// # Example
/// ```ignore
/// assert_parses!("SELECT 42");
/// assert_parses!("CREATE TABLE t (x INT)");
/// ```
#[macro_export]
macro_rules! assert_parses {
    ($sql:expr) => {{
        let result = crate::parse_sql($sql);
        assert!(
            result.is_ok(),
            "SQL should parse successfully.\nSQL: {}\nError: {:?}",
            $sql,
            result.unwrap_err()
        );
    }};
}

/// Assert that SQL text does NOT parse (expected parse error).
///
/// Use this for syntax that DataFusion intentionally doesn't support.
#[macro_export]
macro_rules! assert_parse_error {
    ($sql:expr) => {{
        let result = crate::parse_sql($sql);
        assert!(
            result.is_err(),
            "SQL should fail to parse but succeeded.\nSQL: {}",
            $sql
        );
    }};
    ($sql:expr, $expected_error:expr) => {{
        let result = crate::parse_sql($sql);
        assert!(
            result.is_err(),
            "SQL should fail to parse but succeeded.\nSQL: {}",
            $sql
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains($expected_error),
            "Error message mismatch.\nExpected to contain: {}\nActual: {}",
            $expected_error,
            err
        );
    }};
}

/// Assert that SQL text converts to a logical plan without error.
///
/// This tests both parsing and planning (SQL text -> AST -> LogicalPlan).
///
/// # Example
/// ```ignore
/// assert_plans!("SELECT 42");
/// assert_plans!("SELECT * FROM person WHERE age > 21");
/// ```
#[macro_export]
macro_rules! assert_plans {
    ($sql:expr) => {{
        let result = crate::logical_plan($sql);
        assert!(
            result.is_ok(),
            "SQL should plan successfully.\nSQL: {}\nError: {:?}",
            $sql,
            result.unwrap_err()
        );
    }};
}

/// Assert that SQL text fails to plan (expected planning error).
///
/// Use this for SQL that parses but cannot be converted to a logical plan.
#[macro_export]
macro_rules! assert_plan_error {
    ($sql:expr) => {{
        let result = crate::logical_plan($sql);
        assert!(
            result.is_err(),
            "SQL should fail to plan but succeeded.\nSQL: {}",
            $sql
        );
    }};
    ($sql:expr, $expected_error:expr) => {{
        let result = crate::logical_plan($sql);
        assert!(
            result.is_err(),
            "SQL should fail to plan but succeeded.\nSQL: {}",
            $sql
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains($expected_error),
            "Error message mismatch.\nExpected to contain: {}\nActual: {}\nSQL: {}",
            $expected_error,
            err,
            $sql
        );
    }};
}

/// Assert that a SQL:2016 feature is NOT YET implemented.
///
/// This macro is used to document missing features. When a feature is
/// implemented, this test will start failing, prompting an update to
/// a positive test.
///
/// # Example
/// ```ignore
/// // Feature T151: DISTINCT predicate
/// assert_not_implemented!(
///     "SELECT * FROM t WHERE a IS DISTINCT FROM b",
///     "T151",
///     "DISTINCT predicate"
/// );
/// ```
#[macro_export]
macro_rules! assert_not_implemented {
    ($sql:expr, $feature_id:expr, $description:expr) => {{
        let parse_result = crate::parse_sql($sql);
        let plan_result = crate::logical_plan($sql);

        // Feature is "not implemented" if either parsing or planning fails
        let is_not_implemented = parse_result.is_err() || plan_result.is_err();

        assert!(
            is_not_implemented,
            "Feature {} ({}) appears to be implemented now!\n\
             SQL: {}\n\
             Please update this test to use assert_parses! or assert_plans! instead.",
            $feature_id,
            $description,
            $sql
        );
    }};
}

/// Assert that a feature is fully supported (parses and plans).
///
/// Convenience macro combining parse + plan assertions.
#[macro_export]
macro_rules! assert_feature_supported {
    ($sql:expr, $feature_id:expr, $description:expr) => {{
        // First verify it parses
        let parse_result = crate::parse_sql($sql);
        assert!(
            parse_result.is_ok(),
            "Feature {} ({}) should parse.\nSQL: {}\nError: {:?}",
            $feature_id,
            $description,
            $sql,
            parse_result.unwrap_err()
        );

        // Then verify it plans
        let plan_result = crate::logical_plan($sql);
        assert!(
            plan_result.is_ok(),
            "Feature {} ({}) should plan.\nSQL: {}\nError: {:?}",
            $feature_id,
            $description,
            $sql,
            plan_result.unwrap_err()
        );
    }};
}

// ============================================================================
// Test Infrastructure
// ============================================================================

/// Mock CSV file type for testing
struct MockCsvType {}

impl GetExt for MockCsvType {
    fn get_ext(&self) -> String {
        "csv".to_string()
    }
}

impl FileType for MockCsvType {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl std::fmt::Display for MockCsvType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_ext())
    }
}

/// Mock session state for conformance tests
#[derive(Default)]
pub struct ConformanceSessionState {
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    config_options: ConfigOptions,
}

impl ConformanceSessionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_scalar_function(mut self, f: Arc<ScalarUDF>) -> Self {
        self.scalar_functions.insert(f.name().to_string(), f);
        self
    }

    pub fn with_aggregate_function(mut self, f: Arc<AggregateUDF>) -> Self {
        self.aggregate_functions
            .insert(f.name().to_string().to_lowercase(), f);
        self
    }
}

/// Empty table source for schema-only testing
struct EmptyTable {
    schema: SchemaRef,
}

impl EmptyTable {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl TableSource for EmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Context provider for conformance tests with standard test tables
pub struct ConformanceContextProvider {
    state: ConformanceSessionState,
}

impl ConformanceContextProvider {
    pub fn new() -> Self {
        Self {
            state: ConformanceSessionState::new(),
        }
    }

    pub fn with_state(state: ConformanceSessionState) -> Self {
        Self { state }
    }
}

impl Default for ConformanceContextProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextProvider for ConformanceContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        // Standard test tables for conformance testing
        let schema = match name.table() {
            // Basic test table with various numeric types
            "t" | "t1" | "t2" | "t3" => Ok(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
                Field::new("c", DataType::Utf8, true),
            ])),

            // Numeric types test table
            "numeric_types" => Ok(Schema::new(vec![
                Field::new("tiny", DataType::Int8, true),
                Field::new("small", DataType::Int16, true),
                Field::new("regular", DataType::Int32, true),
                Field::new("big", DataType::Int64, true),
                Field::new("real_col", DataType::Float32, true),
                Field::new("double_col", DataType::Float64, true),
                Field::new("decimal_col", DataType::Decimal128(10, 2), true),
            ])),

            // Character types test table
            "char_types" => Ok(Schema::new(vec![
                Field::new("char_col", DataType::Utf8, true),
                Field::new("varchar_col", DataType::Utf8, true),
                Field::new("text_col", DataType::LargeUtf8, true),
            ])),

            // Date/time types test table
            "datetime_types" => Ok(Schema::new(vec![
                Field::new("date_col", DataType::Date32, true),
                Field::new("time_col", DataType::Time64(TimeUnit::Nanosecond), true),
                Field::new(
                    "timestamp_col",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new(
                    "interval_col",
                    DataType::Interval(IntervalUnit::DayTime),
                    true,
                ),
            ])),

            // Standard person table (from existing tests)
            "person" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("salary", DataType::Float64, false),
                Field::new(
                    "birth_date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),

            // Orders table for join tests
            "orders" => Ok(Schema::new(vec![
                Field::new("order_id", DataType::UInt32, false),
                Field::new("customer_id", DataType::UInt32, false),
                Field::new("item", DataType::Utf8, false),
                Field::new("qty", DataType::Int32, false),
                Field::new("price", DataType::Float64, false),
            ])),

            // Products table for join tests
            "products" => Ok(Schema::new(vec![
                Field::new("product_id", DataType::UInt32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
                Field::new("category", DataType::Utf8, true),
            ])),

            // Array types test table
            "array_types" => Ok(Schema::new(vec![
                Field::new(
                    "int_array",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                    true,
                ),
                Field::new(
                    "str_array",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                ),
            ])),

            // Struct types test table
            "struct_types" => Ok(Schema::new(vec![Field::new(
                "struct_col",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Int32, true),
                ])),
                true,
            )])),

            // JSON data test table
            "json_data" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("person_id", DataType::Int32, true),
                Field::new("data", DataType::Utf8, true), // JSON as string for now
            ])),

            // Events table for JSON testing
            "events" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("event_data", DataType::Utf8, true), // JSON as string
                Field::new("metadata", DataType::Utf8, true), // JSON as string
            ])),

            _ => plan_err!("Table not found: {}", name.table()),
        };

        match schema {
            Ok(s) => Ok(Arc::new(EmptyTable::new(Arc::new(s)))),
            Err(e) => Err(e),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions.get(name).cloned()
    }

    fn options(&self) -> &ConfigOptions {
        &self.state.config_options
    }

    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        Ok(Arc::new(MockCsvType {}))
    }

    fn create_cte_work_table(
        &self,
        _name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(EmptyTable::new(schema)))
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
    }
}

// ============================================================================
// Public API Functions
// ============================================================================

/// Parse SQL text into AST statements.
///
/// Uses the default GenericDialect.
pub fn parse_sql(sql: &str) -> Result<()> {
    let _ = DFParser::parse_sql(sql)?;
    Ok(())
}

/// Parse SQL text with a specific dialect.
pub fn parse_sql_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<()> {
    let _ = DFParser::parse_sql_with_dialect(sql, dialect)?;
    Ok(())
}

/// Convert SQL text to a logical plan.
///
/// Uses the default GenericDialect and ConformanceContextProvider.
pub fn logical_plan(
    sql: &str,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    logical_plan_with_options(sql, ParserOptions::default())
}

/// Convert SQL text to a logical plan with custom parser options.
pub fn logical_plan_with_options(
    sql: &str,
    options: ParserOptions,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    let dialect = GenericDialect {};
    logical_plan_with_dialect_and_options(sql, &dialect, options)
}

/// Convert SQL text to a logical plan with a specific dialect and options.
pub fn logical_plan_with_dialect_and_options(
    sql: &str,
    dialect: &dyn Dialect,
    options: ParserOptions,
) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
    let context = ConformanceContextProvider::new();
    let planner = SqlToRel::new_with_options(&context, options);
    let mut ast = DFParser::parse_sql_with_dialect(sql, dialect)?;
    planner.statement_to_plan(ast.pop_front().unwrap())
}

// ============================================================================
// Feature Status Tracking
// ============================================================================

/// Status of a SQL standard feature
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureStatus {
    /// Feature is fully supported
    Supported,
    /// Feature is partially supported (some subfeatures work)
    Partial,
    /// Feature is not implemented
    NotImplemented,
    /// Feature is not applicable to DataFusion's use case
    NotApplicable,
}

/// Metadata about a SQL standard feature
#[derive(Debug, Clone)]
pub struct FeatureInfo {
    /// Feature ID (e.g., "E011", "T151")
    pub id: &'static str,
    /// Feature description
    pub description: &'static str,
    /// Whether this is a Core SQL feature (mandatory for conformance)
    pub is_core: bool,
    /// Current implementation status
    pub status: FeatureStatus,
    /// Optional notes about the implementation
    pub notes: Option<&'static str>,
}

impl FeatureInfo {
    pub const fn new(
        id: &'static str,
        description: &'static str,
        is_core: bool,
        status: FeatureStatus,
    ) -> Self {
        Self {
            id,
            description,
            is_core,
            status,
            notes: None,
        }
    }

    pub const fn with_notes(mut self, notes: &'static str) -> Self {
        self.notes = Some(notes);
        self
    }
}

// ============================================================================
// Tests for the test infrastructure itself
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql_basic() {
        assert!(parse_sql("SELECT 1").is_ok());
        assert!(parse_sql("SELECT * FROM t").is_ok());
        assert!(parse_sql("CREATE TABLE t (x INT)").is_ok());
    }

    #[test]
    fn test_parse_sql_error() {
        assert!(parse_sql("SELEC 1").is_err()); // typo
        assert!(parse_sql("SELECT FROM").is_err()); // incomplete
    }

    #[test]
    fn test_logical_plan_basic() {
        assert!(logical_plan("SELECT 1").is_ok());
        assert!(logical_plan("SELECT * FROM t").is_ok());
        assert!(logical_plan("SELECT a, b FROM t WHERE a > 10").is_ok());
    }

    #[test]
    fn test_logical_plan_with_joins() {
        assert!(logical_plan("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a").is_ok());
        assert!(logical_plan("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a").is_ok());
        assert!(logical_plan("SELECT * FROM t1 CROSS JOIN t2").is_ok());
    }

    #[test]
    fn test_context_provider_tables() {
        let ctx = ConformanceContextProvider::new();
        assert!(ctx.get_table_source("t".into()).is_ok());
        assert!(ctx.get_table_source("person".into()).is_ok());
        assert!(ctx.get_table_source("numeric_types".into()).is_ok());
        assert!(ctx.get_table_source("nonexistent".into()).is_err());
    }
}
