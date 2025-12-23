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

//! SQL:2016 Part 2 - SQL/Foundation Conformance Tests
//!
//! This module contains tests for ISO/IEC 9075-2:2016 (SQL/Foundation),
//! which defines the core SQL language including:
//!
//! - Data types (numeric, character, boolean, datetime, interval)
//! - Query specification (SELECT, FROM, WHERE, GROUP BY, etc.)
//! - Predicates and search conditions
//! - Query expressions (UNION, INTERSECT, EXCEPT)
//! - Data manipulation (INSERT, UPDATE, DELETE, MERGE)
//! - Schema manipulation (CREATE, ALTER, DROP)
//! - Integrity constraints
//! - Transaction support
//!
//! # Feature Organization
//!
//! Tests are organized by feature ID:
//! - E-series: Core features (E011-E182)
//! - F-series: Extended features (F021-F812)
//! - S-series: Object features (S011-S281)
//! - T-series: Temporal and other features (T011-T652)
//!
//! # Core Features (179 mandatory)
//!
//! The following features are part of Core SQL and are mandatory
//! for SQL:2016 conformance:
//!
//! | ID | Description |
//! |----|-------------|
//! | E011 | Numeric data types |
//! | E021 | Character data types |
//! | E031 | Identifiers |
//! | E051 | Basic query specification |
//! | E061 | Basic predicates and search conditions |
//! | E071 | Basic query expressions |
//! | E081 | Basic privileges |
//! | E091 | Set functions |
//! | E101 | Basic data manipulation |
//! | E111 | Single row SELECT |
//! | E121 | Basic cursor support |
//! | E131 | Null value support |
//! | E141 | Basic integrity constraints |
//! | E151 | Transaction support |
//! | E152 | Basic SET TRANSACTION |
//! | E153 | Updatable queries with subqueries |
//! | E161 | SQL comments |
//! | E171 | SQLSTATE support |
//! | E182 | Module language |
//! | F021 | Basic information schema |
//! | F031 | Basic schema manipulation |
//! | F041 | Basic joined table |
//! | F051 | Basic date and time |
//! | F081 | UNION and EXCEPT in views |
//! | F131 | Grouped operations |
//! | F181 | Multiple module support |
//! | F201 | CAST function |
//! | F221 | Explicit defaults |
//! | F261 | CASE expression |
//! | F311 | Schema definition |
//! | F471 | Scalar subquery values |
//! | F481 | Expanded NULL predicate |
//! | F812 | Basic flagging |
//! | S011 | Distinct data types |
//! | T321 | Basic SQL-invoked routines |

// Feature test modules will be added here as they are created
// Example: pub mod e011_numeric_types;

pub mod b021_utility_statements;
pub mod e011_numeric_types;
pub mod e021_character_types;
pub mod e031_identifiers;
pub mod e051_query_specification;
pub mod e061_predicates;
pub mod e071_query_expressions;
pub mod e081_privileges;
pub mod e091_set_functions;
pub mod e101_data_manipulation;
pub mod e111_misc_core;
pub mod e141_integrity_constraints;
pub mod e151_transactions;
pub mod f021_information_schema;
pub mod f031_schema_manipulation;
pub mod f041_joined_tables;
pub mod f051_datetime;
pub mod f201_cast_function;
pub mod f261_case_expression;
pub mod f471_scalar_subquery_values;
pub mod f591_derived_tables;
pub mod f850_order_limit;
pub mod numeric_functions;
pub mod r010_row_pattern_recognition;
pub mod s091_array_support;
pub mod string_functions;
pub mod t051_row_types;
pub mod t121_with_clause;
pub mod t141_pattern_matching;
pub mod t151_distinct_predicate;
pub mod t611_window_functions;
pub mod t8xx_json;
