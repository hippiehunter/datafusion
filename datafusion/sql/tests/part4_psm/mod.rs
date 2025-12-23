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

//! SQL:2016 Part 4 - SQL/PSM Conformance Tests
//!
//! This module contains tests for ISO/IEC 9075-4:2016 (SQL/PSM),
//! which defines Persistent Stored Modules including:
//!
//! - Stored procedures (CREATE PROCEDURE, CALL)
//! - SQL-invoked functions (CREATE FUNCTION)
//! - Control flow statements (IF, CASE, LOOP, WHILE, REPEAT, FOR)
//! - Variable declarations and assignments (DECLARE, SET)
//! - Exception handling (DECLARE HANDLER, SIGNAL, RESIGNAL)
//! - SQL routines and compound statements
//!
//! # Feature Organization
//!
//! Tests are organized by feature ID:
//! - P-series: PSM features (P001-P999)
//! - T321: Basic SQL-invoked routines (part of Core SQL)
//!
//! # Core Features
//!
//! T321 is part of Core SQL and is mandatory for SQL:2016 conformance.
//! Most other PSM features are optional.
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | P001 | Stored modules | Not Implemented |
//! | T321 | Basic SQL-invoked routines | Partial |
//!
//! # Current Support Status
//!
//! DataFusion currently has limited support for SQL/PSM features.
//! Most tests in this module are expected to fail, documenting
//! the gaps in PSM conformance. These tests serve as a roadmap
//! for future PSM implementation.
//!
//! # Notes
//!
//! SQL/PSM is a complex specification that requires significant
//! infrastructure beyond SQL parsing and planning. Full PSM support
//! would require:
//! - Procedural execution engine
//! - Variable scoping and lifetime management
//! - Control flow handling
//! - Exception handling mechanisms
//! - Stored routine catalog and invocation

pub mod psm_routines;
