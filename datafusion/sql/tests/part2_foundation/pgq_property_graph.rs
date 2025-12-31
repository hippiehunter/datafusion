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

//! SQL/PGQ (Property Graph Queries) - ISO/IEC 9075-16:2023
//!
//! This module contains tests for the SQL/PGQ standard (Part 16),
//! which defines property graph queries and operations including:
//!
//! - CREATE PROPERTY GRAPH - Define property graphs from tables
//! - DROP PROPERTY GRAPH - Remove property graph definitions
//! - GRAPH_TABLE - Query property graphs using pattern matching
//!
//! # Feature Overview
//!
//! | Feature | Description | Status |
//! |---------|-------------|--------|
//! | PGQ001 | CREATE PROPERTY GRAPH | Supported |
//! | PGQ002 | DROP PROPERTY GRAPH | Supported |
//! | PGQ003 | GRAPH_TABLE function | Supported |
//! | PGQ004 | Graph pattern matching | Supported |
//! | PGQ005 | Path patterns | Supported |
//! | PGQ006 | Label expressions | Supported |
//! | PGQ007 | Property access | Supported |
//! | PGQ008 | Row limiting options | Supported |
//!
//! Note: SQL/PGQ is defined in ISO/IEC 9075-16:2023 and is an optional
//! extension to the SQL standard.

use crate::assert_plans;

// ============================================================================
// PGQ001: CREATE PROPERTY GRAPH
// ============================================================================

/// PGQ001: Basic CREATE PROPERTY GRAPH with vertex table
#[test]
fn pgq001_create_property_graph_basic() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with multiple vertex tables
#[test]
fn pgq001_create_property_graph_multiple_vertices() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person,
           Company
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with key specification
#[test]
fn pgq001_create_property_graph_with_key() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person KEY (id)
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with label
#[test]
fn pgq001_create_property_graph_with_label() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person LABEL User
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with properties clause
#[test]
fn pgq001_create_property_graph_with_properties() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person PROPERTIES (name, age)
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with full vertex definition
#[test]
fn pgq001_create_property_graph_full_vertex() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person KEY (id) LABEL User PROPERTIES (name, age, email)
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with edge table
#[test]
fn pgq001_create_property_graph_with_edges() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person
         )
         EDGE TABLES (
           Knows SOURCE KEY (src_id) REFERENCES Person
                 DESTINATION KEY (dst_id) REFERENCES Person
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with edge label
#[test]
fn pgq001_create_property_graph_edge_with_label() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person
         )
         EDGE TABLES (
           Friendship SOURCE KEY (person1_id) REFERENCES Person
                      DESTINATION KEY (person2_id) REFERENCES Person
                      LABEL Knows
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with edge properties
#[test]
fn pgq001_create_property_graph_edge_properties() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person
         )
         EDGE TABLES (
           Knows SOURCE KEY (src_id) REFERENCES Person
                 DESTINATION KEY (dst_id) REFERENCES Person
                 PROPERTIES (since, strength)
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH with multiple edge tables
#[test]
fn pgq001_create_property_graph_multiple_edges() {
    assert_plans!(
        "CREATE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person,
           Company
         )
         EDGE TABLES (
           Knows SOURCE KEY (src) REFERENCES Person
                 DESTINATION KEY (dst) REFERENCES Person,
           WorksAt SOURCE KEY (person_id) REFERENCES Person
                   DESTINATION KEY (company_id) REFERENCES Company
         )"
    );
}

/// PGQ001: CREATE OR REPLACE PROPERTY GRAPH
#[test]
fn pgq001_create_or_replace_property_graph() {
    assert_plans!(
        "CREATE OR REPLACE PROPERTY GRAPH social_network
         VERTEX TABLES (
           Person
         )"
    );
}

/// PGQ001: CREATE PROPERTY GRAPH IF NOT EXISTS
#[test]
fn pgq001_create_property_graph_if_not_exists() {
    assert_plans!(
        "CREATE PROPERTY GRAPH IF NOT EXISTS social_network
         VERTEX TABLES (
           Person
         )"
    );
}

// ============================================================================
// PGQ002: DROP PROPERTY GRAPH
// ============================================================================

/// PGQ002: Basic DROP PROPERTY GRAPH
#[test]
fn pgq002_drop_property_graph_basic() {
    assert_plans!(
        "DROP PROPERTY GRAPH social_network"
    );
}

/// PGQ002: DROP PROPERTY GRAPH IF EXISTS
#[test]
fn pgq002_drop_property_graph_if_exists() {
    assert_plans!(
        "DROP PROPERTY GRAPH IF EXISTS social_network"
    );
}

/// PGQ002: DROP PROPERTY GRAPH CASCADE
#[test]
fn pgq002_drop_property_graph_cascade() {
    assert_plans!(
        "DROP PROPERTY GRAPH social_network CASCADE"
    );
}

/// PGQ002: DROP PROPERTY GRAPH RESTRICT
#[test]
fn pgq002_drop_property_graph_restrict() {
    assert_plans!(
        "DROP PROPERTY GRAPH social_network RESTRICT"
    );
}

/// PGQ002: DROP PROPERTY GRAPH IF EXISTS CASCADE
#[test]
fn pgq002_drop_property_graph_if_exists_cascade() {
    assert_plans!(
        "DROP PROPERTY GRAPH IF EXISTS social_network CASCADE"
    );
}

// ============================================================================
// PGQ003: GRAPH_TABLE Function
// ============================================================================

/// PGQ003: Basic GRAPH_TABLE with simple node pattern
#[test]
fn pgq003_graph_table_simple_node() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (p:Person)
           COLUMNS (p.name)
         ) AS gt"
    );
}

/// PGQ003: GRAPH_TABLE with edge pattern
#[test]
fn pgq003_graph_table_edge_pattern() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e:KNOWS]->(b:Person)
           COLUMNS (a.name AS person1, b.name AS person2)
         ) AS gt"
    );
}

/// PGQ003: GRAPH_TABLE with WHERE clause
#[test]
fn pgq003_graph_table_with_where() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (p:Person)
           WHERE p.age > 21
           COLUMNS (p.name, p.age)
         ) AS gt"
    );
}

/// PGQ003: GRAPH_TABLE with undirected edge
#[test]
fn pgq003_graph_table_undirected_edge() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e:KNOWS]-(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ003: GRAPH_TABLE with left-directed edge
#[test]
fn pgq003_graph_table_left_edge() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)<-[e:KNOWS]-(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ003: GRAPH_TABLE with any-direction edge
#[test]
fn pgq003_graph_table_any_direction() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)<-[e:KNOWS]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

// ============================================================================
// PGQ004: Path Patterns with Quantifiers
// ============================================================================

/// PGQ004: GRAPH_TABLE with kleene star (zero or more)
#[test]
fn pgq004_path_pattern_kleene_star() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ004: GRAPH_TABLE with kleene plus (one or more)
#[test]
fn pgq004_path_pattern_kleene_plus() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e:KNOWS+]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ004: GRAPH_TABLE with range quantifier
#[test]
fn pgq004_path_pattern_range() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e:KNOWS{1,3}]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ004: GRAPH_TABLE with exact quantifier
#[test]
fn pgq004_path_pattern_exact() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e:KNOWS{2}]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

// ============================================================================
// PGQ005: Path Finding Modes
// ============================================================================

/// PGQ005: ANY SHORTEST path
#[test]
fn pgq005_any_shortest_path() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ANY SHORTEST (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ005: ALL SHORTEST paths
#[test]
fn pgq005_all_shortest_paths() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ALL SHORTEST (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ005: WALK path mode
#[test]
fn pgq005_walk_path_mode() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH WALK (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ005: TRAIL path mode
#[test]
fn pgq005_trail_path_mode() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH TRAIL (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ005: SIMPLE path mode
#[test]
fn pgq005_simple_path_mode() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH SIMPLE (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ005: ACYCLIC path mode
#[test]
fn pgq005_acyclic_path_mode() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ACYCLIC (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

// ============================================================================
// PGQ006: Label Expressions
// ============================================================================

/// PGQ006: Multiple labels with OR (|)
#[test]
fn pgq006_label_or() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (p:Person|Company)
           COLUMNS (p.name)
         ) AS gt"
    );
}

/// PGQ006: Label conjunction with AND (&)
#[test]
fn pgq006_label_and() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (p:Person&Manager)
           COLUMNS (p.name)
         ) AS gt"
    );
}

/// PGQ006: Label negation (!)
#[test]
fn pgq006_label_negation() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (p:!Bot)
           COLUMNS (p.name)
         ) AS gt"
    );
}

/// PGQ006: Wildcard label (%)
#[test]
fn pgq006_label_wildcard() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (n:%)
           COLUMNS (n.id)
         ) AS gt"
    );
}

// ============================================================================
// PGQ007: Complex Patterns
// ============================================================================

/// PGQ007: Multiple patterns
#[test]
fn pgq007_multiple_patterns() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a:Person)-[e1:KNOWS]->(b:Person),
                 (b)-[e2:WORKS_AT]->(c:Company)
           COLUMNS (a.name, b.name, c.name)
         ) AS gt"
    );
}

/// PGQ007: Pattern with path variable
#[test]
fn pgq007_path_variable() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH p = (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ007: Pattern without labels
#[test]
fn pgq007_pattern_no_labels() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH (a)-[e]->(b)
           COLUMNS (a.id, b.id)
         ) AS gt"
    );
}

/// PGQ007: Complex path with alternation
#[test]
fn pgq007_pattern_alternation() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ((a:Person)-[e1:KNOWS]->(b:Person) | (a:Person)-[e2:FOLLOWS]->(b:Person))
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

// ============================================================================
// PGQ008: Row Limiting Options
// ============================================================================

/// PGQ008: ONE ROW PER MATCH
#[test]
fn pgq008_one_row_per_match() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ONE ROW PER MATCH (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, b.name)
         ) AS gt"
    );
}

/// PGQ008: ONE ROW PER VERTEX
#[test]
fn pgq008_one_row_per_vertex() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ONE ROW PER VERTEX (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name)
         ) AS gt"
    );
}

/// PGQ008: ONE ROW PER STEP
#[test]
fn pgq008_one_row_per_step() {
    assert_plans!(
        "SELECT * FROM GRAPH_TABLE (
           social_network
           MATCH ONE ROW PER STEP (a:Person)-[e:KNOWS*]->(b:Person)
           COLUMNS (a.name, e.since)
         ) AS gt"
    );
}
