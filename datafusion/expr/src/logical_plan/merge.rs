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

use std::cmp::Ordering;
use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, TableReference};
use sqlparser::ast::{AssignmentTarget, MergeClauseKind, ObjectName};

use crate::logical_plan::dml::make_count_schema;
use crate::{Expr, LogicalPlan};

/// MERGE logical plan node.
#[derive(Clone)]
pub struct Merge {
    /// Target table (base name, without aliases).
    pub target_table: TableReference,
    /// Target input plan (typically a table scan or alias).
    pub target: Arc<LogicalPlan>,
    /// Source input plan.
    pub source: Arc<LogicalPlan>,
    /// Join predicate between target and source.
    pub on: Expr,
    /// Merge clauses, in order.
    pub clauses: Vec<MergeClause>,
    /// Output schema (single count column).
    pub output_schema: DFSchemaRef,
}

impl Merge {
    pub fn new(
        target_table: TableReference,
        target: Arc<LogicalPlan>,
        source: Arc<LogicalPlan>,
        on: Expr,
        clauses: Vec<MergeClause>,
    ) -> Self {
        Self {
            target_table,
            target,
            source,
            on,
            clauses,
            output_schema: make_count_schema(),
        }
    }
}

impl Debug for Merge {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Merge")
            .field("target_table", &self.target_table)
            .field("target", &self.target)
            .field("source", &self.source)
            .field("on", &self.on)
            .field("clauses", &self.clauses)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl PartialEq for Merge {
    fn eq(&self, other: &Self) -> bool {
        self.target_table == other.target_table
            && self.target == other.target
            && self.source == other.source
            && self.on == other.on
            && self.clauses == other.clauses
            && self.output_schema == other.output_schema
    }
}

impl Eq for Merge {}

impl Hash for Merge {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target_table.hash(state);
        self.target.hash(state);
        self.source.hash(state);
        self.on.hash(state);
        self.clauses.hash(state);
        self.output_schema.hash(state);
    }
}

// Manual implementation needed because of `output_schema` field.
// Comparison excludes this field.
impl PartialOrd for Merge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.target_table.partial_cmp(&other.target_table) {
            Some(Ordering::Equal) => match self.target.partial_cmp(&other.target) {
                Some(Ordering::Equal) => match self.source.partial_cmp(&other.source) {
                    Some(Ordering::Equal) => match self.on.partial_cmp(&other.on) {
                        Some(Ordering::Equal) => self.clauses.partial_cmp(&other.clauses),
                        cmp => cmp,
                    },
                    cmp => cmp,
                },
                cmp => cmp,
            },
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeClause {
    pub clause_kind: MergeClauseKind,
    pub predicate: Option<Expr>,
    pub action: MergeAction,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum MergeAction {
    Insert(MergeInsertExpr),
    Update(MergeUpdateExpr),
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum MergeInsertKind {
    Values(Vec<Vec<Expr>>),
    Row,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeInsertExpr {
    pub columns: Vec<ObjectName>,
    pub kind: MergeInsertKind,
    pub insert_predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeUpdateExpr {
    pub assignments: Vec<MergeAssignment>,
    pub update_predicate: Option<Expr>,
    pub delete_predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeAssignment {
    pub target: AssignmentTarget,
    pub value: Expr,
}
