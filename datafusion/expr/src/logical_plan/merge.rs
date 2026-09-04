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
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::logical_plan::dml::{ReturningContext, make_count_schema};
use crate::{Expr, LogicalPlan};
use datafusion_common::{DFSchemaRef, TableReference};

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
    /// Columns requested by a RETURNING clause.
    pub returning_columns: Option<Vec<String>>,
    /// Expressions evaluated over the applied MERGE row image.
    pub returning_exprs: Option<Vec<Expr>>,
    /// Explicit evaluation-row semantics for RETURNING.
    pub returning_context: Option<ReturningContext>,
    /// Output schema (a count column without RETURNING).
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
            returning_columns: None,
            returning_exprs: None,
            returning_context: None,
            output_schema: make_count_schema(),
        }
    }

    /// Attach a fully lowered, parser-independent RETURNING projection.
    pub fn with_returning(
        mut self,
        columns: Vec<String>,
        exprs: Option<Vec<Expr>>,
        context: ReturningContext,
        output_schema: DFSchemaRef,
    ) -> Self {
        self.returning_columns = Some(columns);
        self.returning_exprs = exprs;
        self.returning_context = Some(context);
        self.output_schema = output_schema;
        self
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
            .field("returning_columns", &self.returning_columns)
            .field("returning_exprs", &self.returning_exprs)
            .field("returning_context", &self.returning_context)
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
            && self.returning_columns == other.returning_columns
            && self.returning_exprs == other.returning_exprs
            && self.returning_context == other.returning_context
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
        self.returning_columns.hash(state);
        self.returning_exprs.hash(state);
        self.returning_context.hash(state);
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

/// The row-presence condition for a MERGE arm.
///
/// This is a logical property of the arm, not parser state. It deliberately
/// lives in `datafusion-expr` so MERGE plans do not retain SQL AST types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MergeClauseKind {
    Matched,
    NotMatched,
    NotMatchedByTarget,
    NotMatchedBySource,
}

impl Display for MergeClauseKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Matched => "MATCHED",
            Self::NotMatched => "NOT MATCHED",
            Self::NotMatchedByTarget => "NOT MATCHED BY TARGET",
            Self::NotMatchedBySource => "NOT MATCHED BY SOURCE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum MergeAction {
    Insert(MergeInsertExpr),
    Update(MergeUpdateExpr),
    Delete,
    DoNothing,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum MergeInsertKind {
    Values(Vec<Vec<Expr>>),
    Row,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeInsertExpr {
    /// Full target-row evaluation columns, in table order.
    pub columns: Vec<String>,
    /// Columns the statement actually provides to storage. Identity columns
    /// discarded by `OVERRIDING USER VALUE` are absent here.
    pub provided_columns: Vec<String>,
    /// The statement's identity override contract.
    pub overriding: Option<MergeIdentityOverride>,
    pub kind: MergeInsertKind,
    pub insert_predicate: Option<Expr>,
}

/// Identity-column behavior requested by a MERGE INSERT arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MergeIdentityOverride {
    SystemValue,
    UserValue,
}

impl Display for MergeIdentityOverride {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SystemValue => "OVERRIDING SYSTEM VALUE",
            Self::UserValue => "OVERRIDING USER VALUE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeUpdateExpr {
    pub assignments: Vec<MergeAssignment>,
    pub update_predicate: Option<Expr>,
    pub delete_predicate: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct MergeAssignment {
    pub target: MergeAssignmentTarget,
    pub value: Expr,
}

/// Parser-free target of a MERGE UPDATE assignment.
///
/// Tuple and indirection forms are retained so downstream validation and
/// persisted-plan compatibility remain explicit, but any expressions used to
/// spell an indirection are reduced to a diagnostic string at SQL lowering.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MergeAssignmentTarget {
    ColumnName(Vec<String>),
    Tuple(Vec<Vec<String>>),
    Indirection(String),
}

impl MergeAssignmentTarget {
    /// Return the final name component for a simple column target.
    pub fn column_name(&self) -> Option<&str> {
        match self {
            Self::ColumnName(parts) => parts.last().map(String::as_str),
            Self::Tuple(_) | Self::Indirection(_) => None,
        }
    }
}

impl Display for MergeAssignmentTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnName(parts) => f.write_str(&parts.join(".")),
            Self::Tuple(columns) => {
                f.write_str("(")?;
                for (index, parts) in columns.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    f.write_str(&parts.join("."))?;
                }
                f.write_str(")")
            }
            Self::Indirection(target) => f.write_str(target),
        }
    }
}
