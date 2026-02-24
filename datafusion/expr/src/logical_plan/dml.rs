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
use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{DFSchemaRef, TableReference};
use sqlparser::ast::AssignmentTarget;

// Re-export ConflictTarget from sqlparser for ON CONFLICT clause
pub use sqlparser::ast::ConflictTarget;

use crate::{Expr, LogicalPlan, TableSource};

/// Operator that copies the contents of a database to file(s)
#[derive(Clone)]
pub struct CopyTo {
    /// The relation that determines the tuples to write to the output file(s)
    pub input: Arc<LogicalPlan>,
    /// The location to write the file(s)
    pub output_url: String,
    /// Determines which, if any, columns should be used for hive-style partitioned writes
    pub partition_by: Vec<String>,
    /// File type trait
    pub file_type: Arc<dyn FileType>,
    /// SQL Options that can affect the formats
    pub options: HashMap<String, String>,
    /// The schema of the output (a single column "count")
    pub output_schema: DFSchemaRef,
}

impl Debug for CopyTo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyTo")
            .field("input", &self.input)
            .field("output_url", &self.output_url)
            .field("partition_by", &self.partition_by)
            .field("file_type", &"...")
            .field("options", &self.options)
            .field("output_schema", &self.output_schema)
            .finish_non_exhaustive()
    }
}

// Implement PartialEq manually
impl PartialEq for CopyTo {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input && self.output_url == other.output_url
    }
}

// Implement Eq (no need for additional logic over PartialEq)
impl Eq for CopyTo {}

// Manual implementation needed because of `file_type` and `options` fields.
// Comparison excludes these field.
impl PartialOrd for CopyTo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => match self.output_url.partial_cmp(&other.output_url)
            {
                Some(Ordering::Equal) => {
                    self.partition_by.partial_cmp(&other.partition_by)
                }
                cmp => cmp,
            },
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

// Implement Hash manually
impl Hash for CopyTo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.output_url.hash(state);
    }
}

impl CopyTo {
    pub fn new(
        input: Arc<LogicalPlan>,
        output_url: String,
        partition_by: Vec<String>,
        file_type: Arc<dyn FileType>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            input,
            output_url,
            partition_by,
            file_type,
            options,
            // The output schema is always a single column "count" with the number of rows copied
            output_schema: make_count_schema(),
        }
    }
}

/// Modifies the content of a database
///
/// This operator is used to perform DML operations such as INSERT, DELETE,
/// UPDATE, and CTAS (CREATE TABLE AS SELECT).
///
/// * `INSERT` - Appends new rows to the existing table. Calls
///   [`TableProvider::insert_into`]
///
/// * `DELETE` - Removes rows from the table. Currently NOT supported by the
///   [`TableProvider`] trait or builtin sources.
///
/// * `UPDATE` - Modifies existing rows in the table. Currently NOT supported by
///   the [`TableProvider`] trait or builtin sources.
///
/// * `CREATE TABLE AS SELECT` - Creates a new table and populates it with data
///   from a query. This is similar to the `INSERT` operation, but it creates a new
///   table instead of modifying an existing one.
///
/// Note that the structure is adapted from substrait WriteRel)
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
/// [`TableProvider::insert_into`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#method.insert_into
#[derive(Clone)]
pub struct DmlStatement {
    /// The table name
    pub table_name: TableReference,
    /// this is target table to insert into
    pub target: Arc<dyn TableSource>,
    /// The type of operation to perform
    pub op: WriteOp,
    /// The relation that determines the tuples to add/remove/modify the schema must match with table_schema
    pub input: Arc<LogicalPlan>,
    /// The schema of the output relation
    pub output_schema: DFSchemaRef,
    /// Explicit target columns for INSERT (e.g. `INSERT INTO t (a, b) ...`)
    pub target_columns: Option<Vec<String>>,
    /// Columns requested by a RETURNING clause
    pub returning_columns: Option<Vec<String>>,
    /// Expressions requested by a RETURNING clause
    pub returning_exprs: Option<Vec<Expr>>,
    /// OVERRIDING SYSTEM VALUE was specified (PostgreSQL identity columns)
    pub overriding_system_value: bool,
}
impl Eq for DmlStatement {}
impl Hash for DmlStatement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.target.schema().hash(state);
        self.op.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
        self.target_columns.hash(state);
        self.returning_columns.hash(state);
        self.returning_exprs.hash(state);
        self.overriding_system_value.hash(state);
    }
}

impl PartialEq for DmlStatement {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.target.schema() == other.target.schema()
            && self.op == other.op
            && self.input == other.input
            && self.output_schema == other.output_schema
            && self.target_columns == other.target_columns
            && self.returning_columns == other.returning_columns
            && self.returning_exprs == other.returning_exprs
            && self.overriding_system_value == other.overriding_system_value
    }
}

impl Debug for DmlStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmlStatement")
            .field("table_name", &self.table_name)
            .field("target", &"...")
            .field("target_schema", &self.target.schema())
            .field("op", &self.op)
            .field("input", &self.input)
            .field("output_schema", &self.output_schema)
            .field("target_columns", &self.target_columns)
            .field("returning_columns", &self.returning_columns)
            .field("returning_exprs", &self.returning_exprs)
            .finish()
    }
}

impl DmlStatement {
    /// Creates a new DML statement with the output schema set to a single `count` column.
    pub fn new(
        table_name: TableReference,
        target: Arc<dyn TableSource>,
        op: WriteOp,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            table_name,
            target,
            op,
            input,
            output_schema: make_count_schema(),
            target_columns: None,
            returning_columns: None,
            returning_exprs: None,
            overriding_system_value: false,
        }
    }

    /// Set explicit INSERT target columns.
    pub fn with_target_columns(mut self, columns: Vec<String>) -> Self {
        if !columns.is_empty() {
            self.target_columns = Some(columns);
        }
        self
    }

    /// Set RETURNING clause columns.
    pub fn with_returning_columns(mut self, columns: Vec<String>) -> Self {
        if !columns.is_empty() {
            self.returning_columns = Some(columns);
        }
        self
    }

    /// Set RETURNING clause expressions.
    pub fn with_returning_exprs(mut self, exprs: Vec<Expr>) -> Self {
        if !exprs.is_empty() {
            self.returning_exprs = Some(exprs);
        }
        self
    }

    /// Override the output schema.
    pub fn with_output_schema(mut self, output_schema: DFSchemaRef) -> Self {
        self.output_schema = output_schema;
        self
    }

    /// Mark this INSERT as using OVERRIDING SYSTEM VALUE.
    pub fn with_overriding_system_value(mut self) -> Self {
        self.overriding_system_value = true;
        self
    }

    /// Return a descriptive name of this [`DmlStatement`]
    pub fn name(&self) -> &str {
        self.op.name()
    }
}

// Manual implementation needed because of `table_schema` and `output_schema` fields.
// Comparison excludes these fields.
impl PartialOrd for DmlStatement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.table_name.partial_cmp(&other.table_name) {
            Some(Ordering::Equal) => match self.op.partial_cmp(&other.op) {
                Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
                cmp => cmp,
            },
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// The type of DML operation to perform.
///
/// See [`DmlStatement`] for more details.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum WriteOp {
    /// `INSERT INTO` operation
    Insert(InsertOp),
    /// `DELETE` operation
    Delete,
    /// `UPDATE` operation
    Update,
    /// `CREATE TABLE AS SELECT` operation
    Ctas,
}

impl WriteOp {
    /// Return a descriptive name of this [`WriteOp`]
    pub fn name(&self) -> &str {
        match self {
            WriteOp::Insert(insert) => insert.name(),
            WriteOp::Delete => "Delete",
            WriteOp::Update => "Update",
            WriteOp::Ctas => "Ctas",
        }
    }
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum InsertOp {
    /// Appends new rows to the existing table without modifying any
    /// existing rows. This corresponds to the SQL `INSERT INTO` query.
    Append,
    /// Overwrites all existing rows in the table with the new rows.
    /// This corresponds to the SQL `INSERT OVERWRITE` query.
    Overwrite,
    /// If any existing rows collides with the inserted rows (typically based
    /// on a unique key or primary key), those existing rows are replaced.
    /// This corresponds to the SQL `REPLACE INTO` query and its equivalents.
    Replace,
    /// Insert with ON CONFLICT clause (PostgreSQL/SQLite upsert syntax).
    /// The OnConflict specifies what to do when a uniqueness constraint is violated.
    WithConflictClause(OnConflict),
}

impl InsertOp {
    /// Return a descriptive name of this [`InsertOp`]
    pub fn name(&self) -> &str {
        match self {
            InsertOp::Append => "Insert Into",
            InsertOp::Overwrite => "Insert Overwrite",
            InsertOp::Replace => "Replace Into",
            InsertOp::WithConflictClause(on_conflict) => match &on_conflict.action {
                OnConflictAction::DoNothing => "Insert On Conflict Do Nothing",
                OnConflictAction::DoUpdate(_) => "Insert On Conflict Do Update",
            },
        }
    }
}

impl Display for InsertOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// ON CONFLICT clause for INSERT statements.
///
/// This represents the PostgreSQL/SQLite ON CONFLICT clause which specifies
/// what to do when an INSERT would violate a uniqueness constraint.
///
/// Examples:
/// ```sql
/// -- DO NOTHING: silently skip conflicting rows
/// INSERT INTO t (id, name) VALUES (1, 'bob') ON CONFLICT DO NOTHING;
///
/// -- DO UPDATE: update conflicting rows with new values
/// INSERT INTO t (id, name, value) VALUES (1, 'bob', 200)
/// ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value;
///
/// -- With constraint name
/// INSERT INTO t (id, name) VALUES (1, 'bob')
/// ON CONFLICT ON CONSTRAINT pk_t DO UPDATE SET name = EXCLUDED.name;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct OnConflict {
    /// The conflict target - either column list or constraint name.
    /// If None, applies to any constraint conflict.
    pub conflict_target: Option<ConflictTarget>,
    /// The action to take on conflict.
    pub action: OnConflictAction,
}

impl OnConflict {
    /// Creates a new ON CONFLICT clause.
    pub fn new(conflict_target: Option<ConflictTarget>, action: OnConflictAction) -> Self {
        Self {
            conflict_target,
            action,
        }
    }

    /// Creates an ON CONFLICT DO NOTHING clause.
    pub fn do_nothing(conflict_target: Option<ConflictTarget>) -> Self {
        Self::new(conflict_target, OnConflictAction::DoNothing)
    }

    /// Creates an ON CONFLICT DO UPDATE clause.
    pub fn do_update(
        conflict_target: Option<ConflictTarget>,
        update: DoUpdateAction,
    ) -> Self {
        Self::new(conflict_target, OnConflictAction::DoUpdate(update))
    }
}

impl Display for OnConflict {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ON CONFLICT")?;
        if let Some(target) = &self.conflict_target {
            match target {
                ConflictTarget::Columns(cols) => {
                    write!(f, " (")?;
                    for (i, col) in cols.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", col)?;
                    }
                    write!(f, ")")?;
                }
                ConflictTarget::OnConstraint(name) => {
                    write!(f, " ON CONSTRAINT {}", name)?;
                }
            }
        }
        write!(f, " {}", self.action)
    }
}

/// The action to take when a conflict is detected during INSERT.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum OnConflictAction {
    /// DO NOTHING: silently skip the conflicting row.
    DoNothing,
    /// DO UPDATE: update the existing row with new values.
    DoUpdate(DoUpdateAction),
}

impl Display for OnConflictAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            OnConflictAction::DoNothing => write!(f, "DO NOTHING"),
            OnConflictAction::DoUpdate(update) => {
                write!(f, "DO UPDATE SET ")?;
                for (i, assignment) in update.assignments.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} = {}", assignment.target, assignment.value)?;
                }
                if let Some(selection) = &update.selection {
                    write!(f, " WHERE {}", selection)?;
                }
                Ok(())
            }
        }
    }
}

/// The update action for ON CONFLICT DO UPDATE.
///
/// Contains the assignments to apply when updating a conflicting row.
/// The EXCLUDED pseudo-table refers to the row that would have been inserted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct DoUpdateAction {
    /// Column assignments in the form `column = expression`.
    /// Expressions may reference EXCLUDED.column to get the value
    /// that would have been inserted.
    pub assignments: Vec<ConflictAssignment>,
    /// Optional WHERE clause to filter which conflicts are updated.
    /// If the WHERE condition is not met, the conflicting row is left unchanged.
    pub selection: Option<Expr>,
}

impl DoUpdateAction {
    /// Creates a new DO UPDATE action with the given assignments.
    pub fn new(assignments: Vec<ConflictAssignment>, selection: Option<Expr>) -> Self {
        Self {
            assignments,
            selection,
        }
    }
}

/// A column assignment in an ON CONFLICT DO UPDATE clause.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct ConflictAssignment {
    /// The target column to update.
    pub target: AssignmentTarget,
    /// The value expression (may reference EXCLUDED.column).
    pub value: Expr,
}

/// Operator that copies the contents of a file to a database table
#[derive(Clone)]
pub struct CopyFrom {
    /// The table name to insert into
    pub table_name: TableReference,
    /// The source URL to read from
    pub source_url: String,
    /// Determines which columns to load from the file
    pub columns: Vec<String>,
    /// File type trait
    pub file_type: Arc<dyn FileType>,
    /// SQL Options that can affect the formats
    pub options: HashMap<String, String>,
    /// The schema of the output (a single column "count")
    pub output_schema: DFSchemaRef,
}

impl Debug for CopyFrom {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyFrom")
            .field("table_name", &self.table_name)
            .field("source_url", &self.source_url)
            .field("columns", &self.columns)
            .field("file_type", &"...")
            .field("options", &self.options)
            .field("output_schema", &self.output_schema)
            .finish_non_exhaustive()
    }
}

// Implement PartialEq manually
impl PartialEq for CopyFrom {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.source_url == other.source_url
            && self.columns == other.columns
    }
}

// Implement Eq (no need for additional logic over PartialEq)
impl Eq for CopyFrom {}

// Manual implementation needed because of `file_type` and `options` fields.
// Comparison excludes these fields.
impl PartialOrd for CopyFrom {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.table_name.partial_cmp(&other.table_name) {
            Some(Ordering::Equal) => match self.source_url.partial_cmp(&other.source_url) {
                Some(Ordering::Equal) => self.columns.partial_cmp(&other.columns),
                cmp => cmp,
            },
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

// Implement Hash manually
impl Hash for CopyFrom {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.source_url.hash(state);
        self.columns.hash(state);
    }
}

impl CopyFrom {
    pub fn new(
        table_name: TableReference,
        source_url: String,
        columns: Vec<String>,
        file_type: Arc<dyn FileType>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            table_name,
            source_url,
            columns,
            file_type,
            options,
            // The output schema is always a single column "count" with the number of rows copied
            output_schema: make_count_schema(),
        }
    }
}

pub(crate) fn make_count_schema() -> DFSchemaRef {
    Arc::new(
        Schema::new(vec![Field::new("count", DataType::UInt64, false)])
            .try_into()
            .unwrap(),
    )
}
