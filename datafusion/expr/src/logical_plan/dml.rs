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
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, Result, TableReference, internal_err,
};
/// Target specification for ON CONFLICT clauses.
///
/// Identifies which unique constraint should trigger the conflict handling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ConflictTarget {
    /// Column list: `ON CONFLICT (col_a, col_b)`
    Columns(Vec<String>),
    /// Named constraint: `ON CONFLICT ON CONSTRAINT constraint_name`
    OnConstraint(String),
    /// The unique index an inference clause — `ON CONFLICT (lower(col))
    /// WHERE predicate` — resolved to, named as the table source names it.
    Index(String),
}

use crate::expr::{AllExpr, AnyExpr, Exists, InSubquery, QuantifiedSource};
use crate::logical_plan::Subquery;
use crate::{BoundSqlExpression, Expr, LogicalPlan, TableSource};

/// A row-visibility predicate imposed by an automatically updatable view.
///
/// The SQL frontend binds the predicate while the view AST and catalog
/// provider are available. Logical optimizers and execution lowerers receive
/// only this parser-independent semantic value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct DmlCheckOption {
    /// The view named in a constraint-violation diagnostic.
    pub view_name: TableReference,
    /// The view-stack restriction expressed over the final base relation.
    pub predicate: BoundSqlExpression,
}

/// Non-default, parser-independent evaluation context for a `RETURNING`
/// projection.
///
/// Ordinary INSERT/UPDATE/DELETE projections need no context: the operation
/// identifies their row image. The enum deliberately represents only the two
/// exceptional layouts so a logical plan cannot carry a context with no
/// schema, a dual-image MERGE action column, or another meaningless
/// combination.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ReturningContext {
    /// PostgreSQL OLD/NEW evaluation. The schema contains the target row
    /// twice, before-image fields followed by after-image fields.
    DualImage { eval_schema: DFSchemaRef },
    /// MERGE evaluates against whichever image its applied action produced.
    /// `action_column` identifies the synthetic `merge_action` text field.
    MergeApplied {
        eval_schema: DFSchemaRef,
        action_column: usize,
    },
}

impl ReturningContext {
    pub fn eval_schema(&self) -> &DFSchemaRef {
        match self {
            Self::DualImage { eval_schema } | Self::MergeApplied { eval_schema, .. } => {
                eval_schema
            }
        }
    }
}

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

/// Whether a data-modifying statement reads its target relation's columns,
/// and so needs SELECT access to the target as PostgreSQL's parse analysis
/// records it: a WHERE, SET, RETURNING or ON CONFLICT expression that names a
/// target column, or an ON CONFLICT arbiter. Row-level security filters the
/// rows an UPDATE or DELETE reads, and checks the rows a write produces,
/// against the target's SELECT policies exactly when this is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TargetSelectRights {
    /// No expression of the statement reads a target column.
    NotRequired,
    /// Some expression of the statement reads a target column.
    Required,
}

impl TargetSelectRights {
    /// The rights evaluating `exprs` needs when the target's columns are named
    /// through `target`: required when any of them, or a subquery within
    /// them, names a target column. A column without a qualifier is the
    /// target's.
    pub fn reading<'a>(
        target: &TableReference,
        exprs: impl IntoIterator<Item = &'a Expr>,
    ) -> Result<Self> {
        Self::reading_columns(exprs, |column| {
            column
                .relation
                .as_ref()
                .is_none_or(|relation| relation.resolved_eq(target))
        })
    }

    /// The rights evaluating `exprs` needs when `is_target_column` tells a
    /// column of the target from any other: required when any of them, or a
    /// subquery within them, names a target column.
    pub fn reading_columns<'a>(
        exprs: impl IntoIterator<Item = &'a Expr>,
        is_target_column: impl Fn(&Column) -> bool,
    ) -> Result<Self> {
        for expr in exprs {
            if expr_reads_target(expr, &is_target_column)? {
                return Ok(Self::Required);
            }
        }
        Ok(Self::NotRequired)
    }

    /// The rights a statement needs when one part of it needs `self` and
    /// another needs `other`.
    pub fn and(self, other: Self) -> Self {
        self.max(other)
    }
}

fn expr_reads_target(
    expr: &Expr,
    is_target_column: &dyn Fn(&Column) -> bool,
) -> Result<bool> {
    let mut reads = false;
    expr.apply(|expr| {
        reads = match expr {
            Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => {
                is_target_column(column)
            }
            Expr::Exists(Exists { subquery, .. })
            | Expr::InSubquery(InSubquery { subquery, .. })
            | Expr::ScalarSubquery(subquery)
            | Expr::AnyExpr(AnyExpr {
                source: QuantifiedSource::Subquery(subquery),
                ..
            })
            | Expr::AllExpr(AllExpr {
                source: QuantifiedSource::Subquery(subquery),
                ..
            }) => subquery_reads_target(subquery, is_target_column)?,
            _ => false,
        };
        Ok(if reads {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(reads)
}

/// A subquery reads the target through its outer references, at any depth of
/// nesting; its own columns name the relations it reads itself.
fn subquery_reads_target(
    subquery: &Subquery,
    is_target_column: &dyn Fn(&Column) -> bool,
) -> Result<bool> {
    let mut reads = false;
    subquery.subquery.apply_with_subqueries(|plan| {
        plan.apply_expressions(|expr| {
            expr.apply(|expr| {
                reads = matches!(
                    expr,
                    Expr::OuterReferenceColumn(_, column) if is_target_column(column)
                );
                Ok(if reads {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            })
        })
    })?;
    Ok(reads)
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
    /// The columns the statement names as its targets: an INSERT's column
    /// list (e.g. `INSERT INTO t (a, b) ...`), or the columns an UPDATE's SET
    /// list assigns, in table order, self-assignments and `DEFAULT` included.
    /// An UPDATE's projection cannot say which columns it assigns, since
    /// `SET a = a` projects the stored column unchanged.
    pub target_columns: Option<Vec<String>>,
    /// Columns requested by a RETURNING clause
    pub returning_columns: Option<Vec<String>>,
    /// Expressions requested by a RETURNING clause
    pub returning_exprs: Option<Vec<Expr>>,
    /// Non-default row-image/evaluation semantics for `RETURNING`.
    pub returning_context: Option<ReturningContext>,
    /// OVERRIDING SYSTEM VALUE was specified (PostgreSQL identity columns)
    pub overriding_system_value: bool,
    /// Parser-free check-option obligation inherited while retargeting a write
    /// through an automatically updatable view.
    pub check_option: Option<DmlCheckOption>,
    /// Whether the statement's own expressions read the target's columns.
    pub target_select_rights: TargetSelectRights,
    /// The relations other than the target whose rows an UPDATE's or DELETE's
    /// input joins, each with the junk columns that carry the columns of its
    /// joined row the statement's recheck reads, and its presence column. A
    /// refreshed target row is requalified against these rows, as
    /// PostgreSQL's EvalPlanQual re-runs the plan with every other relation
    /// fixed at the row it joined. Empty for any other statement.
    pub carried: Vec<CarriedJunkColumns>,
}

/// One carried relation of a DML input: its junk columns in the input and its
/// presence column.
///
/// Each named column is a field of the DML input's schema whose metadata marks
/// it as a junk column ([`datafusion_common::JUNK_COLUMN_METADATA_KEY`]). The
/// presence column is a non-null Boolean that is `true` on a row the relation
/// joined and `false` on a row an outer join NULL-extended on its side.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct CarriedJunkColumns {
    pub relation: TableReference,
    /// Names of the junk columns in the DML input's schema.
    pub columns: Vec<String>,
    /// Name of the presence column in the DML input's schema.
    pub presence: String,
}

/// The name of the junk column that carries column `column` of the carried
/// relation numbered `ordinal`.
pub fn carried_junk_column_name(ordinal: usize, column: &str) -> String {
    format!("__carried_{ordinal}_{column}")
}

/// The name of the presence column of the carried relation numbered `ordinal`.
pub fn carried_presence_column_name(ordinal: usize) -> String {
    format!("__carried_{ordinal}_present")
}

/// The row an INSERT's, UPDATE's or DELETE's `RETURNING` reads, over the
/// statement's `input`: the input leads with the target's columns in table
/// order, holding the values written to them (or, for DELETE, read from
/// them), followed by any columns RETURNING reads from joined relations.
/// RETURNING reads the row as stored, so a target column has the column's own
/// field (its type and SQL identity), not the field of the expression
/// assigned to it; a joined column keeps its own.
pub fn written_row_schema(input: &DFSchema, target: &Schema) -> Result<DFSchema> {
    let fields = input
        .iter()
        .enumerate()
        .map(|(index, (qualifier, field))| {
            let Some(column) = target.fields().get(index) else {
                return Ok((qualifier.cloned(), Arc::clone(field)));
            };
            if column.name() != field.name() {
                return internal_err!(
                    "DML input column {index} is {}, not the target column {}",
                    field.name(),
                    column.name()
                );
            }
            Ok((qualifier.cloned(), Arc::clone(column)))
        })
        .collect::<Result<Vec<_>>>()?;
    DFSchema::new_with_metadata(fields, input.metadata().clone())
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
        self.returning_context.hash(state);
        self.overriding_system_value.hash(state);
        self.check_option.hash(state);
        self.target_select_rights.hash(state);
        self.carried.hash(state);
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
            && self.returning_context == other.returning_context
            && self.overriding_system_value == other.overriding_system_value
            && self.check_option == other.check_option
            && self.target_select_rights == other.target_select_rights
            && self.carried == other.carried
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
            .field("returning_context", &self.returning_context)
            .field("check_option", &self.check_option)
            .field("target_select_rights", &self.target_select_rights)
            .field("carried", &self.carried)
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
        target_select_rights: TargetSelectRights,
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
            returning_context: None,
            overriding_system_value: false,
            check_option: None,
            target_select_rights,
            carried: Vec::new(),
        }
    }

    /// Record the carried relations of an UPDATE's or DELETE's input (see
    /// [`Self::carried`]).
    pub fn with_carried(mut self, carried: Vec<CarriedJunkColumns>) -> Self {
        self.carried = carried;
        self
    }

    /// Set the statement's target columns (see [`Self::target_columns`]).
    pub fn with_target_columns(mut self, columns: Vec<String>) -> Self {
        self.target_columns = Some(columns);
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

    /// Set the explicit row-image/evaluation contract for `RETURNING`.
    pub fn with_returning_context(mut self, context: ReturningContext) -> Self {
        self.returning_context = Some(context);
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

    /// Record a bound updatable-view check option.
    pub fn with_check_option(mut self, check_option: DmlCheckOption) -> Self {
        self.check_option = Some(check_option);
        self
    }

    /// The row a `RETURNING` list evaluates over: the explicit context's,
    /// or the written row over the statement's input (see
    /// [`written_row_schema`]).
    pub fn returning_eval_schema(&self) -> Result<DFSchemaRef> {
        match &self.returning_context {
            Some(context) => Ok(Arc::clone(context.eval_schema())),
            None => {
                written_row_schema(self.input.schema(), self.target.schema().as_ref())
                    .map(Arc::new)
            }
        }
    }

    /// The statement's ON CONFLICT DO UPDATE action, if it has one.
    pub fn conflict_update(&self) -> Option<&DoUpdateAction> {
        match &self.op {
            WriteOp::Insert(InsertOp::WithConflictClause(OnConflict {
                action: OnConflictAction::DoUpdate(update),
                ..
            })) => Some(update),
            _ => None,
        }
    }

    /// The statement's ON CONFLICT DO UPDATE action, if it has one.
    pub fn conflict_update_mut(&mut self) -> Option<&mut DoUpdateAction> {
        match &mut self.op {
            WriteOp::Insert(InsertOp::WithConflictClause(OnConflict {
                action: OnConflictAction::DoUpdate(update),
                ..
            })) => Some(update),
            _ => None,
        }
    }

    /// The row an ON CONFLICT DO UPDATE action's assignments and condition
    /// read, or `None` without such an action: the conflicting target row
    /// under the name the action's expressions give it beside the proposed
    /// row as `excluded`. Both carry the target's columns; `excluded` also
    /// carries each sub-select the planner computed on the INSERT source
    /// (the input columns named with [`CONFLICT_SOURCE_COLUMN_PREFIX`]),
    /// which follow the target's columns in the input row.
    pub fn conflict_update_scope(&self) -> Result<Option<DFSchemaRef>> {
        let Some(update) = self.conflict_update() else {
            return Ok(None);
        };
        let target_row = match update.target_row_relation()? {
            Some(relation) => relation,
            None => TableReference::bare(self.table_name.table()),
        };
        let target = self.target.schema();
        let target_row = DFSchema::try_from_qualified_schema(target_row, &target)?;
        let excluded = Schema::new(
            target
                .fields()
                .iter()
                .chain(self.input.schema().fields().iter().filter(|field| {
                    field.name().starts_with(CONFLICT_SOURCE_COLUMN_PREFIX)
                }))
                .cloned()
                .collect::<Vec<_>>(),
        );
        let excluded = DFSchema::try_from_qualified_schema(
            TableReference::bare(EXCLUDED),
            &excluded,
        )?;
        Ok(Some(Arc::new(target_row.join(&excluded)?)))
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
    pub fn new(
        conflict_target: Option<ConflictTarget>,
        action: OnConflictAction,
    ) -> Self {
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
                        write!(f, "{col}")?;
                    }
                    write!(f, ")")?;
                }
                ConflictTarget::OnConstraint(name) => {
                    write!(f, " ON CONSTRAINT {name}")?;
                }
                ConflictTarget::Index(name) => {
                    write!(f, " ON INDEX {name}")?;
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
                    write!(f, " WHERE {selection}")?;
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

    /// The name the action's expressions give the conflicting target row:
    /// the one relation other than `excluded` their columns are qualified
    /// with, or `None` when they read no column of it.
    fn target_row_relation(&self) -> Result<Option<TableReference>> {
        let mut relation: Option<TableReference> = None;
        for expr in self
            .assignments
            .iter()
            .map(|assignment| &assignment.value)
            .chain(&self.selection)
        {
            expr.apply(|node| {
                let Expr::Column(Column {
                    relation: Some(qualifier),
                    ..
                }) = node
                else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                if qualifier.table() == EXCLUDED && qualifier.schema().is_none() {
                    return Ok(TreeNodeRecursion::Continue);
                }
                match &relation {
                    Some(found) if found != qualifier => internal_err!(
                        "ON CONFLICT DO UPDATE reads two target rows, {found} and {qualifier}"
                    ),
                    Some(_) => Ok(TreeNodeRecursion::Continue),
                    None => {
                        relation = Some(qualifier.clone());
                        Ok(TreeNodeRecursion::Continue)
                    }
                }
            })?;
        }
        Ok(relation)
    }
}

/// The name an ON CONFLICT DO UPDATE action gives the proposed row.
pub const EXCLUDED: &str = "excluded";

/// The name prefix of an INSERT source column the planner adds for a DO
/// UPDATE sub-select. The sub-select is computed on the source and the action
/// reads it back as a column of `excluded`.
pub const CONFLICT_SOURCE_COLUMN_PREFIX: &str = "__conflict_src_";

/// A column assignment in an ON CONFLICT DO UPDATE clause.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct ConflictAssignment {
    /// The target column to update.
    ///
    /// Tuple assignments are expanded into one assignment per column while
    /// lowering SQL, so retaining the parser's assignment-target enum here
    /// adds syntax without adding semantics.
    pub target: String,
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
            Some(Ordering::Equal) => match self.source_url.partial_cmp(&other.source_url)
            {
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
