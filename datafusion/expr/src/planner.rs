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

//! [`ContextProvider`] and [`ExprPlanner`] APIs to customize SQL query planning

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::expr::NullTreatment;

use crate::logical_plan::LogicalPlan;
use crate::{
    AggregateUDF, Expr, GetFieldAccess, ScalarUDF, SortExpr, TableSource, WindowFrame,
    WindowFunctionDefinition, WindowUDF,
};
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion_common::datatype::DataTypeExt;
use datafusion_common::{
    DFSchema, DataFusionError, Result, TableReference, config::ConfigOptions,
    file_options::file_type::FileType, not_impl_err,
};

use sqlparser::ast::{Expr as SQLExpr, Ident, ObjectName, TableAlias, TableFactor};

/// The output of a set-returning function call as lists: one list-valued
/// expression per output column, holding the call's `n`th row at position `n`
/// of every column. Unnesting the columns together yields the call's rows;
/// a column that runs short pads with NULL, which is what lets `ROWS FROM`
/// zip several calls into one row set.
#[derive(Debug, Clone)]
pub struct SetReturningColumns {
    /// `(output column name, list-valued expression)` in output order.
    pub columns: Vec<(String, Expr)>,
}

/// Physical sampling strategy requested by a SQL `TABLESAMPLE` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableSampleMethod {
    Bernoulli,
    System,
}

/// Provides the `SQL` query planner meta-data about tables and
/// functions referenced in SQL statements, without a direct dependency on the
/// `datafusion` Catalog structures such as [`TableProvider`]
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
pub trait ContextProvider {
    /// Returns a table by reference, if it exists
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;

    /// Build the engine-owned predicate for a physical table sample. The SQL
    /// planner owns clause parsing and expression typing; the table provider
    /// owns stable row identity and the sampling algorithm.
    fn plan_table_sample(
        &self,
        _name: &TableReference,
        _method: TableSampleMethod,
        _percentage: Expr,
        _repeatable: Option<Expr>,
    ) -> Result<Expr> {
        not_impl_err!("TABLESAMPLE is not supported by this table provider")
    }

    /// Error identity for attaching `TABLESAMPLE` to a CTE or another
    /// non-physical relation.
    fn table_sample_source_error(&self, name: &TableReference) -> DataFusionError {
        DataFusionError::Plan(format!(
            "TABLESAMPLE clause can only be applied to physical tables, not \"{name}\""
        ))
    }

    /// Resolve an unquoted SQL column identifier against a provider-owned
    /// schema when its physical field spelling is not the dialect's canonical
    /// spelling.
    ///
    /// The default preserves DataFusion's exact identifier semantics. Catalog
    /// providers can opt in for narrowly scoped virtual schemas whose field
    /// names are fixed by another database surface.
    fn resolve_unquoted_column_name(
        &self,
        _identifier: &Ident,
        _schema: &DFSchema,
    ) -> Option<String> {
        None
    }

    /// Return the type of a file based on its extension (e.g. `.parquet`)
    ///
    /// This is used to plan `COPY` statements
    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        not_impl_err!("Registered file types are not supported")
    }

    /// Getter for a table function
    fn get_table_function_source(
        &self,
        _name: &str,
        _args: Vec<Expr>,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Table Functions are not supported")
    }

    /// Whether `name` is a set-returning function: a call that produces rows
    /// rather than one value, wherever it is written. The planner asks by
    /// name alone, before any argument is planned.
    fn is_set_returning_function(&self, _name: &str) -> bool {
        false
    }

    /// The row expansion of a set-returning function call, when the function
    /// expands as lists. The arguments were planned against `schema`.
    /// `column_definitions` is the call's `AS (name type, ...)` list — `Some`
    /// (possibly empty) for a FROM item, `None` for a call in expression
    /// position, where no such list can be written. `Ok(None)` leaves the
    /// call to [`Self::get_table_function_source`].
    fn plan_set_returning_function(
        &self,
        _name: &str,
        _args: &[Expr],
        _schema: &DFSchema,
        _column_definitions: Option<&[FieldRef]>,
    ) -> Result<Option<SetReturningColumns>> {
        Ok(None)
    }

    /// Provides an intermediate table that is used to store the results of a CTE during execution
    ///
    /// CTE stands for "Common Table Expression"
    ///
    /// # Notes
    /// We don't directly implement this in [`SqlToRel`] as implementing this function
    /// often requires access to a table that contains
    /// execution-related types that can't be a direct dependency
    /// of the sql crate (for example [`CteWorkTable`]).
    ///
    /// The [`ContextProvider`] provides a way to "hide" this dependency.
    ///
    /// [`SqlToRel`]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html
    /// [`CteWorkTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/cte_worktable/struct.CteWorkTable.html
    fn create_cte_work_table(
        &self,
        _name: &str,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Recursive CTE is not implemented")
    }

    /// Return [`ExprPlanner`] extensions for planning expressions
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
    }

    /// Return [`RelationPlanner`] extensions for planning table factors

    fn get_relation_planners(&self) -> &[Arc<dyn RelationPlanner>] {
        &[]
    }

    /// Gantry: resolve a DML statement's target when it names an
    /// automatically updatable view whose write retargets onto its base
    /// relation. `Ok(None)` when the target is not such a view — including
    /// when an INSTEAD OF trigger or an INSTEAD rule owns the write, in
    /// which case the view stays the target. `Err` when the target is a
    /// view that cannot carry the write at all; the provider owns that
    /// error's identity.
    fn resolve_dml_view_target(
        &self,
        _table: &ObjectName,
        _event: DmlViewEvent,
    ) -> Result<Option<ViewDmlTarget>> {
        Ok(None)
    }

    /// Gantry: the generated columns of a DML target relation, or `None`
    /// when it has none. Drives write rejection and UPDATE-time
    /// recomputation in DML planning.
    fn dml_generated_columns(
        &self,
        _table: &ObjectName,
    ) -> Result<Option<DmlGeneratedColumns>> {
        Ok(None)
    }

    /// Gantry: construct the host's error for a statement that supplies a
    /// non-DEFAULT value for a generated column.
    fn generated_column_write_error(
        &self,
        table_name: &str,
        column: &str,
    ) -> datafusion_common::DataFusionError {
        datafusion_common::DataFusionError::Plan(format!(
            "cannot insert a non-DEFAULT value into column \"{column}\" of relation \
             \"{table_name}\""
        ))
    }

    /// Gantry: construct the host's error for a statement that writes a
    /// non-DEFAULT value to a `GENERATED ALWAYS AS IDENTITY` column without
    /// `OVERRIDING SYSTEM VALUE` (`insert`), or updates one to anything but
    /// DEFAULT (`!insert`).
    fn identity_column_write_error(
        &self,
        _table_name: &str,
        column: &str,
        insert: bool,
    ) -> datafusion_common::DataFusionError {
        datafusion_common::DataFusionError::Plan(if insert {
            format!("cannot insert a non-DEFAULT value into column \"{column}\"")
        } else {
            format!("column \"{column}\" can only be updated to DEFAULT")
        })
    }

    /// Gantry: adapt one INSERT source value for its target column before
    /// planning — canonicalizing a flexible timestamp text literal, or
    /// wrapping a value in a dialect conversion. `None` leaves the value as
    /// written.
    fn adapt_insert_value(&self, _value: &SQLExpr, _target: &Field) -> Option<SQLExpr> {
        None
    }

    /// Gantry: construct the host's error for a view-write failure the SQL
    /// planner detects while retargeting a statement onto the view's base
    /// relation.
    fn dml_view_error(
        &self,
        error: ViewDmlError<'_>,
    ) -> datafusion_common::DataFusionError {
        match error {
            ViewDmlError::ColumnNotUpdatable {
                verb,
                view_name,
                column,
            } => datafusion_common::DataFusionError::Plan(format!(
                "cannot {verb} column \"{column}\" of view \"{view_name}\""
            )),
            ViewDmlError::MergeNotSupported { view_name } => {
                datafusion_common::DataFusionError::Plan(format!(
                    "cannot merge into view \"{view_name}\""
                ))
            }
        }
    }

    /// Return [`TypePlanner`] extensions for planning data types

    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        None
    }

    /// Return the scalar function with a given name, if any
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;

    /// Return the aggregate function with a given name, if any
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;

    /// Return the window function with a given name, if any
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>>;

    /// Return the system/user-defined variable type, if any
    ///
    /// A user defined variable is typically accessed via `@var_name`
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType>;

    /// Return metadata about a system/user-defined variable, if any.
    ///
    /// By default, this wraps [`Self::get_variable_type`] in an Arrow [`Field`]
    /// with nullable set to `true` and no metadata. Implementations that can
    /// provide richer information (such as nullability or extension metadata)
    /// should override this method.
    fn get_variable_field(&self, variable_names: &[String]) -> Option<FieldRef> {
        self.get_variable_type(variable_names)
            .map(|data_type| data_type.into_nullable_field_ref())
    }

    /// Return overall configuration options
    fn options(&self) -> &ConfigOptions;

    /// Return all scalar function names
    fn udf_names(&self) -> Vec<String>;

    /// Return all aggregate function names
    fn udaf_names(&self) -> Vec<String>;

    /// Return all window function names
    fn udwf_names(&self) -> Vec<String>;
}

/// Customize planning of SQL AST expressions to [`Expr`]s
pub trait ExprPlanner: Debug + Send + Sync {
    /// Plan the binary operation between two expressions, returns original
    /// BinaryExpr if not possible
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan the field access expression, such as `foo.bar`
    ///
    /// returns original [`RawFieldAccessExpr`] if not possible
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an array literal, such as `[1, 2, 3]`
    ///
    /// Returns original expression arguments if not possible
    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(exprs))
    }

    /// Plan a `POSITION` expression, such as `POSITION(<expr> in <expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_position(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan a dictionary literal, such as `{ key: value, ...}`
    ///
    /// Returns original expression arguments if not possible
    fn plan_dictionary_literal(
        &self,
        expr: RawDictionaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawDictionaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an extract expression, such as`EXTRACT(month FROM foo)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan an substring expression, such as `SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])`
    ///
    /// Returns original expression arguments if not possible
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a struct literal, such as  `{'field1' : expr1, 'field2' : expr2, ...}`
    ///
    /// This function takes a vector of expressions and a boolean flag
    /// indicating whether the struct uses the optional name
    ///
    /// Returns the original input expressions if planning is not possible.
    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        _is_named_struct: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans an overlay expression, such as `overlay(str PLACING substr FROM pos [FOR count])`
    ///
    /// Returns original expression arguments if not possible
    fn plan_overlay(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a `make_map` expression, such as `make_map(key1, value1, key2, value2, ...)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_make_map(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan a cast expression, such as `CAST(expr AS type)` or `expr::type`
    ///
    /// Returns original cast payload if not possible
    fn plan_cast(
        &self,
        expr: RawCastExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawCastExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an `INTERVAL` literal or expression, such as
    /// `INTERVAL '1 2:03' DAY TO SECOND(2)`; returns the original
    /// expression if not possible.
    fn plan_interval(
        &self,
        expr: RawIntervalExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawIntervalExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans compound identifier such as `db.schema.table` for non-empty nested names
    ///
    /// # Note:
    /// Currently compound identifier for outer query schema is not supported.
    ///
    /// Returns original expression if not possible
    fn plan_compound_identifier(
        &self,
        _field: &Field,
        _qualifier: Option<&TableReference>,
        _nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        not_impl_err!(
            "Default planner compound identifier hasn't been implemented for ExprPlanner"
        )
    }

    /// Plans `ANY` expression, such as `expr = ANY(array_expr)`
    ///
    /// Returns origin binary expression if not possible
    fn plan_any(&self, expr: RawBinaryExpr) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans an assignment through a subscript or field path of a column:
    /// `UPDATE ... SET col[i] = v`, `SET col.f = v`, `INSERT (col[lo:hi])
    /// VALUES (v)`. The result is the column's whole new value.
    ///
    /// Returns the original target if not possible
    fn plan_assignment_target(
        &self,
        target: RawAssignmentTarget,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawAssignmentTarget>> {
        Ok(PlannerResult::Original(target))
    }

    /// Plans `(expr).*`: the fields of a row-valued expression as separate
    /// projection entries.
    ///
    /// Returns `None` when the expression is not a row value this planner
    /// understands.
    fn plan_row_wildcard(
        &self,
        _expr: &Expr,
        _schema: &DFSchema,
    ) -> Result<Option<Vec<Expr>>> {
        Ok(None)
    }

    /// Plans aggregate functions, such as `COUNT(<expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_aggregate(
        &self,
        expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans window functions, such as `COUNT(<expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_window(&self, expr: RawWindowExpr) -> Result<PlannerResult<RawWindowExpr>> {
        Ok(PlannerResult::Original(expr))
    }
}

/// An operator with two arguments to plan
///
/// Note `left` and `right` are DataFusion [`Expr`]s but the `op` is the SQL AST
/// operator.
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawBinaryExpr {
    pub op: sqlparser::ast::BinaryOperator,
    pub left: Expr,
    pub right: Expr,
}

/// An expression with GetFieldAccess to plan
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawFieldAccessExpr {
    pub field_access: GetFieldAccess,
    pub expr: Expr,
}

/// One step of an assignment target's path below its column: `col[i]`,
/// `col[lo:hi]`, `col.field`.
#[derive(Debug, Clone)]
pub enum AssignmentStep {
    Index(Expr),
    Slice {
        lower: Option<Expr>,
        upper: Option<Expr>,
    },
    Field(String),
}

/// An assignment through a subscript or field path to plan
///
/// This structure is used by [`ExprPlanner`] to plan `SET col[i] = v`-style
/// assignments, whose result is the column's whole new value.
#[derive(Debug, Clone)]
pub struct RawAssignmentTarget {
    /// The column's value before this assignment: the stored column, a NULL
    /// of the column's type for an INSERT, or the result of an earlier
    /// assignment to the same column in the same statement.
    pub base: Expr,
    /// The target column, with its declared type and metadata.
    pub column: FieldRef,
    pub path: Vec<AssignmentStep>,
    pub value: Expr,
}

/// A cast expression to plan
///
/// This structure is used by [`ExprPlanner`] to plan casts with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawCastExpr {
    pub cast_kind: sqlparser::ast::CastKind,
    pub expr: Expr,
    pub data_type: DataType,
    pub sql_data_type: sqlparser::ast::DataType,
    pub format: Option<sqlparser::ast::CastFormat>,
}

/// An `INTERVAL` expression to plan: the value (a text or numeric literal,
/// or any expression), the SQL field qualifier and precisions, and whether
/// the literal was negated.
///
/// This structure is used by [`ExprPlanner`] to plan intervals with
/// custom semantics.
#[derive(Debug, Clone)]
pub struct RawIntervalExpr {
    pub value: Expr,
    pub leading_field: Option<sqlparser::ast::DateTimeField>,
    pub last_field: Option<sqlparser::ast::DateTimeField>,
    pub leading_precision: Option<u64>,
    pub fractional_seconds_precision: Option<u64>,
    pub negative: bool,
}

/// A Dictionary literal expression `{ key: value, ...}`
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawDictionaryExpr {
    pub keys: Vec<Expr>,
    pub values: Vec<Expr>,
}

/// This structure is used by `AggregateFunctionPlanner` to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawAggregateExpr {
    pub func: Arc<AggregateUDF>,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub order_by: Vec<SortExpr>,
    pub null_treatment: Option<NullTreatment>,
}

/// This structure is used by `WindowFunctionPlanner` to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawWindowExpr {
    pub func_def: WindowFunctionDefinition,
    pub args: Vec<Expr>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortExpr>,
    pub window_frame: WindowFrame,
    pub filter: Option<Box<Expr>>,
    pub null_treatment: Option<NullTreatment>,
    pub distinct: bool,
}

/// Result of planning a raw expr with [`ExprPlanner`]
#[derive(Debug, Clone)]
pub enum PlannerResult<T> {
    /// The raw expression was successfully planned as a new [`Expr`]
    Planned(Expr),
    /// The raw expression could not be planned, and is returned unmodified
    Original(T),
}

/// The DML statement kind a view-write resolution is asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlViewEvent {
    /// `INSERT INTO view ...`
    Insert,
    /// `UPDATE view SET ...`
    Update,
    /// `DELETE FROM view ...`
    Delete,
    /// `MERGE INTO view ...`
    Merge,
}

/// How a write against an automatically updatable view retargets onto the
/// view's base relation. Produced by
/// [`ContextProvider::resolve_dml_view_target`]; consumed by the SQL
/// planner's DML planning, which performs the retarget in place of any
/// post-planning rewrite.
#[derive(Debug, Clone)]
pub struct ViewDmlTarget {
    /// The base relation the write lands on.
    pub base_relation: ObjectName,
    /// Per view output column in order, the base column backing it. `None`
    /// marks a computed column, which is readable but not writable.
    pub columns: Vec<(String, Option<String>)>,
    /// The view stack's row restrictions, written against the base
    /// relation's namespace, outermost first. An UPDATE or DELETE through
    /// the view may only touch rows the view shows.
    pub row_restrictions: Vec<SQLExpr>,
    /// The outermost view whose check option the written row must satisfy,
    /// recorded on the DML plan node.
    pub check_option_view: Option<TableReference>,
    /// The view's bare name, kept reachable as a qualifier where the
    /// statement uses it and named in errors.
    pub view_name: String,
    /// Column defaults attached to the view itself (`ALTER VIEW ... ALTER
    /// COLUMN ... SET DEFAULT`), which override the base relation's. Each
    /// expression is parsed under its owning grammar by the host. An INSERT
    /// with an explicit column list materializes omitted view-default
    /// columns; a column with no view default stays omitted so the base
    /// relation's own default remains authoritative.
    pub column_defaults: Vec<(String, SQLExpr)>,
}

impl ViewDmlTarget {
    /// The base column backing a view column: outer `None` when the name is
    /// not a view column at all, `Some(None)` when the column is computed.
    pub fn base_column(&self, view_column: &str) -> Option<Option<&str>> {
        self.columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(view_column))
            .map(|(_, base)| base.as_deref())
    }

    /// True when every view column passes through to a same-named base
    /// column.
    pub fn columns_are_passthrough(&self) -> bool {
        self.columns.iter().all(|(name, base)| {
            base.as_deref()
                .is_some_and(|base| base.eq_ignore_ascii_case(name))
        })
    }
}

/// A DML target relation's generated columns, resolved by
/// [`ContextProvider::dml_generated_columns`]. A generated column's value is
/// its generation expression's, so a statement supplying any other value is
/// refused, and an UPDATE recomputes every stored generated column as part
/// of the statement that changes its inputs.
#[derive(Debug, Clone)]
pub struct DmlGeneratedColumns {
    /// Every generated column name; writing one with a non-DEFAULT value is
    /// refused.
    pub columns: Vec<String>,
    /// For stored generated columns, the generation expression parsed under
    /// its owning grammar by the host.
    pub stored_expressions: Vec<(String, SQLExpr)>,
    /// The relation's full positional column list, for rejecting a
    /// positional VALUES row that reaches a generated column.
    pub positional_columns: Vec<String>,
    /// The relation's bare name, for error labels.
    pub table_name: String,
    /// `GENERATED ALWAYS AS IDENTITY` columns: they take only DEFAULT unless
    /// the statement says `OVERRIDING ... VALUE` (PostgreSQL 428C9).
    pub identity_always: Vec<String>,
    /// Every identity column; `OVERRIDING USER VALUE` discards what the
    /// statement wrote for these in favor of the sequence's value.
    pub identity: Vec<String>,
    /// For `SET col = DEFAULT` on a sequence-backed column, the expression
    /// that draws the sequence, parsed under its owning grammar by the host —
    /// the resolution the column's absent planner default cannot supply.
    pub update_default_overrides: Vec<(String, SQLExpr)>,
}

/// A view-write failure the SQL planner detects while retargeting; handed to
/// [`ContextProvider::dml_view_error`] so the host owns the error identity.
#[derive(Debug, Clone, Copy)]
pub enum ViewDmlError<'a> {
    /// The statement writes a view column no base column backs.
    ColumnNotUpdatable {
        /// The verb to name in the error, e.g. `"insert into"`.
        verb: &'a str,
        /// The view's bare name.
        view_name: &'a str,
        /// The column the statement writes.
        column: &'a str,
    },
    /// MERGE on a view that renames columns or restricts rows.
    MergeNotSupported {
        /// The view's bare name.
        view_name: &'a str,
    },
}

/// Result of planning a relation with [`RelationPlanner`]

#[derive(Debug, Clone)]
pub struct PlannedRelation {
    /// The logical plan for the relation
    pub plan: LogicalPlan,
    /// Optional table alias for the relation
    pub alias: Option<TableAlias>,
}

impl PlannedRelation {
    /// Create a new `PlannedRelation` with the given plan and alias
    pub fn new(plan: LogicalPlan, alias: Option<TableAlias>) -> Self {
        Self { plan, alias }
    }
}

/// Result of attempting to plan a relation with extension planners

#[derive(Debug)]
pub enum RelationPlanning {
    /// The relation was successfully planned by an extension planner
    Planned(PlannedRelation),
    /// No extension planner handled the relation, return it for default processing
    Original(TableFactor),
}

/// Customize planning SQL table factors to [`LogicalPlan`]s.

pub trait RelationPlanner: Debug + Send + Sync {
    /// Plan a table factor into a [`LogicalPlan`].
    ///
    /// Returning [`RelationPlanning::Planned`] short-circuits further planning and uses the
    /// provided plan. Returning [`RelationPlanning::Original`] allows the next registered planner,
    /// or DataFusion's default logic, to handle the relation.
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning>;
}

/// Provides utilities for relation planners to interact with DataFusion's SQL
/// planner.
///
/// This trait provides SQL planning utilities specific to relation planning,
/// such as converting SQL expressions to logical expressions and normalizing
/// identifiers. It uses composition to provide access to session context via
/// [`ContextProvider`].

pub trait RelationPlannerContext {
    /// Provides access to the underlying context provider for reading session
    /// configuration, accessing tables, functions, and other metadata.
    fn context_provider(&self) -> &dyn ContextProvider;

    /// Plans the specified relation through the full planner pipeline, starting
    /// from the first registered relation planner.
    fn plan(&mut self, relation: TableFactor) -> Result<LogicalPlan>;

    /// Converts a SQL expression into a logical expression using the current
    /// planner context.
    fn sql_to_expr(&mut self, expr: SQLExpr, schema: &DFSchema) -> Result<Expr>;

    /// Converts a SQL expression into a logical expression without DataFusion
    /// rewrites.
    fn sql_expr_to_logical_expr(
        &mut self,
        expr: SQLExpr,
        schema: &DFSchema,
    ) -> Result<Expr>;

    /// Normalizes an identifier according to session settings.
    fn normalize_ident(&self, ident: Ident) -> String;

    /// Normalizes a SQL object name into a [`TableReference`].
    fn object_name_to_table_reference(&self, name: ObjectName) -> Result<TableReference>;
}

/// Customize planning SQL types to DataFusion (Arrow) types.

pub trait TypePlanner: Debug + Send + Sync {
    /// Plan SQL [`sqlparser::ast::DataType`] to DataFusion [`DataType`]
    ///
    /// Returns None if not possible
    fn plan_type(
        &self,
        _sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<DataType>> {
        Ok(None)
    }

    /// Return additional metadata to attach to the Arrow [`Field`] for
    /// the given SQL type. Returns `None` when no extra metadata is needed.
    fn plan_field_metadata(
        &self,
        _sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<HashMap<String, String>>> {
        Ok(None)
    }

    /// Return the type declaration's default nullability, when it carries one.
    /// Column constraints are applied on top of this value by the SQL planner.
    /// Most SQL types have no declaration-level nullability and therefore use
    /// the default nullable behavior.
    fn plan_field_nullable(
        &self,
        _sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<bool>> {
        Ok(None)
    }
}
