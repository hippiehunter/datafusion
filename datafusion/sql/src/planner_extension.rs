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

//! Extension APIs for customizing SQL AST to logical-plan lowering.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion_common::datatype::DataTypeExt;
use datafusion_common::{
    Constraints, DFSchema, DataFusionError, Result, TableReference,
    config::ConfigOptions, file_options::file_type::FileType, not_impl_err,
};
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{
    AggregateUDF, BoundSqlExpression, Expr, GetFieldAccess, ScalarUDF, SortExpr,
    TableSource, WindowFrame, WindowFunctionDefinition, WindowUDF,
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

/// Properties requested by one `CREATE TABLE ... LIKE` clause.
///
/// The SQL planner owns the clause syntax and collapses its ordered
/// `INCLUDING` / `EXCLUDING` items into this semantic request before asking
/// the provider for a source shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct CreateTableLikeOptions {
    pub defaults: bool,
    pub constraints: bool,
    pub indexes: bool,
    pub identity: bool,
    pub generated: bool,
    pub comments: bool,
    pub storage: bool,
}

/// Provider-resolved input for one `CREATE TABLE ... LIKE` clause.
///
/// This deliberately contains only DataFusion and Arrow semantic values. An
/// embedding catalog can retain richer, engine-specific column properties in
/// Arrow field metadata without making either the SQL or expression crate
/// depend on the embedding engine's types.
#[derive(Debug, Clone)]
pub struct CreateTableLikeSource {
    /// Ordered columns contributed by the source relation or composite type.
    pub schema: SchemaRef,
    /// Included keys and checks, expressed against `schema`.
    pub constraints: Constraints,
    /// Included, already-planned defaults keyed by column name.
    pub column_defaults: Vec<(String, Expr)>,
    /// Included CHECK predicates, already bound against `schema`, in the same
    /// order as the CHECK entries in [`Self::constraints`].
    pub check_expressions: Vec<BoundSqlExpression>,
    /// Included generated-column expressions, already bound against
    /// `schema`, keyed by their copied column name.
    pub generated_expressions: Vec<(String, BoundSqlExpression)>,
}

/// Provides the `SQL` query planner meta-data about tables and
/// functions referenced in SQL statements, without a direct dependency on the
/// `datafusion` Catalog structures such as [`TableProvider`]
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
pub trait ContextProvider {
    /// Resolve the durable identity of a relation before the planner records
    /// it in a [`TableScan`]. Most providers use the SQL-written reference
    /// verbatim. Providers that plan stored objects can qualify a bare name
    /// under the stored object's namespace so later optimizer/executor phases
    /// do not accidentally re-resolve it under the caller's namespace.
    fn resolve_table_reference(&self, name: TableReference) -> Result<TableReference> {
        Ok(name)
    }

    /// Returns a table by reference, if it exists
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;

    /// Resolve the semantic row shape and selected properties copied by one
    /// `CREATE TABLE ... LIKE` clause.
    ///
    /// Keeping this at the provider boundary lets the SQL planner combine the
    /// returned fields with user-authored columns before it resolves table
    /// constraints, without reconstructing catalog state as parser AST.
    fn get_create_table_like_source(
        &self,
        _name: TableReference,
        _options: CreateTableLikeOptions,
    ) -> Result<CreateTableLikeSource> {
        not_impl_err!("CREATE TABLE LIKE is not supported by this table provider")
    }

    /// Provider-specific error identity for a duplicate column introduced by
    /// `CREATE TABLE ... LIKE`. The default remains a normal planning error;
    /// database embeddings can preserve their wire-level error classification.
    fn create_table_like_duplicate_column_error(&self, name: &str) -> DataFusionError {
        DataFusionError::Plan(format!("column \"{name}\" specified more than once"))
    }

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

    /// Getter for a table function whose anonymous record shape is supplied
    /// by the call site's `AS (name type, ...)` column definition list.
    /// Providers that do not need the definitions retain the ordinary table
    /// function behavior.
    fn get_table_function_source_with_columns(
        &self,
        name: &str,
        args: Vec<Expr>,
        _column_definitions: &[FieldRef],
    ) -> Result<Arc<dyn TableSource>> {
        self.get_table_function_source(name, args)
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
    /// call to [`Self::get_table_function_source_with_columns`].
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

    /// Whether a view remains the DML target because an INSTEAD OF ROW
    /// trigger owns the event. UPDATE planning preserves the original row
    /// beside the projected NEW row when this is true.
    fn dml_view_uses_instead_of_trigger(
        &self,
        _table: &ObjectName,
        _event: DmlViewEvent,
    ) -> bool {
        false
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

    /// Provider-owned assignment coercion for semantic SQL types whose Arrow
    /// carrier alone cannot identify the conversion (for example a user
    /// `CREATE CAST ... AS ASSIGNMENT` into an enum). `Ok(None)` asks the SQL
    /// planner to use its ordinary Arrow coercion.
    fn plan_assignment_coercion(
        &self,
        _expr: &Expr,
        _target: &FieldRef,
        _schema: &DFSchema,
    ) -> Result<Option<Expr>> {
        Ok(None)
    }

    /// Gantry: construct the host's error for a statement that supplies a
    /// non-DEFAULT value for a generated column.
    fn generated_column_write_error(
        &self,
        table_name: &str,
        column: &str,
    ) -> DataFusionError {
        DataFusionError::Plan(format!(
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
    ) -> DataFusionError {
        DataFusionError::Plan(if insert {
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
    fn dml_view_error(&self, error: ViewDmlError<'_>) -> DataFusionError {
        match error {
            ViewDmlError::ColumnNotUpdatable {
                verb,
                view_name,
                column,
            } => DataFusionError::Plan(format!(
                "cannot {verb} column \"{column}\" of view \"{view_name}\""
            )),
            ViewDmlError::MergeNotSupported { view_name } => {
                DataFusionError::Plan(format!("cannot merge into view \"{view_name}\""))
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

/// Standard SQL lowering for aggregate-call syntax that is not representable
/// as an ordinary argument list, such as `COUNT(*)` and `COUNT()`.
#[derive(Debug)]
pub struct AggregateFunctionPlanner;

impl ExprPlanner for AggregateFunctionPlanner {
    fn plan_aggregate(
        &self,
        raw_expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        let RawAggregateExpr {
            func,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        } = raw_expr;

        let origin_expr = Expr::AggregateFunction(AggregateFunction {
            func,
            params: AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by,
                null_treatment,
            },
        });
        let saved_name = NamePreserver::new_for_projection().save(&origin_expr);

        let Expr::AggregateFunction(AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
        }) = origin_expr
        else {
            unreachable!()
        };
        let raw_expr = RawAggregateExpr {
            func,
            args,
            distinct,
            filter,
            order_by,
            null_treatment,
        };

        #[expect(deprecated)]
        if raw_expr.func.name() == "count"
            && (raw_expr.args.len() == 1
                && matches!(raw_expr.args[0], Expr::Wildcard { .. })
                || raw_expr.args.is_empty())
        {
            let RawAggregateExpr {
                func,
                args: _,
                distinct,
                filter,
                order_by,
                null_treatment,
            } = raw_expr;
            let new_expr = Expr::AggregateFunction(AggregateFunction::new_udf(
                func,
                vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
                distinct,
                filter,
                order_by,
                null_treatment,
            ));
            return Ok(PlannerResult::Planned(saved_name.restore(new_expr)));
        }

        Ok(PlannerResult::Original(raw_expr))
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
    /// relation's namespace, outermost first. These expressions were bound
    /// when the view definition entered the catalog; an UPDATE or DELETE
    /// through the view may only touch rows the view shows.
    pub row_restrictions: Vec<BoundSqlExpression>,
    /// The outermost view whose check option the written row must satisfy,
    /// recorded on the DML plan node.
    pub check_option_view: Option<TableReference>,
    /// The check-option subset of `row_restrictions`, already rewritten over
    /// the final base relation. They are already bound semantic values; the
    /// SQL planner only combines and stamps them on the DML plan.
    pub check_option_restrictions: Vec<BoundSqlExpression>,
    /// The view's bare name, kept reachable as a qualifier where the
    /// statement uses it and named in errors.
    pub view_name: String,
    /// Column defaults attached to the view itself (`ALTER VIEW ... ALTER
    /// COLUMN ... SET DEFAULT`), which override the base relation's. The host
    /// returns already-bound semantics; INSERT planning applies them directly
    /// to omitted columns without manufacturing or reparsing SQL syntax.
    pub column_defaults: Vec<(String, BoundSqlExpression)>,
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
    /// For stored generated columns, the parser-free expression bound by the
    /// host at CREATE/ALTER or explicit catalog restoration.
    pub stored_expressions: Vec<(String, BoundSqlExpression)>,
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
    /// For `SET col = DEFAULT` on a sequence-backed column, the already-bound
    /// expression that draws the sequence — the resolution the column's
    /// absent planner default cannot supply.
    pub update_default_overrides: Vec<(String, BoundSqlExpression)>,
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
