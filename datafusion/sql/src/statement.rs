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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::parser::{
    CopyToSource, CopyToStatement, CreateExternalTable, DFParser, ExplainStatement,
    LexOrdering, ResetStatement, Statement as DFStatement,
    CopyFromStatement,
};
use crate::planner::{
    ContextProvider, PlannerContext, SqlToRel, object_name_to_qualifier,
};
use crate::utils::normalize_ident;

use arrow::datatypes::{Field, FieldRef, Fields};
use datafusion_common::error::_plan_err;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    Column, Constraint, Constraints, DFSchema, DFSchemaRef, DataFusionError, MatchType,
    ReferentialAction, Result, ScalarValue, SchemaError, SchemaReference, TableReference,
    ToDFSchema, exec_err, not_impl_err, plan_datafusion_err, plan_err, schema_err,
    unqualified_field_not_found,
};
use datafusion_expr::dml::{CopyFrom, CopyTo, InsertOp};
use datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use datafusion_expr::logical_plan::{DdlStatement, build_join_schema};
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    Analyze, AnalyzeTable, Call, CreateCatalog, CreateCatalogSchema,
    CreateExternalTable as PlanCreateExternalTable, CreateFunction, CreateFunctionBody,
    CreateIndex as PlanCreateIndex, CreateMemoryTable, CreateProcedure, CreateRole,
    CreateView, Deallocate, DescribeTable, DmlStatement, DropCatalogSchema, DropFunction,
    DropIndex, DropRole, DropSequence, DropTable, DropView, EmptyRelation, Execute, Explain,
    ExplainFormat, Expr, ExprSchemable, Filter, Grant, GrantRole, JoinType, LogicalPlan,
    LogicalPlanBuilder, Merge, MergeAction, MergeAssignment, MergeClause, MergeInsertExpr,
    MergeInsertKind, MergeUpdateExpr, OperateFunctionArg, PlanType, Prepare,
    ReleaseSavepoint, ResetVariable, Revoke, RevokeRole, RollbackToSavepoint, Savepoint, SetTransaction,
    SetVariable, SortExpr, Statement as PlanStatement, ToStringifiedPlan,
    TransactionAccessMode, TransactionConclusion, TransactionEnd,
    TransactionIsolationLevel, TransactionStart, TruncateTable, UseDatabase, Vacuum,
    Volatility, WriteOp, cast, col,
};
use datafusion_expr::logical_plan::psm::{ParameterMode, ProcedureArg};
use sqlparser::ast::{
    self, BeginTransactionKind, IndexColumn, IndexType, OrderByExpr, OrderByOptions, Set,
    ShowStatementIn, ShowStatementOptions, SqliteOnConflict, TableObject,
    UpdateTableFromKind, ValueWithSpan,
};
use sqlparser::ast::{
    Assignment, AssignmentTarget, ColumnDef, CreateIndex, CreateTable,
    CreateTableOptions, Delete, DescribeAlias, Expr as SQLExpr,
    ForeignKeyColumnOrPeriod, FromTable, Ident, Insert, ObjectName, ObjectType, Query,
    SchemaName, SetExpr, ShowCreateObject, ShowStatementFilter, SqlOption, Statement,
    TableConstraint, TableFactor, TableWithJoins, TransactionMode, UnaryOperator, Value,
};
use sqlparser::parser::ParserError::ParserError;

fn ident_to_string(ident: &Ident) -> String {
    normalize_ident(ident.to_owned())
}

fn object_name_to_string(object_name: &ObjectName) -> String {
    object_name
        .0
        .iter()
        .map(|object_name_part| {
            object_name_part
                .as_ident()
                // TODO: It might be better to return an error
                // than to silently use a default value.
                .map_or_else(String::new, ident_to_string)
        })
        .collect::<Vec<String>>()
        .join(".")
}

fn get_schema_name(schema_name: &SchemaName) -> String {
    match schema_name {
        SchemaName::Simple(schema_name) => object_name_to_string(schema_name),
        SchemaName::UnnamedAuthorization(auth) => ident_to_string(auth),
        SchemaName::NamedAuthorization(schema_name, auth) => format!(
            "{}.{}",
            object_name_to_string(schema_name),
            ident_to_string(auth)
        ),
    }
}

/// Construct `TableConstraint`(s) for the given columns by iterating over
/// `columns` and extracting individual inline constraint definitions.
fn calc_inline_constraints_from_columns(columns: &[ColumnDef]) -> Vec<TableConstraint> {
    use ast::{
        CheckConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint,
    };

    let mut constraints = vec![];
    for column in columns {
        for ast::ColumnOptionDef { name, option } in &column.options {
            match option {
                ast::ColumnOption::Unique(unique_constraint) => {
                    // Create a new UniqueConstraint with the column from this definition
                    let column_expr = IndexColumn {
                        column: OrderByExpr {
                            expr: SQLExpr::Identifier(column.name.clone()),
                            options: OrderByOptions {
                                asc: None,
                                nulls_first: None,
                            },
                            with_fill: None,
                        },
                        operator_class: None,
                    };
                    constraints.push(TableConstraint::Unique(UniqueConstraint {
                        name: name.clone(),
                        index_name: unique_constraint.index_name.clone(),
                        index_type_display: unique_constraint.index_type_display,
                        index_type: unique_constraint.index_type.clone(),
                        columns: vec![column_expr],
                        index_options: unique_constraint.index_options.clone(),
                        characteristics: unique_constraint.characteristics,
                        nulls_distinct: unique_constraint.nulls_distinct.clone(),
                    }));
                }
                ast::ColumnOption::PrimaryKey(pk_constraint) => {
                    let column_expr = IndexColumn {
                        column: OrderByExpr {
                            expr: SQLExpr::Identifier(column.name.clone()),
                            options: OrderByOptions {
                                asc: None,
                                nulls_first: None,
                            },
                            with_fill: None,
                        },
                        operator_class: None,
                    };
                    constraints.push(TableConstraint::PrimaryKey(PrimaryKeyConstraint {
                        name: name.clone(),
                        index_name: pk_constraint.index_name.clone(),
                        index_type: pk_constraint.index_type.clone(),
                        columns: vec![column_expr],
                        index_options: pk_constraint.index_options.clone(),
                        characteristics: pk_constraint.characteristics,
                        period_without_overlaps: None,
                    }));
                }
                ast::ColumnOption::ForeignKey(fk_constraint) => {
                    constraints.push(TableConstraint::ForeignKey(ForeignKeyConstraint {
                        name: name.clone(),
                        index_name: fk_constraint.index_name.clone(),
                        columns: vec![ForeignKeyColumnOrPeriod::Column(column.name.clone())],
                        foreign_table: fk_constraint.foreign_table.clone(),
                        referred_columns: fk_constraint.referred_columns.clone(),
                        on_delete: fk_constraint.on_delete.clone(),
                        on_update: fk_constraint.on_update.clone(),
                        match_kind: fk_constraint.match_kind.clone(),
                        characteristics: fk_constraint.characteristics,
                    }));
                }
                ast::ColumnOption::Check(check_constraint) => {
                    constraints.push(TableConstraint::Check(CheckConstraint {
                        name: name.clone(),
                        expr: check_constraint.expr.clone(),
                        enforced: check_constraint.enforced,
                    }));
                }
                // Other options are not constraint related.
                ast::ColumnOption::Default(_)
                | ast::ColumnOption::Null
                | ast::ColumnOption::NotNull
                | ast::ColumnOption::DialectSpecific(_)
                | ast::ColumnOption::CharacterSet(_)
                | ast::ColumnOption::Generated { .. }
                | ast::ColumnOption::Comment(_)
                | ast::ColumnOption::Options(_)
                | ast::ColumnOption::OnUpdate(_)
                | ast::ColumnOption::Materialized(_)
                | ast::ColumnOption::Ephemeral(_)
                | ast::ColumnOption::Identity(_)
                | ast::ColumnOption::OnConflict(_)
                | ast::ColumnOption::Policy(_)
                | ast::ColumnOption::Alias(_)
                | ast::ColumnOption::Srid(_)
                | ast::ColumnOption::Collation(_)
                | ast::ColumnOption::Invisible => {}
            }
        }
    }
    constraints
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(s),
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s),
            DFStatement::CopyTo(s) => self.copy_to_plan(s),
            DFStatement::CopyFrom(s) => self.copy_from_plan(s),
            DFStatement::Explain(ExplainStatement {
                verbose,
                analyze,
                format,
                statement,
            }) => self.explain_to_plan(verbose, analyze, format, *statement),
            DFStatement::Reset(statement) => self.reset_statement_to_plan(statement),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context_impl(
            statement,
            &mut PlannerContext::new(),
        )
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan_with_context(
        &self,
        statement: Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context_impl(statement, planner_context)
    }

    fn sql_statement_to_plan_with_context_impl(
        &self,
        statement: Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match statement {
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc, // only parse 'DESCRIBE table_name' or 'DESC table_name' and not 'EXPLAIN table_name'
                table_name,
                ..
            } => self.describe_table_to_plan(table_name),
            Statement::Explain {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc, // only parse 'DESCRIBE statement' or 'DESC statement' and not 'EXPLAIN statement'
                statement,
                ..
            } => match *statement {
                Statement::Query(query) => self.describe_query_to_plan(*query),
                _ => {
                    not_impl_err!("Describing statements other than SELECT not supported")
                }
            },
            Statement::Explain {
                verbose,
                statement,
                analyze,
                format,
                describe_alias: _,
                ..
            } => {
                let format = format.map(|format| format.to_string());
                let statement = DFStatement::Statement(statement);
                self.explain_to_plan(verbose, analyze, format, statement)
            }
            Statement::Query(query) => self.query_to_plan(*query, planner_context),
            Statement::ShowVariable { variable, .. } => self.show_variable_to_plan(&variable),
            Statement::Set(statement) => self.set_statement_to_plan(statement.inner),
            Statement::CreateTable(CreateTable {
                temporary,
                external,
                global,
                transient,
                volatile,
                file_format,
                location,
                query,
                name,
                columns,
                constraints,
                if_not_exists,
                or_replace,
                without_rowid,
                like,
                clone,
                comment,
                on_commit,
                on_cluster,
                primary_key,
                order_by,
                partition_by,
                cluster_by,
                strict,
                iceberg,
                inherits,
                table_options,
                dynamic,
                version,
                ..
            }) => {
                if external {
                    return not_impl_err!("External tables not supported")?;
                }
                if global.is_some() {
                    return not_impl_err!("Global tables not supported")?;
                }
                if transient {
                    return not_impl_err!("Transient tables not supported")?;
                }
                if volatile {
                    return not_impl_err!("Volatile tables not supported")?;
                }
                if file_format.is_some() {
                    return not_impl_err!("File format not supported")?;
                }
                if location.is_some() {
                    return not_impl_err!("Location not supported")?;
                }
                if without_rowid {
                    return not_impl_err!("Without rowid not supported")?;
                }
                if like.is_some() {
                    return not_impl_err!("Like not supported")?;
                }
                if clone.is_some() {
                    return not_impl_err!("Clone not supported")?;
                }
                if comment.is_some() {
                    return not_impl_err!("Comment not supported")?;
                }
                if on_commit.is_some() {
                    return not_impl_err!("On commit not supported")?;
                }
                if on_cluster.is_some() {
                    return not_impl_err!("On cluster not supported")?;
                }
                if primary_key.is_some() {
                    return not_impl_err!("Primary key not supported")?;
                }
                if order_by.is_some() {
                    return not_impl_err!("Order by not supported")?;
                }
                if partition_by.is_some() {
                    return not_impl_err!("Partition by not supported")?;
                }
                if cluster_by.is_some() {
                    return not_impl_err!("Cluster by not supported")?;
                }
                if strict {
                    return not_impl_err!("Strict not supported")?;
                }
                if iceberg {
                    return not_impl_err!("Iceberg not supported")?;
                }
                if inherits.is_some() {
                    return not_impl_err!("Table inheritance not supported")?;
                }
                if dynamic {
                    return not_impl_err!("Dynamic tables not supported")?;
                }
                if version.is_some() {
                    return not_impl_err!("Version not supported")?;
                }
                let storage_parameters = match table_options {
                    CreateTableOptions::None => BTreeMap::new(),
                    CreateTableOptions::With(options) => {
                        self.parse_storage_parameters(options)?
                    }
                    other => {
                        return not_impl_err!(
                            "CREATE TABLE options not supported: {other}"
                        )?;
                    }
                };
                // Merge inline constraints and existing constraints
                let mut all_constraints = constraints;
                let inline_constraints = calc_inline_constraints_from_columns(&columns);
                all_constraints.extend(inline_constraints);
                // Build column default values
                let column_defaults =
                    self.build_column_defaults(&columns, planner_context)?;

                let has_columns = !columns.is_empty();
                let schema = self.build_schema(columns)?.to_dfschema_ref()?;
                if has_columns {
                    planner_context.set_table_schema(Some(Arc::clone(&schema)));
                }

                match query {
                    Some(query) => {
                        let plan = self.query_to_plan(*query, planner_context)?;
                        let input_schema = plan.schema();

                        let plan = if has_columns {
                            if schema.fields().len() != input_schema.fields().len() {
                                return plan_err!(
                                    "Mismatch: {} columns specified, but result has {} columns",
                                    schema.fields().len(),
                                    input_schema.fields().len()
                                );
                            }
                            let input_fields = input_schema.fields();
                            let project_exprs = schema
                                .fields()
                                .iter()
                                .zip(input_fields)
                                .map(|(field, input_field)| {
                                    cast(
                                        col(input_field.name()),
                                        field.data_type().clone(),
                                    )
                                    .alias(field.name())
                                })
                                .collect::<Vec<_>>();

                            LogicalPlanBuilder::from(plan.clone())
                                .project(project_exprs)?
                                .build()?
                        } else {
                            plan
                        };

                        let constraints = self.new_constraint_from_table_constraints(
                            &all_constraints,
                            plan.schema(),
                        )?;

                        Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                            CreateMemoryTable {
                                name: self.object_name_to_table_reference(name)?,
                                constraints,
                                input: Arc::new(plan),
                                if_not_exists,
                                or_replace,
                                column_defaults,
                                temporary,
                                storage_parameters: storage_parameters.clone(),
                            },
                        )))
                    }

                    None => {
                        let plan = EmptyRelation {
                            produce_one_row: false,
                            schema,
                        };
                        let plan = LogicalPlan::EmptyRelation(plan);
                        let constraints = self.new_constraint_from_table_constraints(
                            &all_constraints,
                            plan.schema(),
                        )?;
                        Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                            CreateMemoryTable {
                                name: self.object_name_to_table_reference(name)?,
                                constraints,
                                input: Arc::new(plan),
                                if_not_exists,
                                or_replace,
                                column_defaults,
                                temporary,
                                storage_parameters,
                            },
                        )))
                    }
                }
            }
            Statement::CreateView(view) => {
                if view.materialized {
                    return not_impl_err!("Materialized views not supported")?;
                }
                if !view.cluster_by.is_empty() {
                    return not_impl_err!("Cluster by not supported")?;
                }
                if view.comment.is_some() {
                    return not_impl_err!("Comment not supported")?;
                }
                if view.with_no_schema_binding {
                    return not_impl_err!("With no schema binding not supported")?;
                }
                if view.to.is_some() {
                    return not_impl_err!("To not supported")?;
                }
                if view.or_replace && view.if_not_exists {
                    return plan_err!(
                        "CREATE VIEW cannot have both OR REPLACE and IF NOT EXISTS clauses"
                    );
                }

                // put the statement back together temporarily to get the SQL
                // string representation
                let sql = Statement::CreateView(view.clone()).to_string();

                let columns = view
                    .columns
                    .into_iter()
                    .map(|view_column_def| {
                        if let Some(options) = view_column_def.options {
                            plan_err!(
                                "Options not supported for view columns: {options:?}"
                            )
                        } else {
                            Ok(view_column_def.name)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                let mut plan =
                    self.query_to_plan(*view.query, &mut PlannerContext::new())?;
                plan = self.apply_expr_alias(plan, columns)?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                    name: self.object_name_to_table_reference(view.name)?,
                    input: Arc::new(plan),
                    or_replace: view.or_replace,
                    if_not_exists: view.if_not_exists,
                    definition: Some(sql),
                    temporary: view.temporary,
                })))
            }
            Statement::AlterTable(mut alter_table) => {
                if alter_table.on_cluster.is_some() {
                    return not_impl_err!("ALTER TABLE ... ON CLUSTER not supported");
                }
                if alter_table.table_type.is_some() {
                    return not_impl_err!("ALTER TABLE type modifiers not supported");
                }

                let mut operations = Vec::with_capacity(alter_table.operations.len());
                for operation in alter_table.operations {
                    use ast::AlterColumnOperation;
                    use ast::AlterTableOperation;

                    match operation {
                        AlterTableOperation::AddColumn { column_position, .. }
                            if column_position.is_some() =>
                        {
                            return not_impl_err!(
                                "ALTER TABLE ADD COLUMN position not supported"
                            );
                        }
                        AlterTableOperation::AddColumn { .. }
                        | AlterTableOperation::DropColumn { .. }
                        | AlterTableOperation::AddConstraint { .. }
                        | AlterTableOperation::DropConstraint { .. }
                        | AlterTableOperation::RenameColumn { .. }
                        | AlterTableOperation::RenameTable { .. } => {
                            operations.push(operation);
                        }
                        AlterTableOperation::AlterColumn { column_name, op } => {
                            match op {
                                AlterColumnOperation::SetNotNull
                                | AlterColumnOperation::DropNotNull
                                | AlterColumnOperation::SetDefault { .. }
                                | AlterColumnOperation::DropDefault
                                | AlterColumnOperation::SetDataType { .. } => {
                                    operations.push(AlterTableOperation::AlterColumn {
                                        column_name,
                                        op,
                                    });
                                }
                                _ => {
                                    return not_impl_err!(
                                        "ALTER TABLE ALTER COLUMN operation not supported: {op:?}"
                                    );
                                }
                            }
                        }
                        other => {
                            return not_impl_err!(
                                "ALTER TABLE operation not supported: {other:?}"
                            );
                        }
                    }
                }

                alter_table.operations = operations;

                Ok(LogicalPlan::Ddl(DdlStatement::AlterTable(alter_table)))
            }
            Statement::CreateDomain(create_domain) => {
                Ok(LogicalPlan::Ddl(DdlStatement::CreateDomain(create_domain)))
            }
            Statement::DropDomain(drop_domain) => {
                Ok(LogicalPlan::Ddl(DdlStatement::DropDomain(drop_domain)))
            }
            Statement::ShowCreate { obj_type, obj_name, .. } => match obj_type {
                ShowCreateObject::Table => self.show_create_table_to_plan(obj_name),
                _ => {
                    not_impl_err!("Only `SHOW CREATE TABLE  ...` statement is supported")
                }
            },
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => Ok(LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(
                CreateCatalogSchema {
                    schema_name: get_schema_name(&schema_name),
                    if_not_exists,
                    schema: Arc::new(DFSchema::empty()),
                },
            ))),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Ok(LogicalPlan::Ddl(DdlStatement::CreateCatalog(
                CreateCatalog {
                    catalog_name: object_name_to_string(&db_name),
                    if_not_exists,
                    schema: Arc::new(DFSchema::empty()),
                },
            ))),
            Statement::Drop {
                object_type,
                if_exists,
                mut names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
                ..
            } => {
                // We don't support multiple object names
                let object_name = match names.len() {
                    0 => {
                        return Err(
                            ParserError("Missing table name.".to_string()).into()
                        );
                    }
                    1 => names.pop().unwrap(),
                    _ => {
                        return Err(ParserError(
                            "Multiple objects not supported".to_string(),
                        )
                        .into());
                    }
                };
                let name = self.object_name_to_table_reference(object_name.clone())?;

                match object_type {
                    ObjectType::Table => {
                        Ok(LogicalPlan::Ddl(DdlStatement::DropTable(DropTable {
                            name,
                            if_exists,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))
                    }
                    ObjectType::View => {
                        Ok(LogicalPlan::Ddl(DdlStatement::DropView(DropView {
                            name,
                            if_exists,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))
                    }
                    ObjectType::Schema => {
                        let name = match name {
                            TableReference::Bare { table } => {
                                Ok(SchemaReference::Bare { schema: table })
                            }
                            TableReference::Partial { schema, table } => {
                                Ok(SchemaReference::Full {
                                    schema: table,
                                    catalog: schema,
                                })
                            }
                            TableReference::Full {
                                catalog: _,
                                schema: _,
                                table: _,
                            } => Err(ParserError(
                                "Invalid schema specifier (has 3 parts)".to_string(),
                            )),
                        }?;
                        Ok(LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(
                            DropCatalogSchema {
                                name,
                                if_exists,
                                cascade,
                                schema: DFSchemaRef::new(DFSchema::empty()),
                            },
                        )))
                    }
                    ObjectType::Sequence => Ok(LogicalPlan::Ddl(DdlStatement::DropSequence(
                        DropSequence {
                            name: object_name,
                            if_exists,
                            cascade,
                            restrict,
                            purge,
                            temporary,
                            table,
                        },
                    ))),
                    ObjectType::Role => {
                        let role_name = object_name_to_string(&object_name);
                        Ok(LogicalPlan::Ddl(DdlStatement::DropRole(DropRole {
                            name: role_name,
                            if_exists,
                            cascade,
                        })))
                    }
                    ObjectType::Index => {
                        let index_name = object_name_to_string(&object_name);
                        if index_name.is_empty() {
                            return plan_err!("DROP INDEX requires a non-empty index name");
                        }
                        Ok(LogicalPlan::Ddl(DdlStatement::DropIndex(DropIndex {
                            name: index_name,
                            if_exists,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        })))
                    }
                    _ => not_impl_err!(
                        "Only `DROP TABLE/VIEW/SCHEMA/ROLE/INDEX  ...` statement is supported currently"
                    ),
                }
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
                ..
            } => {
                // Convert parser data types to DataFusion data types
                let mut fields: Vec<FieldRef> = data_types
                    .into_iter()
                    .map(|t| self.convert_data_type_to_field(&t))
                    .collect::<Result<_>>()?;

                // Create planner context with parameters
                let mut planner_context =
                    PlannerContext::new().with_prepare_param_data_types(fields.clone());

                // Build logical plan for inner statement of the prepare statement
                let plan = self.sql_statement_to_plan_with_context_impl(
                    *statement,
                    &mut planner_context,
                )?;

                if fields.is_empty() {
                    let map_types = plan.get_parameter_fields()?;
                    let param_types: Vec<_> = (1..=map_types.len())
                        .filter_map(|i| {
                            let key = format!("${i}");
                            map_types.get(&key).and_then(|opt| opt.clone())
                        })
                        .collect();
                    fields.extend(param_types.iter().cloned());
                    planner_context.with_prepare_param_data_types(param_types);
                }

                Ok(LogicalPlan::Statement(PlanStatement::Prepare(Prepare {
                    name: ident_to_string(&name),
                    fields,
                    input: Arc::new(plan),
                })))
            }
            Statement::Execute {
                name,
                parameters,
                using,
                // has_parentheses specifies the syntax, but the plan is the
                // same no matter the syntax used, so ignore it
                has_parentheses: _,
                immediate,
                into,
                output,
                default,
                ..
            } => {
                if immediate {
                    return not_impl_err!(
                        "Execute statement with IMMEDIATE is not supported"
                    );
                }
                if !into.is_empty() {
                    return not_impl_err!("Execute statement with INTO is not supported");
                }
                if output {
                    return not_impl_err!(
                        "Execute statement with OUTPUT is not supported"
                    );
                }
                if default {
                    return not_impl_err!(
                        "Execute statement with DEFAULT is not supported"
                    );
                }
                let empty_schema = DFSchema::empty();

                // Combine parameters from both sources: parenthesized parameters and USING clause
                let mut all_parameters = Vec::new();
                for expr in parameters {
                    all_parameters.push(self.sql_to_expr(expr, &empty_schema, planner_context)?);
                }
                for expr_with_alias in using {
                    all_parameters.push(self.sql_to_expr(expr_with_alias.expr, &empty_schema, planner_context)?);
                }

                let statement_name = name.ok_or_else(|| {
                    plan_datafusion_err!("EXECUTE statement requires a prepared statement name")
                })?;

                Ok(LogicalPlan::Statement(PlanStatement::Execute(Execute {
                    name: object_name_to_string(&statement_name),
                    parameters: all_parameters,
                })))
            }
            Statement::Deallocate {
                name,
                // Similar to PostgreSQL, the PREPARE keyword is ignored
                prepare: _,
                ..
            } => Ok(LogicalPlan::Statement(PlanStatement::Deallocate(
                Deallocate {
                    name: ident_to_string(&name),
                },
            ))),
            Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                as_grantor,
                granted_by,
                ..
            } => Ok(LogicalPlan::Statement(PlanStatement::Grant(Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                as_grantor,
                granted_by,
            }))),
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                cascade,
                ..
            } => Ok(LogicalPlan::Statement(PlanStatement::Revoke(Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                cascade,
            }))),
            Statement::GrantRole {
                roles,
                grantees,
                with_admin_option,
                granted_by,
                ..
            } => Ok(LogicalPlan::Statement(PlanStatement::GrantRole(GrantRole {
                roles,
                grantees,
                with_admin_option,
                granted_by,
            }))),
            Statement::RevokeRole {
                roles,
                grantees,
                granted_by,
                cascade,
                admin_option_for,
                ..
            } => Ok(LogicalPlan::Statement(PlanStatement::RevokeRole(RevokeRole {
                roles,
                grantees,
                granted_by,
                cascade,
                admin_option_for,
            }))),

            Statement::ShowTables {
                extended,
                full,
                terse,
                history,
                external,
                show_options,
                ..
            } => {
                // We only support the basic "SHOW TABLES"
                // https://github.com/apache/datafusion/issues/3188
                if extended {
                    return not_impl_err!("SHOW TABLES EXTENDED not supported")?;
                }
                if full {
                    return not_impl_err!("SHOW FULL TABLES not supported")?;
                }
                if terse {
                    return not_impl_err!("SHOW TERSE TABLES not supported")?;
                }
                if history {
                    return not_impl_err!("SHOW TABLES HISTORY not supported")?;
                }
                if external {
                    return not_impl_err!("SHOW EXTERNAL TABLES not supported")?;
                }
                let ShowStatementOptions {
                    show_in,
                    starts_with,
                    limit,
                    limit_from,
                    filter_position,
                } = show_options;
                if show_in.is_some() {
                    return not_impl_err!("SHOW TABLES IN not supported")?;
                }
                if starts_with.is_some() {
                    return not_impl_err!("SHOW TABLES LIKE not supported")?;
                }
                if limit.is_some() {
                    return not_impl_err!("SHOW TABLES LIMIT not supported")?;
                }
                if limit_from.is_some() {
                    return not_impl_err!("SHOW TABLES LIMIT FROM not supported")?;
                }
                if filter_position.is_some() {
                    return not_impl_err!("SHOW TABLES FILTER not supported")?;
                }
                self.show_tables_to_plan()
            }

            Statement::ShowColumns {
                extended,
                full,
                show_options,
                ..
            } => {
                let ShowStatementOptions {
                    show_in,
                    starts_with,
                    limit,
                    limit_from,
                    filter_position,
                } = show_options;
                if starts_with.is_some() {
                    return not_impl_err!("SHOW COLUMNS LIKE not supported")?;
                }
                if limit.is_some() {
                    return not_impl_err!("SHOW COLUMNS LIMIT not supported")?;
                }
                if limit_from.is_some() {
                    return not_impl_err!("SHOW COLUMNS LIMIT FROM not supported")?;
                }
                if filter_position.is_some() {
                    return not_impl_err!(
                        "SHOW COLUMNS with WHERE or LIKE is not supported"
                    )?;
                }
                let Some(ShowStatementIn {
                    // specifies if the syntax was `SHOW COLUMNS IN` or `SHOW
                    // COLUMNS FROM` which is not different in DataFusion
                    clause: _,
                    parent_type,
                    parent_name,
                }) = show_in
                else {
                    return plan_err!("SHOW COLUMNS requires a table name");
                };

                if let Some(parent_type) = parent_type {
                    return not_impl_err!("SHOW COLUMNS IN {parent_type} not supported");
                }
                let Some(table_name) = parent_name else {
                    return plan_err!("SHOW COLUMNS requires a table name");
                };

                self.show_columns_to_plan(extended, full, table_name)
            }

            Statement::ShowFunctions { filter, .. } => {
                self.show_functions_to_plan(filter)
            }

            Statement::Insert(Insert {
                or,
                into,
                columns,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
                on,
                returning,
                ignore,
                table_alias,
                mut replace_into,
                priority,
                insert_alias,
                assignments,
                has_table_keyword,
                settings,
                format_clause,
                ..
            }) => {
                let table_name = match table {
                    TableObject::TableName(table_name) => table_name,
                    TableObject::TableFunction(_) => {
                        return not_impl_err!(
                            "INSERT INTO Table functions not supported"
                        );
                    }
                };
                if let Some(or) = or {
                    match or {
                        SqliteOnConflict::Replace => replace_into = true,
                        _ => plan_err!("Inserts with {or} clause is not supported")?,
                    }
                }
                if partitioned.is_some() {
                    plan_err!("Partitioned inserts not yet supported")?;
                }
                if !after_columns.is_empty() {
                    plan_err!("After-columns clause not supported")?;
                }
                if on.is_some() {
                    plan_err!("Insert-on clause not supported")?;
                }
                if returning.is_some() {
                    plan_err!("Insert-returning clause not supported")?;
                }
                if ignore {
                    plan_err!("Insert-ignore clause not supported")?;
                }
                if let Some(table_alias) = table_alias {
                    plan_err!(
                        "Inserts with a table alias not supported: {table_alias:?}"
                    )?
                };
                if let Some(priority) = priority {
                    plan_err!(
                        "Inserts with a `PRIORITY` clause not supported: {priority:?}"
                    )?
                };
                if insert_alias.is_some() {
                    plan_err!("Inserts with an alias not supported")?;
                }
                if !assignments.is_empty() {
                    plan_err!("Inserts with assignments not supported")?;
                }
                if settings.is_some() {
                    plan_err!("Inserts with settings not supported")?;
                }
                if format_clause.is_some() {
                    plan_err!("Inserts with format clause not supported")?;
                }
                // optional keywords don't change behavior
                let _ = into;
                let _ = has_table_keyword;

                // Handle INSERT DEFAULT VALUES
                if source.is_none() {
                    self.insert_default_values_to_plan(table_name, columns, overwrite, replace_into, planner_context)
                } else {
                    self.insert_to_plan(table_name, columns, source.unwrap(), overwrite, replace_into, planner_context)
                }
            }
            Statement::Update(update) => {
                let from_clauses =
                    update.from.map(|update_table_from_kind| match update_table_from_kind {
                        UpdateTableFromKind::BeforeSet(from_clauses) => from_clauses,
                        UpdateTableFromKind::AfterSet(from_clauses) => from_clauses,
                    });
                // TODO: support multiple tables in UPDATE SET FROM
                if from_clauses.as_ref().is_some_and(|f| f.len() > 1) {
                    plan_err!("Multiple tables in UPDATE SET FROM not yet supported")?;
                }
                let update_from = from_clauses.and_then(|mut f| f.pop());
                if update.returning.is_some() {
                    plan_err!("Update-returning clause not yet supported")?;
                }
                if update.or.is_some() {
                    plan_err!("ON conflict not supported")?;
                }
                if update.limit.is_some() {
                    return not_impl_err!("Update-limit clause not supported")?;
                }
                self.update_to_plan(update.table, &update.assignments, update_from, update.selection, planner_context)
            }

            Statement::Delete(Delete {
                tables,
                using,
                selection,
                returning,
                from,
                order_by,
                limit,
                ..
            }) => {
                if !tables.is_empty() {
                    plan_err!("DELETE <TABLE> not supported")?;
                }

                if using.is_some() {
                    plan_err!("Using clause not supported")?;
                }

                if returning.is_some() {
                    plan_err!("Delete-returning clause not yet supported")?;
                }

                if !order_by.is_empty() {
                    plan_err!("Delete-order-by clause not yet supported")?;
                }

                if limit.is_some() {
                    plan_err!("Delete-limit clause not yet supported")?;
                }

                let table = self.get_delete_target(from)?;
                self.delete_to_plan(table, selection, planner_context)
            }
            Statement::Merge { into, table, source, on, clauses, output, .. } => {
                self.merge_to_plan(into, table, source, on, clauses, output, planner_context)
            }

            Statement::StartTransaction {
                modes,
                begin: _,
                modifier,
                transaction,
                statements,
                has_end_keyword,
                exception,
                ..
            } => {
                if let Some(modifier) = modifier {
                    return not_impl_err!(
                        "Transaction modifier not supported: {modifier}"
                    );
                }
                if !statements.is_empty() {
                    return not_impl_err!(
                        "Transaction with multiple statements not supported"
                    );
                }
                if exception.is_some() {
                    return not_impl_err!(
                        "Transaction with exception statements not supported"
                    );
                }
                if has_end_keyword {
                    return not_impl_err!("Transaction with END keyword not supported");
                }
                self.validate_transaction_kind(transaction.as_ref())?;
                let isolation_level: ast::TransactionIsolationLevel = modes
                    .iter()
                    .filter_map(|m: &TransactionMode| match m {
                        TransactionMode::AccessMode(_) => None,
                        TransactionMode::IsolationLevel(level) => Some(level),
                    })
                    .next_back()
                    .copied()
                    .unwrap_or(ast::TransactionIsolationLevel::Serializable);
                let access_mode: ast::TransactionAccessMode = modes
                    .iter()
                    .filter_map(|m: &TransactionMode| match m {
                        TransactionMode::AccessMode(mode) => Some(mode),
                        TransactionMode::IsolationLevel(_) => None,
                    })
                    .next_back()
                    .copied()
                    .unwrap_or(ast::TransactionAccessMode::ReadWrite);
                let isolation_level = match isolation_level {
                    ast::TransactionIsolationLevel::ReadUncommitted => {
                        TransactionIsolationLevel::ReadUncommitted
                    }
                    ast::TransactionIsolationLevel::ReadCommitted => {
                        TransactionIsolationLevel::ReadCommitted
                    }
                    ast::TransactionIsolationLevel::RepeatableRead => {
                        TransactionIsolationLevel::RepeatableRead
                    }
                    ast::TransactionIsolationLevel::Serializable => {
                        TransactionIsolationLevel::Serializable
                    }
                    ast::TransactionIsolationLevel::Snapshot => {
                        TransactionIsolationLevel::Snapshot
                    }
                };
                let access_mode = match access_mode {
                    ast::TransactionAccessMode::ReadOnly => {
                        TransactionAccessMode::ReadOnly
                    }
                    ast::TransactionAccessMode::ReadWrite => {
                        TransactionAccessMode::ReadWrite
                    }
                };
                let statement = PlanStatement::TransactionStart(TransactionStart {
                    access_mode,
                    isolation_level,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::Commit {
                chain,
                end: _,
                modifier,
                ..
            } => {
                if let Some(modifier) = modifier {
                    return not_impl_err!("COMMIT {modifier} not supported");
                };
                let statement = PlanStatement::TransactionEnd(TransactionEnd {
                    conclusion: TransactionConclusion::Commit,
                    chain,
                });
                Ok(LogicalPlan::Statement(statement))
            }
            Statement::Savepoint { name, .. } => Ok(LogicalPlan::Statement(
                PlanStatement::Savepoint(Savepoint { name }),
            )),
            Statement::ReleaseSavepoint { name, .. } => Ok(LogicalPlan::Statement(
                PlanStatement::ReleaseSavepoint(ReleaseSavepoint { name }),
            )),
            Statement::Rollback { chain, savepoint, .. } => {
                if let Some(savepoint) = savepoint {
                    let statement =
                        PlanStatement::RollbackToSavepoint(RollbackToSavepoint {
                            name: savepoint,
                            chain,
                        });
                    Ok(LogicalPlan::Statement(statement))
                } else {
                    let statement = PlanStatement::TransactionEnd(TransactionEnd {
                        conclusion: TransactionConclusion::Rollback,
                        chain,
                    });
                    Ok(LogicalPlan::Statement(statement))
                }
            }
            Statement::CreateFunction(ast::CreateFunction {
                or_replace,
                temporary,
                name,
                args,
                return_type,
                function_body,
                behavior,
                language,
                ..
            }) => {
                let return_type = match return_type {
                    Some(t) => Some(self.convert_data_type_to_field(&t)?),
                    None => None,
                };
                let mut planner_context = PlannerContext::new();
                let empty_schema = &DFSchema::empty();

                let args = match args {
                    Some(function_args) => {
                        let function_args = function_args
                            .into_iter()
                            .map(|arg| {
                                let data_type =
                                    self.convert_data_type_to_field(&arg.data_type)?;

                                let default_expr = match arg.default_expr {
                                    Some(expr) => Some(self.sql_to_expr(
                                        expr,
                                        empty_schema,
                                        &mut planner_context,
                                    )?),
                                    None => None,
                                };
                                Ok(OperateFunctionArg {
                                    name: arg.name,
                                    default_expr,
                                    data_type: data_type.data_type().clone(),
                                })
                            })
                            .collect::<Result<Vec<OperateFunctionArg>>>();
                        Some(function_args?)
                    }
                    None => None,
                };
                // Validate default arguments
                let first_default = match args.as_ref() {
                    Some(arg) => arg.iter().position(|t| t.default_expr.is_some()),
                    None => None,
                };
                let last_non_default = match args.as_ref() {
                    Some(arg) => arg
                        .iter()
                        .rev()
                        .position(|t| t.default_expr.is_none())
                        .map(|reverse_pos| arg.len() - reverse_pos - 1),
                    None => None,
                };
                if let (Some(pos_default), Some(pos_non_default)) =
                    (first_default, last_non_default)
                    && pos_non_default > pos_default
                {
                    return plan_err!(
                        "Non-default arguments cannot follow default arguments."
                    );
                }
                // At the moment functions can't be qualified `schema.name`
                let name = match &name.0[..] {
                    [] => exec_err!("Function should have name")?,
                    [n] => n.as_ident().unwrap().value.clone(),
                    [..] => not_impl_err!("Qualified functions are not supported")?,
                };
                //
                // Convert resulting expression to data fusion expression
                //
                let arg_types = args.as_ref().map(|arg| {
                    arg.iter()
                        .map(|t| {
                            let name = match t.name.clone() {
                                Some(name) => name.value,
                                None => "".to_string(),
                            };
                            Arc::new(Field::new(name, t.data_type.clone(), true))
                        })
                        .collect::<Vec<_>>()
                });
                // Validate parameter style
                if let Some(ref fields) = arg_types {
                    let count_positional =
                        fields.iter().filter(|f| f.name() == "").count();
                    if !(count_positional == 0 || count_positional == fields.len()) {
                        return plan_err!(
                            "All function arguments must use either named or positional style."
                        );
                    }
                }
                let mut planner_context = PlannerContext::new()
                    .with_prepare_param_data_types(arg_types.unwrap_or_default());

                // Add function parameters to PSM schema so they can be referenced in the body
                if let Some(ref function_args) = args {
                    for arg in function_args {
                        if let Some(ref param_name) = arg.name {
                            planner_context
                                .add_psm_variable(&param_name.value, arg.data_type.clone())?;
                        }
                    }
                }

                let function_body = match function_body {
                    Some(r) => Some(self.sql_to_expr(
                        match r {
                            ast::CreateFunctionBody::AsBeforeOptions(expr) => expr,
                            ast::CreateFunctionBody::AsAfterOptions(expr) => expr,
                            ast::CreateFunctionBody::Return(expr) => expr,
                            ast::CreateFunctionBody::AsBeginEnd(begin_end) => {
                                // Plan the PSM block and store in psm_body field
                                let psm_body =
                                    self.plan_psm_block(&begin_end, &mut planner_context)?;
                                let statement =
                                    DdlStatement::CreateFunction(CreateFunction {
                                        or_replace,
                                        temporary,
                                        name,
                                        return_type: return_type.map(|f| f.data_type().clone()),
                                        args,
                                        params: CreateFunctionBody {
                                            language,
                                            behavior: behavior.map(|b| match b {
                                                ast::FunctionBehavior::Immutable => {
                                                    Volatility::Immutable
                                                }
                                                ast::FunctionBehavior::Stable => {
                                                    Volatility::Stable
                                                }
                                                ast::FunctionBehavior::Volatile => {
                                                    Volatility::Volatile
                                                }
                                            }),
                                            function_body: None,
                                        },
                                        psm_body: Some(psm_body),
                                        schema: DFSchemaRef::new(DFSchema::empty()),
                                    });
                                return Ok(LogicalPlan::Ddl(statement));
                            }
                            ast::CreateFunctionBody::AsReturnExpr(_)
                            | ast::CreateFunctionBody::AsReturnSelect(_) => {
                                return not_impl_err!(
                                    "AS RETURN function syntax is not supported"
                                )?
                            }
                        },
                        &planner_context.psm_schema(),
                        &mut planner_context,
                    )?),
                    None => None,
                };

                let params = CreateFunctionBody {
                    language,
                    behavior: behavior.map(|b| match b {
                        ast::FunctionBehavior::Immutable => Volatility::Immutable,
                        ast::FunctionBehavior::Stable => Volatility::Stable,
                        ast::FunctionBehavior::Volatile => Volatility::Volatile,
                    }),
                    function_body,
                };

                let statement = DdlStatement::CreateFunction(CreateFunction {
                    or_replace,
                    temporary,
                    name,
                    return_type: return_type.map(|f| f.data_type().clone()),
                    args,
                    params,
                    psm_body: None,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                });

                Ok(LogicalPlan::Ddl(statement))
            }
            Statement::DropFunction(drop_func) => {
                // According to postgresql documentation it can be only one function
                // specified in drop statement
                if let Some(desc) = drop_func.func_desc.first() {
                    // At the moment functions can't be qualified `schema.name`
                    let name = match &desc.name.0[..] {
                        [] => exec_err!("Function should have name")?,
                        [n] => n.as_ident().unwrap().value.clone(),
                        [..] => not_impl_err!("Qualified functions are not supported")?,
                    };
                    let statement = DdlStatement::DropFunction(DropFunction {
                        if_exists: drop_func.if_exists,
                        name,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    });
                    Ok(LogicalPlan::Ddl(statement))
                } else {
                    exec_err!("Function name not provided")
                }
            }
            Statement::CreateIndex(CreateIndex {
                name,
                table_name,
                using,
                columns,
                unique,
                if_not_exists,
                ..
            }) => {
                let name: Option<String> = name.as_ref().map(object_name_to_string);
                let table = self.object_name_to_table_reference(table_name)?;
                let table_schema = self
                    .context_provider
                    .get_table_source(table.clone())?
                    .schema()
                    .to_dfschema_ref()?;
                let using: Option<String> =
                    using.as_ref().map(|index_type| match index_type {
                        IndexType::Custom(ident) => ident_to_string(ident),
                        _ => index_type.to_string().to_ascii_lowercase(),
                    });
                let order_by_exprs: Vec<OrderByExpr> =
                    columns.into_iter().map(|col| col.column).collect();
                let columns = self.order_by_to_sort_expr(
                    order_by_exprs,
                    &table_schema,
                    planner_context,
                    false,
                    None,
                )?;
                Ok(LogicalPlan::Ddl(DdlStatement::CreateIndex(
                    PlanCreateIndex {
                        name,
                        table,
                        using,
                        columns,
                        unique,
                        if_not_exists,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    },
                )))
            }
            Statement::CreateRole(create_role) => {
                if create_role.names.is_empty() {
                    return plan_err!("CREATE ROLE requires at least one role name");
                }
                if create_role.names.len() > 1 {
                    return not_impl_err!(
                        "CREATE ROLE with multiple roles is not supported"
                    );
                }

                let name = object_name_to_string(&create_role.names[0]);

                if name.is_empty() {
                    return plan_err!("CREATE ROLE requires a non-empty role name");
                }

                Ok(LogicalPlan::Ddl(DdlStatement::CreateRole(CreateRole {
                    name,
                    if_not_exists: create_role.if_not_exists,
                })))
            }
            Statement::Analyze(analyze) => {
                let table_name = object_name_to_string(&analyze.table_name);
                Ok(LogicalPlan::Statement(PlanStatement::AnalyzeTable(
                    AnalyzeTable { table_name },
                )))
            }
            Statement::Truncate(truncate) => {
                if truncate.table_names.is_empty() {
                    return plan_err!("TRUNCATE TABLE requires at least one table name");
                }
                if truncate.table_names.len() > 1 {
                    return not_impl_err!(
                        "TRUNCATE TABLE with multiple tables is not supported"
                    );
                }

                let table_name = object_name_to_string(&truncate.table_names[0].name);

                if table_name.is_empty() {
                    return plan_err!("TRUNCATE TABLE requires a non-empty table name");
                }

                Ok(LogicalPlan::Statement(PlanStatement::TruncateTable(
                    TruncateTable { table_name },
                )))
            }
            Statement::Vacuum(vacuum) => {
                let table_name = vacuum.table_name.map(|n| object_name_to_string(&n));
                Ok(LogicalPlan::Statement(PlanStatement::Vacuum(Vacuum {
                    table_name,
                })))
            }
            Statement::Use(use_stmt) => {
                let db_name = match use_stmt {
                    ast::Use::Catalog(name) => object_name_to_string(&name),
                    ast::Use::Schema(name) => object_name_to_string(&name),
                    ast::Use::Database(name) => object_name_to_string(&name),
                    ast::Use::Warehouse(name) => object_name_to_string(&name),
                    ast::Use::Role(name) => object_name_to_string(&name),
                    ast::Use::Object(name) => object_name_to_string(&name),
                    ast::Use::Default => "default".to_string(),
                };
                Ok(LogicalPlan::Statement(PlanStatement::UseDatabase(
                    UseDatabase { db_name },
                )))
            }
            Statement::CreateProcedure {
                or_alter,
                name,
                params,
                body,
                ..
            } => {
                // Extract procedure name
                let proc_name = object_name_to_string(&name);

                // Convert parameters to ProcedureArg
                let args = match params {
                    Some(procedure_params) => {
                        let planned_args: Result<Vec<ProcedureArg>> = procedure_params
                            .into_iter()
                            .map(|p| {
                                let mode = match p.mode {
                                    Some(ast::ArgMode::In) | None => ParameterMode::In,
                                    Some(ast::ArgMode::Out) => ParameterMode::Out,
                                    Some(ast::ArgMode::InOut) => ParameterMode::InOut,
                                };
                                // Convert data type using the field helper
                                let field =
                                    self.convert_data_type_to_field(&p.data_type)?;
                                Ok(ProcedureArg {
                                    mode,
                                    name: Some(p.name),
                                    data_type: field.data_type().clone(),
                                    default: None,
                                })
                            })
                            .collect();
                        Some(planned_args?)
                    }
                    None => None,
                };

                // Plan the procedure body
                let mut planner_context = PlannerContext::new();

                // Create a schema from procedure parameters so they can be referenced in the body
                if let Some(ref procedure_args) = args {
                    for arg in procedure_args {
                        if let Some(ref param_name) = arg.name {
                            planner_context
                                .add_psm_variable(&param_name.value, arg.data_type.clone())?;
                        }
                    }
                }

                let psm_body =
                    self.plan_psm_block_from_conditional(&body, &mut planner_context)?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreateProcedure(
                    CreateProcedure {
                        or_replace: or_alter,
                        name: proc_name,
                        args,
                        body: psm_body,
                    },
                )))
            }
            Statement::Call(function) => {
                let procedure_name = function.name.to_string();
                let mut planner_context = PlannerContext::new();
                let schema = DFSchema::empty();

                // Extract and plan call arguments from FunctionArguments
                let args = match &function.args {
                    ast::FunctionArguments::None => vec![],
                    ast::FunctionArguments::Subquery(_) => {
                        return not_impl_err!(
                            "CALL with subquery argument is not supported"
                        );
                    }
                    ast::FunctionArguments::List(arg_list) => {
                        let mut planned_args = Vec::new();
                        for arg in &arg_list.args {
                            // Extract expression from FunctionArg
                            let sql_expr = match arg {
                                ast::FunctionArg::Named { arg, .. } => match arg {
                                    ast::FunctionArgExpr::Expr(e) => e.clone(),
                                    ast::FunctionArgExpr::Wildcard => {
                                        return not_impl_err!(
                                            "Wildcard in CALL is not supported"
                                        );
                                    }
                                    ast::FunctionArgExpr::QualifiedWildcard(_) => {
                                        return not_impl_err!(
                                            "Qualified wildcard in CALL is not supported"
                                        );
                                    }
                                },
                                ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                                    ast::FunctionArgExpr::Expr(e) => e.clone(),
                                    _ => {
                                        return not_impl_err!(
                                            "Non-expression args in CALL not supported"
                                        );
                                    }
                                },
                                ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                    ast::FunctionArgExpr::Expr(e) => e.clone(),
                                    ast::FunctionArgExpr::Wildcard => {
                                        return not_impl_err!(
                                            "Wildcard in CALL is not supported"
                                        );
                                    }
                                    ast::FunctionArgExpr::QualifiedWildcard(_) => {
                                        return not_impl_err!(
                                            "Qualified wildcard in CALL is not supported"
                                        );
                                    }
                                },
                                other => {
                                    return not_impl_err!(
                                        "Unsupported CALL argument type: {other:?}"
                                    );
                                }
                            };
                            let planned =
                                self.sql_to_expr(sql_expr, &schema, &mut planner_context)?;
                            planned_args.push(planned);
                        }
                        planned_args
                    }
                };

                Ok(LogicalPlan::Statement(PlanStatement::Call(Call {
                    procedure_name,
                    args,
                })))
            }
            stmt => {
                not_impl_err!("Unsupported SQL statement: {stmt}")
            }
        }
    }

    fn get_delete_target(&self, from: FromTable) -> Result<TableWithJoins> {
        let mut from = match from {
            FromTable::WithFromKeyword(v) => v,
            FromTable::WithoutKeyword(v) => v,
        };

        if from.len() != 1 {
            return not_impl_err!(
                "DELETE FROM only supports single table, got {}: {from:?}",
                from.len()
            );
        }
        let table_with_joins = from.pop().unwrap();
        if !table_with_joins.joins.is_empty() {
            return not_impl_err!("DELETE FROM only supports single table, got: joins");
        }

        Ok(table_with_joins)
    }

    /// Generate a logical plan from a "SHOW TABLES" query
    fn show_tables_to_plan(&self) -> Result<LogicalPlan> {
        if self.has_table("information_schema", "tables") {
            let query = "SELECT * FROM information_schema.tables;";
            let mut rewrite = DFParser::parse_sql(query)?;
            assert_eq!(rewrite.len(), 1);
            self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
        } else {
            plan_err!("SHOW TABLES is not supported unless information_schema is enabled")
        }
    }

    fn describe_table_to_plan(&self, table_name: ObjectName) -> Result<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(table_name)?;

        let table_source = self.context_provider.get_table_source(table_ref)?;

        let schema = table_source.schema();

        let output_schema = DFSchema::try_from(LogicalPlan::describe_schema()).unwrap();

        Ok(LogicalPlan::DescribeTable(DescribeTable {
            schema,
            output_schema: Arc::new(output_schema),
        }))
    }

    fn describe_query_to_plan(&self, query: Query) -> Result<LogicalPlan> {
        let plan = self.query_to_plan(query, &mut PlannerContext::new())?;

        let schema = Arc::new(plan.schema().as_arrow().clone());

        let output_schema = DFSchema::try_from(LogicalPlan::describe_schema()).unwrap();

        Ok(LogicalPlan::DescribeTable(DescribeTable {
            schema,
            output_schema: Arc::new(output_schema),
        }))
    }

    fn copy_to_plan(&self, statement: CopyToStatement) -> Result<LogicalPlan> {
        // Determine if source is table or query and handle accordingly
        let copy_source = statement.source;
        let (input, input_schema, table_ref) = match copy_source {
            CopyToSource::Relation(object_name) => {
                let table_name = object_name_to_string(&object_name);
                let table_ref = self.object_name_to_table_reference(object_name)?;
                let table_source =
                    self.context_provider.get_table_source(table_ref.clone())?;
                let plan =
                    LogicalPlanBuilder::scan(table_name, table_source, None)?.build()?;
                let input_schema = Arc::clone(plan.schema());
                (plan, input_schema, Some(table_ref))
            }
            CopyToSource::Query(query) => {
                let plan = self.query_to_plan(*query, &mut PlannerContext::new())?;
                let input_schema = Arc::clone(plan.schema());
                (plan, input_schema, None)
            }
        };

        let options_map = self.parse_options_map(statement.options, true)?;

        let maybe_file_type = if let Some(stored_as) = &statement.stored_as {
            self.context_provider.get_file_type(stored_as).ok()
        } else {
            None
        };

        let file_type = match maybe_file_type {
            Some(ft) => ft,
            None => {
                let e = || {
                    DataFusionError::Configuration(
                        "Format not explicitly set and unable to get file extension! Use STORED AS to define file format."
                            .to_string(),
                    )
                };
                // Try to infer file format from file extension
                let extension: &str = &Path::new(&statement.target)
                    .extension()
                    .ok_or_else(e)?
                    .to_str()
                    .ok_or_else(e)?
                    .to_lowercase();

                self.context_provider.get_file_type(extension)?
            }
        };

        let partition_by = statement
            .partitioned_by
            .iter()
            .map(|col| input_schema.field_with_name(table_ref.as_ref(), col))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|f| f.name().to_owned())
            .collect();

        Ok(LogicalPlan::Copy(CopyTo::new(
            Arc::new(input),
            statement.target,
            partition_by,
            file_type,
            options_map,
        )))
    }

    fn copy_from_plan(&self, statement: CopyFromStatement) -> Result<LogicalPlan> {
        let table_name = self.object_name_to_table_reference(statement.table_name)?;
        
        // Parse options into a HashMap
        let options_map = self.parse_options_map(statement.options, true)?;

        // Determine file type from stored_as or file extension
        let maybe_file_type = if let Some(stored_as) = &statement.stored_as {
            self.context_provider.get_file_type(stored_as).ok()
        } else {
            None
        };

        let file_type = match maybe_file_type {
            Some(ft) => ft,
            None => {
                let e = || {
                    DataFusionError::Configuration(
                        "Format not explicitly set and unable to get file extension! Use STORED AS to define file format."
                            .to_string(),
                    )
                };
                // Try to infer file format from file extension
                let extension: &str = &Path::new(&statement.source)
                    .extension()
                    .ok_or_else(e)?
                    .to_str()
                    .ok_or_else(e)?
                    .to_lowercase();

                self.context_provider.get_file_type(extension)?
            }
        };

        Ok(LogicalPlan::CopyFrom(CopyFrom::new(
            table_name,
            statement.source,
            statement.columns,
            file_type,
            options_map,
        )))
    }

    fn build_order_by(
        &self,
        order_exprs: Vec<LexOrdering>,
        schema: &DFSchemaRef,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Vec<SortExpr>>> {
        if !order_exprs.is_empty() && schema.fields().is_empty() {
            let results = order_exprs
                .iter()
                .map(|lex_order| {
                    let result = lex_order
                        .iter()
                        .map(|order_by_expr| {
                            let ordered_expr = &order_by_expr.expr;
                            let ordered_expr = ordered_expr.to_owned();
                            let ordered_expr = self.sql_expr_to_logical_expr(
                                ordered_expr,
                                schema,
                                planner_context,
                            )?;
                            let asc = order_by_expr.options.asc.unwrap_or(true);
                            let nulls_first =
                                order_by_expr.options.nulls_first.unwrap_or_else(|| {
                                    self.options.default_null_ordering.nulls_first(asc)
                                });

                            Ok(SortExpr::new(ordered_expr, asc, nulls_first))
                        })
                        .collect::<Result<Vec<SortExpr>>>()?;
                    Ok(result)
                })
                .collect::<Result<Vec<Vec<SortExpr>>>>()?;

            return Ok(results);
        }

        let mut all_results = vec![];
        for expr in order_exprs {
            // Convert each OrderByExpr to a SortExpr:
            let expr_vec =
                self.order_by_to_sort_expr(expr, schema, planner_context, true, None)?;
            // Verify that columns of all SortExprs exist in the schema:
            for sort in expr_vec.iter() {
                for column in sort.expr.column_refs().iter() {
                    if !schema.has_column(column) {
                        // Return an error if any column is not in the schema:
                        return plan_err!("Column {column} is not in schema");
                    }
                }
            }
            // If all SortExprs are valid, return them as an expression vector
            all_results.push(expr_vec)
        }
        Ok(all_results)
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    fn external_table_to_plan(
        &self,
        statement: CreateExternalTable,
    ) -> Result<LogicalPlan> {
        let definition = Some(statement.to_string());
        let CreateExternalTable {
            name,
            columns,
            file_type,
            location,
            table_partition_cols,
            if_not_exists,
            temporary,
            order_exprs,
            unbounded,
            options,
            constraints,
            or_replace,
        } = statement;

        // Merge inline constraints and existing constraints
        let mut all_constraints = constraints;
        let inline_constraints = calc_inline_constraints_from_columns(&columns);
        all_constraints.extend(inline_constraints);

        let options_map = self.parse_options_map(options, false)?;

        let compression = options_map
            .get("format.compression")
            .map(|c| CompressionTypeVariant::from_str(c))
            .transpose()?;
        if (file_type == "PARQUET" || file_type == "AVRO" || file_type == "ARROW")
            && compression
                .map(|c| c != CompressionTypeVariant::UNCOMPRESSED)
                .unwrap_or(false)
        {
            plan_err!(
                "File compression type cannot be set for PARQUET, AVRO, or ARROW files."
            )?;
        }

        let mut planner_context = PlannerContext::new();

        let column_defaults = self
            .build_column_defaults(&columns, &mut planner_context)?
            .into_iter()
            .collect();

        let schema = self.build_schema(columns)?;
        let df_schema = schema.to_dfschema_ref()?;
        df_schema.check_names()?;

        let ordered_exprs =
            self.build_order_by(order_exprs, &df_schema, &mut planner_context)?;

        let name = self.object_name_to_table_reference(name)?;
        let constraints =
            self.new_constraint_from_table_constraints(&all_constraints, &df_schema)?;
        Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
            PlanCreateExternalTable::builder(name, location, file_type, df_schema)
                .with_partition_cols(table_partition_cols)
                .with_if_not_exists(if_not_exists)
                .with_or_replace(or_replace)
                .with_temporary(temporary)
                .with_definition(definition)
                .with_order_exprs(ordered_exprs)
                .with_unbounded(unbounded)
                .with_options(options_map)
                .with_constraints(constraints)
                .with_column_defaults(column_defaults)
                .build(),
        )))
    }

    /// Get the indices of the constraint columns in the schema.
    /// If any column is not found, return an error.
    fn get_constraint_column_indices(
        &self,
        df_schema: &DFSchemaRef,
        columns: &[IndexColumn],
        constraint_name: &str,
    ) -> Result<Vec<usize>> {
        let field_names = df_schema.field_names();
        columns
            .iter()
            .map(|index_column| {
                let expr = &index_column.column.expr;
                let ident = if let SQLExpr::Identifier(ident) = expr {
                    ident
                } else {
                    return Err(plan_datafusion_err!(
                        "Column name for {constraint_name} must be an identifier: {expr}"
                    ));
                };
                let column = self.ident_normalizer.normalize(ident.clone());
                field_names
                    .iter()
                    .position(|item| *item == column)
                    .ok_or_else(|| {
                        plan_datafusion_err!(
                            "Column for {constraint_name} not found in schema: {column}"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Convert each [TableConstraint] to corresponding [Constraint]
    pub fn new_constraint_from_table_constraints(
        &self,
        constraints: &[TableConstraint],
        df_schema: &DFSchemaRef,
    ) -> Result<Constraints> {
        let constraints = constraints
            .iter()
            .map(|c: &TableConstraint| match c {
                TableConstraint::Unique(unique) => {
                    let constraint_name = match &unique.name {
                        Some(name) => &format!("unique constraint with name '{name}'"),
                        None => "unique constraint",
                    };
                    // Get unique constraint indices in the schema
                    let indices = self.get_constraint_column_indices(
                        df_schema,
                        &unique.columns,
                        constraint_name,
                    )?;
                    Ok(Constraint::Unique(indices))
                }
                TableConstraint::PrimaryKey(pk) => {
                    // Get primary key indices in the schema
                    let indices = self.get_constraint_column_indices(
                        df_schema,
                        &pk.columns,
                        "primary key",
                    )?;
                    Ok(Constraint::PrimaryKey(indices))
                }
                TableConstraint::ForeignKey(fk) => {
                    // Convert sqlparser ReferentialAction to DataFusion ReferentialAction
                    let convert_action = |action: Option<ast::ReferentialAction>| match action {
                        None | Some(ast::ReferentialAction::NoAction) => {
                            ReferentialAction::NoAction
                        }
                        Some(ast::ReferentialAction::Restrict) => ReferentialAction::Restrict,
                        Some(ast::ReferentialAction::Cascade) => ReferentialAction::Cascade,
                        Some(ast::ReferentialAction::SetNull) => ReferentialAction::SetNull,
                        Some(ast::ReferentialAction::SetDefault) => {
                            ReferentialAction::SetDefault
                        }
                    };

                    // Convert match kind to MatchType
                    let match_type = match fk.match_kind {
                        Some(ast::ConstraintReferenceMatchKind::Full) => MatchType::Full,
                        _ => MatchType::Simple,
                    };

                    let columns: Vec<String> = fk
                        .columns
                        .iter()
                        .map(|c| match c {
                            ForeignKeyColumnOrPeriod::Column(ident) => ident.value.clone(),
                            ForeignKeyColumnOrPeriod::Period(ident) => ident.value.clone(),
                        })
                        .collect();
                    let referenced_columns: Vec<String> = fk
                        .referred_columns
                        .iter()
                        .map(|c| match c {
                            ForeignKeyColumnOrPeriod::Column(ident) => ident.value.clone(),
                            ForeignKeyColumnOrPeriod::Period(ident) => ident.value.clone(),
                        })
                        .collect();

                    Ok(Constraint::ForeignKey {
                        name: fk.name.as_ref().map(|n| n.value.clone()),
                        columns,
                        referenced_table: fk.foreign_table.to_string(),
                        referenced_columns,
                        on_delete: convert_action(fk.on_delete.clone()),
                        on_update: convert_action(fk.on_update.clone()),
                        match_type,
                    })
                }
                TableConstraint::Check(check) => {
                    Ok(Constraint::Check {
                        name: check.name.as_ref().map(|n| n.value.clone()),
                        expr: check.expr.to_string(),
                        enforced: check.enforced,
                    })
                }
                TableConstraint::Index { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                TableConstraint::FulltextOrSpatial { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                TableConstraint::Period { .. } => {
                    _plan_err!("PERIOD constraints are not currently supported")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }

    fn parse_options_map(
        &self,
        options: Vec<(String, Value)>,
        allow_duplicates: bool,
    ) -> Result<HashMap<String, String>> {
        let mut options_map = HashMap::new();
        for (key, value) in options {
            if !allow_duplicates && options_map.contains_key(&key) {
                return plan_err!("Option {key} is specified multiple times");
            }

            let Some(value_string) = crate::utils::value_to_string(&value) else {
                return plan_err!("Unsupported Value {}", value);
            };

            if !(&key.contains('.')) {
                // If config does not belong to any namespace, assume it is
                // a format option and apply the format prefix for backwards
                // compatibility.
                let renamed_key = format!("format.{key}");
                options_map.insert(renamed_key.to_lowercase(), value_string);
            } else {
                options_map.insert(key.to_lowercase(), value_string);
            }
        }

        Ok(options_map)
    }

    fn parse_storage_parameters(
        &self,
        options: Vec<SqlOption>,
    ) -> Result<BTreeMap<String, String>> {
        let mut storage_parameters = BTreeMap::new();
        for option in options {
            match option {
                SqlOption::KeyValue { key, value } => {
                    let key = ident_to_string(&key);
                    if storage_parameters.contains_key(&key) {
                        return plan_err!(
                            "Storage parameter {key} is specified multiple times"
                        );
                    }
                    let value_string =
                        self.storage_parameter_value_to_string(value)?;
                    storage_parameters.insert(key, value_string);
                }
                other => {
                    return not_impl_err!(
                        "Only key = value storage parameters are supported, got {other:?}"
                    );
                }
            }
        }

        Ok(storage_parameters)
    }

    fn storage_parameter_value_to_string(&self, value: SQLExpr) -> Result<String> {
        match value {
            SQLExpr::Identifier(ident) => Ok(ident_to_string(&ident)),
            SQLExpr::Value(value) => match crate::utils::value_to_string(&value.value)
            {
                Some(value_string) => Ok(value_string),
                None => {
                    plan_err!("Unsupported storage parameter value {:?}", value.value)
                }
            },
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => Ok(format!("+{expr}")),
                UnaryOperator::Minus => Ok(format!("-{expr}")),
                _ => plan_err!(
                    "Unsupported unary op {:?} in storage parameter value",
                    op
                ),
            },
            _ => plan_err!("Unsupported storage parameter value {:?}", value),
        }
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    /// Note this is the sqlparser explain statement, not the
    /// datafusion `EXPLAIN` statement.
    fn explain_to_plan(
        &self,
        verbose: bool,
        analyze: bool,
        format: Option<String>,
        statement: DFStatement,
    ) -> Result<LogicalPlan> {
        let plan = self.statement_to_plan(statement)?;
        if matches!(plan, LogicalPlan::Explain(_)) {
            return plan_err!("Nested EXPLAINs are not supported");
        }

        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if verbose && format.is_some() {
            return plan_err!("EXPLAIN VERBOSE with FORMAT is not supported");
        }

        if analyze {
            if format.is_some() {
                return plan_err!("EXPLAIN ANALYZE with FORMAT is not supported");
            }
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans =
                vec![plan.to_stringified(PlanType::InitialLogicalPlan)];

            // default to configuration value
            // verbose mode only supports indent format
            let options = self.context_provider.options();
            let format = if verbose {
                ExplainFormat::Indent
            } else if let Some(format) = format {
                ExplainFormat::from_str(&format)?
            } else {
                options.explain.format.clone()
            };

            Ok(LogicalPlan::Explain(Explain {
                verbose,
                explain_format: format,
                plan,
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            }))
        }
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        if !self.has_table("information_schema", "df_settings") {
            return plan_err!(
                "SHOW [VARIABLE] is not supported unless information_schema is enabled"
            );
        }

        let verbose = variable
            .last()
            .map(|s| ident_to_string(s) == "verbose")
            .unwrap_or(false);
        let mut variable_vec = variable.to_vec();
        let mut columns: String = "name, value".to_owned();

        if verbose {
            columns = format!("{columns}, description");
            variable_vec = variable_vec.split_at(variable_vec.len() - 1).0.to_vec();
        }

        let variable = object_name_to_string(&ObjectName::from(variable_vec));
        let base_query = format!("SELECT {columns} FROM information_schema.df_settings");
        let query = if variable == "all" {
            // Add an ORDER BY so the output comes out in a consistent order
            format!("{base_query} ORDER BY name")
        } else if variable == "timezone" || variable == "time.zone" {
            // we could introduce alias in OptionDefinition if this string matching thing grows
            format!("{base_query} WHERE name = 'datafusion.execution.time_zone'")
        } else {
            // These values are what are used to make the information_schema table, so we just
            // check here, before actually planning or executing the query, if it would produce no
            // results, and error preemptively if it would (for a better UX)
            let is_valid_variable = self
                .context_provider
                .options()
                .entries()
                .iter()
                .any(|opt| opt.key == variable);

            // Check if it's a runtime variable
            let is_runtime_variable = variable.starts_with("datafusion.runtime.");

            if !is_valid_variable && !is_runtime_variable {
                return plan_err!(
                    "'{variable}' is not a variable which can be viewed with 'SHOW'"
                );
            }

            format!("{base_query} WHERE name = '{variable}'")
        };

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);

        self.statement_to_plan(rewrite.pop_front().unwrap())
    }

    /// Converts a SQL expression to a string value for SET statement processing
    fn sql_expr_to_set_value_string(&self, expr: &SQLExpr) -> Result<String> {
        match expr {
            SQLExpr::Identifier(i) => Ok(ident_to_string(i)),
            SQLExpr::Value(v) => match crate::utils::value_to_string(&v.value) {
                None => {
                    plan_err!("Unsupported value {:?}", v.value)
                }
                Some(s) => Ok(s),
            },
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => Ok(format!("+{expr}")),
                UnaryOperator::Minus => Ok(format!("-{expr}")),
                _ => plan_err!("Unsupported unary op {:?}", op),
            },
            _ => plan_err!("Unsupported expr {:?}", expr),
        }
    }

    fn set_statement_to_plan(&self, statement: Set) -> Result<LogicalPlan> {
        match statement {
            Set::SingleAssignment {
                scope: _scope,
                variable,
                values,
                ..
            } => {
                let variable = object_name_to_string(&variable);
                let mut variable_lower = variable.to_lowercase();

                // Map PostgreSQL "timezone" and MySQL "time.zone" aliases to DataFusion's canonical name
                if variable_lower == "timezone" || variable_lower == "time.zone" {
                    variable_lower = "datafusion.execution.time_zone".to_string();
                }

                if values.len() != 1 {
                    return plan_err!("SET only supports single value assignment");
                }

                let value_string = self.sql_expr_to_set_value_string(&values[0])?;

                Ok(LogicalPlan::Statement(PlanStatement::SetVariable(
                    SetVariable {
                        variable: variable_lower,
                        value: value_string,
                    },
                )))
            }
            Set::SetTransaction {
                modes,
                snapshot,
                session,
            } => Ok(LogicalPlan::Statement(PlanStatement::SetTransaction(
                SetTransaction {
                    modes,
                    snapshot,
                    session,
                },
            ))),
            Set::SetTimeZone { local: _, value } => {
                let variable_lower = "datafusion.execution.time_zone".to_string();

                let value_string = self.sql_expr_to_set_value_string(&value)?;

                Ok(LogicalPlan::Statement(PlanStatement::SetVariable(
                    SetVariable {
                        variable: variable_lower,
                        value: value_string,
                    },
                )))
            }
            other => not_impl_err!("SET variant not implemented yet: {other:?}"),
        }
    }

    fn reset_statement_to_plan(&self, statement: ResetStatement) -> Result<LogicalPlan> {
        match statement {
            ResetStatement::Variable(variable) => {
                let variable = object_name_to_string(&variable);
                let mut variable_lower = variable.to_lowercase();

                // Map PostgreSQL "timezone" and MySQL "time.zone" aliases to DataFusion's canonical name
                if variable_lower == "timezone" || variable_lower == "time.zone" {
                    variable_lower = "datafusion.execution.time_zone".to_string();
                }

                Ok(LogicalPlan::Statement(PlanStatement::ResetVariable(
                    ResetVariable {
                        variable: variable_lower,
                    },
                )))
            }
        }
    }

    fn delete_to_plan(
        &self,
        table: TableWithJoins,
        predicate_expr: Option<SQLExpr>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Extract table name from the TableWithJoins
        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => name.clone(),
            _ => plan_err!("Cannot delete from non-table relation!")?,
        };

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_ref.clone())?;

        // Clone the outer planner context to inherit CTEs
        let mut planner_context = outer_planner_context.clone();

        // Build scan using plan_from_tables to properly apply the alias
        let scan = self.plan_from_tables(vec![table], &mut planner_context)?;

        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr(
                    predicate_expr,
                    scan.schema(),
                    &mut planner_context,
                )?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[scan.schema()]],
                    &[using_columns],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        let plan = LogicalPlan::Dml(DmlStatement::new(
            table_ref,
            table_source,
            WriteOp::Delete,
            Arc::new(source),
        ));
        Ok(plan)
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_to_plan(
        &self,
        _into: bool,
        table: TableFactor,
        source: TableFactor,
        on: Box<ast::Expr>,
        clauses: Vec<ast::MergeClause>,
        output: Option<ast::OutputClause>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if output.is_some() {
            return not_impl_err!("MERGE OUTPUT/RETURNING not supported");
        }

        let (table_name, table_alias) = match table {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if args.is_some() {
                    return not_impl_err!("MERGE target table functions not supported");
                }
                if !with_hints.is_empty() {
                    return not_impl_err!("MERGE target table hints not supported");
                }
                if version.is_some() {
                    return not_impl_err!("MERGE target table version not supported");
                }
                if with_ordinality {
                    return not_impl_err!("MERGE target WITH ORDINALITY not supported");
                }
                if !partitions.is_empty() {
                    return not_impl_err!("MERGE target partitions not supported");
                }
                if json_path.is_some() {
                    return not_impl_err!("MERGE target JSON path not supported");
                }
                if sample.is_some() {
                    return not_impl_err!("MERGE target TABLESAMPLE not supported");
                }
                if !index_hints.is_empty() {
                    return not_impl_err!("MERGE target index hints not supported");
                }
                (name, alias)
            }
            _ => plan_err!("MERGE target must be a table")?,
        };

        let table_ref = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_ref.clone())?;
        let mut target_plan =
            LogicalPlanBuilder::scan(table_ref.clone(), table_source, None)?.build()?;
        if let Some(alias) = table_alias {
            target_plan = self.apply_table_alias(target_plan, alias)?;
        }

        let source_plan = self.plan_table_with_joins(
            TableWithJoins {
                relation: source,
                joins: vec![],
            },
            planner_context,
        )?;

        let join_schema =
            build_join_schema(target_plan.schema(), source_plan.schema(), &JoinType::Inner)?;

        let mut normalize_expr =
            |sql_expr: SQLExpr| -> Result<Expr> {
                let expr =
                    self.sql_to_expr(sql_expr, &join_schema, planner_context)?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&expr, &mut using_columns)?;
                normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[&join_schema]],
                    &[using_columns],
                )
            };

        let on_expr = normalize_expr(*on)?;

        let mut merge_clauses = Vec::with_capacity(clauses.len());
        for clause in clauses {
            let predicate = match clause.predicate {
                Some(predicate) => Some(normalize_expr(predicate)?),
                None => None,
            };

            let action = match clause.action {
                ast::MergeAction::Insert(insert) => {
                    let kind = match insert.kind {
                        ast::MergeInsertKind::Values(values) => {
                            let mut rows =
                                Vec::with_capacity(values.rows.len());
                            for row in values.rows {
                                let mut expr_row =
                                    Vec::with_capacity(row.len());
                                for value in row {
                                    expr_row.push(normalize_expr(value)?);
                                }
                                rows.push(expr_row);
                            }
                            MergeInsertKind::Values(rows)
                        }
                        ast::MergeInsertKind::Row => MergeInsertKind::Row,
                    };
                    let columns = insert.columns.into_iter()
                        .map(|ident| ObjectName::from(vec![ident]))
                        .collect();
                    MergeAction::Insert(MergeInsertExpr {
                        columns,
                        kind,
                        insert_predicate: None,
                    })
                }
                ast::MergeAction::Update { assignments: update_assignments } => {
                    let mut assignments =
                        Vec::with_capacity(update_assignments.len());
                    for assignment in update_assignments {
                        let value = normalize_expr(assignment.value)?;
                        assignments.push(MergeAssignment {
                            target: assignment.target,
                            value,
                        });
                    }
                    MergeAction::Update(MergeUpdateExpr {
                        assignments,
                        update_predicate: None,
                        delete_predicate: None,
                    })
                }
                ast::MergeAction::Delete { .. } => MergeAction::Delete,
            };

            merge_clauses.push(MergeClause {
                clause_kind: clause.clause_kind,
                predicate,
                action,
            });
        }

        Ok(LogicalPlan::Merge(Merge::new(
            table_ref,
            Arc::new(target_plan),
            Arc::new(source_plan),
            on_expr,
            merge_clauses,
        )))
    }

    fn update_to_plan(
        &self,
        table: TableWithJoins,
        assignments: &[Assignment],
        from: Option<TableWithJoins>,
        predicate_expr: Option<SQLExpr>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let (table_name, table_alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => (name.clone(), alias.clone()),
            _ => plan_err!("Cannot update non-table relation!")?,
        };

        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_name.clone())?;
        let table_schema = Arc::new(DFSchema::try_from_qualified_schema(
            table_name.clone(),
            &table_source.schema(),
        )?);

        // Overwrite with assignment expressions
        // Clone the outer planner context to inherit CTEs
        let mut planner_context = outer_planner_context.clone();
        let mut assign_map: HashMap<String, SQLExpr> = HashMap::new();

        // Helper function to extract column name from ObjectName
        let extract_column_name = |obj_name: &ObjectName| -> Result<String> {
            let ident = obj_name
                .0
                .iter()
                .last()
                .ok_or_else(|| plan_datafusion_err!("Empty column id"))?
                .as_ident()
                .unwrap();
            Ok(ident.value.clone())
        };

        // Process each assignment
        for assign in assignments {
            match &assign.target {
                AssignmentTarget::ColumnName(cols) => {
                    // Single column assignment
                    let col_name = extract_column_name(cols)?;
                    // Validate that the assignment target column exists
                    table_schema.field_with_unqualified_name(&col_name)?;
                    if assign_map.contains_key(&col_name) {
                        return plan_err!("Column '{}' assigned more than once", col_name);
                    }
                    assign_map.insert(col_name, assign.value.clone());
                }
                AssignmentTarget::Tuple(col_names) => {
                    // Tuple assignment: (a, b) = (val1, val2)
                    // Extract column names
                    let columns: Vec<String> = col_names
                        .iter()
                        .map(|obj| extract_column_name(obj))
                        .collect::<Result<Vec<_>>>()?;

                    // Validate columns exist
                    for col in &columns {
                        table_schema.field_with_unqualified_name(col)?;
                    }

                    // Expand tuple value
                    let values = match &assign.value {
                        SQLExpr::Tuple(exprs) => exprs.clone(),
                        SQLExpr::Nested(inner) => {
                            // Handle ((a, b)) case
                            if let SQLExpr::Tuple(exprs) = inner.as_ref() {
                                exprs.clone()
                            } else {
                                vec![*inner.clone()]
                            }
                        }
                        SQLExpr::Subquery(query) => {
                            // For subqueries, the subquery is expected to return exactly 1 row with N columns
                            // matching the N target columns in the tuple assignment.
                            // For (a, b) = (SELECT x, y FROM t), we transform it to:
                            //   a = (SELECT x FROM (SELECT x, y FROM t))
                            //   b = (SELECT y FROM (SELECT x, y FROM t))
                            //
                            // This creates N separate scalar subqueries, each selecting one column from the result.

                            // Get the projection list from the query
                            let projection = if let SetExpr::Select(select) = query.body.as_ref() {
                                &select.projection
                            } else {
                                return plan_err!(
                                    "Tuple assignment with subquery requires a SELECT statement"
                                );
                            };

                            // Validate that the subquery returns the expected number of columns
                            if projection.len() != columns.len() {
                                return plan_err!(
                                    "Tuple assignment mismatch: {} columns but subquery returns {} columns",
                                    columns.len(),
                                    projection.len()
                                );
                            }

                            // For each target column, create a scalar subquery that selects just that column
                            (0..columns.len())
                                .map(|idx| {
                                    // Build a new query that wraps the original and selects just column idx
                                    // SELECT projection[idx] FROM (original_query) AS __tmp
                                    let mut wrapper_query = (**query).clone();

                                    // Modify the query to select only the idx-th column from the projection
                                    if let SetExpr::Select(select) = wrapper_query.body.as_mut() {
                                        // Replace the projection with just the idx-th item
                                        select.projection = vec![projection[idx].clone()];
                                    }

                                    SQLExpr::Subquery(Box::new(wrapper_query))
                                })
                                .collect()
                        }
                        other => {
                            return plan_err!(
                                "Expected tuple value for tuple assignment, got: {:?}",
                                other
                            );
                        }
                    };

                    // Validate counts match
                    if columns.len() != values.len() {
                        return plan_err!(
                            "Tuple assignment mismatch: {} columns but {} values",
                            columns.len(),
                            values.len()
                        );
                    }

                    // Add each column-value pair
                    for (col, val) in columns.into_iter().zip(values.into_iter()) {
                        if assign_map.contains_key(&col) {
                            return plan_err!("Column '{}' assigned more than once", col);
                        }
                        assign_map.insert(col, val);
                    }
                }
            }
        }

        // Build scan, join with from table if it exists.
        let mut input_tables = vec![table];
        input_tables.extend(from);
        let scan = self.plan_from_tables(input_tables, &mut planner_context)?;

        // Filter
        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr(
                    predicate_expr,
                    scan.schema(),
                    &mut planner_context,
                )?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[scan.schema()]],
                    &[using_columns],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        // Build updated values for each column, using the previous value if not modified
        let exprs = table_schema
            .iter()
            .map(|(qualifier, field)| {
                let expr = match assign_map.remove(field.name()) {
                    Some(new_value) => {
                        let mut expr = self.sql_to_expr(
                            new_value,
                            source.schema(),
                            &mut planner_context,
                        )?;
                        // Update placeholder's datatype to the type of the target column
                        if let Expr::Placeholder(placeholder) = &mut expr {
                            placeholder.field = placeholder
                                .field
                                .take()
                                .or_else(|| Some(Arc::clone(field)));
                        }
                        // Cast to target column type, if necessary
                        expr.cast_to(field.data_type(), source.schema())?
                    }
                    None => {
                        // If the target table has an alias, use it to qualify the column name
                        if let Some(alias) = &table_alias {
                            Expr::Column(Column::new(
                                Some(self.ident_normalizer.normalize(alias.name.clone())),
                                field.name(),
                            ))
                        } else {
                            Expr::Column(Column::from((qualifier, field)))
                        }
                    }
                };
                Ok(expr.alias(field.name()))
            })
            .collect::<Result<Vec<_>>>()?;

        let source = project(source, exprs)?;

        let plan = LogicalPlan::Dml(DmlStatement::new(
            table_name,
            table_source,
            WriteOp::Update,
            Arc::new(source),
        ));
        Ok(plan)
    }

    fn insert_to_plan(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
        overwrite: bool,
        replace_into: bool,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_name.clone())?;
        let table_schema = DFSchema::try_from(table_source.schema())?;

        // Get insert fields and target table's value indices
        //
        // If value_indices[i] = Some(j), it means that the value of the i-th target table's column is
        // derived from the j-th output of the source.
        //
        // If value_indices[i] = None, it means that the value of the i-th target table's column is
        // not provided, and should be filled with a default value later.
        let (fields, value_indices) = if columns.is_empty() {
            // Empty means we're inserting into all columns of the table
            (
                table_schema.fields().clone(),
                (0..table_schema.fields().len())
                    .map(Some)
                    .collect::<Vec<_>>(),
            )
        } else {
            let mut value_indices = vec![None; table_schema.fields().len()];
            let fields = columns
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let c = self.ident_normalizer.normalize(c);
                    let column_index = table_schema
                        .index_of_column_by_name(None, &c)
                        .ok_or_else(|| unqualified_field_not_found(&c, &table_schema))?;

                    if value_indices[column_index].is_some() {
                        return schema_err!(SchemaError::DuplicateUnqualifiedField {
                            name: c,
                        });
                    } else {
                        value_indices[column_index] = Some(i);
                    }
                    Ok(Arc::clone(table_schema.field(column_index)))
                })
                .collect::<Result<Vec<_>>>()?;
            (Fields::from(fields), value_indices)
        };

        // infer types for Values clause... other types should be resolvable the regular way
        let mut prepare_param_data_types = BTreeMap::new();
        if let SetExpr::Values(ast::Values { rows, .. }) = (*source.body).clone() {
            for row in rows.iter() {
                for (idx, val) in row.iter().enumerate() {
                    if let SQLExpr::Value(ValueWithSpan {
                        value: Value::Placeholder(name),
                        span: _,
                    }) = val
                    {
                        let name =
                            name.replace('$', "").parse::<usize>().map_err(|_| {
                                plan_datafusion_err!("Can't parse placeholder: {name}")
                            })? - 1;
                        let field = fields.get(idx).ok_or_else(|| {
                            plan_datafusion_err!(
                                "Placeholder ${} refers to a non existent column",
                                idx + 1
                            )
                        })?;
                        let _ = prepare_param_data_types.insert(name, Arc::clone(field));
                    }
                }
            }
        }
        let prepare_param_data_types = prepare_param_data_types.into_values().collect();

        // Projection
        // Create a new context with INSERT-specific settings, starting from the outer context to inherit CTEs
        let mut planner_context = outer_planner_context
            .clone()
            .with_prepare_param_data_types(prepare_param_data_types);
        planner_context.set_table_schema(Some(DFSchemaRef::new(
            DFSchema::from_unqualified_fields(fields.clone(), Default::default())?,
        )));
        if matches!(source.body.as_ref(), SetExpr::Values(_)) {
            let defaults = fields
                .iter()
                .map(|field| table_source.get_column_default(field.name()).cloned())
                .collect::<Vec<_>>();
            planner_context.set_values_defaults(Some(defaults));
        }
        let source = self.query_to_plan(*source, &mut planner_context)?;
        if fields.len() != source.schema().fields().len() {
            plan_err!("Column count doesn't match insert query!")?;
        }

        let exprs = value_indices
            .into_iter()
            .enumerate()
            .map(|(i, value_index)| {
                let target_field = table_schema.field(i);
                let expr = match value_index {
                    Some(v) => {
                        Expr::Column(Column::from(source.schema().qualified_field(v)))
                            .cast_to(target_field.data_type(), source.schema())?
                    }
                    // The value is not specified. Fill in the default value for the column.
                    None => table_source
                        .get_column_default(target_field.name())
                        .cloned()
                        .unwrap_or_else(|| {
                            // If there is no default for the column, then the default is NULL
                            Expr::Literal(ScalarValue::Null, None)
                        })
                        .cast_to(target_field.data_type(), &DFSchema::empty())?,
                };
                Ok(expr.alias(target_field.name()))
            })
            .collect::<Result<Vec<Expr>>>()?;
        let source = project(source, exprs)?;

        let insert_op = match (overwrite, replace_into) {
            (false, false) => InsertOp::Append,
            (true, false) => InsertOp::Overwrite,
            (false, true) => InsertOp::Replace,
            (true, true) => plan_err!(
                "Conflicting insert operations: `overwrite` and `replace_into` cannot both be true"
            )?,
        };

        let plan = LogicalPlan::Dml(DmlStatement::new(
            table_name,
            Arc::clone(&table_source),
            WriteOp::Insert(insert_op),
            Arc::new(source),
        ));
        Ok(plan)
    }

    fn insert_default_values_to_plan(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        overwrite: bool,
        replace_into: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_name.clone())?;
        let table_schema = DFSchema::try_from(table_source.schema())?;

        // For INSERT DEFAULT VALUES, we create a synthetic VALUES clause with DEFAULT for each column
        let fields = if columns.is_empty() {
            table_schema.fields().clone()
        } else {
            let fields: Vec<FieldRef> = columns
                .iter()
                .map(|c| {
                    let c = self.ident_normalizer.normalize(c.clone());
                    let column_index = table_schema
                        .index_of_column_by_name(None, &c)
                        .ok_or_else(|| unqualified_field_not_found(&c, &table_schema))?;
                    Ok(Arc::clone(table_schema.field(column_index)))
                })
                .collect::<Result<Vec<_>>>()?;
            Fields::from(fields)
        };

        // Create a VALUES clause with one DEFAULT identifier for each field
        let default_row: Vec<SQLExpr> = fields
            .iter()
            .map(|_| SQLExpr::Identifier(Ident::new("DEFAULT")))
            .collect();

        let source = Box::new(Query {
            with: None,
            body: Box::new(SetExpr::Values(ast::Values {
                explicit_row: false,
                rows: vec![default_row],
                value_keyword: false,
            })),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            settings: None,
            format_clause: None,
        });

        // Convert TableReference back to ObjectName
        let mut parts = vec![];
        if let Some(catalog_name) = table_name.catalog() {
            parts.push(Ident::new(catalog_name));
        }
        if let Some(schema_name) = table_name.schema() {
            parts.push(Ident::new(schema_name));
        }
        parts.push(Ident::new(table_name.table()));
        let object_name = ObjectName::from(parts);

        self.insert_to_plan(
            object_name,
            columns,
            source,
            overwrite,
            replace_into,
            planner_context,
        )
    }

    fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        sql_table_name: ObjectName,
    ) -> Result<LogicalPlan> {
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        )?;

        if !self.has_table("information_schema", "columns") {
            return plan_err!(
                "SHOW COLUMNS is not supported unless information_schema is enabled"
            );
        }

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        let _ = self.context_provider.get_table_source(table_ref)?;

        // Treat both FULL and EXTENDED as the same
        let select_list = if full || extended {
            "*"
        } else {
            "table_catalog, table_schema, table_name, column_name, data_type, is_nullable"
        };

        let query = format!(
            "SELECT {select_list} FROM information_schema.columns WHERE {where_clause}"
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    /// Rewrite `SHOW FUNCTIONS` to another SQL query
    /// The query is based on the `information_schema.routines` and `information_schema.parameters` tables
    ///
    /// The output columns:
    /// - function_name: The name of function
    /// - return_type: The return type of the function
    /// - parameters: The name of parameters (ordered by the ordinal position)
    /// - parameter_types: The type of parameters (ordered by the ordinal position)
    /// - description: The description of the function (the description defined in the document)
    /// - syntax_example: The syntax_example of the function (the syntax_example defined in the document)
    fn show_functions_to_plan(
        &self,
        filter: Option<ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        let where_clause = if let Some(filter) = filter {
            match filter {
                ShowStatementFilter::Like(like) => {
                    format!("WHERE p.function_name like '{like}'")
                }
                _ => return plan_err!("Unsupported SHOW FUNCTIONS filter"),
            }
        } else {
            "".to_string()
        };

        let query = format!(
            r#"
SELECT DISTINCT
    p.*,
    r.function_type function_type,
    r.description description,
    r.syntax_example syntax_example
FROM
    (
        SELECT
            i.specific_name function_name,
            o.data_type return_type,
            array_agg(i.parameter_name ORDER BY i.ordinal_position ASC) parameters,
            array_agg(i.data_type ORDER BY i.ordinal_position ASC) parameter_types
        FROM (
                 SELECT
                     specific_catalog,
                     specific_schema,
                     specific_name,
                     ordinal_position,
                     parameter_name,
                     data_type,
                     rid
                 FROM
                     information_schema.parameters
                 WHERE
                     parameter_mode = 'IN'
             ) i
                 JOIN
             (
                 SELECT
                     specific_catalog,
                     specific_schema,
                     specific_name,
                     ordinal_position,
                     parameter_name,
                     data_type,
                     rid
                 FROM
                     information_schema.parameters
                 WHERE
                     parameter_mode = 'OUT'
             ) o
             ON i.specific_catalog = o.specific_catalog
                 AND i.specific_schema = o.specific_schema
                 AND i.specific_name = o.specific_name
                 AND i.rid = o.rid
        GROUP BY 1, 2, i.rid
    ) as p
JOIN information_schema.routines r
ON p.function_name = r.routine_name
{where_clause}
            "#
        );
        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    fn show_create_table_to_plan(
        &self,
        sql_table_name: ObjectName,
    ) -> Result<LogicalPlan> {
        if !self.has_table("information_schema", "tables") {
            return plan_err!(
                "SHOW CREATE TABLE is not supported unless information_schema is enabled"
            );
        }
        // Figure out the where clause
        let where_clause = object_name_to_qualifier(
            &sql_table_name,
            self.options.enable_ident_normalization,
        )?;

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(sql_table_name)?;
        let _ = self.context_provider.get_table_source(table_ref)?;

        let query = format!(
            "SELECT table_catalog, table_schema, table_name, definition FROM information_schema.views WHERE {where_clause}"
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap()) // length of rewrite is 1
    }

    /// Return true if there is a table provider available for "schema.table"
    fn has_table(&self, schema: &str, table: &str) -> bool {
        let tables_reference = TableReference::Partial {
            schema: schema.into(),
            table: table.into(),
        };
        self.context_provider
            .get_table_source(tables_reference)
            .is_ok()
    }

    fn validate_transaction_kind(
        &self,
        kind: Option<&BeginTransactionKind>,
    ) -> Result<()> {
        match kind {
            // BEGIN
            None => Ok(()),
            // BEGIN TRANSACTION
            Some(BeginTransactionKind::Transaction) => Ok(()),
            // BEGIN WORK
            Some(BeginTransactionKind::Work) => Ok(()),
        }
    }
}
