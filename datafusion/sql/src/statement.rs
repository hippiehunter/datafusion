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

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::parser::{
    CopyFromStatement, CopyToSource, CopyToStatement, CreateExternalTable, DFParser,
    ExplainStatement, LexOrdering, ResetStatement, Statement as DFStatement,
};
use crate::planner::{
    ContextProvider, PlannerContext, SqlToRel, ValuesAssembly, ValuesDefault,
    object_name_to_qualifier,
};
use crate::utils::normalize_ident;
use crate::values::is_default_identifier;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion_common::error::_plan_err;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, Constraint, Constraints, DFSchema, DFSchemaRef, DataFusionError, MatchType,
    NullsDistinct, ReferentialAction, Result, ScalarValue, SchemaError, SchemaReference,
    TableReference, ToDFSchema, exec_err, not_impl_err, plan_datafusion_err, plan_err,
    schema_err, unqualified_field_not_found,
};
use datafusion_expr::dml::{
    ConflictAssignment, ConflictTarget, CopyFrom, CopyTo, DoUpdateAction, InsertOp,
    OnConflict, OnConflictAction,
};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check;
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::logical_plan::psm::{ParameterMode, ProcedureArg};
use datafusion_expr::logical_plan::{
    DdlStatement, TableScanRowLock, TableScanRowLockMode, TableScanRowLockWaitPolicy,
    build_join_schema,
};
use datafusion_expr::planner::{
    AssignmentStep, DmlViewEvent, PlannerResult, RawAssignmentTarget, ViewDmlError,
};
use datafusion_expr::utils::{expr_to_columns, exprlist_to_fields};
use datafusion_expr::{
    AlterMaterializedView, AlterSequence, Analyze, AnalyzeTable, Call, CreateAssertion,
    CreateCatalog, CreateCatalogSchema, CreateExternalTable as PlanCreateExternalTable,
    CreateFunction, CreateFunctionBody, CreateIndex as PlanCreateIndex,
    CreateMaterializedView, CreateMemoryTable, CreateProcedure, CreatePropertyGraph,
    CreateRole, CreateSequence, CreateTablePartitionBound,
    CreateTablePartitionBoundValue, CreateTablePartitionKey, CreateTablePartitionOf,
    CreateTablePartitioning, CreateTablePartitioningStrategy, CreateView, Deallocate,
    DescribeTable, DmlStatement, DropAssertion, DropCatalogSchema, DropFunction,
    DropIndex, DropMaterializedView, DropPropertyGraph, DropRole, DropSequence,
    DropTable, DropView, EmptyRelation, Execute, Explain, ExplainFormat, Expr,
    ExprSchemable, Filter, Grant, GrantRole, GraphEdgeEndpoint, GraphEdgeTableDefinition,
    GraphKeyClause, GraphPropertiesClause, GraphPropertyDefinition,
    GraphVertexTableDefinition, JoinType, LogicalPlan, LogicalPlanBuilder, Merge,
    MergeAction, MergeAssignment, MergeClause, MergeInsertExpr, MergeInsertKind,
    MergeUpdateExpr, OperateFunctionArg, PlanType, Prepare, RefreshMaterializedView,
    ReleaseSavepoint, ResetVariable, Revoke, RevokeRole, RollbackToSavepoint, Savepoint,
    SetTransaction, SetVariable, SortExpr, Statement as PlanStatement, ToStringifiedPlan,
    TransactionAccessMode, TransactionConclusion, TransactionEnd,
    TransactionIsolationLevel, TransactionStart, TruncateTable, UseDatabase, Vacuum,
    Volatility, WriteOp, cast, col,
};
use datafusion_expr::{Subquery, TableSource};
use sqlparser::ast::{
    self, AstBox as SQLBox, BeginTransactionKind, IndexColumn, IndexType,
    OnConflict as SqlOnConflict, OnConflictAction as SqlOnConflictAction, OnInsert,
    OrderByExpr, OrderByOptions, OverridingKind, Set, ShowStatementIn,
    ShowStatementOptions, TableObject, UpdateTableFromKind, ValueWithSpan,
};

use sqlparser::ast::{
    AccessExpr, Assignment, AssignmentTarget, ColumnDef, ColumnTarget, CreateIndex,
    CreateTable, CreateTableOptions, CreateTableWithData, Delete, DescribeAlias,
    Expr as SQLExpr, ForeignKeyColumnOrPeriod, FromTable, Ident, Insert, ObjectName,
    ObjectType, Query, SchemaName, SelectItem, SetExpr, ShowCreateObject,
    ShowStatementFilter, SqlOption, Statement, Subscript, TableConstraint, TableFactor,
    TableWithJoins, TransactionMode, UnaryOperator, Value,
};
use sqlparser::parser::ParserError::ParserError;

/// Attach the write-intent lock to the target relation before UPDATE/DELETE
/// joins any auxiliary FROM/USING inputs. Descendant expansion then inherits
/// the same target-row contract, while non-target relations remain unlocked.
fn lock_dml_target_scan(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan(mut scan) => {
            scan.row_lock = Some(TableScanRowLock {
                mode: TableScanRowLockMode::ForUpdate,
                wait_policy: TableScanRowLockWaitPolicy::Block,
            });
            Ok(LogicalPlan::TableScan(scan))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let input = lock_dml_target_scan(Arc::unwrap_or_clone(alias.input))?;
            datafusion_expr::SubqueryAlias::try_new(Arc::new(input), alias.alias)
                .map(LogicalPlan::SubqueryAlias)
        }
        other => Ok(other),
    }
}

/// A quoted reference to `field` of the relation `qualifier`, as the
/// expression planner reads one back.
fn column_reference(qualifier: Option<&TableReference>, field: &Field) -> SQLExpr {
    let quoted = |name: &str| Ident::with_quote('"', name);
    let mut parts = Vec::with_capacity(4);
    if let Some(qualifier) = qualifier {
        if let Some(catalog) = qualifier.catalog() {
            parts.push(quoted(catalog));
        }
        if let Some(schema) = qualifier.schema() {
            parts.push(quoted(schema));
        }
        parts.push(quoted(qualifier.table()));
    }
    parts.push(quoted(field.name()));
    if parts.len() == 1 {
        SQLExpr::Identifier(parts.remove(0))
    } else {
        SQLExpr::CompoundIdentifier(parts)
    }
}

fn normalize_graph_properties(
    properties: ast::GraphPropertiesClause,
) -> Result<GraphPropertiesClause> {
    match properties {
        ast::GraphPropertiesClause::AllColumns { except } => {
            Ok(GraphPropertiesClause::AllColumns {
                except: except.into_iter().map(normalize_ident).collect(),
            })
        }
        ast::GraphPropertiesClause::Named(properties) => properties
            .into_iter()
            .map(|property| {
                let ast::Expr::Identifier(column) = property.expression else {
                    return not_impl_err!(
                        "Property graph expression properties are not supported by DataFusion"
                    );
                };
                Ok(GraphPropertyDefinition {
                    column: normalize_ident(column),
                    alias: property.alias.map(normalize_ident),
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(GraphPropertiesClause::Named),
        ast::GraphPropertiesClause::NoProperties => Ok(GraphPropertiesClause::NoProperties),
    }
}

fn ident_to_string(ident: &Ident) -> String {
    normalize_ident(ident.to_owned())
}

/// Convert the `WITH (...)` options of a `CREATE MATERIALIZED VIEW` into
/// a flat string-keyed map. Each option's value is rendered using its
/// sqlparser `Display` impl so the engine layer sees the user's literal
/// without losing escaping; the engine layer is responsible for
/// interpreting the string ("manual", "incremental", "60s", …).
fn mv_with_options_to_map(options: Vec<SqlOption>) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for option in options {
        let name = match option {
            SqlOption::KeyValue { key, value } => {
                let key_str = ident_to_string(&key);
                if out.contains_key(&key_str) {
                    return plan_err!(
                        "duplicate option in CREATE MATERIALIZED VIEW WITH clause: {key_str}"
                    );
                }
                out.insert(key_str, value.to_string());
                continue;
            }
            other => other,
        };
        return not_impl_err!(
            "CREATE MATERIALIZED VIEW WITH option not supported: {name:?}"
        );
    }
    Ok(out)
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

fn select_items_to_column_names(items: &[SelectItem]) -> Vec<String> {
    let names: Vec<String> = items
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => match expr {
                SQLExpr::Identifier(ident) => ident_to_string(ident),
                SQLExpr::CompoundIdentifier(idents) => idents
                    .last()
                    .map(ident_to_string)
                    .unwrap_or_else(|| "*".to_string()),
                _ => "*".to_string(),
            },
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                "*".to_string()
            }
            SelectItem::ExprWithAlias { alias, .. } => ident_to_string(alias),
        })
        .collect();
    // `*` stands for the whole row, over which every other item is then
    // computed by name; naming a column beside it would list it twice.
    if names.iter().any(|name| name == "*") {
        return vec!["*".to_string()];
    }
    names
}

fn returning_clause_items(
    returning: Option<&ast::ReturningClause>,
) -> Result<Option<&[SelectItem]>> {
    let Some(returning) = returning else {
        return Ok(None);
    };
    if returning.bulk_collect || returning.into.is_some() {
        return not_impl_err!("Oracle RETURNING INTO is not supported");
    }
    Ok(Some(&returning.expressions))
}

fn returning_clause_into_items(
    returning: Option<ast::ReturningClause>,
) -> Result<Option<Vec<SelectItem>>> {
    let Some(returning) = returning else {
        return Ok(None);
    };
    if returning.bulk_collect || returning.into.is_some() {
        return not_impl_err!("Oracle RETURNING INTO is not supported");
    }
    Ok(Some(returning.expressions))
}

/// Internal columns Gantry injects into a DML target's source schema (`_rowid`
/// surrogate key for PK-less tables, `ctid` physical row id). They are not
/// user-visible and must be excluded from `RETURNING *` expansion, otherwise a
/// `RETURNING *` on such a target would leak them to the client and desync the
/// output schema from the user-visible column-index resolution.
const GANTRY_HIDDEN_DML_COLUMNS: [&str; 2] = ["_rowid", "ctid"];

fn is_gantry_hidden_dml_column(name: &str) -> bool {
    GANTRY_HIDDEN_DML_COLUMNS.contains(&name)
}

/// Restate every column qualified by `from` under `to`, leaving the rest alone.
fn rename_column_qualifier(expr: Expr, from: &TableReference, to: &str) -> Result<Expr> {
    if from.table() == to {
        return Ok(expr);
    }
    expr.transform(|expr| match expr {
        Expr::Column(column)
            if column
                .relation
                .as_ref()
                .is_some_and(|relation| relation.resolved_eq(from)) =>
        {
            Ok(Transformed::yes(Expr::Column(Column::new(
                Some(TableReference::bare(to)),
                column.name,
            ))))
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}

fn returning_columns_to_output_schema(
    target_schema: &DFSchema,
    returning_cols: &[String],
    metadata: &HashMap<String, String>,
) -> Result<Option<DFSchemaRef>> {
    if returning_cols.is_empty() {
        return Ok(None);
    }

    let mut qualified_fields = Vec::new();
    for col_name in returning_cols {
        if col_name == "*" {
            for idx in 0..target_schema.fields().len() {
                let (qualifier, field) = target_schema.qualified_field(idx);
                if is_gantry_hidden_dml_column(field.name()) {
                    continue;
                }
                qualified_fields.push((qualifier.cloned(), Arc::clone(field)));
            }
            continue;
        }

        let Some(index) = target_schema.index_of_column_by_name(None, col_name) else {
            // Complex RETURNING expression/alias; leave output schema unchanged and let
            // existing DML RETURNING handling proceed as before.
            return Ok(None);
        };

        let (qualifier, field) = target_schema.qualified_field(index);
        qualified_fields.push((qualifier.cloned(), Arc::clone(field)));
    }

    Ok(Some(Arc::new(DFSchema::new_with_metadata(
        qualified_fields,
        metadata.clone(),
    )?)))
}

fn relation_matches_target(
    relation: &TableReference,
    target_table: &TableReference,
    target_alias: Option<&str>,
) -> bool {
    let relation_table = relation.table().to_ascii_lowercase();
    if relation_table == target_table.table().to_ascii_lowercase() {
        return true;
    }
    if let Some(alias) = target_alias {
        return relation_table == alias.to_ascii_lowercase();
    }
    false
}

fn rewrite_update_returning_exprs(
    exprs: Vec<Expr>,
    source_schema: &DFSchema,
    target_column_names: &HashSet<String>,
    target_table: &TableReference,
    target_alias: Option<&str>,
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut passthrough_aliases: HashMap<Column, String> = HashMap::new();
    let mut passthrough_exprs: Vec<Expr> = Vec::new();
    let mut next_passthrough_idx = 0usize;
    let mut rewritten_exprs = Vec::with_capacity(exprs.len());

    for expr in exprs {
        // The output keeps the name the user's expression had; a joined-source
        // column is renamed to its passthrough slot below and must not leak
        // that slot's name into the result set.
        let output_name = match &expr {
            Expr::Alias(alias) => alias.name.clone(),
            Expr::Column(column) => column.name.clone(),
            other => other.schema_name().to_string(),
        };
        let rewritten = expr
            .transform_up(|node| {
                let Expr::Column(column) = node else {
                    return Ok(Transformed::no(node));
                };

                let relation_is_target = column
                    .relation
                    .as_ref()
                    .map(|rel| relation_matches_target(rel, target_table, target_alias))
                    .unwrap_or(false);

                // After UPDATE, RETURNING should observe target-table columns as their
                // post-update values. These live in the projected DML input as unqualified
                // columns named after the target table fields.
                if target_column_names.contains(&column.name)
                    && (column.relation.is_none() || relation_is_target)
                {
                    return Ok(Transformed::yes(Expr::Column(Column::from_name(
                        column.name,
                    ))));
                }

                let resolved_column =
                    match source_schema.qualified_field_from_column(&column) {
                        Ok((qualifier, field)) => Column::from((qualifier, field)),
                        Err(_) => column.clone(),
                    };

                let passthrough_alias = if let Some(alias) =
                    passthrough_aliases.get(&resolved_column)
                {
                    alias.clone()
                } else {
                    let alias = format!("__returning_src_{}", next_passthrough_idx);
                    next_passthrough_idx += 1;
                    passthrough_aliases.insert(resolved_column.clone(), alias.clone());
                    passthrough_exprs
                        .push(Expr::Column(resolved_column).alias(alias.clone()));
                    alias
                };

                Ok(Transformed::yes(Expr::Column(Column::from_name(
                    passthrough_alias,
                ))))
            })?
            .data;
        let rewritten = match rewritten {
            Expr::Alias(alias) => Expr::Alias(alias),
            other if other.schema_name().to_string() == output_name => other,
            other => other.alias(output_name),
        };
        rewritten_exprs.push(rewritten);
    }

    Ok((rewritten_exprs, passthrough_exprs))
}

fn expr_contains_subquery(expr: &Expr) -> Result<bool> {
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
        ))
    })
}

/// A RETURNING expression that contains a subquery is evaluated in the
/// source plan, where subquery unnesting can see it, and reaches RETURNING
/// as one more passthrough column. Returns the RETURNING list with those
/// expressions replaced by their passthrough column (keeping the output
/// name) and the projections to append to the source plan.
fn lift_subquery_returning_exprs(
    exprs: Vec<Expr>,
    first_passthrough_idx: usize,
) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut lifted = Vec::new();
    let mut rewritten = Vec::with_capacity(exprs.len());
    for expr in exprs {
        if !expr_contains_subquery(&expr)? {
            rewritten.push(expr);
            continue;
        }
        let (inner, output_name) = match expr {
            Expr::Alias(alias) => (*alias.expr, alias.name),
            other => {
                let name = other.schema_name().to_string();
                (other, name)
            }
        };
        let passthrough =
            format!("__returning_src_{}", first_passthrough_idx + lifted.len());
        lifted.push(inner.alias(passthrough.clone()));
        rewritten.push(Expr::Column(Column::from_name(passthrough)).alias(output_name));
    }
    Ok((rewritten, lifted))
}

/// Widen `source` with `lifted` expressions computed over its current
/// output, keeping every existing column in place.
fn project_with_lifted_exprs(
    source: LogicalPlan,
    lifted: Vec<Expr>,
) -> Result<LogicalPlan> {
    if lifted.is_empty() {
        return Ok(source);
    }
    // The lifted expressions read the projected row by name. Folding them
    // into that projection keeps the DML input one projection over its
    // relation, the shape the mutation lowering reads assignments from.
    if let LogicalPlan::Projection(projection) = source {
        let outputs: HashMap<String, Expr> = projection
            .expr
            .iter()
            .map(|expr| {
                let inner = match expr {
                    Expr::Alias(alias) => alias.expr.as_ref().clone(),
                    other => other.clone(),
                };
                (expr.schema_name().to_string(), inner)
            })
            .collect();
        let mut exprs = projection.expr.clone();
        for expr in lifted {
            let folded = expr
                .transform_up(|node| {
                    if let Expr::Column(column) = &node
                        && column.relation.is_none()
                        && let Some(inner) = outputs.get(&column.name)
                    {
                        return Ok(Transformed::yes(inner.clone()));
                    }
                    Ok(Transformed::no(node))
                })?
                .data;
            exprs.push(folded);
        }
        return project(Arc::unwrap_or_clone(projection.input), exprs);
    }
    let mut exprs: Vec<Expr> = source
        .schema()
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
        .collect();
    exprs.extend(lifted);
    project(source, exprs)
}

/// Expand the planned RETURNING items into one expression per output
/// column; `*` and `t.*` cover the target table's user-visible columns.
fn expand_returning_select_exprs(
    prepared: Vec<datafusion_expr::select_expr::SelectExpr>,
    table_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    prepared
        .into_iter()
        .flat_map(|select_expr| match select_expr {
            datafusion_expr::select_expr::SelectExpr::Expression(expr) => {
                vec![Ok(expr)]
            }
            datafusion_expr::select_expr::SelectExpr::Wildcard(_) => table_schema
                .fields()
                .iter()
                .filter(|field| !is_gantry_hidden_dml_column(field.name()))
                .map(|field| Ok(Expr::Column(Column::from_name(field.name()))))
                .collect(),
            datafusion_expr::select_expr::SelectExpr::QualifiedWildcard(qualifier, _) => {
                table_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, _)| {
                        let (q, field) = table_schema.qualified_field(idx);
                        if is_gantry_hidden_dml_column(field.name()) {
                            return None;
                        }
                        if q.map(|q| q.to_string().to_ascii_lowercase())
                            == Some(qualifier.to_string().to_ascii_lowercase())
                        {
                            Some(Ok(Expr::Column(Column::from_name(field.name()))))
                        } else {
                            None
                        }
                    })
                    .collect()
            }
        })
        .collect::<Result<Vec<_>>>()
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

fn multiple_assignments_err<T>(column: &str) -> Result<T> {
    plan_err!("multiple assignments to same column \"{column}\"")
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
                            using: None,
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
                        period_without_overlaps: unique_constraint
                            .period_without_overlaps
                            .clone(),
                        index_details: unique_constraint.index_details.clone(),
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
                            using: None,
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
                        index_details: pk_constraint.index_details.clone(),
                        period_without_overlaps: None,
                    }));
                }
                ast::ColumnOption::ForeignKey(fk_constraint) => {
                    constraints.push(TableConstraint::ForeignKey(ForeignKeyConstraint {
                        name: name.clone(),
                        index_name: fk_constraint.index_name.clone(),
                        columns: vec![ForeignKeyColumnOrPeriod::Column(
                            column.name.clone(),
                        )],
                        foreign_table: fk_constraint.foreign_table.clone(),
                        referred_columns: fk_constraint.referred_columns.clone(),
                        on_delete: fk_constraint.on_delete.clone(),
                        on_update: fk_constraint.on_update.clone(),
                        match_kind: fk_constraint.match_kind.clone(),
                        characteristics: fk_constraint.characteristics,
                        on_delete_columns: fk_constraint.on_delete_columns.clone(),
                    }));
                }
                ast::ColumnOption::Check(check_constraint) => {
                    constraints.push(TableConstraint::Check(CheckConstraint {
                        name: name.clone(),
                        expr: check_constraint.expr.clone(),
                        enforced: check_constraint.enforced,
                        no_inherit: check_constraint.no_inherit,
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
                | ast::ColumnOption::Identity(_)
                | ast::ColumnOption::Srid(_)
                | ast::ColumnOption::Collation(_)
                | ast::ColumnOption::Invisible
                | ast::ColumnOption::NotNullNoInherit
                | ast::ColumnOption::NoInherit
                | ast::ColumnOption::Storage(_)
                | ast::ColumnOption::Compression(_)
                | ast::ColumnOption::ConstraintAttribute(_)
                | ast::ColumnOption::GenericOptions(_) => {}
            }
        }
    }
    constraints
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    fn plan_create_table_partition_bound_value(
        &self,
        value: ast::Expr,
        planner_context: &mut PlannerContext,
    ) -> Result<CreateTablePartitionBoundValue> {
        if let ast::Expr::Identifier(identifier) = &value {
            if identifier.value.eq_ignore_ascii_case("minvalue") {
                return Ok(CreateTablePartitionBoundValue::MinValue);
            }
            if identifier.value.eq_ignore_ascii_case("maxvalue") {
                return Ok(CreateTablePartitionBoundValue::MaxValue);
            }
        }
        let empty_schema = DFSchema::empty();
        self.sql_expr_to_logical_expr(value, &empty_schema, planner_context)
            .map(CreateTablePartitionBoundValue::Expr)
    }

    fn plan_create_table_partition_of(
        &self,
        parent: Option<ObjectName>,
        bound: Option<ast::PartitionBoundSpec>,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<CreateTablePartitionOf>> {
        let Some(parent) = parent else {
            return Ok(None);
        };
        let bound = match bound.ok_or_else(|| {
            DataFusionError::Plan(
                "PARTITION OF is missing its partition bound".to_string(),
            )
        })? {
            ast::PartitionBoundSpec::Range { from, to } => {
                CreateTablePartitionBound::Range {
                    lower: from
                        .into_iter()
                        .map(|value| {
                            self.plan_create_table_partition_bound_value(
                                value,
                                planner_context,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                    upper: to
                        .into_iter()
                        .map(|value| {
                            self.plan_create_table_partition_bound_value(
                                value,
                                planner_context,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            ast::PartitionBoundSpec::List { values } => {
                let empty_schema = DFSchema::empty();
                CreateTablePartitionBound::List {
                    values: values
                        .into_iter()
                        .map(|value| {
                            let values = match value {
                                ast::Expr::Tuple(values) => values,
                                value => vec![value],
                            };
                            values
                                .into_iter()
                                .map(|value| {
                                    self.sql_expr_to_logical_expr(
                                        value,
                                        &empty_schema,
                                        planner_context,
                                    )
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            ast::PartitionBoundSpec::Hash { modulus, remainder } => {
                CreateTablePartitionBound::Hash { modulus, remainder }
            }
            ast::PartitionBoundSpec::Default => CreateTablePartitionBound::Default,
        };
        Ok(Some(CreateTablePartitionOf {
            parent: self.object_name_to_table_reference(parent)?,
            bound,
        }))
    }

    fn plan_create_table_partitioning(
        &self,
        partition_by: Option<ast::PartitionByClause>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<CreateTablePartitioning>> {
        let Some(partition_by) = partition_by else {
            return Ok(None);
        };
        if partition_by.interval.is_some() || !partition_by.partitions.is_empty() {
            return not_impl_err!(
                "Oracle interval/inline partition definitions are not supported"
            );
        }
        let strategy = match partition_by.strategy {
            ast::PartitionStrategy::Range => CreateTablePartitioningStrategy::Range,
            ast::PartitionStrategy::List => CreateTablePartitioningStrategy::List,
            ast::PartitionStrategy::Hash => CreateTablePartitioningStrategy::Hash,
            ast::PartitionStrategy::Reference => {
                return not_impl_err!("reference partitioning is not supported");
            }
        };
        let mut keys = Vec::with_capacity(partition_by.columns.len());
        for key in partition_by.columns {
            let (column_name, sql_expr) = match key.column_or_expr {
                ast::PartitionKeyExpr::Column(identifier) => (
                    Some(identifier.value.clone()),
                    SQLExpr::Identifier(identifier),
                ),
                ast::PartitionKeyExpr::Expr(expr) => (None, expr),
            };
            let expr = self.sql_to_expr(sql_expr, schema, planner_context)?;
            let result_type = expr.get_type(schema)?;
            keys.push(CreateTablePartitionKey {
                column_name,
                expr,
                result_type,
                opclass: key.opclass.map(|name| name.to_string()),
                collation: key.collation.map(|name| name.to_string()),
            });
        }
        Ok(Some(CreateTablePartitioning { strategy, keys }))
    }

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

    /// Generate a logical plan while borrowing an SQL statement.
    ///
    /// The returned plan owns all translated semantic data and never retains
    /// the syntax reference. Query and DML traversal stays borrowed; branches
    /// that require a semantic AST rewrite create an explicit scratch clone.
    pub fn sql_statement_to_plan_ref(
        &self,
        statement: &Statement,
    ) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context_ref(statement, &mut PlannerContext::new())
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan_with_context(
        &self,
        statement: Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.sql_statement_to_plan_with_context_impl(statement, planner_context)
    }

    /// Generate a logical plan with caller-provided context while borrowing
    /// the SQL statement.
    pub fn sql_statement_to_plan_with_context_ref(
        &self,
        statement: &Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match statement {
            Statement::Query(query) => {
                self.query_to_plan_ref(query.as_ref(), planner_context)
            }
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                table_name,
                ..
            } => self.describe_table_to_plan(table_name.clone()),
            Statement::Explain {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                statement,
                ..
            } => match statement.as_ref() {
                Statement::Query(query) => {
                    self.describe_query_to_plan_ref(query.as_ref())
                }
                _ => {
                    not_impl_err!("Describing statements other than SELECT not supported")
                }
            },
            Statement::Insert(insert) => {
                self.insert_statement_to_plan_ref(insert, planner_context)
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
                    return plan_err!("DELETE <TABLE> not supported");
                }
                if !order_by.is_empty() {
                    return plan_err!("Delete-order-by clause not yet supported");
                }
                if limit.is_some() {
                    return plan_err!("Delete-limit clause not yet supported");
                }
                let table = self.get_delete_target_ref(from)?;
                self.delete_to_plan_ref(
                    table,
                    using.as_deref(),
                    selection.as_ref(),
                    returning_clause_items(returning.as_ref())?,
                    planner_context,
                )
            }
            Statement::Update(update) => {
                if update.error_logging.is_some() {
                    return not_impl_err!("Oracle DML error logging is not supported");
                }
                let from_clauses = update.from.as_ref().map(|from| match from {
                    UpdateTableFromKind::AfterSet(from_clauses) => {
                        from_clauses.as_slice()
                    }
                });
                if from_clauses.is_some_and(|from| from.len() > 1) {
                    return plan_err!(
                        "Multiple tables in UPDATE SET FROM not yet supported"
                    );
                }
                if update.limit.is_some() {
                    return not_impl_err!("Update-limit clause not supported");
                }
                self.update_to_plan_ref(
                    &update.table,
                    &update.assignments,
                    from_clauses.and_then(|from| from.first()),
                    update.selection.as_ref(),
                    returning_clause_items(update.returning.as_ref())?,
                    planner_context,
                )
            }
            // Statement families whose DataFusion logical nodes intentionally
            // retain parser AST payloads detach at this explicit boundary.
            _ => self.sql_statement_to_plan_with_context_impl(
                statement.clone(),
                planner_context,
            ),
        }
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
            } => match SQLBox::into_owned(statement) {
                Statement::Query(query) => {
                    self.describe_query_to_plan(SQLBox::into_owned(query))
                }
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
                let statement =
                    DFStatement::Statement(Box::new(SQLBox::into_owned(statement)));
                self.explain_to_plan(verbose, analyze, format, statement)
            }
            Statement::Query(query) => {
                self.query_to_plan(SQLBox::into_owned(query), planner_context)
            }
            Statement::ShowVariable { variable, .. } => {
                self.show_variable_to_plan(&variable)
            }
            Statement::Set(statement) => self.set_statement_to_plan(statement.inner),
            Statement::CreateTable(CreateTable {
                temporary,
                external,
                global,
                volatile,
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
                inherits,
                table_options,
                dynamic,
                version,
                unlogged,
                column_aliases,
                like_elements,
                execute,
                with_data,
                partition_by,
                partition_of,
                partition_bound,
                ..
            }) => {
                if external {
                    return not_impl_err!("External tables not supported")?;
                }
                if unlogged {
                    return not_impl_err!("UNLOGGED tables are not supported");
                }
                if !column_aliases.is_empty() {
                    return not_impl_err!(
                        "CREATE TABLE ... AS with a column name list is not supported"
                    );
                }
                if !like_elements.is_empty() {
                    return not_impl_err!("LIKE not supported");
                }
                if execute.is_some() {
                    return not_impl_err!("CREATE TABLE ... AS EXECUTE is not supported");
                }
                if global.is_some() {
                    return not_impl_err!("Global tables not supported")?;
                }
                if volatile {
                    return not_impl_err!("Volatile tables not supported")?;
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
                if dynamic {
                    return not_impl_err!("Dynamic tables not supported")?;
                }
                if version.is_some() {
                    return not_impl_err!("Version not supported")?;
                }
                let mut storage_parameters = match table_options {
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
                if let Some(on_commit) = on_commit {
                    if !temporary {
                        return plan_err!(
                            "ON COMMIT can only be used on temporary tables"
                        );
                    }
                    let on_commit_value = match on_commit {
                        ast::OnCommit::PreserveRows => "preserve_rows",
                        ast::OnCommit::DeleteRows => "delete_rows",
                        ast::OnCommit::Drop => "drop",
                    };
                    // Internal marker consumed by downstream planners.
                    storage_parameters.insert(
                        "__dbl_on_commit".to_string(),
                        on_commit_value.to_string(),
                    );
                }
                // Merge inline constraints and existing constraints
                let mut all_constraints = constraints;
                let inline_constraints = calc_inline_constraints_from_columns(&columns);
                all_constraints.extend(inline_constraints);
                // Build column default values
                let column_defaults =
                    self.build_column_defaults(&columns, planner_context)?;

                let has_columns = !columns.is_empty();
                let schema = self.build_schema(columns)?.to_dfschema_ref()?;
                // A subpartition declaration inherits its columns from the
                // parent. Plan its typed partition keys against that parent
                // schema while preserving the empty local CREATE input for
                // the downstream inheritance merge.
                let partition_schema = if schema.fields().is_empty()
                    && let Some(parent) = partition_of.as_ref()
                {
                    let parent_ref =
                        self.object_name_to_table_reference(parent.clone())?;
                    let parent_source =
                        self.context_provider.get_table_source(parent_ref.clone())?;
                    Arc::new(DFSchema::try_from_qualified_schema(
                        parent_ref,
                        parent_source.schema().as_ref(),
                    )?)
                } else {
                    Arc::clone(&schema)
                };
                let partitioning = self.plan_create_table_partitioning(
                    partition_by,
                    partition_schema.as_ref(),
                    planner_context,
                )?;
                let partition_of = self.plan_create_table_partition_of(
                    partition_of,
                    partition_bound,
                    planner_context,
                )?;
                let inherits = inherits
                    .unwrap_or_default()
                    .into_iter()
                    .map(|parent| self.object_name_to_table_reference(parent))
                    .collect::<Result<Vec<_>>>()?;
                if has_columns {
                    planner_context.set_table_schema(Some(Arc::clone(&schema)));
                }

                match query {
                    Some(query) => {
                        let plan = self
                            .query_to_plan(SQLBox::into_owned(query), planner_context)?;
                        // WITH NO DATA still plans the query so the created
                        // relation receives its names and types, but exposes a
                        // zero-row input to the CTAS executor.
                        let plan = if with_data == Some(CreateTableWithData::WithNoData) {
                            LogicalPlanBuilder::from(plan).limit(0, Some(0))?.build()?
                        } else {
                            plan
                        };
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
                                partitioning: partitioning.clone(),
                                partition_of: partition_of.clone(),
                                inherits: inherits.clone(),
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
                                partitioning,
                                partition_of,
                                inherits,
                            },
                        )))
                    }
                }
            }
            Statement::CreateView(view) => {
                if view
                    .oracle
                    .as_ref()
                    .is_some_and(|oracle| !oracle.is_empty())
                {
                    return not_impl_err!("Oracle CREATE VIEW options are not supported");
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

                let mut plan = self.query_to_plan(
                    SQLBox::into_owned(view.query),
                    &mut PlannerContext::new(),
                )?;
                plan = self.apply_expr_alias(plan, columns)?;

                if view.materialized {
                    let with_options = match view.options {
                        CreateTableOptions::With(opts) => mv_with_options_to_map(opts)?,
                        CreateTableOptions::None => Default::default(),
                        other => {
                            return not_impl_err!(
                                "CREATE MATERIALIZED VIEW with options form not supported: {other:?}"
                            );
                        }
                    };
                    return Ok(LogicalPlan::Ddl(DdlStatement::CreateMaterializedView(
                        CreateMaterializedView {
                            name: self.object_name_to_table_reference(view.name)?,
                            input: Arc::new(plan),
                            or_replace: view.or_replace,
                            if_not_exists: false,
                            definition: Some(sql),
                            with_options,
                        },
                    )));
                }

                Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                    name: self.object_name_to_table_reference(view.name)?,
                    input: Arc::new(plan),
                    or_replace: view.or_replace,
                    if_not_exists: false,
                    definition: Some(sql),
                    temporary: false,
                })))
            }
            Statement::RefreshMaterializedView {
                name,
                concurrently,
                method,
                with_data,
            } => {
                if with_data == Some(false) {
                    return not_impl_err!(
                        "REFRESH MATERIALIZED VIEW ... WITH NO DATA is not supported"
                    );
                }
                Ok(LogicalPlan::Ddl(DdlStatement::RefreshMaterializedView(
                    RefreshMaterializedView {
                        name: self.object_name_to_table_reference(name)?,
                        concurrently,
                        method,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    },
                )))
            }
            Statement::AlterMaterializedView {
                name,
                operation,
                if_exists,
            } => {
                if if_exists {
                    return not_impl_err!(
                        "ALTER MATERIALIZED VIEW IF EXISTS is not supported"
                    );
                }
                Ok(LogicalPlan::Ddl(DdlStatement::AlterMaterializedView(
                    AlterMaterializedView {
                        name: self.object_name_to_table_reference(name)?,
                        operation,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    },
                )))
            }
            Statement::AlterTable(mut alter_table) => {
                let mut operations = Vec::with_capacity(alter_table.operations.len());
                for operation in alter_table.operations {
                    use ast::AlterColumnOperation;
                    use ast::AlterTableOperation;

                    match operation {
                        AlterTableOperation::AddColumn {
                            column_position, ..
                        } if column_position.is_some() => {
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
            Statement::CreateSequence {
                temporary,
                if_not_exists,
                name,
                data_type,
                sequence_options,
                owned_by,
                ..
            } => Ok(LogicalPlan::Ddl(DdlStatement::CreateSequence(
                CreateSequence {
                    name,
                    temporary,
                    if_not_exists,
                    data_type,
                    sequence_options,
                    owned_by,
                },
            ))),
            Statement::AlterSequence {
                name,
                if_exists,
                sequence_options,
                owned_by,
                ..
            } => Ok(LogicalPlan::Ddl(DdlStatement::AlterSequence(
                AlterSequence {
                    name,
                    if_exists,
                    sequence_options,
                    owned_by,
                },
            ))),
            Statement::CreateAssertion(ast::CreateAssertion { name, expr, .. }) => Ok(
                LogicalPlan::Ddl(DdlStatement::CreateAssertion(CreateAssertion {
                    name,
                    expr: Box::new(SQLBox::into_owned(expr)),
                })),
            ),
            Statement::DropAssertion(ast::DropAssertion {
                name, if_exists, ..
            }) => Ok(LogicalPlan::Ddl(DdlStatement::DropAssertion(
                DropAssertion { name, if_exists },
            ))),
            // SQL/MED (Management of External Data) statements
            Statement::CreateServer(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::CreateServer(stmt)))
            }
            Statement::AlterServer(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::AlterServer(stmt)))
            }
            Statement::DropServer(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::DropServer(stmt)))
            }
            Statement::CreateForeignDataWrapper(stmt) => Ok(LogicalPlan::Ddl(
                DdlStatement::CreateForeignDataWrapper(stmt),
            )),
            Statement::AlterForeignDataWrapper(stmt) => Ok(LogicalPlan::Ddl(
                DdlStatement::AlterForeignDataWrapper(stmt),
            )),
            Statement::DropForeignDataWrapper(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::DropForeignDataWrapper(stmt)))
            }
            Statement::CreateForeignTable(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::CreateForeignTable(stmt)))
            }
            Statement::AlterForeignTable(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::AlterForeignTable(stmt)))
            }
            Statement::DropForeignTable(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::DropForeignTable(stmt)))
            }
            Statement::CreateUserMapping(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::CreateUserMapping(stmt)))
            }
            Statement::AlterUserMapping(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::AlterUserMapping(stmt)))
            }
            Statement::DropUserMapping(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::DropUserMapping(stmt)))
            }
            Statement::ImportForeignSchema(stmt) => {
                Ok(LogicalPlan::Ddl(DdlStatement::ImportForeignSchema(stmt)))
            }
            Statement::ShowCreate {
                obj_type, obj_name, ..
            } => match obj_type {
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
                        return Err(ParserError("Missing table name.".to_string()).into());
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
                    ObjectType::MaterializedView => Ok(LogicalPlan::Ddl(
                        DdlStatement::DropMaterializedView(DropMaterializedView {
                            name,
                            if_exists,
                            cascade,
                            schema: DFSchemaRef::new(DFSchema::empty()),
                        }),
                    )),
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
                    ObjectType::Sequence => {
                        Ok(LogicalPlan::Ddl(DdlStatement::DropSequence(DropSequence {
                            name: object_name,
                            if_exists,
                            cascade,
                            restrict,
                            purge,
                            temporary,
                            table,
                        })))
                    }
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
                            return plan_err!(
                                "DROP INDEX requires a non-empty index name"
                            );
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
                    SQLBox::into_owned(statement),
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
                    all_parameters.push(self.sql_to_expr(
                        expr,
                        &empty_schema,
                        planner_context,
                    )?);
                }
                for expr_with_alias in using {
                    all_parameters.push(self.sql_to_expr(
                        expr_with_alias.expr,
                        &empty_schema,
                        planner_context,
                    )?);
                }

                let statement_name = name.ok_or_else(|| {
                    plan_datafusion_err!(
                        "EXECUTE statement requires a prepared statement name"
                    )
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
            } => Ok(LogicalPlan::Statement(PlanStatement::GrantRole(
                GrantRole {
                    roles,
                    grantees,
                    with_admin_option,
                    granted_by,
                },
            ))),
            Statement::RevokeRole {
                roles,
                grantees,
                granted_by,
                cascade,
                admin_option_for,
                ..
            } => Ok(LogicalPlan::Statement(PlanStatement::RevokeRole(
                RevokeRole {
                    roles,
                    grantees,
                    granted_by,
                    cascade,
                    admin_option_for,
                },
            ))),

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
                into,
                columns,
                column_targets,
                overwrite,
                source,
                partitioned,
                after_columns,
                table,
                on,
                returning,
                ignore,
                table_alias,
                replace_into,
                priority,
                insert_alias,
                assignments,
                has_table_keyword,
                overriding,
                error_logging,
                ..
            }) => {
                if error_logging.is_some() {
                    return not_impl_err!("Oracle DML error logging is not supported");
                }
                let table_name = match table {
                    TableObject::TableName(table_name) => table_name,
                    TableObject::TableFunction(_) => {
                        return not_impl_err!(
                            "INSERT INTO Table functions not supported"
                        );
                    }
                };
                if partitioned.is_some() {
                    plan_err!("Partitioned inserts not yet supported")?;
                }
                if !after_columns.is_empty() {
                    plan_err!("After-columns clause not supported")?;
                }
                // ON CONFLICT (PostgreSQL/SQLite) and ON DUPLICATE KEY (MySQL) support
                let on_conflict = match on {
                    Some(OnInsert::OnConflict(conflict)) => Some(conflict),
                    Some(OnInsert::DuplicateKeyUpdate(_)) => {
                        return plan_err!("ON DUPLICATE KEY UPDATE not supported");
                    }
                    Some(other) => {
                        return plan_err!("Unsupported INSERT ON clause: {other:?}");
                    }
                    None => None,
                };
                if ignore {
                    plan_err!("Insert-ignore clause not supported")?;
                }
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
                // optional keywords don't change behavior
                let _ = into;
                let _ = has_table_keyword;

                let is_overriding_system =
                    matches!(overriding, Some(OverridingKind::SystemValue));

                // Handle INSERT DEFAULT VALUES
                let mut plan = if source.is_none() {
                    self.insert_default_values_to_plan(
                        table_name,
                        columns,
                        overwrite,
                        replace_into,
                        on_conflict,
                        table_alias.as_ref(),
                        planner_context,
                    )?
                } else {
                    self.insert_to_plan(
                        table_name,
                        columns,
                        column_targets,
                        Box::new(SQLBox::into_owned(source.unwrap())),
                        overwrite,
                        replace_into,
                        on_conflict,
                        returning_clause_into_items(returning)?,
                        table_alias.as_ref(),
                        overriding.as_ref(),
                        planner_context,
                    )?
                };

                if is_overriding_system {
                    if let LogicalPlan::Dml(ref mut dml) = plan {
                        dml.overriding_system_value = true;
                    }
                }

                Ok(plan)
            }
            Statement::Update(update) => {
                if update.error_logging.is_some() {
                    return not_impl_err!("Oracle DML error logging is not supported");
                }
                let from_clauses =
                    update.from.map(
                        |update_table_from_kind| match update_table_from_kind {
                            UpdateTableFromKind::AfterSet(from_clauses) => from_clauses,
                        },
                    );
                // TODO: support multiple tables in UPDATE SET FROM
                if from_clauses.as_ref().is_some_and(|f| f.len() > 1) {
                    plan_err!("Multiple tables in UPDATE SET FROM not yet supported")?;
                }
                let update_from = from_clauses.and_then(|mut f| f.pop());
                if update.limit.is_some() {
                    return not_impl_err!("Update-limit clause not supported")?;
                }
                self.update_to_plan(
                    update.table,
                    &update.assignments,
                    update_from,
                    update.selection,
                    returning_clause_into_items(update.returning)?,
                    planner_context,
                )
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

                if !order_by.is_empty() {
                    plan_err!("Delete-order-by clause not yet supported")?;
                }

                if limit.is_some() {
                    plan_err!("Delete-limit clause not yet supported")?;
                }

                let table = self.get_delete_target(from)?;
                self.delete_to_plan(
                    table,
                    using,
                    selection,
                    returning_clause_into_items(returning)?,
                    planner_context,
                )
            }
            Statement::Merge {
                into,
                table,
                source,
                source_joins,
                on,
                clauses,
                output,
                error_logging,
                ..
            } => {
                if error_logging.is_some() {
                    return not_impl_err!("Oracle DML error logging is not supported");
                }
                self.merge_to_plan(
                    into,
                    table,
                    source,
                    source_joins,
                    Box::new(SQLBox::into_owned(on)),
                    clauses,
                    output,
                    planner_context,
                )
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
                        TransactionMode::AccessMode(_)
                        | TransactionMode::Deferrable(_) => None,
                        TransactionMode::IsolationLevel(level) => Some(level),
                    })
                    .next_back()
                    .copied()
                    .unwrap_or(ast::TransactionIsolationLevel::Serializable);
                let access_mode: ast::TransactionAccessMode = modes
                    .iter()
                    .filter_map(|m: &TransactionMode| match m {
                        TransactionMode::AccessMode(mode) => Some(mode),
                        TransactionMode::IsolationLevel(_)
                        | TransactionMode::Deferrable(_) => None,
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
                oracle,
                ..
            } => {
                if let Some(modifier) = modifier {
                    return not_impl_err!("COMMIT {modifier} not supported");
                };
                if oracle.is_some() {
                    return not_impl_err!("Oracle COMMIT options are not supported");
                }
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
            Statement::Rollback {
                chain, savepoint, ..
            } => {
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
                            planner_context.add_psm_variable(
                                &param_name.value,
                                arg.data_type.clone(),
                            )?;
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
                                let psm_body = self
                                    .plan_psm_block(&begin_end, &mut planner_context)?;
                                let statement =
                                    DdlStatement::CreateFunction(CreateFunction {
                                        or_replace,
                                        temporary,
                                        name,
                                        return_type: return_type
                                            .map(|f| f.data_type().clone()),
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
                                )?;
                            }
                            ast::CreateFunctionBody::AsObjectFileLinkSymbol { .. } => {
                                return not_impl_err!(
                                    "AS 'obj_file', 'link_symbol' function bodies are not supported"
                                );
                            }
                            ast::CreateFunctionBody::BeginAtomic(_) => {
                                return not_impl_err!(
                                    "BEGIN ATOMIC function bodies are not supported"
                                );
                            }
                            ast::CreateFunctionBody::Multiple(_) => {
                                return plan_err!(
                                    "duplicate function body specifications"
                                );
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
                if truncate.oracle_storage.is_some() {
                    return not_impl_err!(
                        "Oracle TRUNCATE storage options are not supported"
                    );
                }
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
                                    Some(ast::ArgMode::Variadic) => {
                                        ParameterMode::Variadic
                                    }
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
                            planner_context.add_psm_variable(
                                &param_name.value,
                                arg.data_type.clone(),
                            )?;
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
                                    ast::FunctionArgExpr::Query(_) => {
                                        return not_impl_err!(
                                            "Subquery argument in CALL is not supported"
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
                                    ast::FunctionArgExpr::Query(_) => {
                                        return not_impl_err!(
                                            "Subquery argument in CALL is not supported"
                                        );
                                    }
                                },
                                other => {
                                    return not_impl_err!(
                                        "Unsupported CALL argument type: {other:?}"
                                    );
                                }
                            };
                            let planned = self.sql_to_expr(
                                sql_expr,
                                &schema,
                                &mut planner_context,
                            )?;
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
            Statement::CreatePropertyGraph(create_property_graph) => {
                let name = self
                    .object_name_to_table_reference(create_property_graph.name.clone())?;

                let vertex_tables = create_property_graph
                    .vertex_tables
                    .into_iter()
                    .map(|vt| {
                        Ok(GraphVertexTableDefinition {
                            table: self.object_name_to_table_reference(vt.table)?,
                            key: vt.key.map(|k| GraphKeyClause {
                                columns: k
                                    .columns
                                    .into_iter()
                                    .map(|c| normalize_ident(c))
                                    .collect(),
                            }),
                            label: vt.label.map(|l| normalize_ident(l)),
                            properties: vt
                                .properties
                                .map(normalize_graph_properties)
                                .transpose()?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let edge_tables = create_property_graph
                    .edge_tables
                    .into_iter()
                    .map(|et| {
                        Ok(GraphEdgeTableDefinition {
                            table: self.object_name_to_table_reference(et.table)?,
                            source: GraphEdgeEndpoint {
                                key: et.source.key.map(|k| GraphKeyClause {
                                    columns: k
                                        .columns
                                        .into_iter()
                                        .map(|c| normalize_ident(c))
                                        .collect(),
                                }),
                                references: self.object_name_to_table_reference(
                                    et.source.references,
                                )?,
                                referenced_columns: et.source.referenced_columns.map(
                                    |columns| {
                                        columns.into_iter().map(normalize_ident).collect()
                                    },
                                ),
                            },
                            destination: GraphEdgeEndpoint {
                                key: et.destination.key.map(|k| GraphKeyClause {
                                    columns: k
                                        .columns
                                        .into_iter()
                                        .map(|c| normalize_ident(c))
                                        .collect(),
                                }),
                                references: self.object_name_to_table_reference(
                                    et.destination.references,
                                )?,
                                referenced_columns: et
                                    .destination
                                    .referenced_columns
                                    .map(|columns| {
                                        columns.into_iter().map(normalize_ident).collect()
                                    }),
                            },
                            key: et.key.map(|key| GraphKeyClause {
                                columns: key
                                    .columns
                                    .into_iter()
                                    .map(normalize_ident)
                                    .collect(),
                            }),
                            label: et.label.map(|l| normalize_ident(l)),
                            properties: et
                                .properties
                                .map(normalize_graph_properties)
                                .transpose()?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreatePropertyGraph(
                    CreatePropertyGraph {
                        name,
                        or_replace: create_property_graph.or_replace,
                        if_not_exists: create_property_graph.if_not_exists,
                        vertex_tables,
                        edge_tables,
                    },
                )))
            }
            Statement::DropPropertyGraph(drop_property_graph) => {
                let name = self
                    .object_name_to_table_reference(drop_property_graph.name.clone())?;

                Ok(LogicalPlan::Ddl(DdlStatement::DropPropertyGraph(
                    DropPropertyGraph {
                        name,
                        if_exists: drop_property_graph.if_exists,
                        drop_behavior: drop_property_graph.drop_behavior,
                    },
                )))
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

    fn get_delete_target_ref<'a>(
        &self,
        from: &'a FromTable,
    ) -> Result<&'a TableWithJoins> {
        let from = match from {
            FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
        };
        if from.len() != 1 {
            return not_impl_err!(
                "DELETE FROM only supports single table, got {}: {from:?}",
                from.len()
            );
        }
        let table = &from[0];
        if !table.joins.is_empty() {
            return not_impl_err!("DELETE FROM only supports single table, got: joins");
        }
        Ok(table)
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
        self.describe_query_to_plan_ref(&query)
    }

    fn describe_query_to_plan_ref(&self, query: &Query) -> Result<LogicalPlan> {
        let plan = self.query_to_plan_ref(query, &mut PlannerContext::new())?;

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
            // EXCLUDE constraints are not representable as a DataFusion
            // `Constraint`; the embedding engine carries them out-of-band
            // (storage-parameter constraint hints), so drop them here.
            .filter(|c| !matches!(c, TableConstraint::Exclude(_)))
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

                    // Convert sqlparser NullsDistinctOption to DataFusion NullsDistinct
                    let nulls_distinct = match &unique.nulls_distinct {
                        ast::NullsDistinctOption::Distinct => NullsDistinct::Distinct,
                        ast::NullsDistinctOption::NotDistinct => {
                            NullsDistinct::NotDistinct
                        }
                        ast::NullsDistinctOption::None => NullsDistinct::Distinct, // SQL default
                    };

                    Ok(Constraint::Unique {
                        columns: indices,
                        nulls_distinct,
                    })
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
                    let convert_action =
                        |action: Option<ast::ReferentialAction>| match action {
                            None | Some(ast::ReferentialAction::NoAction) => {
                                ReferentialAction::NoAction
                            }
                            Some(ast::ReferentialAction::Restrict) => {
                                ReferentialAction::Restrict
                            }
                            Some(ast::ReferentialAction::Cascade) => {
                                ReferentialAction::Cascade
                            }
                            Some(ast::ReferentialAction::SetNull) => {
                                ReferentialAction::SetNull
                            }
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
                            ForeignKeyColumnOrPeriod::Column(ident) => {
                                ident.value.clone()
                            }
                            ForeignKeyColumnOrPeriod::Period(ident) => {
                                ident.value.clone()
                            }
                        })
                        .collect();
                    let referenced_columns: Vec<String> = fk
                        .referred_columns
                        .iter()
                        .map(|c| match c {
                            ForeignKeyColumnOrPeriod::Column(ident) => {
                                ident.value.clone()
                            }
                            ForeignKeyColumnOrPeriod::Period(ident) => {
                                ident.value.clone()
                            }
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
                TableConstraint::Check(check) => Ok(Constraint::Check {
                    name: check.name.as_ref().map(|n| n.value.clone()),
                    expr: check.expr.to_string(),
                    enforced: check.enforced,
                }),
                TableConstraint::Index { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                // Filtered out above; kept only for match exhaustiveness.
                TableConstraint::Exclude { .. } => {
                    _plan_err!("Exclusion constraints are handled out-of-band")
                }
                TableConstraint::FulltextOrSpatial { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                TableConstraint::Period { .. } => {
                    _plan_err!("PERIOD constraints are not currently supported")
                }
                TableConstraint::NotNull(not_null) => {
                    _plan_err!(
                        "Table-level NOT NULL constraints are not currently supported: {not_null}"
                    )
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
                    let value_string = self.storage_parameter_value_to_string(value)?;
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
            SQLExpr::Value(value) => match crate::utils::value_to_string(&value.value) {
                Some(value_string) => Ok(value_string),
                None => {
                    plan_err!("Unsupported storage parameter value {:?}", value.value)
                }
            },
            SQLExpr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => Ok(format!("+{expr}")),
                UnaryOperator::Minus => Ok(format!("-{expr}")),
                _ => {
                    plan_err!("Unsupported unary op {:?} in storage parameter value", op)
                }
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
        using: Option<Vec<TableWithJoins>>,
        predicate_expr: Option<SQLExpr>,
        returning: Option<Vec<SelectItem>>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.delete_to_plan_ref(
            &table,
            using.as_deref(),
            predicate_expr.as_ref(),
            returning.as_deref(),
            outer_planner_context,
        )
    }

    fn delete_to_plan_ref(
        &self,
        table: &TableWithJoins,
        using: Option<&[TableWithJoins]>,
        predicate_expr: Option<&SQLExpr>,
        returning: Option<&[SelectItem]>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // A delete through an automatically updatable view retargets onto its
        // base relation, restricted to the rows the view shows.
        if let TableFactor::Table { name, .. } = &table.relation
            && let Some(view_target) = self
                .context_provider
                .resolve_dml_view_target(name, DmlViewEvent::Delete)?
        {
            let rewritten = crate::view_dml::rewrite_delete(
                &view_target,
                table,
                predicate_expr,
                returning,
            );
            let plan = self.delete_to_plan_ref(
                &rewritten.table,
                using,
                rewritten.predicate.as_ref(),
                rewritten.returning.as_deref(),
                outer_planner_context,
            )?;
            return crate::view_dml::stamp_check_option(
                plan,
                view_target.check_option_view,
            );
        }
        // Extract table name from the TableWithJoins
        let (table_name, table_alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => (name.clone(), alias.clone()),
            _ => plan_err!("Cannot delete from non-table relation!")?,
        };

        // Do a table lookup to verify the table exists
        let table_ref = self.object_name_to_table_reference(table_name)?;
        let table_source = self.context_provider.get_table_source(table_ref.clone())?;
        let table_schema = DFSchema::try_from(table_source.schema())?;

        // Clone the outer planner context to inherit CTEs
        let mut planner_context = outer_planner_context.clone();

        // Build scan, joining with USING tables if present (similar to UPDATE FROM)
        // This implements PostgreSQL's DELETE ... USING syntax where additional
        // tables can be specified to form joins for the WHERE clause.
        let mut scan = lock_dml_target_scan(
            self.plan_table_with_joins_ref(table, &mut planner_context)?,
        )?;
        if let Some(using_tables) = using {
            let old_outer_from_schema =
                planner_context.set_outer_from_schema(Some(Arc::clone(scan.schema())));
            for using_table in using_tables {
                let right =
                    self.plan_table_with_joins_ref(using_table, &mut planner_context)?;
                scan = LogicalPlanBuilder::from(scan).cross_join(right)?.build()?;
                planner_context.set_outer_from_schema(Some(Arc::clone(scan.schema())));
            }
            planner_context.set_outer_from_schema(old_outer_from_schema);
        }

        let source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr_ref(
                    predicate_expr,
                    scan.schema(),
                    &mut planner_context,
                )?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[scan.schema()]],
                    &[using_columns.into()],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        let mut source = source;
        let mut returning_exprs = None;
        let mut returning_col_names = None;
        let mut returning_output_schema = None;
        if let Some(returning_items) = returning {
            let plain_columns = returning_items.iter().all(|item| {
                matches!(
                    item,
                    SelectItem::UnnamedExpr(SQLExpr::Identifier(_))
                        | SelectItem::Wildcard(_)
                        | SelectItem::QualifiedWildcard(_, _)
                )
            });
            if plain_columns {
                let cols = select_items_to_column_names(returning_items);
                returning_output_schema = returning_columns_to_output_schema(
                    &table_schema,
                    &cols,
                    source.schema().metadata(),
                )?;
                returning_col_names = Some(cols);
            } else {
                // Anything beyond bare target columns is planned as an
                // expression over the deleted row: the row's own columns are
                // projected under their names, and USING-source columns or
                // subqueries ride alongside as passthrough columns.
                let target_alias = table_alias
                    .as_ref()
                    .map(|alias| self.ident_normalizer.normalize(alias.name.clone()));
                // The row is read under the relation it was scanned as; a
                // USING table may carry a column of the same name.
                let mut projected_exprs = table_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let column = match &target_alias {
                            Some(alias) => Expr::Column(Column::new(
                                Some(alias.clone()),
                                field.name(),
                            )),
                            None => Expr::Column(Column::new(
                                Some(table_ref.clone()),
                                field.name(),
                            )),
                        };
                        column.alias(field.name())
                    })
                    .collect::<Vec<_>>();
                let prepared = self.prepare_select_exprs_ref(
                    &source,
                    returning_items,
                    false,
                    &mut planner_context,
                )?;
                let logical_exprs =
                    expand_returning_select_exprs(prepared, &table_schema)?;
                let target_column_names = table_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect::<HashSet<_>>();
                let (rewritten, passthrough_exprs) = rewrite_update_returning_exprs(
                    logical_exprs,
                    source.schema(),
                    &target_column_names,
                    &table_ref,
                    target_alias.as_deref(),
                )?;
                let (rewritten, lifted_exprs) =
                    lift_subquery_returning_exprs(rewritten, passthrough_exprs.len())?;
                projected_exprs.extend(passthrough_exprs);
                source = project(source, projected_exprs)?;
                source = project_with_lifted_exprs(source, lifted_exprs)?;

                let fields = exprlist_to_fields(rewritten.iter(), &source)?;
                let schema = Arc::new(DFSchema::new_with_metadata(
                    fields,
                    source.schema().metadata().clone(),
                )?);
                returning_col_names =
                    Some(schema.fields().iter().map(|f| f.name().clone()).collect());
                returning_output_schema = Some(schema);
                returning_exprs = Some(rewritten);
            }
        }

        let mut dml =
            DmlStatement::new(table_ref, table_source, WriteOp::Delete, Arc::new(source));
        if let Some(ret_cols) = returning_col_names {
            dml = dml.with_returning_columns(ret_cols);
        }
        if let Some(ret_exprs) = returning_exprs {
            dml = dml.with_returning_exprs(ret_exprs);
        }
        if let Some(output_schema) = returning_output_schema {
            dml = dml.with_output_schema(output_schema);
        }
        let plan = LogicalPlan::Dml(dml);
        Ok(plan)
    }

    #[allow(clippy::too_many_arguments)]
    /// Restate one MERGE `INSERT` value row in the target row's own column
    /// order, filling every column the statement does not name from its
    /// declared default (NULL when it has none) — the resolution an omitted
    /// INSERT value takes. `named_columns` empty means the row was written
    /// positionally over the whole target row.
    fn merge_insert_row_in_table_order(
        &self,
        target_schema: &Schema,
        named_columns: &[String],
        values: Vec<Expr>,
        table_source: &dyn TableSource,
        value_schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        let positions: HashMap<&str, usize> = if named_columns.is_empty() {
            if !values.is_empty() && values.len() != target_schema.fields().len() {
                return plan_err!(
                    "MERGE INSERT has {} values but target table has {} columns",
                    values.len(),
                    target_schema.fields().len()
                );
            }
            // A positional row covers the target in its own order; an empty row
            // is DEFAULT VALUES and every column comes from its default.
            target_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(index, _)| *index < values.len())
                .map(|(index, field)| (field.name().as_str(), index))
                .collect()
        } else {
            if named_columns.len() != values.len() {
                return plan_err!(
                    "MERGE INSERT has {} target columns but {} values",
                    named_columns.len(),
                    values.len()
                );
            }
            let mut positions = HashMap::with_capacity(named_columns.len());
            for (index, column) in named_columns.iter().enumerate() {
                if target_schema.index_of(column).is_err() {
                    return plan_err!(
                        "MERGE INSERT column \"{column}\" does not exist in the target table"
                    );
                }
                if positions.insert(column.as_str(), index).is_some() {
                    return plan_err!(
                        "MERGE INSERT column \"{column}\" is specified more than once"
                    );
                }
            }
            positions
        };

        let mut row = Vec::with_capacity(target_schema.fields().len());
        for field in target_schema.fields() {
            // Every value lands in the column's declared type, so an untyped
            // literal such as NULL reaches the writer as that column's value
            // rather than as an unresolved null.
            let expr = match positions.get(field.name().as_str()) {
                Some(index) => values[*index]
                    .clone()
                    .cast_to(field.data_type(), value_schema)?,
                None => table_source
                    .get_column_default(field.name())
                    .cloned()
                    .unwrap_or_else(|| Expr::Literal(ScalarValue::Null, None))
                    .cast_to(field.data_type(), &DFSchema::empty())?,
            };
            row.push(expr);
        }
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_to_plan(
        &self,
        _into: bool,
        table: TableFactor,
        source: TableFactor,
        source_joins: Vec<ast::Join>,
        on: Box<ast::Expr>,
        clauses: Vec<ast::MergeClause>,
        output: Option<ast::OutputClause>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if output.is_some() {
            return not_impl_err!("MERGE OUTPUT/RETURNING not supported");
        }

        // MERGE's target row set feeds both the join and every clause's
        // assignments, so only a view whose namespace and row set are
        // identical to its base relation's may retarget; anything else is
        // refused rather than rewritten wrong. A passthrough view stack has
        // no row restriction, so there is no check-option obligation to
        // record.
        let view_target = match &table {
            TableFactor::Table { name, .. } => self
                .context_provider
                .resolve_dml_view_target(name, DmlViewEvent::Merge)?,
            _ => None,
        };
        let mut table = table;
        if let Some(view_target) = view_target {
            if !crate::view_dml::merge_target_is_passthrough(&view_target) {
                return Err(self.context_provider.dml_view_error(
                    ViewDmlError::MergeNotSupported {
                        view_name: &view_target.view_name,
                    },
                ));
            }
            if let TableFactor::Table { name, .. } = &mut table {
                *name = view_target.base_relation.clone();
            }
        }

        // MERGE insert clauses face the same generated- and identity-column
        // contract as a standalone INSERT.
        let generated_columns = if let TableFactor::Table { name, .. } = &table {
            crate::dml_front::prepare_merge_insert_clauses(
                self.context_provider,
                name,
                &clauses,
            )?
        } else {
            None
        };

        let (table_name, table_alias) = match table {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                only: _,
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
        let target_schema = table_source.schema();
        let mut target_plan =
            LogicalPlanBuilder::scan(table_ref.clone(), Arc::clone(&table_source), None)?
                .build()?;
        if let Some(alias) = table_alias {
            target_plan = self.apply_table_alias(target_plan, alias)?;
        }

        // `USING a JOIN b ON ...` makes the whole join the source relation, so
        // dropping the joins would let rows the join excludes reach the WHEN
        // clauses and be written.
        let source_plan = self.plan_table_with_joins(
            TableWithJoins {
                relation: source,
                joins: source_joins,
            },
            planner_context,
        )?;

        let join_schema = build_join_schema(
            target_plan.schema(),
            source_plan.schema(),
            &JoinType::Inner,
        )?;

        // A clause sees only the rows that exist for its match kind: a NOT
        // MATCHED [BY TARGET] clause has no target row, and a NOT MATCHED BY
        // SOURCE clause has no source row. Resolving their expressions against
        // the whole join would make a name the two sides share ambiguous, and
        // would let a clause read a row that is not there.
        let mut normalize_expr_in =
            |sql_expr: SQLExpr, schema: &DFSchema| -> Result<Expr> {
                let expr = self.sql_to_expr(sql_expr, schema, planner_context)?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&expr, &mut using_columns)?;
                normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[schema]],
                    &[using_columns.into()],
                )
            };

        let on_expr = normalize_expr_in(*on, &join_schema)?;

        let mut merge_clauses = Vec::with_capacity(clauses.len());
        for clause in clauses {
            let clause_schema = match clause.clause_kind {
                ast::MergeClauseKind::NotMatched
                | ast::MergeClauseKind::NotMatchedByTarget => {
                    source_plan.schema().as_ref()
                }
                ast::MergeClauseKind::NotMatchedBySource => target_plan.schema().as_ref(),
                ast::MergeClauseKind::Matched => &join_schema,
            };
            let mut normalize_expr = |sql_expr: SQLExpr| -> Result<Expr> {
                normalize_expr_in(sql_expr, clause_schema)
            };
            let predicate = match clause.predicate {
                Some(predicate) => Some(normalize_expr(predicate)?),
                None => None,
            };

            let action = match clause.action {
                ast::MergeAction::Insert(insert) => {
                    let insert_predicate =
                        insert.where_clause.map(&mut normalize_expr).transpose()?;
                    let overriding = insert.overriding.clone();
                    let named_columns = insert
                        .columns
                        .iter()
                        .map(|ident| self.ident_normalizer.normalize(ident.clone()))
                        .collect::<Vec<_>>();
                    let positional_count = match &insert.kind {
                        ast::MergeInsertKind::DefaultValues => 0,
                        ast::MergeInsertKind::Values(values) => {
                            values.rows.first().map_or(0, Vec::len)
                        }
                        ast::MergeInsertKind::Row => target_schema.fields().len(),
                    };
                    let mut provided_column_names = if named_columns.is_empty() {
                        target_schema
                            .fields()
                            .iter()
                            .take(positional_count)
                            .map(|field| field.name().clone())
                            .collect::<Vec<_>>()
                    } else {
                        named_columns.clone()
                    };
                    if matches!(overriding, Some(ast::OverridingKind::UserValue))
                        && let Some(generated) = &generated_columns
                    {
                        provided_column_names.retain(|column| {
                            !generated.identity.iter().any(|identity| identity == column)
                        });
                    }
                    let kind = match insert.kind {
                        // DEFAULT VALUES is the whole row taken from defaults:
                        // an empty explicit row that the expansion below fills.
                        ast::MergeInsertKind::DefaultValues => {
                            MergeInsertKind::Values(vec![
                                self.merge_insert_row_in_table_order(
                                    &target_schema,
                                    &named_columns,
                                    Vec::new(),
                                    table_source.as_ref(),
                                    clause_schema,
                                )?,
                            ])
                        }
                        ast::MergeInsertKind::Values(values) => {
                            let mut rows = Vec::with_capacity(values.rows.len());
                            for row in values.rows {
                                let mut expr_row = Vec::with_capacity(row.len());
                                for value in row {
                                    expr_row.push(normalize_expr(value)?);
                                }
                                rows.push(self.merge_insert_row_in_table_order(
                                    &target_schema,
                                    &named_columns,
                                    expr_row,
                                    table_source.as_ref(),
                                    clause_schema,
                                )?);
                            }
                            MergeInsertKind::Values(rows)
                        }
                        ast::MergeInsertKind::Row => MergeInsertKind::Row,
                    };
                    // The column list the expansion produced is the target
                    // row's own, in table order.
                    let columns = target_schema
                        .fields()
                        .iter()
                        .map(|field| ObjectName::from(vec![Ident::new(field.name())]))
                        .collect();
                    let provided_columns = provided_column_names
                        .into_iter()
                        .map(|name| ObjectName::from(vec![Ident::new(name)]))
                        .collect();
                    MergeAction::Insert(MergeInsertExpr {
                        columns,
                        provided_columns,
                        overriding,
                        kind,
                        insert_predicate,
                    })
                }
                ast::MergeAction::Update {
                    assignments: update_assignments,
                    where_clause,
                    delete_where,
                } => {
                    let mut assignments = Vec::with_capacity(update_assignments.len());
                    for assignment in update_assignments {
                        let value = normalize_expr(assignment.value)?;
                        assignments.push(MergeAssignment {
                            target: assignment.target,
                            value,
                        });
                    }
                    MergeAction::Update(MergeUpdateExpr {
                        assignments,
                        update_predicate: where_clause
                            .map(&mut normalize_expr)
                            .transpose()?,
                        delete_predicate: delete_where
                            .map(&mut normalize_expr)
                            .transpose()?,
                    })
                }
                ast::MergeAction::Delete { .. } => MergeAction::Delete,
                ast::MergeAction::DoNothing => MergeAction::DoNothing,
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

    /// Plan one assignment through a subscript or field path below a column.
    /// `base` is the column's value before the assignment; the result is its
    /// whole value after it.
    /// The values of a `ROW(...)` on the right of a tuple assignment, a
    /// wildcard argument expanded to the columns of the relation it names.
    fn row_constructor_values<'a>(
        &self,
        function: &'a ast::Function,
        schema: &DFSchema,
    ) -> Result<Vec<Cow<'a, SQLExpr>>> {
        let ast::FunctionArguments::List(list) = &function.args else {
            return plan_err!("ROW() in a tuple assignment takes a value list");
        };
        let mut values = Vec::with_capacity(list.args.len());
        for arg in &list.args {
            match arg {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                    values.push(Cow::Borrowed(expr));
                }
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::QualifiedWildcard(
                    name,
                )) => {
                    let relation = self.object_name_to_table_reference(name.clone())?;
                    let mut expanded = 0;
                    for (qualifier, field) in schema.iter() {
                        if qualifier
                            .is_some_and(|qualifier| qualifier.resolved_eq(&relation))
                        {
                            values.push(Cow::Owned(column_reference(qualifier, field)));
                            expanded += 1;
                        }
                    }
                    if expanded == 0 {
                        return plan_err!("Invalid qualifier {relation}");
                    }
                }
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                    for (qualifier, field) in schema.iter() {
                        values.push(Cow::Owned(column_reference(qualifier, field)));
                    }
                }
                other => {
                    return plan_err!(
                        "Unsupported ROW() argument in a tuple assignment: {other}"
                    );
                }
            }
        }
        Ok(values)
    }

    pub(crate) fn plan_assignment_target(
        &self,
        base: Expr,
        column: FieldRef,
        path: &[AccessExpr],
        value: Expr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut steps = Vec::with_capacity(path.len());
        for access in path {
            steps.push(match access {
                AccessExpr::Subscript(Subscript::Index { index }) => {
                    AssignmentStep::Index(self.sql_to_expr_ref(
                        index,
                        schema,
                        planner_context,
                    )?)
                }
                AccessExpr::Subscript(Subscript::Slice {
                    lower_bound,
                    upper_bound,
                    stride: None,
                }) => AssignmentStep::Slice {
                    lower: lower_bound
                        .as_ref()
                        .map(|bound| self.sql_to_expr_ref(bound, schema, planner_context))
                        .transpose()?,
                    upper: upper_bound
                        .as_ref()
                        .map(|bound| self.sql_to_expr_ref(bound, schema, planner_context))
                        .transpose()?,
                },
                AccessExpr::Dot(SQLExpr::Identifier(ident)) => {
                    AssignmentStep::Field(self.ident_normalizer.normalize(ident.clone()))
                }
                other => {
                    return not_impl_err!(
                        "Assignment target path element is not supported: {other}"
                    );
                }
            });
        }
        let mut target = RawAssignmentTarget {
            base,
            column,
            path: steps,
            value,
        };
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_assignment_target(target, schema)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(original) => target = original,
            }
        }
        not_impl_err!(
            "Assignment to a subscripted or field target is not supported: {}",
            target.column.name()
        )
    }

    fn update_to_plan(
        &self,
        table: TableWithJoins,
        assignments: &[Assignment],
        from: Option<TableWithJoins>,
        predicate_expr: Option<SQLExpr>,
        returning: Option<Vec<SelectItem>>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.update_to_plan_ref(
            &table,
            assignments,
            from.as_ref(),
            predicate_expr.as_ref(),
            returning.as_deref(),
            outer_planner_context,
        )
    }

    fn update_to_plan_ref(
        &self,
        table: &TableWithJoins,
        assignments: &[Assignment],
        from: Option<&TableWithJoins>,
        predicate_expr: Option<&SQLExpr>,
        returning: Option<&[SelectItem]>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // A write on an automatically updatable view retargets onto its base
        // relation before planning, so defaults, hidden row identity and
        // RETURNING all resolve against the base relation. The recursion
        // terminates because the retargeted statement names a base table.
        if let TableFactor::Table { name, .. } = &table.relation
            && let Some(view_target) = self
                .context_provider
                .resolve_dml_view_target(name, DmlViewEvent::Update)?
        {
            let rewritten = crate::view_dml::rewrite_update(
                &view_target,
                table,
                assignments,
                predicate_expr,
                returning,
            )
            .map_err(|failure| {
                self.context_provider
                    .dml_view_error(failure.as_view_error(&view_target.view_name))
            })?;
            let plan = self.update_to_plan_ref(
                &rewritten.table,
                &rewritten.assignments,
                from,
                rewritten.predicate.as_ref(),
                rewritten.returning.as_deref(),
                outer_planner_context,
            )?;
            return crate::view_dml::stamp_check_option(
                plan,
                view_target.check_option_view,
            );
        }
        // Reject writes to generated columns and recompute the stored ones as
        // part of the statement that changes their inputs, so the new row the
        // plan produces — `RETURNING new.*` included — already carries their
        // values.
        let generated_assignments;
        let assignments = if let TableFactor::Table { name, .. } = &table.relation
            && let Some(extended) = crate::dml_front::prepare_update_assignments(
                self.context_provider,
                name,
                assignments,
            )? {
            generated_assignments = extended;
            &generated_assignments[..]
        } else {
            assignments
        };
        let (table_name, table_alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => (name.clone(), alias.clone()),
            _ => plan_err!("Cannot update non-table relation!")?,
        };
        let preserve_view_old = self
            .context_provider
            .dml_view_uses_instead_of_trigger(&table_name, DmlViewEvent::Update);

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
        let mut assign_map: HashMap<String, Cow<'_, SQLExpr>> = HashMap::new();

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

        // Assignments through a subscript or field path, per column in
        // statement order. A column takes either one whole-value assignment
        // or any number of path assignments, which apply in order.
        let mut path_assign_map: HashMap<String, Vec<(Vec<AccessExpr>, &SQLExpr)>> =
            HashMap::new();

        // A dotted target names the column first: `SET fn.first = ..` writes
        // field `first` of column `fn`, as PostgreSQL reads it. A leading
        // name that is not a column (a table alias) is a qualifier instead.
        let split_column_target =
            |name: &ObjectName| -> Result<(String, Vec<AccessExpr>)> {
                let idents = name
                    .0
                    .iter()
                    .map(|part| {
                        part.as_ident().cloned().ok_or_else(|| {
                            plan_datafusion_err!(
                                "Assignment target must be a column name"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let Some(first) = idents.first() else {
                    return plan_err!("Empty column id");
                };
                let first_name = self.ident_normalizer.normalize(first.clone());
                if idents.len() > 1
                    && table_schema
                        .field_with_unqualified_name(&first_name)
                        .is_ok()
                {
                    let fields = idents[1..]
                        .iter()
                        .map(|ident| AccessExpr::Dot(SQLExpr::Identifier(ident.clone())))
                        .collect();
                    return Ok((first_name, fields));
                }
                Ok((extract_column_name(name)?, Vec::new()))
            };

        // Build scan, join with from table if it exists.
        let mut scan = lock_dml_target_scan(
            self.plan_table_with_joins_ref(table, &mut planner_context)?,
        )?;
        if let Some(from) = from {
            let old_outer_from_schema =
                planner_context.set_outer_from_schema(Some(Arc::clone(scan.schema())));
            let right = self.plan_table_with_joins_ref(from, &mut planner_context)?;
            scan = LogicalPlanBuilder::from(scan).cross_join(right)?.build()?;
            planner_context.set_outer_from_schema(old_outer_from_schema);
        }

        // Process each assignment
        for assign in assignments {
            match &assign.target {
                AssignmentTarget::Indirection(target) => {
                    let (col_name, mut path) = split_column_target(&target.column)?;
                    table_schema.field_with_unqualified_name(&col_name)?;
                    if assign_map.contains_key(&col_name) {
                        return multiple_assignments_err(&col_name);
                    }
                    path.extend(target.indirection.iter().cloned());
                    path_assign_map
                        .entry(col_name)
                        .or_default()
                        .push((path, &assign.value));
                }
                AssignmentTarget::ColumnName(cols) => {
                    let (col_name, path) = split_column_target(cols)?;
                    // Validate that the assignment target column exists
                    table_schema.field_with_unqualified_name(&col_name)?;
                    if !path.is_empty() {
                        if assign_map.contains_key(&col_name) {
                            return multiple_assignments_err(&col_name);
                        }
                        path_assign_map
                            .entry(col_name)
                            .or_default()
                            .push((path, &assign.value));
                        continue;
                    }
                    if assign_map.contains_key(&col_name)
                        || path_assign_map.contains_key(&col_name)
                    {
                        return multiple_assignments_err(&col_name);
                    }
                    assign_map.insert(col_name, Cow::Borrowed(&assign.value));
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
                        SQLExpr::Tuple(exprs) => {
                            exprs.iter().map(Cow::Borrowed).collect()
                        }
                        SQLExpr::Nested(inner) => {
                            // Handle ((a, b)) case
                            if let SQLExpr::Tuple(exprs) = inner.as_ref() {
                                exprs.iter().map(Cow::Borrowed).collect()
                            } else {
                                vec![Cow::Borrowed(inner.as_ref())]
                            }
                        }
                        SQLExpr::Function(function)
                            if function.name.to_string().eq_ignore_ascii_case("row") =>
                        {
                            self.row_constructor_values(function, scan.schema())?
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
                            let projection = if let SetExpr::Select(select) =
                                query.body.as_ref()
                            {
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
                                    if let SetExpr::Select(select) =
                                        wrapper_query.body.as_mut()
                                    {
                                        // Replace the projection with just the idx-th item
                                        select.projection = vec![projection[idx].clone()];
                                    }

                                    Cow::Owned(SQLExpr::Subquery(SQLBox::new(
                                        wrapper_query,
                                    )))
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
                        if assign_map.contains_key(&col)
                            || path_assign_map.contains_key(&col)
                        {
                            return multiple_assignments_err(&col);
                        }
                        assign_map.insert(col, val);
                    }
                }
            }
        }

        // Filter
        let mut source = match predicate_expr {
            None => scan,
            Some(predicate_expr) => {
                let filter_expr = self.sql_to_expr_ref(
                    predicate_expr,
                    scan.schema(),
                    &mut planner_context,
                )?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[scan.schema()]],
                    &[using_columns.into()],
                )?;
                LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
            }
        };

        // Build updated values for each column, using the previous value if not modified
        let mut projected_exprs = table_schema
            .iter()
            .map(|(qualifier, field)| {
                let stored_column = || {
                    // If the target table has an alias, use it to qualify the column name
                    if let Some(alias) = &table_alias {
                        Expr::Column(Column::new(
                            Some(self.ident_normalizer.normalize(alias.name.clone())),
                            field.name(),
                        ))
                    } else {
                        Expr::Column(Column::from((qualifier, field)))
                    }
                };
                if let Some(path_assignments) = path_assign_map.remove(field.name()) {
                    let mut base = stored_column();
                    for (path, value) in path_assignments {
                        let value = self.sql_to_expr_ref(
                            value,
                            source.schema(),
                            &mut planner_context,
                        )?;
                        base = self.plan_assignment_target(
                            base,
                            Arc::clone(field),
                            &path,
                            value,
                            source.schema(),
                            &mut planner_context,
                        )?;
                    }
                    let expr = base.cast_to(field.data_type(), source.schema())?;
                    return Ok(expr.alias(field.name()));
                }
                let expr = match assign_map.remove(field.name()) {
                    // `SET col = DEFAULT` writes the column's declared default,
                    // or NULL when it has none — the same resolution an
                    // omitted INSERT value takes.
                    Some(new_value)
                        if matches!(
                            new_value.as_ref(),
                            SQLExpr::Identifier(ident) if is_default_identifier(ident)
                        ) =>
                    {
                        table_source
                            .get_column_default(field.name())
                            .cloned()
                            .unwrap_or_else(|| Expr::Literal(ScalarValue::Null, None))
                            .cast_to(field.data_type(), &DFSchema::empty())?
                    }
                    Some(new_value) => {
                        let mut expr = self.sql_to_expr_ref(
                            new_value.as_ref(),
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
                        match self.context_provider.plan_assignment_coercion(
                            &expr,
                            field,
                            source.schema(),
                        )? {
                            Some(coerced) => coerced,
                            None => expr.cast_to(field.data_type(), source.schema())?,
                        }
                    }
                    None => stored_column(),
                };
                Ok(expr.alias(field.name()))
            })
            .collect::<Result<Vec<_>>>()?;
        if preserve_view_old {
            projected_exprs.extend(table_schema.iter().enumerate().map(
                |(index, (qualifier, field))| {
                    let column = match &table_alias {
                        Some(alias) => Expr::Column(Column::new(
                            Some(self.ident_normalizer.normalize(alias.name.clone())),
                            field.name(),
                        )),
                        None => Expr::Column(Column::from((qualifier, field))),
                    };
                    column.alias(format!("__dbl_view_old_{index}"))
                },
            ));
        }

        let mut returning_exprs = None;
        let mut returning_col_names = None;
        let mut output_schema = None;
        if let Some(returning_items) = returning {
            let prepared = self.prepare_select_exprs_ref(
                &source,
                returning_items,
                false,
                &mut planner_context,
            )?;
            let logical_exprs = expand_returning_select_exprs(prepared, &table_schema)?;

            let target_column_names = table_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<HashSet<_>>();
            let target_alias = table_alias
                .as_ref()
                .map(|alias| self.ident_normalizer.normalize(alias.name.clone()));
            let (rewritten_returning_exprs, passthrough_exprs) =
                rewrite_update_returning_exprs(
                    logical_exprs,
                    source.schema(),
                    &target_column_names,
                    &table_name,
                    target_alias.as_deref(),
                )?;
            let (rewritten_returning_exprs, lifted_exprs) =
                lift_subquery_returning_exprs(
                    rewritten_returning_exprs,
                    passthrough_exprs.len(),
                )?;

            projected_exprs.extend(passthrough_exprs);
            source = project(source, projected_exprs)?;
            source = project_with_lifted_exprs(source, lifted_exprs)?;

            let fields = exprlist_to_fields(rewritten_returning_exprs.iter(), &source)?;
            let returning_output_schema = Arc::new(DFSchema::new_with_metadata(
                fields,
                source.schema().metadata().clone(),
            )?);
            returning_col_names = Some(
                returning_output_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
            );
            output_schema = Some(returning_output_schema);
            returning_exprs = Some(rewritten_returning_exprs);
        } else {
            source = project(source, projected_exprs)?;
        }

        let mut dml = DmlStatement::new(
            table_name,
            table_source,
            WriteOp::Update,
            Arc::new(source),
        );
        if let Some(ret_cols) = returning_col_names {
            dml = dml.with_returning_columns(ret_cols);
        }
        if let Some(ret_exprs) = returning_exprs {
            dml = dml.with_returning_exprs(ret_exprs);
        }
        if let Some(schema) = output_schema {
            dml = dml.with_output_schema(schema);
        }
        let plan = LogicalPlan::Dml(dml);
        Ok(plan)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_statement_to_plan_ref(
        &self,
        insert: &Insert,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if insert.error_logging.is_some() {
            return not_impl_err!("Oracle DML error logging is not supported");
        }
        let table_name = match &insert.table {
            TableObject::TableName(table_name) => table_name,
            TableObject::TableFunction(_) => {
                return not_impl_err!("INSERT INTO table functions not supported");
            }
        };
        if insert.partitioned.is_some() {
            return plan_err!("Partitioned inserts not yet supported");
        }
        if !insert.after_columns.is_empty() {
            return plan_err!("After-columns clause not supported");
        }
        let on_conflict = match &insert.on {
            Some(OnInsert::OnConflict(conflict)) => Some(conflict),
            Some(OnInsert::DuplicateKeyUpdate(_)) => {
                return plan_err!("ON DUPLICATE KEY UPDATE not supported");
            }
            Some(other) => {
                return plan_err!("Unsupported INSERT ON clause: {other:?}");
            }
            None => None,
        };
        if insert.ignore {
            return plan_err!("Insert-ignore clause not supported");
        }
        if let Some(priority) = &insert.priority {
            return plan_err!(
                "Inserts with a `PRIORITY` clause not supported: {priority:?}"
            );
        }
        if insert.insert_alias.is_some() {
            return plan_err!("Inserts with an alias not supported");
        }
        if !insert.assignments.is_empty() {
            return plan_err!("Inserts with assignments not supported");
        }

        let is_overriding_system =
            matches!(insert.overriding, Some(OverridingKind::SystemValue));
        let mut plan = if let Some(source) = &insert.source {
            self.insert_to_plan_ref(
                table_name,
                &insert.columns,
                insert.column_targets.as_deref(),
                source.as_ref(),
                insert.overwrite,
                insert.replace_into,
                on_conflict,
                returning_clause_items(insert.returning.as_ref())?,
                insert.table_alias.as_ref(),
                insert.overriding.as_ref(),
                planner_context,
            )?
        } else {
            self.insert_default_values_to_plan(
                table_name.clone(),
                insert.columns.clone(),
                insert.overwrite,
                insert.replace_into,
                on_conflict.cloned(),
                insert.table_alias.as_ref(),
                planner_context,
            )?
        };

        if is_overriding_system && let LogicalPlan::Dml(dml) = &mut plan {
            dml.overriding_system_value = true;
        }
        Ok(plan)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_to_plan(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        column_targets: Option<Vec<ColumnTarget>>,
        source: Box<Query>,
        overwrite: bool,
        replace_into: bool,
        on_conflict: Option<SqlOnConflict>,
        returning: Option<Vec<SelectItem>>,
        table_alias: Option<&Ident>,
        overriding: Option<&OverridingKind>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.insert_to_plan_ref(
            &table_name,
            &columns,
            column_targets.as_deref(),
            source.as_ref(),
            overwrite,
            replace_into,
            on_conflict.as_ref(),
            returning.as_deref(),
            table_alias,
            overriding,
            outer_planner_context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_to_plan_ref(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        column_targets: Option<&[ColumnTarget]>,
        source: &Query,
        overwrite: bool,
        replace_into: bool,
        on_conflict: Option<&SqlOnConflict>,
        returning: Option<&[SelectItem]>,
        table_alias: Option<&Ident>,
        overriding: Option<&OverridingKind>,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // An insert into an automatically updatable view retargets onto its
        // base relation, with the column list materialized so the base
        // relation's own defaults fill the unwritten columns.
        if let Some(view_target) = self
            .context_provider
            .resolve_dml_view_target(table_name, DmlViewEvent::Insert)?
        {
            let rewritten = crate::view_dml::rewrite_insert(
                &view_target,
                table_name,
                columns,
                Some(source),
                returning,
            )
            .map_err(|failure| {
                self.context_provider
                    .dml_view_error(failure.as_view_error(&view_target.view_name))
            })?;
            let plan = self.insert_to_plan_ref(
                &rewritten.table,
                &rewritten.columns,
                column_targets,
                rewritten.source.as_ref().unwrap_or(source),
                overwrite,
                replace_into,
                on_conflict,
                rewritten.returning.as_deref(),
                table_alias,
                overriding,
                outer_planner_context,
            )?;
            return crate::view_dml::stamp_check_option(
                plan,
                view_target.check_option_view,
            );
        }
        // Generated columns accept only DEFAULT (their value is the
        // generation expression's), a GENERATED ALWAYS identity takes a write
        // only under an OVERRIDING clause, and an ON CONFLICT DO UPDATE
        // recomputes the stored generated columns as part of the statement
        // that changes their inputs.
        crate::dml_front::reject_insert_generated_writes(
            self.context_provider,
            table_name,
            columns,
            Some(source),
            overriding,
        )?;
        let overriding_user_value =
            matches!(overriding, Some(OverridingKind::UserValue));
        let overriding_user_identity_columns = if overriding_user_value {
            self.context_provider
                .dml_generated_columns(table_name)?
                .map(|generated| generated.identity)
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let adapted_conflict = crate::dml_front::prepare_on_conflict_assignments(
            self.context_provider,
            table_name,
            on_conflict,
        )?;
        let on_conflict = match &adapted_conflict {
            Some(conflict) => Some(conflict),
            None => on_conflict,
        };
        // Do a table lookup to verify the table exists
        let table_name = self.object_name_to_table_reference(table_name.clone())?;
        let table_source = self.context_provider.get_table_source(table_name.clone())?;
        let table_schema = DFSchema::try_from(table_source.schema())?;

        // A VALUES position every row leaves to the column's default is not a
        // storage-provided column. Keep that typed contract separately from
        // the parsed source; planning still sees the statement's declared
        // target shape unchanged.
        let positional_columns: Vec<String> = table_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        // A subscript or field target (`f2[1]`, `f3.if1`) never resolves to
        // the column's default: the planner refuses `DEFAULT` in such a slot.
        let written_columns = if column_targets.is_some() {
            None
        } else {
            crate::dml_front::insert_written_columns(columns, source, &positional_columns)
        };

        // Get insert fields and target table's value indices
        //
        // If value_indices[i] = Some(j), it means that the value of the i-th target table's column is
        // derived from the j-th output of the source.
        //
        // If value_indices[i] = None, it means that the value of the i-th target table's column is
        // not provided, and should be filled with a default value later.
        // A column written through several paths is one target column.
        let provided_columns = written_columns.as_deref().unwrap_or(columns);
        let mut target_col_names: Vec<String> = if overriding_user_value
            && written_columns.is_none()
            && columns.is_empty()
        {
            positional_columns.clone()
        } else {
            Vec::with_capacity(provided_columns.len())
        };
        for column in provided_columns {
            let name = self.ident_normalizer.normalize(column.clone());
            if !target_col_names.contains(&name) {
                target_col_names.push(name);
            }
        }
        if overriding_user_value {
            target_col_names.retain(|column| {
                !overriding_user_identity_columns
                    .iter()
                    .any(|identity| identity == column)
            });
        }

        // Per target table column, the source columns feeding it, each with
        // the subscript/field path below the column it writes (`a[2]`,
        // `fn.first`); a whole-column target has an empty path.
        let empty_path: &[AccessExpr] = &[];
        let (fields, value_sources): (Fields, Vec<Vec<(usize, &[AccessExpr])>>) =
            if columns.is_empty() {
                // Empty means we're inserting into all columns of the table
                (
                    table_schema.fields().clone(),
                    (0..table_schema.fields().len())
                        .map(|i| vec![(i, empty_path)])
                        .collect::<Vec<_>>(),
                )
            } else {
                let mut value_sources: Vec<Vec<(usize, &[AccessExpr])>> =
                    vec![Vec::new(); table_schema.fields().len()];
                let fields = columns
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, c)| {
                        let c = self.ident_normalizer.normalize(c);
                        let column_index =
                            table_schema.index_of_column_by_name(None, &c).ok_or_else(
                                || unqualified_field_not_found(&c, &table_schema),
                            )?;
                        let path = column_targets
                            .and_then(|targets| targets.get(i))
                            .map(|target| target.indirection.as_slice())
                            .unwrap_or(empty_path);
                        let sources = &mut value_sources[column_index];
                        // A column may be written through several paths, but
                        // only once as a whole.
                        if path.is_empty() && !sources.is_empty()
                            || sources.iter().any(|(_, p)| p.is_empty())
                        {
                            return schema_err!(SchemaError::DuplicateUnqualifiedField {
                                name: c,
                            });
                        }
                        sources.push((i, path));
                        let field = table_schema.field(column_index);
                        // A value written through an element subscript has the
                        // element's type, and one written through a slice the
                        // column's own. A value written into a field of a
                        // composite has the field's type, which the dialect's
                        // type registry holds rather than this schema, so the
                        // slot is left untyped for the value to type.
                        Ok(match path {
                            [] | [AccessExpr::Subscript(Subscript::Slice { .. })] => {
                                Arc::clone(field)
                            }
                            [AccessExpr::Subscript(Subscript::Index { .. })] => {
                                match field.data_type() {
                                    DataType::List(item)
                                    | DataType::LargeList(item)
                                    | DataType::FixedSizeList(item, _) => Arc::new(
                                        Field::new(
                                            field.name(),
                                            item.data_type().clone(),
                                            true,
                                        )
                                        .with_metadata(field.metadata().clone()),
                                    ),
                                    _ => Arc::clone(field),
                                }
                            }
                            _ => Arc::new(Field::new(field.name(), DataType::Null, true)),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                (Fields::from(fields), value_sources)
            };

        // Adapt source literals to their target columns — canonicalizing a
        // flexible timestamp text literal, or wrapping a value in a dialect
        // conversion — before anything below reads them.
        let adapted_source = crate::dml_front::adapt_insert_source_values(
            self.context_provider,
            source,
            &fields,
        );
        let source = adapted_source.as_ref().unwrap_or(source);

        // infer types for Values clause... other types should be resolvable the regular way
        let mut prepare_param_data_types = BTreeMap::new();
        if let SetExpr::Values(ast::Values { rows, .. }) = source.body.as_ref() {
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
        // A source row is a row of the inserted relation, not of the target
        // table: several of its fields may write parts of one column, so they
        // are named by position, the way a values list names its columns.
        // A values list under such a part target assembles the table's rows
        // itself, so each slot is typed by the part it writes while it is
        // still the written expression; any other source is assembled by a
        // projection over it.
        let assemble_values = matches!(source.body.as_ref(), SetExpr::Values(_))
            && value_sources
                .iter()
                .flatten()
                .any(|(_, path)| !path.is_empty());
        let row_fields = if assemble_values {
            table_schema.fields().clone()
        } else {
            fields.clone()
        };
        let source_fields = Fields::from(
            row_fields
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    Arc::new(field.as_ref().clone().with_name(format!("column{}", i + 1)))
                })
                .collect::<Vec<_>>(),
        );
        planner_context.set_table_schema(Some(DFSchemaRef::new(
            DFSchema::from_unqualified_fields(source_fields, Default::default())?,
        )));
        if matches!(source.body.as_ref(), SetExpr::Values(_)) {
            let mut defaults = vec![ValuesDefault::Column(None); fields.len()];
            for (column_index, sources) in value_sources.iter().enumerate() {
                for (slot, path) in sources {
                    defaults[*slot] = match path.last() {
                        None => ValuesDefault::Column(
                            table_source
                                .get_column_default(
                                    table_schema.field(column_index).name(),
                                )
                                .cloned(),
                        ),
                        Some(AccessExpr::Subscript(_)) => ValuesDefault::Refused(
                            "cannot set an array element to DEFAULT",
                        ),
                        Some(_) => {
                            ValuesDefault::Refused("cannot set a subfield to DEFAULT")
                        }
                    };
                }
            }
            planner_context.set_values_defaults(Some(crate::planner::ValuesDefaults {
                slots: defaults,
                fill_omitted_trailing: columns.is_empty(),
            }));
        }
        if assemble_values {
            planner_context.set_values_assembly(Some(ValuesAssembly {
                fields: table_schema.fields().clone(),
                sources: value_sources
                    .iter()
                    .map(|sources| {
                        sources
                            .iter()
                            .map(|(slot, path)| (*slot, path.to_vec()))
                            .collect()
                    })
                    .collect(),
                defaults: table_schema
                    .fields()
                    .iter()
                    .map(|field| table_source.get_column_default(field.name()).cloned())
                    .collect(),
            }));
        }
        let source = self.query_to_plan_ref(source, &mut planner_context)?;
        let (fields, value_sources) = if assemble_values {
            (
                table_schema.fields().clone(),
                (0..table_schema.fields().len())
                    .map(|i| vec![(i, empty_path)])
                    .collect::<Vec<_>>(),
            )
        } else {
            (fields, value_sources)
        };
        if fields.len() != source.schema().fields().len() {
            plan_err!("Column count doesn't match insert query!")?;
        }

        let exprs = value_sources
            .into_iter()
            .enumerate()
            .map(|(i, sources)| {
                let target_field = table_schema.field(i);
                let expr = match sources.as_slice() {
                    [(v, path)] if path.is_empty() => {
                        let value = Expr::Column(Column::from(
                            source.schema().qualified_field(*v),
                        ));
                        match self.context_provider.plan_assignment_coercion(
                            &value,
                            target_field,
                            source.schema(),
                        )? {
                            Some(coerced) => coerced,
                            None => value
                                .cast_to(target_field.data_type(), source.schema())?,
                        }
                    }
                    // The value is not specified. Fill in the default value for the column.
                    [] => table_source
                        .get_column_default(target_field.name())
                        .cloned()
                        .unwrap_or_else(|| {
                            // If there is no default for the column, then the default is NULL
                            Expr::Literal(ScalarValue::Null, None)
                        })
                        .cast_to(target_field.data_type(), &DFSchema::empty())?,
                    // Written through paths: each applies to the result of the
                    // previous one, starting from a NULL of the column's type.
                    paths => {
                        let mut base = Expr::Literal(ScalarValue::Null, None)
                            .cast_to(target_field.data_type(), &DFSchema::empty())?;
                        for (v, path) in paths {
                            let value = Expr::Column(Column::from(
                                source.schema().qualified_field(*v),
                            ));
                            base = self.plan_assignment_target(
                                base,
                                Arc::clone(target_field),
                                path,
                                value,
                                source.schema(),
                                &mut planner_context,
                            )?;
                        }
                        base.cast_to(target_field.data_type(), source.schema())?
                    }
                };
                Ok(expr.alias(target_field.name()))
            })
            .collect::<Result<Vec<Expr>>>()?;
        let mut source = project(source, exprs)?;

        let insert_op = match (overwrite, replace_into, on_conflict) {
            (false, false, None) => InsertOp::Append,
            (true, false, None) => InsertOp::Overwrite,
            (false, true, None) => InsertOp::Replace,
            (false, false, Some(conflict)) => {
                // Convert sqlparser's OnConflict to DataFusion's OnConflict
                let (planned_conflict, widened_source) = self.plan_on_conflict(
                    conflict,
                    &table_name,
                    table_alias,
                    &table_source,
                    &table_schema,
                    source,
                    outer_planner_context,
                )?;
                source = widened_source;
                InsertOp::WithConflictClause(planned_conflict)
            }
            (true, _, Some(_)) => {
                plan_err!("ON CONFLICT clause cannot be used with INSERT OVERWRITE")?
            }
            (_, true, Some(_)) => {
                plan_err!("ON CONFLICT clause cannot be used with REPLACE INTO")?
            }
            (true, true, None) => plan_err!(
                "Conflicting insert operations: `overwrite` and `replace_into` cannot both be true"
            )?,
        };

        let returning_col_names = returning.map(select_items_to_column_names);
        let returning_output_schema = returning_col_names
            .as_ref()
            .map(|cols| {
                returning_columns_to_output_schema(
                    &table_schema,
                    cols,
                    source.schema().metadata(),
                )
            })
            .transpose()?
            .flatten();

        let mut dml = DmlStatement::new(
            table_name,
            Arc::clone(&table_source),
            WriteOp::Insert(insert_op),
            Arc::new(source),
        );
        if written_columns.is_some() || !columns.is_empty() || overriding_user_value {
            dml = dml.with_target_columns(target_col_names);
        }
        if let Some(ret_cols) = returning_col_names {
            dml = dml.with_returning_columns(ret_cols);
        }
        if let Some(output_schema) = returning_output_schema {
            dml = dml.with_output_schema(output_schema);
        }
        let plan = LogicalPlan::Dml(dml);
        Ok(plan)
    }

    /// Converts a sqlparser OnConflict clause to a DataFusion OnConflict.
    ///
    /// For DO UPDATE SET expressions, we need to plan them in a context that includes
    /// both the target table columns and the EXCLUDED pseudo-table (which contains
    /// the values that would have been inserted).
    /// Plan an `ON CONFLICT` clause against the INSERT `source`. A DO UPDATE
    /// sub-select is computed on the source, which comes back widened by one
    /// column per sub-select.
    #[allow(clippy::too_many_arguments)]
    fn plan_on_conflict(
        &self,
        conflict: &SqlOnConflict,
        table_name: &TableReference,
        table_alias: Option<&Ident>,
        table_source: &Arc<dyn TableSource>,
        table_schema: &DFSchema,
        source: LogicalPlan,
        planner_context: &mut PlannerContext,
    ) -> Result<(OnConflict, LogicalPlan)> {
        let mut source = source;
        // Plan the action
        let action = match &conflict.action {
            SqlOnConflictAction::DoNothing => OnConflictAction::DoNothing,
            SqlOnConflictAction::DoUpdate(do_update) => {
                // Create qualified schemas for both the target table and EXCLUDED pseudo-table.
                // The EXCLUDED table has the same columns as the target table and represents
                // the row that would have been inserted.

                // Qualify the target row with the name the conflict clause
                // sees it under. `INSERT INTO t AS alias` renames the target
                // for the whole statement, so `alias.column` resolves and
                // `t.column` deliberately does not.
                let table_ref = match table_alias {
                    Some(alias) => TableReference::bare(
                        self.ident_normalizer.normalize(alias.clone()),
                    ),
                    None => TableReference::bare(table_name.table()),
                };
                let qualified_table_schema = DFSchema::try_from_qualified_schema(
                    table_ref.clone(),
                    &table_schema.as_arrow().clone(),
                )?;

                let excluded_schema = DFSchema::try_from_qualified_schema(
                    TableReference::bare("excluded"),
                    &table_schema.as_arrow().clone(),
                )?;

                // Build a combined schema for expression planning:
                // target table columns + EXCLUDED columns
                let combined_schema = qualified_table_schema.join(&excluded_schema)?;

                // Plan the assignments
                let mut assignments = Vec::with_capacity(do_update.assignments.len());
                for assignment in &do_update.assignments {
                    let scalar_assignments = match &assignment.target {
                        AssignmentTarget::Indirection(target) => {
                            return not_impl_err!(
                                "Assignment to a subscripted or field target is not supported: {target}"
                            );
                        }
                        AssignmentTarget::ColumnName(_) => {
                            vec![(
                                assignment.target.clone(),
                                Cow::Borrowed(&assignment.value),
                            )]
                        }
                        AssignmentTarget::Tuple(targets) => {
                            let mut value = &assignment.value;
                            while let SQLExpr::Nested(inner) = value {
                                value = inner;
                            }
                            let values: Vec<Cow<'_, SQLExpr>> = match value {
                                SQLExpr::Tuple(values) => {
                                    values.iter().map(Cow::Borrowed).collect()
                                }
                                SQLExpr::Subquery(query) => {
                                    tuple_subquery_column_values(query, targets.len())?
                                }
                                other => {
                                    return plan_err!(
                                        "Expected tuple value for tuple assignment, got: {other}"
                                    );
                                }
                            };
                            if targets.len() != values.len() {
                                return plan_err!(
                                    "Tuple assignment mismatch: {} columns but {} values",
                                    targets.len(),
                                    values.len()
                                );
                            }
                            targets
                                .iter()
                                .cloned()
                                .zip(values)
                                .map(|(target, value)| {
                                    (AssignmentTarget::ColumnName(target), value)
                                })
                                .collect()
                        }
                    };

                    for (target, sql_value) in scalar_assignments {
                        let value = self.sql_to_expr_ref(
                            sql_value.as_ref(),
                            &combined_schema,
                            planner_context,
                        )?;
                        // Normalize column references
                        let mut using_columns = HashSet::new();
                        expr_to_columns(&value, &mut using_columns)?;
                        let value = normalize_col_with_schemas_and_ambiguity_check(
                            value,
                            &[&[&combined_schema]],
                            &[using_columns.into()],
                        )?;

                        assignments.push(ConflictAssignment { target, value });
                    }
                }

                // Plan the optional WHERE clause
                let selection = if let Some(selection) = &do_update.selection {
                    let expr = self.sql_to_expr_ref(
                        selection,
                        &combined_schema,
                        planner_context,
                    )?;
                    let mut using_columns = HashSet::new();
                    expr_to_columns(&expr, &mut using_columns)?;
                    Some(normalize_col_with_schemas_and_ambiguity_check(
                        expr,
                        &[&[&combined_schema]],
                        &[using_columns.into()],
                    )?)
                } else {
                    None
                };

                // A sub-select is computed on the INSERT source and read back
                // as a column of EXCLUDED.
                let excluded_ref = TableReference::bare("excluded");
                let mut hoisted = Vec::new();
                let assignments = assignments
                    .into_iter()
                    .map(|assignment| {
                        Ok(ConflictAssignment {
                            target: assignment.target,
                            value: hoist_conflict_subqueries(
                                assignment.value,
                                &table_ref,
                                &excluded_ref,
                                &mut hoisted,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let selection = selection
                    .map(|selection| {
                        hoist_conflict_subqueries(
                            selection,
                            &table_ref,
                            &excluded_ref,
                            &mut hoisted,
                        )
                    })
                    .transpose()?;
                if !hoisted.is_empty() {
                    let mut exprs: Vec<Expr> = source
                        .schema()
                        .iter()
                        .map(|(qualifier, field)| {
                            Expr::Column(Column::from((qualifier, field)))
                        })
                        .collect();
                    exprs.extend(hoisted);
                    source = project(source, exprs)?;
                }

                // The alias named the target row while the clause was being
                // read; downstream consumers know the row by the relation it
                // belongs to, so restate the planned references under that name.
                let assignments = assignments
                    .into_iter()
                    .map(|assignment| {
                        Ok(ConflictAssignment {
                            target: assignment.target,
                            value: rename_column_qualifier(
                                assignment.value,
                                &table_ref,
                                table_name.table(),
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let selection = selection
                    .map(|selection| {
                        rename_column_qualifier(selection, &table_ref, table_name.table())
                    })
                    .transpose()?;

                OnConflictAction::DoUpdate(DoUpdateAction::new(assignments, selection))
            }
        };

        let conflict_target = match conflict.conflict_target.as_ref() {
            None => None,
            Some(ast::ConflictTarget::Columns(idents)) => Some(ConflictTarget::Columns(
                idents.iter().map(|ident| ident.value.clone()).collect(),
            )),
            Some(ast::ConflictTarget::OnConstraint(name)) => {
                Some(ConflictTarget::OnConstraint(name.to_string()))
            }
            Some(ast::ConflictTarget::Inference(inference)) => {
                let plain_columns = inference.predicate.is_none()
                    && inference.elements.iter().all(|element| {
                        element.collation.is_none()
                            && element.opclass.is_none()
                            && matches!(element.expr, SQLExpr::Identifier(_))
                    });
                if plain_columns {
                    Some(ConflictTarget::Columns(
                        inference
                            .elements
                            .iter()
                            .filter_map(|element| match &element.expr {
                                SQLExpr::Identifier(ident) => Some(ident.value.clone()),
                                _ => None,
                            })
                            .collect(),
                    ))
                } else {
                    // The elements and the predicate are planned over the
                    // table's own columns, the way the table source planned
                    // the keys of its unique indexes, and matched to an index
                    // whose keys are the same set. A partial index arbitrates
                    // only when the clause names its predicate.
                    let inference_schema = DFSchema::try_from(table_source.schema())?;
                    let mut elements = Vec::with_capacity(inference.elements.len());
                    for element in &inference.elements {
                        elements.push(self.sql_to_expr_ref(
                            &element.expr,
                            &inference_schema,
                            planner_context,
                        )?);
                    }
                    let predicate = inference
                        .predicate
                        .as_ref()
                        .map(|predicate| {
                            self.sql_to_expr_ref(
                                predicate,
                                &inference_schema,
                                planner_context,
                            )
                        })
                        .transpose()?;
                    let arbiter =
                        table_source.unique_index_arbiters().iter().find(|arbiter| {
                            arbiter.key_exprs.len() == elements.len()
                                && elements
                                    .iter()
                                    .all(|element| arbiter.key_exprs.contains(element))
                                && arbiter
                                    .key_exprs
                                    .iter()
                                    .all(|key| elements.contains(key))
                                && match &arbiter.predicate {
                                    None => true,
                                    Some(index_predicate) => {
                                        predicate.as_ref() == Some(index_predicate)
                                    }
                                }
                        });
                    match arbiter {
                        Some(arbiter) => {
                            Some(ConflictTarget::Index(arbiter.name.clone()))
                        }
                        None => {
                            return plan_err!(
                                "there is no unique or exclusion constraint matching the ON CONFLICT specification"
                            );
                        }
                    }
                }
            }
        };

        Ok((OnConflict::new(conflict_target, action), source))
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn insert_default_values_to_plan(
        &self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        overwrite: bool,
        replace_into: bool,
        on_conflict: Option<SqlOnConflict>,
        table_alias: Option<&Ident>,
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
            body: SQLBox::new(SetExpr::Values(ast::Values {
                explicit_row: false,
                rows: vec![default_row],
                value_keyword: false,
            })),
            order_by: None,
            limit_clause: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
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
            None,
            source,
            overwrite,
            replace_into,
            on_conflict,
            None,
            table_alias,
            None,
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

/// `(a, b) = (SELECT x, y ...)` assigns one scalar sub-select per target
/// column: the `idx`-th value is the original query narrowed to its `idx`-th
/// projection item, so `a = (SELECT x ...)` and `b = (SELECT y ...)`.
/// A sub-select in a DO UPDATE assignment or WHERE is computed on the INSERT
/// source, where it plans like any other sub-select: it becomes a column of
/// the source row after the table's own, and the conflict clause reads it
/// back as a column of `EXCLUDED`. Its references to `EXCLUDED` are the
/// source row's columns. A reference to the conflicting target row cannot be
/// computed before the conflict is found and is refused.
fn hoist_conflict_subqueries(
    expr: Expr,
    target: &TableReference,
    excluded: &TableReference,
    hoisted: &mut Vec<Expr>,
) -> Result<Expr> {
    expr.transform_down(|node| {
        if !matches!(
            node,
            Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)
        ) {
            return Ok(Transformed::no(node));
        }
        let name = format!("__conflict_src_{}", hoisted.len());
        let computed = excluded_refs_to_source(node, target, excluded)?;
        hoisted.push(computed.alias(&name));
        Ok(Transformed::new(
            Expr::Column(Column::new(Some(excluded.clone()), name)),
            true,
            TreeNodeRecursion::Jump,
        ))
    })
    .map(|transformed| transformed.data)
}

fn excluded_refs_to_source(
    expr: Expr,
    target: &TableReference,
    excluded: &TableReference,
) -> Result<Expr> {
    expr.transform_down(|node| {
        Ok(match node {
            Expr::Column(column) => Transformed::yes(Expr::Column(source_row_column(
                column, target, excluded,
            )?)),
            Expr::ScalarSubquery(subquery) => Transformed::yes(Expr::ScalarSubquery(
                subquery_refs_to_source(subquery, target, excluded)?,
            )),
            Expr::Exists(exists) => Transformed::yes(Expr::Exists(Exists {
                subquery: subquery_refs_to_source(exists.subquery, target, excluded)?,
                negated: exists.negated,
            })),
            Expr::InSubquery(in_subquery) => {
                Transformed::yes(Expr::InSubquery(InSubquery {
                    expr: in_subquery.expr,
                    subquery: subquery_refs_to_source(
                        in_subquery.subquery,
                        target,
                        excluded,
                    )?,
                    negated: in_subquery.negated,
                }))
            }
            other => Transformed::no(other),
        })
    })
    .map(|transformed| transformed.data)
}

fn source_row_column(
    column: Column,
    target: &TableReference,
    excluded: &TableReference,
) -> Result<Column> {
    match &column.relation {
        Some(relation) if relation.resolved_eq(excluded) => {
            Ok(Column::from_name(column.name))
        }
        Some(relation) if relation.resolved_eq(target) => not_impl_err!(
            "ON CONFLICT DO UPDATE sub-select referencing the conflicting row is not supported"
        ),
        _ => Ok(column),
    }
}

fn subquery_refs_to_source(
    subquery: Subquery,
    target: &TableReference,
    excluded: &TableReference,
) -> Result<Subquery> {
    let outer_ref_columns = subquery
        .outer_ref_columns
        .into_iter()
        .map(|expr| match expr {
            Expr::OuterReferenceColumn(field, column) => Ok(Expr::OuterReferenceColumn(
                field,
                source_row_column(column, target, excluded)?,
            )),
            other => Ok(other),
        })
        .collect::<Result<Vec<_>>>()?;
    let plan = Arc::unwrap_or_clone(subquery.subquery)
        .transform_down_with_subqueries(|plan| {
            plan.map_expressions(|expr| {
                expr.transform(|expr| {
                    Ok(match expr {
                        Expr::OuterReferenceColumn(field, column) => {
                            Transformed::yes(Expr::OuterReferenceColumn(
                                field,
                                source_row_column(column, target, excluded)?,
                            ))
                        }
                        other => Transformed::no(other),
                    })
                })
            })
        })?
        .data;
    Ok(Subquery {
        subquery: Arc::new(plan),
        outer_ref_columns,
        spans: subquery.spans,
    })
}

fn tuple_subquery_column_values(
    query: &Query,
    column_count: usize,
) -> Result<Vec<Cow<'static, SQLExpr>>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return plan_err!("Tuple assignment with subquery requires a SELECT statement");
    };
    if select.projection.len() != column_count {
        return plan_err!(
            "Tuple assignment mismatch: {} columns but subquery returns {} columns",
            column_count,
            select.projection.len()
        );
    }
    Ok((0..column_count)
        .map(|idx| {
            let mut column_query = query.clone();
            if let SetExpr::Select(column_select) = column_query.body.as_mut() {
                column_select.projection = vec![select.projection[idx].clone()];
            }
            Cow::Owned(SQLExpr::Subquery(SQLBox::new(column_query)))
        })
        .collect())
}
