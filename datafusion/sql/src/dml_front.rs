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

//! DML front-end semantics resolved during planning, driven by the host
//! through [`ContextProvider`]: generated-column write rejection and
//! UPDATE-time recomputation, narrowing an INSERT to the columns its VALUES
//! actually write, and per-value adaptation of INSERT source literals.
//!
//! [`ContextProvider`]: crate::planner::ContextProvider

use crate::planner::{ContextProvider, DmlGeneratedColumns};
use arrow::datatypes::Fields;
use datafusion_common::Result;
use sqlparser::ast::{
    Assignment, AssignmentTarget, AstBox, Expr as SQLExpr, Ident, MergeAction,
    MergeClause, MergeInsertKind, ObjectName, OverridingKind, Query, SelectItem, SetExpr,
    Value,
};

use crate::values::is_default_identifier;

/// `DEFAULT` or a bound placeholder in a value slot, the two spellings a
/// generated column accepts.
fn is_default_value(expr: &SQLExpr) -> bool {
    matches!(expr, SQLExpr::Value(value) if matches!(value.value, Value::Placeholder(_)))
        || matches!(expr, SQLExpr::Identifier(identifier)
            if identifier.value.eq_ignore_ascii_case("default"))
}

/// Reject an INSERT that supplies a non-DEFAULT value for a generated column
/// — the column's value is its generation expression's — or, without an
/// OVERRIDING clause, for a `GENERATED ALWAYS AS IDENTITY` column. PostgreSQL
/// refuses these writes rather than silently discarding what was written.
pub(crate) fn reject_insert_generated_writes(
    provider: &dyn ContextProvider,
    table: &ObjectName,
    columns: &[Ident],
    source: Option<&Query>,
    overriding: Option<&OverridingKind>,
) -> Result<()> {
    let Some(generated) = provider.dml_generated_columns(table)? else {
        return Ok(());
    };
    let rows = source.and_then(|source| match source.body.as_ref() {
        SetExpr::Values(values) => Some(values.rows.as_slice()),
        _ => None,
    });
    check_insert_rows(provider, &generated, columns, rows, overriding)
}

/// Check one INSERT column list and VALUES rows against the generated and
/// identity columns of the target. `OVERRIDING SYSTEM VALUE` admits the
/// written value for an identity; `OVERRIDING USER VALUE` discards it for the
/// sequence's. Neither is a write the identity refuses.
fn check_insert_rows(
    provider: &dyn ContextProvider,
    generated: &DmlGeneratedColumns,
    columns: &[Ident],
    rows: Option<&[Vec<SQLExpr>]>,
    overriding: Option<&OverridingKind>,
) -> Result<()> {
    let identity_always: &[String] = if overriding.is_some() {
        &[]
    } else {
        &generated.identity_always
    };
    if generated.columns.is_empty() && identity_always.is_empty() {
        return Ok(());
    }
    let targets: Vec<&str> = if columns.is_empty() {
        generated
            .positional_columns
            .iter()
            .map(String::as_str)
            .collect()
    } else {
        columns.iter().map(|column| column.value.as_str()).collect()
    };
    for (position, column) in targets.iter().enumerate() {
        let is_generated = generated.columns.iter().any(|name| name == column);
        let is_identity = identity_always.iter().any(|name| name == column);
        if !is_generated && !is_identity {
            continue;
        }
        // A named generated column is a write whatever the value; a
        // positional row reaches it only when it is long enough, and must
        // spell DEFAULT there.
        let written = match rows {
            Some(rows) => rows.iter().any(|row| {
                row.get(position)
                    .is_some_and(|value| !is_default_value(value))
            }),
            None => !columns.is_empty(),
        };
        if is_generated && (!columns.is_empty() || written) {
            return Err(
                provider.generated_column_write_error(&generated.table_name, column)
            );
        }
        if is_identity && written {
            return Err(provider.identity_column_write_error(
                &generated.table_name,
                column,
                true,
            ));
        }
    }
    Ok(())
}

/// Check MERGE insert clauses against the target's generated and identity
/// columns and return their typed storage contract. `OVERRIDING USER VALUE`
/// is represented by the logical MERGE insert's provided-column set; this
/// validation never edits the parsed statement.
pub(crate) fn prepare_merge_insert_clauses(
    provider: &dyn ContextProvider,
    table: &ObjectName,
    clauses: &[MergeClause],
) -> Result<Option<DmlGeneratedColumns>> {
    let Some(generated) = provider.dml_generated_columns(table)? else {
        return Ok(None);
    };
    for clause in clauses {
        let MergeAction::Insert(insert) = &clause.action else {
            continue;
        };
        let rows = match &insert.kind {
            MergeInsertKind::Values(values) => Some(values.rows.as_slice()),
            _ => None,
        };
        check_insert_rows(
            provider,
            &generated,
            &insert.columns,
            rows,
            insert.overriding.as_ref(),
        )?;
    }
    Ok(Some(generated))
}

/// Reject writes to generated columns in an UPDATE's assignment list. Default
/// substitution happens after expression binding in the UPDATE projection,
/// where the host's parser-free semantics can be used directly.
pub(crate) fn validate_update_assignments(
    provider: &dyn ContextProvider,
    generated: Option<&DmlGeneratedColumns>,
    assignments: &[Assignment],
) -> Result<()> {
    let Some(generated) = generated else {
        return Ok(());
    };
    reject_generated_assignments(provider, generated, assignments)
}

fn reject_generated_assignments(
    provider: &dyn ContextProvider,
    generated: &DmlGeneratedColumns,
    assignments: &[Assignment],
) -> Result<()> {
    for assignment in assignments {
        let AssignmentTarget::ColumnName(column) = &assignment.target else {
            continue;
        };
        let Some(leaf) = column.0.last() else {
            continue;
        };
        let leaf = leaf.to_string();
        if is_default_value(&assignment.value) {
            continue;
        }
        if generated.columns.iter().any(|name| *name == leaf) {
            return Err(
                provider.generated_column_write_error(&generated.table_name, &leaf)
            );
        }
        if generated.identity_always.iter().any(|name| *name == leaf) {
            return Err(provider.identity_column_write_error(
                &generated.table_name,
                &leaf,
                false,
            ));
        }
    }
    Ok(())
}

/// Return the typed set of columns an INSERT over a VALUES source actually
/// provides. A position every row explicitly leaves to the column's default
/// is omitted from the storage write contract. The parsed VALUES rows remain
/// untouched and are still planned in their declared target shape. `None`
/// means every target position is provided.
pub(crate) fn insert_written_columns(
    columns: &[Ident],
    source: &Query,
    positional_columns: &[String],
) -> Option<Vec<Ident>> {
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    // The columns the values are positional against: the statement's own
    // list, or the relation's columns when it wrote none.
    let target_columns: Vec<&str> = if columns.is_empty() {
        positional_columns.iter().map(String::as_str).collect()
    } else {
        columns.iter().map(|ident| ident.value.as_str()).collect()
    };
    if target_columns.is_empty() {
        return None;
    }
    let written: Vec<usize> = (0..target_columns.len())
        .filter(|&position| {
            values.rows.iter().any(|row| {
                row.get(position)
                    .is_some_and(|value| !is_default_identifier_expr(value))
            })
        })
        .collect();
    if written.len() == target_columns.len() {
        return None;
    }
    Some(
        written
            .iter()
            .map(|&position| Ident::new(target_columns[position]))
            .collect(),
    )
}

fn is_default_identifier_expr(expr: &SQLExpr) -> bool {
    matches!(expr, SQLExpr::Identifier(ident) if is_default_identifier(ident))
}

/// Adapt INSERT source values to their target columns through the host's
/// [`ContextProvider::adapt_insert_value`] — VALUES rows and SELECT
/// projections positionally, set-operation arms recursively. `None` when
/// nothing changes.
///
/// [`ContextProvider::adapt_insert_value`]: crate::planner::ContextProvider::adapt_insert_value
pub(crate) fn adapt_insert_source_values(
    provider: &dyn ContextProvider,
    source: &Query,
    fields: &Fields,
) -> Option<Query> {
    let adapted = adapt_set_expr(provider, source.body.as_ref(), fields)?;
    let mut source = source.clone();
    *source.body = adapted;
    Some(source)
}

fn adapt_set_expr(
    provider: &dyn ContextProvider,
    body: &SetExpr,
    fields: &Fields,
) -> Option<SetExpr> {
    match body {
        SetExpr::Values(values) => {
            let mut changed = false;
            let mut values = values.clone();
            for row in &mut values.rows {
                for (position, value) in row.iter_mut().enumerate() {
                    let Some(field) = fields.get(position) else {
                        continue;
                    };
                    if let Some(adapted) = provider.adapt_insert_value(value, field) {
                        *value = adapted;
                        changed = true;
                    }
                }
            }
            changed.then(|| SetExpr::Values(values))
        }
        SetExpr::Select(select) => {
            let mut changed = false;
            let mut select = select.clone();
            for (position, item) in select.projection.iter_mut().enumerate() {
                let Some(field) = fields.get(position) else {
                    continue;
                };
                let expr = match item {
                    SelectItem::UnnamedExpr(expr) => expr,
                    SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => continue,
                };
                if let Some(adapted) = provider.adapt_insert_value(expr, field) {
                    *expr = adapted;
                    changed = true;
                }
            }
            changed.then(|| SetExpr::Select(select))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let adapted_left = adapt_set_expr(provider, left, fields);
            let adapted_right = adapt_set_expr(provider, right, fields);
            if adapted_left.is_none() && adapted_right.is_none() {
                return None;
            }
            Some(SetExpr::SetOperation {
                op: *op,
                set_quantifier: *set_quantifier,
                left: AstBox::new(adapted_left.unwrap_or_else(|| (**left).clone())),
                right: AstBox::new(adapted_right.unwrap_or_else(|| (**right).clone())),
            })
        }
        _ => None,
    }
}
