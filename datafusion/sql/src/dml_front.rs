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
//! [`ContextProvider`]: datafusion_expr::planner::ContextProvider

use std::collections::HashMap;

use arrow::datatypes::Fields;
use datafusion_common::Result;
use datafusion_expr::planner::{ContextProvider, DmlGeneratedColumns};
use sqlparser::ast::{
    Assignment, AssignmentTarget, AstBox, Expr as SQLExpr, Ident, MergeAction, MergeClause,
    MergeInsertKind, ObjectName, OnConflict, OnConflictAction, OverridingKind, Query, SelectItem,
    SetExpr, Value,
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
            return Err(provider.generated_column_write_error(&generated.table_name, column));
        }
        if is_identity && written {
            return Err(provider.identity_column_write_error(&generated.table_name, column, true));
        }
    }
    Ok(())
}

/// Apply `OVERRIDING USER VALUE` to an INSERT over a VALUES source: whatever
/// the statement wrote for an identity column is discarded for the sequence's
/// value, which spelling DEFAULT there says in the form insert planning
/// already resolves. `None` when nothing changes.
pub(crate) fn apply_insert_user_value_override(
    provider: &dyn ContextProvider,
    table: &ObjectName,
    columns: &[Ident],
    source: &Query,
) -> Result<Option<Query>> {
    let Some(generated) = provider.dml_generated_columns(table)? else {
        return Ok(None);
    };
    if generated.identity.is_empty() {
        return Ok(None);
    }
    let SetExpr::Values(_) = source.body.as_ref() else {
        return Ok(None);
    };
    let targets: Vec<&str> = if columns.is_empty() {
        generated
            .positional_columns
            .iter()
            .map(String::as_str)
            .collect()
    } else {
        columns.iter().map(|column| column.value.as_str()).collect()
    };
    let identity_positions: Vec<usize> = targets
        .iter()
        .enumerate()
        .filter(|(_, column)| generated.identity.iter().any(|name| name == *column))
        .map(|(position, _)| position)
        .collect();
    if identity_positions.is_empty() {
        return Ok(None);
    }
    let mut source = source.clone();
    let mut changed = false;
    if let SetExpr::Values(values) = source.body.as_mut() {
        for row in &mut values.rows {
            for &position in &identity_positions {
                if let Some(value) = row.get_mut(position)
                    && !is_default_identifier_expr(value)
                {
                    *value = SQLExpr::Identifier(Ident::new("DEFAULT"));
                    changed = true;
                }
            }
        }
    }
    Ok(changed.then_some(source))
}

/// Check MERGE insert clauses against the target's generated and identity
/// columns, and apply `OVERRIDING USER VALUE` where a clause carries it. A
/// MERGE insert list has no DEFAULT marker, so the identity columns are
/// dropped from the list and their values from every row: an omitted identity
/// draws its sequence.
pub(crate) fn prepare_merge_insert_clauses(
    provider: &dyn ContextProvider,
    table: &ObjectName,
    clauses: &mut [MergeClause],
) -> Result<()> {
    let Some(generated) = provider.dml_generated_columns(table)? else {
        return Ok(());
    };
    for clause in clauses.iter_mut() {
        let MergeAction::Insert(insert) = &mut clause.action else {
            continue;
        };
        {
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
        if !matches!(insert.overriding, Some(OverridingKind::UserValue))
            || generated.identity.is_empty()
            || insert.columns.is_empty()
        {
            continue;
        }
        let dropped: Vec<usize> = insert
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| generated.identity.iter().any(|name| *name == column.value))
            .map(|(position, _)| position)
            .collect();
        if dropped.is_empty() {
            continue;
        }
        let mut position = 0usize;
        insert.columns.retain(|_| {
            let keep = !dropped.contains(&position);
            position += 1;
            keep
        });
        if let MergeInsertKind::Values(values) = &mut insert.kind {
            for row in values.rows.iter_mut() {
                let mut position = 0usize;
                row.retain(|_| {
                    let keep = !dropped.contains(&position);
                    position += 1;
                    keep
                });
            }
        }
    }
    Ok(())
}

/// Reject writes to generated columns in an UPDATE's assignment list, then
/// recompute every stored generated column as part of the statement that
/// changes its inputs — so the new row the plan produces, and every surface
/// reading it (`RETURNING new.*` included), already carries their values.
/// `SET col = DEFAULT` on a sequence-backed column takes the host's override
/// expression, which draws the sequence. `None` leaves the caller's
/// assignments authoritative.
pub(crate) fn prepare_update_assignments(
    provider: &dyn ContextProvider,
    table: &ObjectName,
    assignments: &[Assignment],
) -> Result<Option<Vec<Assignment>>> {
    let Some(generated) = provider.dml_generated_columns(table)? else {
        return Ok(None);
    };
    reject_generated_assignments(provider, &generated, assignments)?;
    let overridden = substitute_update_default_overrides(&generated, assignments);
    let base = overridden.as_deref().unwrap_or(assignments);
    match append_generated_assignments(&generated, base) {
        Some(extended) => Ok(Some(extended)),
        None => Ok(overridden),
    }
}

/// Replace `SET col = DEFAULT` with the host's override expression for the
/// sequence-backed columns that have one. `None` when nothing changes.
fn substitute_update_default_overrides(
    generated: &DmlGeneratedColumns,
    assignments: &[Assignment],
) -> Option<Vec<Assignment>> {
    if generated.update_default_overrides.is_empty() {
        return None;
    }
    let mut assignments = assignments.to_vec();
    let mut changed = false;
    for assignment in &mut assignments {
        let AssignmentTarget::ColumnName(column) = &assignment.target else {
            continue;
        };
        let Some(leaf) = column.0.last().and_then(|part| part.as_ident()) else {
            continue;
        };
        if !is_default_identifier_expr(&assignment.value) {
            continue;
        }
        if let Some((_, expression)) = generated
            .update_default_overrides
            .iter()
            .find(|(name, _)| *name == leaf.value)
        {
            assignment.value = expression.clone();
            changed = true;
        }
    }
    changed.then_some(assignments)
}

/// The recompute pass for `INSERT ... ON CONFLICT DO UPDATE SET`. `None`
/// leaves the caller's clause authoritative.
pub(crate) fn prepare_on_conflict_assignments(
    provider: &dyn ContextProvider,
    table: &ObjectName,
    on_conflict: Option<&OnConflict>,
) -> Result<Option<OnConflict>> {
    let Some(on_conflict) = on_conflict else {
        return Ok(None);
    };
    let OnConflictAction::DoUpdate(do_update) = &on_conflict.action else {
        return Ok(None);
    };
    let Some(generated) = provider.dml_generated_columns(table)? else {
        return Ok(None);
    };
    let Some(extended) = append_generated_assignments(&generated, &do_update.assignments) else {
        return Ok(None);
    };
    let mut rewritten = on_conflict.clone();
    if let OnConflictAction::DoUpdate(do_update) = &mut rewritten.action {
        do_update.assignments = extended;
    }
    Ok(Some(rewritten))
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
            return Err(provider.generated_column_write_error(&generated.table_name, &leaf));
        }
        if generated.identity_always.iter().any(|name| *name == leaf) {
            return Err(provider.identity_column_write_error(&generated.table_name, &leaf, false));
        }
    }
    Ok(())
}

/// Append recomputation assignments for stored generated columns. `None` when
/// nothing needed appending.
fn append_generated_assignments(
    generated: &DmlGeneratedColumns,
    assignments: &[Assignment],
) -> Option<Vec<Assignment>> {
    if generated.stored_expressions.is_empty() {
        return None;
    }
    let assigned: HashMap<String, SQLExpr> = assignments
        .iter()
        .filter_map(|assignment| match &assignment.target {
            AssignmentTarget::ColumnName(name) => Some((
                name.0.last()?.as_ident()?.value.clone(),
                assignment.value.clone(),
            )),
            _ => None,
        })
        .collect();
    let mut extended = assignments.to_vec();
    let before = extended.len();
    for (column, expression) in &generated.stored_expressions {
        // A generated column the statement already assigns is a write the
        // caller is not allowed to make; that is reported by the rejection
        // pass.
        if assigned.contains_key(column) {
            continue;
        }
        let mut value = expression.clone();
        substitute_assigned_columns(&mut value, &assigned);
        extended.push(Assignment {
            target: AssignmentTarget::ColumnName(ObjectName::from(vec![Ident::new(
                column.as_str(),
            )])),
            value,
        });
    }
    (extended.len() != before).then_some(extended)
}

/// Replace every column reference the statement assigns with the value being
/// assigned. The substituted value is the statement's own expression, naming
/// the row's *old* columns, so it is never descended into — `SET a = a + 1`
/// would otherwise substitute itself forever.
fn substitute_assigned_columns(expr: &mut SQLExpr, assigned: &HashMap<String, SQLExpr>) {
    let referenced = match expr {
        SQLExpr::Identifier(identifier) => Some(identifier.value.clone()),
        SQLExpr::CompoundIdentifier(parts) => match parts.as_slice() {
            [_, leaf] => Some(leaf.value.clone()),
            _ => None,
        },
        _ => None,
    };
    if let Some(referenced) = referenced {
        if let Some(value) = assigned.get(&referenced) {
            *expr = SQLExpr::Nested(AstBox::new(value.clone()));
        }
        return;
    }
    match expr {
        SQLExpr::Nested(inner) => substitute_assigned_columns(inner, assigned),
        SQLExpr::UnaryOp { expr, .. } => substitute_assigned_columns(expr, assigned),
        SQLExpr::BinaryOp { left, right, .. } => {
            substitute_assigned_columns(left, assigned);
            substitute_assigned_columns(right, assigned);
        }
        SQLExpr::Cast { expr, .. } => substitute_assigned_columns(expr, assigned),
        SQLExpr::IsNull { expr, .. } | SQLExpr::IsNotNull { expr, .. } => {
            substitute_assigned_columns(expr, assigned);
        }
        SQLExpr::Function(function) => {
            if let sqlparser::ast::FunctionArguments::List(arguments) = &mut function.args {
                for argument in &mut arguments.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(argument),
                    )
                    | sqlparser::ast::FunctionArg::Named {
                        arg: sqlparser::ast::FunctionArgExpr::Expr(argument),
                        ..
                    } = argument
                    {
                        substitute_assigned_columns(argument, assigned);
                    }
                }
            }
        }
        SQLExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                substitute_assigned_columns(operand, assigned);
            }
            for condition in conditions {
                substitute_assigned_columns(&mut condition.condition, assigned);
                substitute_assigned_columns(&mut condition.result, assigned);
            }
            if let Some(else_result) = else_result {
                substitute_assigned_columns(else_result, assigned);
            }
        }
        _ => {}
    }
}

pub(crate) struct NarrowedInsert {
    pub columns: Vec<Ident>,
    pub source: Query,
}

/// Narrow an INSERT over a VALUES source to the columns some row actually
/// writes. A position every row leaves to the column's default — a short
/// row's missing tail included — is a column the statement does not write;
/// naming only the written columns says that in the form the planner and the
/// write path both read, which is what separates `DEFAULT` from a written
/// NULL for a column the storage layer generates. `None` when nothing
/// changes.
pub(crate) fn narrow_insert_to_written_columns(
    columns: &[Ident],
    source: &Query,
    positional_columns: &[String],
) -> Option<NarrowedInsert> {
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
    if written.is_empty() {
        // `INSERT INTO t VALUES (DEFAULT, DEFAULT)` writes no column and has
        // no column-list spelling; it keeps the padded form.
        if !columns.is_empty() {
            return None;
        }
        let needs_padding = values
            .rows
            .iter()
            .any(|row| row.len() < target_columns.len());
        if !needs_padding {
            return None;
        }
        let mut source = source.clone();
        if let SetExpr::Values(values) = source.body.as_mut() {
            for row in &mut values.rows {
                if row.len() < target_columns.len() {
                    row.extend(
                        std::iter::repeat_with(|| SQLExpr::Identifier(Ident::new("DEFAULT")))
                            .take(target_columns.len() - row.len()),
                    );
                }
            }
        }
        return Some(NarrowedInsert {
            columns: columns.to_vec(),
            source,
        });
    }
    let mut source = source.clone();
    if let SetExpr::Values(values) = source.body.as_mut() {
        for row in &mut values.rows {
            *row = written
                .iter()
                .map(|&position| {
                    row.get(position)
                        .cloned()
                        .unwrap_or_else(|| SQLExpr::Identifier(Ident::new("DEFAULT")))
                })
                .collect();
        }
    }
    let narrowed_columns = written
        .iter()
        .map(|&position| Ident::new(target_columns[position]))
        .collect();
    Some(NarrowedInsert {
        columns: narrowed_columns,
        source,
    })
}

fn is_default_identifier_expr(expr: &SQLExpr) -> bool {
    matches!(expr, SQLExpr::Identifier(ident) if is_default_identifier(ident))
}

/// Adapt INSERT source values to their target columns through the host's
/// [`ContextProvider::adapt_insert_value`] — VALUES rows and SELECT
/// projections positionally, set-operation arms recursively. `None` when
/// nothing changes.
///
/// [`ContextProvider::adapt_insert_value`]: datafusion_expr::planner::ContextProvider::adapt_insert_value
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
