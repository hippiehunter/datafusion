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

//! Retargeting a write on an automatically updatable view onto its base
//! relation, the analogue of PostgreSQL's `rewriteTargetView` performed by
//! the SQL planner itself.
//!
//! The host resolves whether a DML target is an automatically updatable view
//! through [`ContextProvider::resolve_dml_view_target`]; these helpers apply
//! the retarget to the statement pieces each DML planner holds — the target
//! name, column lists, assignments, predicates and RETURNING items — so the
//! planner then plans an ordinary base-relation write. Column defaults, the
//! hidden row identity and RETURNING all resolve against the base relation
//! with no view-shaped special case downstream.
//!
//! [`ContextProvider::resolve_dml_view_target`]: datafusion_expr::planner::ContextProvider::resolve_dml_view_target

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, TableReference};
use datafusion_expr::LogicalPlan;
use datafusion_expr::planner::{ViewDmlError, ViewDmlTarget};
use sqlparser::ast::{
    Assignment, AssignmentTarget, AstBox, BinaryOperator, Expr as SQLExpr, Ident, ObjectName,
    ObjectNamePart, Query, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, Visit,
    VisitMut, Visitor, VisitorMut,
};
use std::ops::ControlFlow;

/// Record the retargeted write's check-option obligation on its DML plan
/// node. The predicate itself is re-derived from the view at lowering time,
/// so a cached plan honors `ALTER VIEW ... RESET (check_option)`. A RETURNING
/// clause can leave the write under a projection, so the DML node is found
/// rather than assumed to be the root.
pub(crate) fn stamp_check_option(
    plan: LogicalPlan,
    view: Option<TableReference>,
) -> Result<LogicalPlan> {
    let Some(view) = view else {
        return Ok(plan);
    };
    plan.transform_down(|node| match node {
        LogicalPlan::Dml(mut dml) => {
            dml.check_option_view = Some(view.clone());
            Ok(Transformed::yes(LogicalPlan::Dml(dml)))
        }
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}

/// A rewrite failure, mapped to the host's error through
/// [`ViewDmlError`] by the caller.
pub(crate) struct RewriteFailure {
    pub verb: &'static str,
    pub column: String,
}

impl RewriteFailure {
    pub(crate) fn as_view_error<'a>(&'a self, view_name: &'a str) -> ViewDmlError<'a> {
        ViewDmlError::ColumnNotUpdatable {
            verb: self.verb,
            view_name,
            column: &self.column,
        }
    }
}

/// The relation spellings a qualified column reference in the statement may
/// carry: the view's bare name and the statement's own spelling of it.
pub(crate) fn view_reference_qualifiers(
    target: &ViewDmlTarget,
    written: &ObjectName,
) -> Vec<String> {
    let mut qualifiers = vec![target.view_name.clone()];
    if let Some(last) = written.0.last().and_then(ObjectNamePart::as_ident)
        && !qualifiers
            .iter()
            .any(|qualifier| qualifier.eq_ignore_ascii_case(&last.value))
    {
        qualifiers.push(last.value.clone());
    }
    qualifiers
}

pub(crate) struct RewrittenInsert {
    pub table: ObjectName,
    pub columns: Vec<Ident>,
    /// `Some` when view-level column defaults were materialized into the
    /// VALUES rows; the caller plans this source instead of the statement's.
    pub source: Option<Query>,
    pub returning: Option<Vec<SelectItem>>,
}

/// Retarget an INSERT's pieces onto the base relation. Without a column list
/// the write covers the view's columns in order, as wide as the source is;
/// naming them explicitly is what lets the base relation's own defaults fill
/// the rest.
pub(crate) fn rewrite_insert(
    target: &ViewDmlTarget,
    written_table: &ObjectName,
    columns: &[Ident],
    source: Option<&Query>,
    returning: Option<&[SelectItem]>,
) -> Result<RewrittenInsert, RewriteFailure> {
    // Defaults attached to the view itself override the base relation's.
    // With an explicit target list, materialize omitted view-default columns
    // before the retarget; a column with no view default stays omitted so the
    // base relation's own default remains authoritative.
    let mut materialized_columns;
    let mut materialized_source = None;
    let mut columns = columns;
    if !columns.is_empty() && !target.column_defaults.is_empty() {
        let additions: Vec<(&String, &SQLExpr)> = target
            .column_defaults
            .iter()
            .filter(|(name, _)| {
                !columns
                    .iter()
                    .any(|column| column.value.eq_ignore_ascii_case(name))
            })
            .map(|(name, expr)| (name, expr))
            .collect();
        if !additions.is_empty()
            && let Some(query) = source
            && let SetExpr::Values(_) = query.body.as_ref()
        {
            let mut query = query.clone();
            materialized_columns = columns.to_vec();
            if let SetExpr::Values(values) = query.body.as_mut() {
                for (name, expression) in additions {
                    materialized_columns.push(Ident::new(name.clone()));
                    for row in &mut values.rows {
                        row.push((*expression).clone());
                    }
                }
            }
            materialized_source = Some(query);
            columns = &materialized_columns;
        }
    }
    let source = materialized_source.as_ref().or(source);

    let rewritten_columns = if columns.is_empty() {
        let width = source
            .and_then(insert_source_width)
            .unwrap_or(target.columns.len());
        let mut rewritten = Vec::with_capacity(width);
        for (view_column, base_column) in target.columns.iter().take(width) {
            let Some(base_column) = base_column else {
                return Err(RewriteFailure {
                    verb: "insert into",
                    column: view_column.clone(),
                });
            };
            rewritten.push(Ident::new(base_column.clone()));
        }
        rewritten
    } else {
        let mut rewritten = Vec::with_capacity(columns.len());
        for column in columns {
            let base_column = target.base_column(&column.value).flatten().ok_or_else(|| {
                RewriteFailure {
                    verb: "insert into",
                    column: column.value.clone(),
                }
            })?;
            let mut renamed = column.clone();
            renamed.value = base_column.to_string();
            rewritten.push(renamed);
        }
        rewritten
    };

    // The written row's own column list already names base columns, so a
    // RETURNING item only has to be re-labelled where the view renames.
    let qualifiers = view_reference_qualifiers(target, written_table);
    let returning =
        returning.map(|items| rewrite_returning_items(items, target, &qualifiers, true));
    Ok(RewrittenInsert {
        table: target.base_relation.clone(),
        columns: rewritten_columns,
        source: materialized_source,
        returning,
    })
}

/// How many columns an INSERT without a column list assigns. Only a `VALUES`
/// source can be narrower than the relation; every other source is planned
/// against the full column list.
fn insert_source_width(source: &Query) -> Option<usize> {
    let SetExpr::Values(values) = source.body.as_ref() else {
        return None;
    };
    values.rows.first().map(Vec::len)
}

pub(crate) struct RewrittenUpdate {
    pub table: TableWithJoins,
    pub assignments: Vec<Assignment>,
    pub predicate: Option<SQLExpr>,
    pub returning: Option<Vec<SelectItem>>,
}

pub(crate) fn rewrite_update(
    target: &ViewDmlTarget,
    table: &TableWithJoins,
    assignments: &[Assignment],
    predicate: Option<&SQLExpr>,
    returning: Option<&[SelectItem]>,
) -> Result<RewrittenUpdate, RewriteFailure> {
    let TableFactor::Table { name: written, .. } = &table.relation else {
        return Ok(RewrittenUpdate {
            table: table.clone(),
            assignments: assignments.to_vec(),
            predicate: predicate.cloned(),
            returning: returning.map(<[SelectItem]>::to_vec),
        });
    };
    let qualifiers = view_reference_qualifiers(target, written);
    // Keep the view's name reachable as a qualifier, but only when the
    // statement actually uses it: `UPDATE v SET .. WHERE v.c = 1` stays valid
    // after the target becomes the base relation, while the far more common
    // unqualified statement keeps its unaliased shape.
    let needs_alias = assignments
        .iter()
        .any(|assignment| references_qualifier(&assignment.value, &qualifiers))
        || predicate.is_some_and(|predicate| references_qualifier(predicate, &qualifiers))
        || returning.is_some_and(|items| returning_references_qualifier(items, &qualifiers));

    let mut rewritten_table = table.clone();
    if let TableFactor::Table { name, alias, .. } = &mut rewritten_table.relation {
        *name = target.base_relation.clone();
        if alias.is_none() && needs_alias {
            *alias = Some(TableAlias {
                name: Ident::new(target.view_name.clone()),
                columns: Vec::new(),
                implicit: false,
            });
        }
    }

    let mut rewritten_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let mut assignment = assignment.clone();
        rewrite_assignment_target(&mut assignment, target)?;
        rewrite_expr(&mut assignment.value, target, &qualifiers, "update")?;
        rewritten_assignments.push(assignment);
    }

    let mut rewritten_predicate = predicate.cloned();
    if let Some(predicate) = rewritten_predicate.as_mut() {
        rewrite_expr(predicate, target, &qualifiers, "update")?;
    }
    // An UPDATE through the view may only touch rows the view shows.
    let rewritten_predicate = restrict(rewritten_predicate, target);

    let returning =
        returning.map(|items| rewrite_returning_items(items, target, &qualifiers, true));
    Ok(RewrittenUpdate {
        table: rewritten_table,
        assignments: rewritten_assignments,
        predicate: rewritten_predicate,
        returning,
    })
}

pub(crate) struct RewrittenDelete {
    pub table: TableWithJoins,
    pub predicate: Option<SQLExpr>,
    pub returning: Option<Vec<SelectItem>>,
}

pub(crate) fn rewrite_delete(
    target: &ViewDmlTarget,
    table: &TableWithJoins,
    predicate: Option<&SQLExpr>,
    returning: Option<&[SelectItem]>,
) -> RewrittenDelete {
    let TableFactor::Table { name: written, .. } = &table.relation else {
        return RewrittenDelete {
            table: table.clone(),
            predicate: predicate.cloned(),
            returning: returning.map(<[SelectItem]>::to_vec),
        };
    };
    let qualifiers = view_reference_qualifiers(target, written);
    let needs_alias =
        predicate.is_some_and(|predicate| references_qualifier(predicate, &qualifiers));

    let mut rewritten_table = table.clone();
    if let TableFactor::Table { name, alias, .. } = &mut rewritten_table.relation {
        *name = target.base_relation.clone();
        if alias.is_none() && needs_alias {
            *alias = Some(TableAlias {
                name: Ident::new(target.view_name.clone()),
                columns: Vec::new(),
                implicit: false,
            });
        }
    }

    // The predicate rewrite cannot fail for a DELETE: reading a non-updatable
    // view column in the predicate is legal, and the host's analysis already
    // replaced it with the value the view shows.
    let mut rewritten_predicate = predicate.cloned();
    if let Some(predicate) = rewritten_predicate.as_mut() {
        rewrite_predicate(predicate, target, &qualifiers);
    }
    let rewritten_predicate = restrict(rewritten_predicate, target);

    let returning =
        returning.map(|items| rewrite_returning_items(items, target, &qualifiers, false));
    RewrittenDelete {
        table: rewritten_table,
        predicate: rewritten_predicate,
        returning,
    }
}

/// MERGE's target row set feeds both the join and every clause's assignments,
/// so a view that renames columns or restricts rows would need the whole
/// statement rewritten in step. Retargeting is allowed only when the view's
/// namespace and row set are identical to the base relation's.
pub(crate) fn merge_target_is_passthrough(target: &ViewDmlTarget) -> bool {
    target.row_restrictions.is_empty() && target.columns_are_passthrough()
}

fn rewrite_assignment_target(
    assignment: &mut Assignment,
    target: &ViewDmlTarget,
) -> Result<(), RewriteFailure> {
    let names: Vec<&mut ObjectName> = match &mut assignment.target {
        AssignmentTarget::ColumnName(name) => vec![name],
        AssignmentTarget::Tuple(names) => names.iter_mut().collect(),
        AssignmentTarget::Indirection(_) => Vec::new(),
    };
    for name in names {
        let Some(written) = name.0.last().and_then(ObjectNamePart::as_ident).cloned() else {
            continue;
        };
        let base_column =
            target
                .base_column(&written.value)
                .flatten()
                .ok_or_else(|| RewriteFailure {
                    verb: "update",
                    column: written.value.clone(),
                })?;
        let last = name.0.len().saturating_sub(1);
        name.0[last] = ObjectNamePart::Identifier(Ident {
            value: base_column.to_string(),
            quote_style: written.quote_style,
            span: written.span,
        });
    }
    Ok(())
}

fn returning_references_qualifier(items: &[SelectItem], qualifiers: &[String]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            references_qualifier(expr, qualifiers)
        }
        SelectItem::Wildcard(_) => false,
        SelectItem::QualifiedWildcard(_, _) => true,
    })
}

fn references_qualifier(expr: &SQLExpr, qualifiers: &[String]) -> bool {
    struct Search<'a> {
        qualifiers: &'a [String],
        found: bool,
    }
    impl Visitor for Search<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SQLExpr) -> ControlFlow<Self::Break> {
            if let SQLExpr::CompoundIdentifier(parts) = expr
                && parts.len() == 2
                && self
                    .qualifiers
                    .iter()
                    .any(|qualifier| parts[0].value.eq_ignore_ascii_case(qualifier))
            {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut search = Search {
        qualifiers,
        found: false,
    };
    let _ = expr.visit(&mut search);
    search.found
}

/// Move an expression from the view's namespace into the base relation's,
/// refusing a write that reads a column the view computes.
fn rewrite_expr(
    expr: &mut SQLExpr,
    target: &ViewDmlTarget,
    qualifiers: &[String],
    verb: &'static str,
) -> Result<(), RewriteFailure> {
    rewrite_view_column_references(expr, target, qualifiers)
        .map_err(|column| RewriteFailure { verb, column })
}

/// Move a read-only expression into the base relation's namespace. A reference
/// to a computed view column is left as written: it is not a write target, and
/// the base relation has no column for it.
fn rewrite_predicate(expr: &mut SQLExpr, target: &ViewDmlTarget, qualifiers: &[String]) {
    let _ = rewrite_view_column_references(expr, target, qualifiers);
}

fn rewrite_returning_items(
    items: &[SelectItem],
    target: &ViewDmlTarget,
    qualifiers: &[String],
    label_with_view_names: bool,
) -> Vec<SelectItem> {
    items
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                let view_label = label_with_view_names
                    .then(|| returned_column_label(expr, target))
                    .flatten();
                let mut expr = expr.clone();
                rewrite_predicate(&mut expr, target, qualifiers);
                match view_label {
                    Some(label) => SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::new(label),
                    },
                    None => SelectItem::UnnamedExpr(expr),
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut expr = expr.clone();
                rewrite_predicate(&mut expr, target, qualifiers);
                SelectItem::ExprWithAlias {
                    expr,
                    alias: alias.clone(),
                }
            }
            other => other.clone(),
        })
        .collect()
}

/// The label `RETURNING <column>` carries on a view: the view's own column
/// name, which the base relation may spell differently. `None` when the item
/// is not a plain column or the name does not change.
fn returned_column_label(expr: &SQLExpr, target: &ViewDmlTarget) -> Option<String> {
    let name = match expr {
        SQLExpr::Identifier(ident) => ident.value.clone(),
        SQLExpr::CompoundIdentifier(parts) => parts.last()?.value.clone(),
        _ => return None,
    };
    let base = target.base_column(&name)??;
    (!base.eq_ignore_ascii_case(&name)).then_some(name)
}

/// Add the view stack's row restrictions to a predicate.
fn restrict(selection: Option<SQLExpr>, target: &ViewDmlTarget) -> Option<SQLExpr> {
    let mut restrictions = target.row_restrictions.clone();
    if let Some(selection) = selection {
        restrictions.insert(0, selection);
    }
    conjunction(restrictions)
}

/// Combine restrictions into a single `AND` chain, or `None` when there are
/// none.
fn conjunction(restrictions: Vec<SQLExpr>) -> Option<SQLExpr> {
    restrictions
        .into_iter()
        .reduce(|left, right| SQLExpr::BinaryOp {
            left: AstBox::new(nest_if_needed(left)),
            op: BinaryOperator::And,
            right: AstBox::new(nest_if_needed(right)),
        })
}

/// Parenthesize a restriction before it is combined with another, so an `OR`
/// inside a view body cannot swallow the conjunct next to it.
fn nest_if_needed(expr: SQLExpr) -> SQLExpr {
    match expr {
        expr @ (SQLExpr::Identifier(_)
        | SQLExpr::CompoundIdentifier(_)
        | SQLExpr::Value(_)
        | SQLExpr::Nested(_)) => expr,
        other => SQLExpr::Nested(AstBox::new(other)),
    }
}

/// Rename the column a reference names, mapping the view's namespace onto the
/// base relation's. `qualifiers` names the relation spellings a qualified
/// reference may carry; a reference qualified by anything else belongs to
/// another relation and is left alone. `Err` carries the computed view column
/// the expression tried to write through.
fn rewrite_view_column_references(
    expr: &mut SQLExpr,
    target: &ViewDmlTarget,
    qualifiers: &[String],
) -> Result<(), String> {
    let mut failed = None;
    visit_expr_mut(expr, &mut |expr| {
        if failed.is_some() {
            return;
        }
        let name = match expr {
            SQLExpr::Identifier(ident) => ident.value.clone(),
            SQLExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                if !qualifiers
                    .iter()
                    .any(|qualifier| parts[0].value.eq_ignore_ascii_case(qualifier))
                {
                    return;
                }
                parts[1].value.clone()
            }
            _ => return,
        };
        match target.base_column(&name) {
            None => {}
            Some(None) => failed = Some(name),
            Some(Some(base)) => {
                let base = Ident::new(base.to_string());
                *expr = match expr {
                    SQLExpr::CompoundIdentifier(parts) => {
                        SQLExpr::CompoundIdentifier(vec![parts[0].clone(), base])
                    }
                    _ => SQLExpr::Identifier(base),
                };
            }
        }
    });
    match failed {
        Some(column) => Err(column),
        None => Ok(()),
    }
}

fn visit_expr_mut(expr: &mut SQLExpr, visit: &mut impl FnMut(&mut SQLExpr)) {
    struct Walk<'a, F: FnMut(&mut SQLExpr)> {
        visit: &'a mut F,
    }
    impl<F: FnMut(&mut SQLExpr)> VisitorMut for Walk<'_, F> {
        type Break = ();
        fn post_visit_expr(&mut self, expr: &mut SQLExpr) -> ControlFlow<Self::Break> {
            (self.visit)(expr);
            ControlFlow::Continue(())
        }
    }
    let mut walk = Walk { visit };
    let _ = expr.visit(&mut walk);
}
