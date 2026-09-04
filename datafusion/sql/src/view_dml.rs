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
//! only the structural write retarget: relation names, INSERT column lists,
//! and UPDATE assignment targets. Predicates, assignment values, and RETURNING
//! remain user syntax until the statement planner binds them against the
//! view's semantic read scope and substitutes those bound expressions onto the
//! base row. Column defaults and hidden row identity then resolve through the
//! ordinary base-relation write path.
//!
//! [`ContextProvider::resolve_dml_view_target`]: crate::planner::ContextProvider::resolve_dml_view_target

use crate::ast_walk::Walk as AstWalk;
use crate::planner::{ViewDmlError, ViewDmlTarget};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{BoundSqlExpression, DmlCheckOption, LogicalPlan};
use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr as SQLExpr, Ident, ObjectName, ObjectNamePart,
    Query, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, Visitor,
};
use std::ops::ControlFlow;

/// Combine the already-bound check-option restrictions retained by the
/// catalog. View source is never reopened during DML planning.
pub(crate) fn bind_check_option(
    target: &ViewDmlTarget,
) -> Result<Option<DmlCheckOption>> {
    let Some(view_name) = target.check_option_view.clone() else {
        return Ok(None);
    };
    let predicate = target
        .check_option_restrictions
        .iter()
        .map(|restriction| restriction.expression().clone())
        .reduce(|left, right| left.and(right))
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "view {view_name} has a check-option marker without a row restriction"
            ))
        })?;
    Ok(Some(DmlCheckOption {
        view_name,
        predicate: BoundSqlExpression::new(predicate),
    }))
}

/// Record the retargeted write's already-bound check-option obligation on its
/// DML plan node. A RETURNING clause can leave the write under a projection,
/// so the DML node is found rather than assumed to be the root.
pub(crate) fn stamp_check_option(
    plan: LogicalPlan,
    check_option: Option<DmlCheckOption>,
) -> Result<LogicalPlan> {
    let Some(check_option) = check_option else {
        return Ok(plan);
    };
    plan.transform_down(|node| match node {
        LogicalPlan::Dml(mut dml) => {
            dml.check_option = Some(check_option.clone());
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
    /// View-level defaults for omitted columns, already mapped onto the final
    /// base relation. The INSERT planner applies these semantic expressions
    /// at the same projection boundary as ordinary table defaults.
    pub column_defaults: Vec<(String, BoundSqlExpression)>,
    pub returning: Option<Vec<SelectItem>>,
}

/// Retarget an INSERT's pieces onto the base relation. Without a column list
/// the write covers the view's columns in order, as wide as the source is;
/// naming them explicitly is what lets the base relation's own defaults fill
/// the rest.
pub(crate) fn rewrite_insert(
    target: &ViewDmlTarget,
    _written_table: &ObjectName,
    columns: &[Ident],
    source: Option<&Query>,
    returning: Option<&[SelectItem]>,
) -> Result<RewrittenInsert, RewriteFailure> {
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
            let base_column =
                target.base_column(&column.value).flatten().ok_or_else(|| {
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

    // Defaults attached to the view itself override the base relation's, but
    // only for view columns this statement omitted. Keep them semantic and
    // map their names onto the final base relation instead of appending AST
    // expressions to a VALUES list (which also missed INSERT ... SELECT).
    let mut column_defaults = Vec::new();
    if !columns.is_empty() {
        for (view_column, expression) in &target.column_defaults {
            if columns
                .iter()
                .any(|column| column.value.eq_ignore_ascii_case(view_column))
            {
                continue;
            }
            let base_column =
                target.base_column(view_column).flatten().ok_or_else(|| {
                    RewriteFailure {
                        verb: "insert into",
                        column: view_column.clone(),
                    }
                })?;
            column_defaults.push((base_column.to_string(), expression.clone()));
        }
    }

    // RETURNING remains in the view namespace. After binding, the DML planner
    // substitutes the view's semantic read expressions over the base row.
    let returning = returning.map(<[SelectItem]>::to_vec);
    Ok(RewrittenInsert {
        table: target.base_relation.clone(),
        columns: rewritten_columns,
        column_defaults,
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
        || predicate
            .is_some_and(|predicate| references_qualifier(predicate, &qualifiers))
        || returning
            .is_some_and(|items| returning_references_qualifier(items, &qualifiers));

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
        rewritten_assignments.push(assignment);
    }

    let rewritten_predicate = predicate.cloned();
    let returning = returning.map(<[SelectItem]>::to_vec);
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

    let rewritten_predicate = predicate.cloned();
    let returning = returning.map(<[SelectItem]>::to_vec);
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
        let Some(written) = name.0.last().and_then(ObjectNamePart::as_ident).cloned()
        else {
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
    let _ = expr.walk(&mut search);
    search.found
}
