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

//! Row locking clauses (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`,
//! `FOR KEY SHARE`). Each `OF` target resolves among the FROM items of the
//! clause's own query level by reference name, as PostgreSQL's parse analysis
//! resolves it; a clause may not lock a relation an outer join can
//! null-extend, since no row of that relation stands behind a null-extended
//! result row; and the lock is recorded on every table scan it reaches. A
//! clause locks no row of a common table expression: one without `OF` passes
//! over CTE references, at its own level and in the derived tables it locks,
//! and one whose `OF` names a CTE is refused, as PostgreSQL's
//! `transformLockingClause` treats `RTE_CTE`.

use std::sync::Arc;

use datafusion_common::plan_err;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, sqlstate_datafusion_err};
use datafusion_expr::{
    LogicalPlan, SubqueryAlias, TableScanRowLock, TableScanRowLockMode,
    TableScanRowLockWaitPolicy,
};
use sqlparser::ast::{
    JoinOperator, LockClause, LockType, NonBlock, ObjectNamePart,
    OraclePartitionedJoinKind, Query, Select, SetExpr, TableAlias, TableFactor,
    TableWithJoins,
};

use crate::planner::{PlannerContext, SqlToRel};

const SYNTAX_ERROR: &str = "42601";
const UNDEFINED_TABLE: &str = "42P01";
const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// A locking clause planned against its query: the lock it records on a
/// table scan, its strength as the statement spelled it, and the reference
/// name of its `OF` target.
struct PlannedLock {
    row_lock: TableScanRowLock,
    strength: LockType,
    target: Option<String>,
}

/// What kind of FROM item a range entry is, which decides whether a locking
/// clause may name it.
enum EntryKind<'q> {
    /// A stored relation (a table or a view).
    Relation,
    /// A derived table; locking it locks every relation it reads.
    Subquery(&'q Query),
    /// A common table expression reference.
    Cte,
    /// A set-returning function call.
    Function,
    /// A table-producing construct other than a function call (`JSON_TABLE`,
    /// `XMLTABLE`, a graph or pattern-matching table, an external table).
    TableFunction,
    /// An aliased parenthesized join.
    Join,
}

/// A FROM item as a locking clause addresses it: PostgreSQL's range-table
/// entry.
struct RangeEntry<'q> {
    /// The item's reference name: its alias, or the unqualified name of a
    /// relation or function. An unaliased derived table has none.
    name: Option<String>,
    kind: EntryKind<'q>,
    /// An outer join of this query level can null-extend the item's rows.
    nullable: bool,
}

impl RangeEntry<'_> {
    /// A stored relation or a derived table: the items a locking clause
    /// without `OF` locks. The other kinds are named in the FROM clause but
    /// hold no rows a lock could mark.
    fn lockable(&self) -> bool {
        matches!(self.kind, EntryKind::Relation | EntryKind::Subquery(_))
    }
}

impl SqlToRel<'_> {
    /// Apply `query`'s locking clauses to `plan`, the plan of its body.
    /// Every `OF` target resolves before any lock is checked against the
    /// join tree, PostgreSQL's order: an unresolvable target is reported even
    /// when another clause locks a null-extended relation.
    pub(crate) fn apply_query_locks(
        &self,
        plan: LogicalPlan,
        query: &Query,
        planner_context: &PlannerContext,
    ) -> Result<LogicalPlan> {
        if query.locks.is_empty() {
            return Ok(plan);
        }
        let locks = query
            .locks
            .iter()
            .map(|lock| self.plan_lock(lock))
            .collect::<Result<Vec<_>>>()?;

        let ctes = match query.body.as_ref() {
            SetExpr::Select(select) => {
                Some(self.cte_references(select, planner_context, &[]))
            }
            _ => None,
        };
        let join_aliases = match query.body.as_ref() {
            SetExpr::Select(select) => {
                let range = self.lock_range(select, planner_context, &[]);
                let targets = locks
                    .iter()
                    .map(|lock| resolve_target(&range, lock))
                    .collect::<Result<Vec<_>>>()?;
                self.reject_null_extended_locks(
                    &range,
                    &locks,
                    &targets,
                    planner_context,
                )?;
                range
                    .iter()
                    .filter(|entry| matches!(entry.kind, EntryKind::Join))
                    .filter_map(|entry| entry.name.clone())
                    .collect()
            }
            _ => Vec::new(),
        };

        let mut matched = vec![false; locks.len()];
        let plan = apply_locks_to_plan(
            plan,
            &LockScope::Level,
            &join_aliases,
            ctes.as_ref(),
            &locks,
            &mut matched,
        )?;
        for (lock, matched) in locks.iter().zip(matched) {
            if let Some(target) = &lock.target
                && !matched
            {
                return Err(target_not_found(target, lock.strength));
            }
        }
        Ok(plan)
    }

    fn plan_lock(&self, lock: &LockClause) -> Result<PlannedLock> {
        let target = match &lock.of {
            None => None,
            Some(name) => {
                let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
                    return Err(sqlstate_datafusion_err(
                        SYNTAX_ERROR,
                        format!(
                            "FOR {} must specify unqualified relation names",
                            lock.lock_type
                        ),
                    ));
                };
                Some(self.ident_normalizer.normalize(ident.clone()))
            }
        };
        Ok(PlannedLock {
            row_lock: table_scan_row_lock(lock),
            strength: lock.lock_type,
            target,
        })
    }

    /// The FROM items of one query level, in PostgreSQL's range-table order:
    /// a parenthesized join's own entry follows its members'. `nested_ctes`
    /// names the common table expressions a nested query declares, beside
    /// the ones the planner context already holds.
    fn lock_range<'q>(
        &self,
        select: &'q Select,
        planner_context: &PlannerContext,
        nested_ctes: &[String],
    ) -> Vec<RangeEntry<'q>> {
        let mut range = Vec::new();
        for item in &select.from {
            self.collect_table_with_joins(
                item,
                false,
                planner_context,
                nested_ctes,
                &mut range,
            );
        }
        range
    }

    fn collect_table_with_joins<'q>(
        &self,
        item: &'q TableWithJoins,
        nullable: bool,
        planner_context: &PlannerContext,
        nested_ctes: &[String],
        range: &mut Vec<RangeEntry<'q>>,
    ) {
        let start = range.len();
        self.collect_factor(
            &item.relation,
            nullable,
            planner_context,
            nested_ctes,
            range,
        );
        for join in &item.joins {
            let right_start = range.len();
            let (left_nullable, right_nullable) =
                outer_join_nullable_sides(&join.join_operator);
            self.collect_factor(
                &join.relation,
                nullable || right_nullable,
                planner_context,
                nested_ctes,
                range,
            );
            if left_nullable {
                for entry in &mut range[start..right_start] {
                    entry.nullable = true;
                }
            }
        }
    }

    fn collect_factor<'q>(
        &self,
        factor: &'q TableFactor,
        nullable: bool,
        planner_context: &PlannerContext,
        nested_ctes: &[String],
        range: &mut Vec<RangeEntry<'q>>,
    ) {
        let alias_name = |alias: &Option<TableAlias>| {
            alias.as_ref().map(|alias| self.alias_name(alias))
        };
        let (name, kind) = match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } => {
                let relation = name
                    .0
                    .last()
                    .and_then(ObjectNamePart::as_ident)
                    .map(|ident| self.ident_normalizer.normalize(ident.clone()));
                let cte_reference = name.0.len() == 1
                    && relation.as_ref().is_some_and(|relation| {
                        nested_ctes.contains(relation)
                            || planner_context.get_cte(relation).is_some()
                    });
                let kind = if cte_reference {
                    EntryKind::Cte
                } else {
                    EntryKind::Relation
                };
                (alias_name(alias).or(relation), kind)
            }
            TableFactor::Table {
                name,
                alias,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name, alias, .. } => {
                let function = name
                    .0
                    .last()
                    .and_then(ObjectNamePart::as_ident)
                    .map(|ident| self.ident_normalizer.normalize(ident.clone()));
                (alias_name(alias).or(function), EntryKind::Function)
            }
            TableFactor::UNNEST { alias, .. } => (
                alias_name(alias).or_else(|| Some("unnest".to_string())),
                EntryKind::Function,
            ),
            TableFactor::TableFunction { alias, .. }
            | TableFactor::RowsFrom { alias, .. } => {
                (alias_name(alias), EntryKind::Function)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => (alias_name(alias), EntryKind::Subquery(subquery)),
            TableFactor::JsonTable { alias, .. }
            | TableFactor::OpenJsonTable { alias, .. }
            | TableFactor::OracleExternal { alias, .. }
            | TableFactor::Pivot { alias, .. }
            | TableFactor::Unpivot { alias, .. }
            | TableFactor::MatchRecognize { alias, .. }
            | TableFactor::XmlTable { alias, .. }
            | TableFactor::GraphTable { alias, .. } => {
                (alias_name(alias), EntryKind::TableFunction)
            }
            TableFactor::SqlJsonTable(table) => {
                (alias_name(&table.alias), EntryKind::TableFunction)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                self.collect_table_with_joins(
                    table_with_joins,
                    nullable,
                    planner_context,
                    nested_ctes,
                    range,
                );
                let Some(alias) = alias else {
                    return;
                };
                (Some(self.alias_name(alias)), EntryKind::Join)
            }
        };
        range.push(RangeEntry {
            name,
            kind,
            nullable,
        });
    }

    /// The CTE references among `select`'s FROM items and those of the
    /// derived tables it reads. `ctes` names the common table expressions an
    /// enclosing nested query declares, beside the planner context's.
    fn cte_references(
        &self,
        select: &Select,
        planner_context: &PlannerContext,
        ctes: &[String],
    ) -> CteReferences {
        let mut references = CteReferences::default();
        for entry in self.lock_range(select, planner_context, ctes) {
            match entry.kind {
                EntryKind::Cte => references.names.extend(entry.name),
                EntryKind::Subquery(query) => {
                    let inner = self.query_cte_references(query, planner_context, ctes);
                    match entry.name {
                        Some(name) => references.derived.push((name, inner)),
                        None => {
                            references.names.extend(inner.names);
                            references.derived.extend(inner.derived);
                        }
                    }
                }
                EntryKind::Relation
                | EntryKind::Function
                | EntryKind::TableFunction
                | EntryKind::Join => {}
            }
        }
        references
    }

    /// [`Self::cte_references`] of the body of `query`, a derived table,
    /// with the common table expressions it declares in scope.
    fn query_cte_references(
        &self,
        query: &Query,
        planner_context: &PlannerContext,
        enclosing_ctes: &[String],
    ) -> CteReferences {
        let mut ctes = enclosing_ctes.to_vec();
        if let Some(with) = &query.with {
            ctes.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| self.alias_name(&cte.alias)),
            );
        }
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                self.cte_references(select, planner_context, &ctes)
            }
            _ => CteReferences::default(),
        }
    }

    fn alias_name(&self, alias: &TableAlias) -> String {
        self.ident_normalizer.normalize(alias.name.clone())
    }

    /// Refuse a lock on a relation an outer join can null-extend. A locked
    /// derived table locks every relation it reads, so an outer join inside
    /// it counts as well.
    fn reject_null_extended_locks(
        &self,
        range: &[RangeEntry<'_>],
        locks: &[PlannedLock],
        targets: &[Option<usize>],
        planner_context: &PlannerContext,
    ) -> Result<()> {
        let mut strengths: Vec<Option<LockType>> = vec![None; range.len()];
        for (lock, target) in locks.iter().zip(targets) {
            let locked: Vec<usize> = match target {
                Some(index) => vec![*index],
                None => (0..range.len())
                    .filter(|&index| range[index].lockable())
                    .collect(),
            };
            for index in locked {
                strengths[index] = Some(match strengths[index] {
                    Some(existing) => stronger(existing, lock.strength),
                    None => lock.strength,
                });
            }
        }
        for (entry, strength) in range.iter().zip(strengths) {
            let Some(strength) = strength else {
                continue;
            };
            let null_extended = entry.nullable
                || match entry.kind {
                    EntryKind::Subquery(subquery) => {
                        self.locks_null_extended_rows(subquery, planner_context, &[])
                    }
                    _ => false,
                };
            if null_extended {
                return Err(sqlstate_datafusion_err(
                    FEATURE_NOT_SUPPORTED,
                    format!(
                        "FOR {strength} cannot be applied to the nullable side of an outer join"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Whether locking every relation `query` reads locks one an outer join
    /// of its own can null-extend.
    fn locks_null_extended_rows(
        &self,
        query: &Query,
        planner_context: &PlannerContext,
        enclosing_ctes: &[String],
    ) -> bool {
        let mut ctes = enclosing_ctes.to_vec();
        if let Some(with) = &query.with {
            ctes.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| self.alias_name(&cte.alias)),
            );
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };
        self.lock_range(select, planner_context, &ctes)
            .iter()
            .filter(|entry| entry.lockable())
            .any(|entry| {
                entry.nullable
                    || match entry.kind {
                        EntryKind::Subquery(subquery) => self.locks_null_extended_rows(
                            subquery,
                            planner_context,
                            &ctes,
                        ),
                        _ => false,
                    }
            })
    }
}

/// The range entry a locking clause's `OF` target names: the first FROM item
/// with that reference name, which must be one a lock can mark.
fn resolve_target(range: &[RangeEntry<'_>], lock: &PlannedLock) -> Result<Option<usize>> {
    let Some(target) = &lock.target else {
        return Ok(None);
    };
    let Some(index) = range
        .iter()
        .position(|entry| entry.name.as_deref() == Some(target.as_str()))
    else {
        return Err(target_not_found(target, lock.strength));
    };
    let refused = match range[index].kind {
        EntryKind::Relation | EntryKind::Subquery(_) => return Ok(Some(index)),
        EntryKind::Cte => "a WITH query",
        EntryKind::Function => "a function",
        EntryKind::TableFunction => "a table function",
        EntryKind::Join => "a join",
    };
    Err(sqlstate_datafusion_err(
        FEATURE_NOT_SUPPORTED,
        format!("FOR {} cannot be applied to {refused}", lock.strength),
    ))
}

fn target_not_found(
    target: &str,
    strength: LockType,
) -> datafusion_common::DataFusionError {
    sqlstate_datafusion_err(
        UNDEFINED_TABLE,
        format!(
            "relation \"{target}\" in FOR {strength} clause not found in FROM clause"
        ),
    )
}

/// The stronger of two lock strengths, as a relation locked by several
/// clauses holds the strongest of them.
fn stronger(left: LockType, right: LockType) -> LockType {
    let rank = |strength: LockType| match strength {
        LockType::KeyShare => 0,
        LockType::Share => 1,
        LockType::NoKeyUpdate => 2,
        LockType::Update => 3,
    };
    if rank(right) > rank(left) {
        right
    } else {
        left
    }
}

/// Which inputs of a join, `(left, right)`, the join can null-extend.
fn outer_join_nullable_sides(operator: &JoinOperator) -> (bool, bool) {
    match operator {
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) | JoinOperator::OuterApply => {
            (false, true)
        }
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => (true, false),
        JoinOperator::FullOuter(_) => (true, true),
        JoinOperator::OraclePartitioned { kind, .. } => {
            match kind {
                OraclePartitionedJoinKind::Left
                | OraclePartitionedJoinKind::LeftOuter => (false, true),
                OraclePartitionedJoinKind::Right
                | OraclePartitionedJoinKind::RightOuter => (true, false),
                OraclePartitionedJoinKind::FullOuter => (true, true),
            }
        }
        JoinOperator::Join(_)
        | JoinOperator::Inner(_)
        | JoinOperator::CrossJoin(_)
        | JoinOperator::Semi(_)
        | JoinOperator::LeftSemi(_)
        | JoinOperator::RightSemi(_)
        | JoinOperator::Anti(_)
        | JoinOperator::LeftAnti(_)
        | JoinOperator::RightAnti(_)
        | JoinOperator::CrossApply
        | JoinOperator::AsOf { .. }
        | JoinOperator::StraightJoin(_) => (false, false),
    }
}

fn table_scan_row_lock(lock: &LockClause) -> TableScanRowLock {
    let mode = match lock.lock_type {
        LockType::KeyShare => TableScanRowLockMode::ForKeyShare,
        LockType::Share => TableScanRowLockMode::ForShare,
        LockType::NoKeyUpdate => TableScanRowLockMode::ForNoKeyUpdate,
        LockType::Update => TableScanRowLockMode::ForUpdate,
    };
    let wait_policy = match lock.nonblock {
        None => TableScanRowLockWaitPolicy::Block,
        Some(NonBlock::Nowait) => TableScanRowLockWaitPolicy::Nowait,
        Some(NonBlock::SkipLocked) => TableScanRowLockWaitPolicy::SkipLocked,
        Some(NonBlock::Wait(seconds)) => TableScanRowLockWaitPolicy::Wait(seconds),
    };
    TableScanRowLock { mode, wait_policy }
}

/// The FROM items of one query level that are common table expression
/// references, by reference name, and those of each aliased derived table
/// it reads. An unaliased derived table's are the level's own, since its
/// FROM items reach the plan without an alias of their own.
#[derive(Default)]
struct CteReferences {
    names: Vec<String>,
    derived: Vec<(String, CteReferences)>,
}

impl CteReferences {
    /// The CTE references of the derived table `name` reads.
    fn of_derived(&self, name: &str) -> Option<&CteReferences> {
        self.derived
            .iter()
            .find(|(derived, _)| derived == name)
            .map(|(_, references)| references)
    }
}

/// Where in a query level's plan a table scan sits, for the locks it takes.
#[derive(Clone)]
enum LockScope {
    /// Among the FROM items of the query level itself: an aliased item or a
    /// bare table scan is addressed by its reference name.
    Level,
    /// Inside a FROM item; the locks naming that item reach every scan in it.
    Within(Vec<usize>),
}

/// Record the locks on the table scans they reach: a clause without `OF` on
/// every scan, and one with `OF` on every scan of the FROM item it names.
/// The plan nodes of an aliased parenthesized join (`join_aliases`) stay at
/// the level, since its members are FROM items of the level themselves. No
/// lock reaches into a CTE reference `ctes` names.
fn apply_locks_to_plan(
    plan: LogicalPlan,
    scope: &LockScope,
    join_aliases: &[String],
    ctes: Option<&CteReferences>,
    locks: &[PlannedLock],
    matched: &mut [bool],
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            let name = alias.alias.table();
            if ctes.is_some_and(|ctes| ctes.names.iter().any(|cte| cte == name)) {
                return Ok(LogicalPlan::SubqueryAlias(alias));
            }
            let (inner_scope, inner_ctes) = match scope {
                LockScope::Level if join_aliases.iter().any(|join| join == name) => {
                    (LockScope::Level, ctes)
                }
                LockScope::Level => (
                    LockScope::Within(targeting(locks, name, matched)),
                    ctes.and_then(|ctes| ctes.of_derived(name)),
                ),
                LockScope::Within(_) => {
                    (scope.clone(), ctes.and_then(|ctes| ctes.of_derived(name)))
                }
            };
            let input = apply_locks_to_plan(
                Arc::unwrap_or_clone(alias.input),
                &inner_scope,
                join_aliases,
                inner_ctes,
                locks,
                matched,
            )?;
            SubqueryAlias::try_new(Arc::new(input), alias.alias)
                .map(LogicalPlan::SubqueryAlias)
        }
        LogicalPlan::TableScan(mut scan) => {
            let mut applied: Vec<usize> = locks
                .iter()
                .enumerate()
                .filter(|(_, lock)| lock.target.is_none())
                .map(|(index, _)| index)
                .collect();
            match scope {
                LockScope::Level => {
                    applied.extend(targeting(locks, scan.table_name.table(), matched))
                }
                LockScope::Within(inherited) => applied.extend(inherited),
            }
            for index in applied {
                let lock = locks[index].row_lock;
                scan.row_lock = Some(match scan.row_lock {
                    None => lock,
                    Some(existing) => combine_row_locks(existing, lock)?,
                });
            }
            Ok(LogicalPlan::TableScan(scan))
        }
        other => other
            .map_children(|child| {
                apply_locks_to_plan(child, scope, join_aliases, ctes, locks, matched)
                    .map(Transformed::yes)
            })
            .map(|transformed| transformed.data),
    }
}

/// The locks whose `OF` target is `name`, marked as having reached a scan.
fn targeting(locks: &[PlannedLock], name: &str, matched: &mut [bool]) -> Vec<usize> {
    let mut found = Vec::new();
    for (index, lock) in locks.iter().enumerate() {
        if lock.target.as_deref() == Some(name) {
            matched[index] = true;
            found.push(index);
        }
    }
    found
}

fn combine_row_locks(
    left: TableScanRowLock,
    right: TableScanRowLock,
) -> Result<TableScanRowLock> {
    if left.wait_policy != TableScanRowLockWaitPolicy::Block
        && right.wait_policy != TableScanRowLockWaitPolicy::Block
        && left.wait_policy != right.wait_policy
    {
        return plan_err!(
            "conflicting row lock wait policies for the same table scan: {} and {}",
            left.wait_policy,
            right.wait_policy
        );
    }

    let wait_policy = if left.wait_policy != TableScanRowLockWaitPolicy::Block {
        left.wait_policy
    } else {
        right.wait_policy
    };

    Ok(TableScanRowLock {
        mode: left.mode.max(right.mode),
        wait_policy,
    })
}
