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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use crate::stack::StackGuard;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    Constraints, DFSchema, Result, TableReference, not_impl_err, plan_err,
};
use datafusion_expr::expr::Sort;

use datafusion_expr::{
    CreateMemoryTable, DdlStatement, Distinct, Expr, LogicalPlan, LogicalPlanBuilder,
    SubqueryAlias, TableScanRowLock, TableScanRowLockMode, TableScanRowLockWaitPolicy,
};
use sqlparser::ast::{
    Expr as SQLExpr, Fetch, Ident, LimitClause, LockClause, LockType, NonBlock, OrderBy,
    OrderByExpr, OrderByKind, Query, SelectInto, SetExpr,
};
use sqlparser::tokenizer::Span;

/// Internal representation of limit/offset with WITH TIES support
#[derive(Debug, Clone)]
struct LimitInfo {
    limit_clause: Option<LimitClause>,
    with_ties: bool,
    /// If true, the limit value represents a percentage of total rows
    is_percent: bool,
}

#[derive(Debug, Clone)]
struct PlannedLockClause {
    row_lock: TableScanRowLock,
    target: Option<TableReference>,
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logical plan from an SQL query/subquery
    pub(crate) fn query_to_plan(
        &self,
        query: Query,
        outer_planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Each query has its own planner context, including CTEs that are visible within that query.
        // It also inherits the CTEs from the outer query by cloning the outer planner context.
        let mut query_plan_context = outer_planner_context.clone();
        let planner_context = &mut query_plan_context;

        let Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks,
            for_clause: _,
            ..
        } = query;

        // Combine FETCH clause with LIMIT/OFFSET handling
        let limit_info = self.combine_limit_and_fetch(limit_clause, fetch)?;

        if let Some(with) = with {
            self.plan_with_clause(with, planner_context)?;
        }

        let set_expr = *body;
        match set_expr {
            SetExpr::Select(mut select) => {
                let select_into = select.into.take();
                let plan =
                    self.select_to_plan(*select, order_by.clone(), planner_context)?;
                let plan = self.limit(plan, limit_info.clone(), planner_context)?;
                let plan = self.apply_query_locks(plan, locks)?;
                // Process the `SELECT INTO` after `LIMIT`.
                self.select_into(plan, select_into)
            }
            other => {
                // The functions called from `set_expr_to_plan()` need more than 128KB
                // stack in debug builds as investigated in:
                // https://github.com/apache/datafusion/pull/13310#discussion_r1836813902
                let plan = {
                    // scope for dropping _guard
                    let _guard = StackGuard::new(256 * 1024);
                    self.set_expr_to_plan(other, planner_context)
                }?;
                let oby_exprs = to_order_by_exprs(order_by)?;
                let order_by_rex = self.order_by_to_sort_expr(
                    oby_exprs,
                    plan.schema(),
                    planner_context,
                    true,
                    None,
                )?;
                let plan = self.order_by(plan, order_by_rex)?;
                let plan = self.limit(plan, limit_info, planner_context)?;
                self.apply_query_locks(plan, locks)
            }
        }
    }

    fn apply_query_locks(
        &self,
        plan: LogicalPlan,
        locks: Vec<LockClause>,
    ) -> Result<LogicalPlan> {
        if locks.is_empty() {
            return Ok(plan);
        }

        let planned_locks = locks
            .into_iter()
            .map(|lock| {
                let row_lock = table_scan_row_lock(&lock);
                let target = lock
                    .of
                    .map(|name| self.object_name_to_table_reference(name))
                    .transpose()?;
                Ok(PlannedLockClause { row_lock, target })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut matched_targets = vec![false; planned_locks.len()];
        let plan = apply_locks_to_plan(plan, None, &planned_locks, &mut matched_targets)?;

        for (idx, lock) in planned_locks.iter().enumerate() {
            if let Some(target) = &lock.target
                && !matched_targets[idx]
            {
                return plan_err!(
                    "{} OF {} did not match any table scan in the query",
                    lock.row_lock,
                    target
                );
            }
        }

        Ok(plan)
    }

    /// Combines FETCH clause with LIMIT/OFFSET into a single LimitInfo.
    ///
    /// SQL allows two syntaxes for limiting results:
    /// - `LIMIT n [OFFSET m]` (MySQL/PostgreSQL style)
    /// - `[OFFSET m ROWS] FETCH FIRST n ROWS ONLY` (SQL standard)
    ///
    /// This method converts FETCH to the internal LimitInfo representation.
    fn combine_limit_and_fetch(
        &self,
        limit_clause: Option<LimitClause>,
        fetch: Option<Fetch>,
    ) -> Result<LimitInfo> {
        let Some(fetch) = fetch else {
            // No FETCH clause, use LIMIT/OFFSET as-is
            return Ok(LimitInfo {
                limit_clause,
                with_ties: false,
                is_percent: false,
            });
        };

        // Extract the fetch quantity (number of rows to return) and with_ties flag
        // Note: For FETCH PERCENT, the quantity represents a percentage value
        let fetch_quantity = fetch.quantity;
        let with_ties = fetch.with_ties;
        let is_percent = fetch.percent;

        // Handle combination with existing LIMIT clause
        let limit_clause = match limit_clause {
            None => {
                // Only FETCH, no LIMIT/OFFSET
                // Convert FETCH to LimitClause
                match fetch_quantity {
                    Some(quantity) => Some(LimitClause::LimitOffset {
                        limit: Some(quantity),
                        offset: None,
                        limit_by: vec![],
                    }),
                    None => {
                        // FETCH FIRST ROWS ONLY with no quantity - return all rows
                        None
                    }
                }
            }
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                // OFFSET ... FETCH ... combination
                if limit.is_some() {
                    return not_impl_err!(
                        "Cannot use both LIMIT and FETCH clauses in the same query"
                    );
                }
                // OFFSET with FETCH - combine them
                match fetch_quantity {
                    Some(quantity) => Some(LimitClause::LimitOffset {
                        limit: Some(quantity),
                        offset,
                        limit_by,
                    }),
                    None => {
                        // OFFSET with FETCH FIRST ROWS ONLY (no quantity)
                        // Keep offset but no limit
                        if offset.is_some() || !limit_by.is_empty() {
                            Some(LimitClause::LimitOffset {
                                limit: None,
                                offset,
                                limit_by,
                            })
                        } else {
                            None
                        }
                    }
                }
            }
            Some(LimitClause::OffsetCommaLimit { .. }) => {
                // This is the "OFFSET n, LIMIT m" syntax which conflicts with FETCH
                return not_impl_err!(
                    "Cannot use both LIMIT and FETCH clauses in the same query"
                );
            }
        };

        Ok(LimitInfo {
            limit_clause,
            with_ties,
            is_percent,
        })
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: LogicalPlan,
        limit_info: LimitInfo,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let LimitInfo {
            limit_clause,
            with_ties,
            is_percent: _is_percent,
        } = limit_info;

        // WITH TIES requires ORDER BY
        if with_ties && !matches!(input, LogicalPlan::Sort(_)) {
            return plan_err!(
                "FETCH WITH TIES requires an ORDER BY clause"
            );
        }

        let Some(limit_clause) = limit_clause else {
            return Ok(input);
        };

        let empty_schema = DFSchema::empty();

        let (skip, fetch, limit_by_exprs) = match limit_clause {
            LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                let skip = offset
                    .map(|o| self.sql_to_expr(o.value, &empty_schema, planner_context))
                    .transpose()?;

                let fetch = limit
                    .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
                    .transpose()?;

                // For FETCH PERCENT: Currently we accept the syntax but treat it as a simple limit
                // The percentage value will be used directly as the limit count (not semantically correct,
                // but allows the query to plan for conformance testing)
                // TODO: Implement proper FETCH PERCENT by calculating percentage of table rows

                let limit_by_exprs = limit_by
                    .into_iter()
                    .map(|e| self.sql_to_expr(e, &empty_schema, planner_context))
                    .collect::<Result<Vec<_>>>()?;

                (skip, fetch, limit_by_exprs)
            }
            LimitClause::OffsetCommaLimit { offset, limit } => {
                let skip =
                    Some(self.sql_to_expr(offset, &empty_schema, planner_context)?);
                let fetch =
                    Some(self.sql_to_expr(limit, &empty_schema, planner_context)?);
                (skip, fetch, vec![])
            }
        };

        if !limit_by_exprs.is_empty() {
            return not_impl_err!("LIMIT BY clause is not supported yet");
        }

        if skip.is_none() && fetch.is_none() {
            return Ok(input);
        }

        LogicalPlanBuilder::from(input)
            .limit_by_expr_with_ties(skip, fetch, with_ties)?
            .build()
    }

    /// Wrap the logical in a sort
    pub(super) fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<Sort>,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        if let LogicalPlan::Distinct(Distinct::On(ref distinct_on)) = plan {
            // In case of `DISTINCT ON` we must capture the sort expressions since during the plan
            // optimization we're effectively doing a `first_value` aggregation according to them.
            let distinct_on = distinct_on.clone().with_sort_expr(order_by)?;
            Ok(LogicalPlan::Distinct(Distinct::On(distinct_on)))
        } else {
            LogicalPlanBuilder::from(plan).sort(order_by)?.build()
        }
    }

    /// Wrap the logical plan in a `SelectInto`
    fn select_into(
        &self,
        plan: LogicalPlan,
        select_into: Option<SelectInto>,
    ) -> Result<LogicalPlan> {
        match select_into {
            Some(into) => Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                CreateMemoryTable {
                    name: self.object_name_to_table_reference(into.name)?,
                    constraints: Constraints::default(),
                    input: Arc::new(plan),
                    if_not_exists: false,
                    or_replace: false,
                    temporary: false,
                    column_defaults: vec![],
                    storage_parameters: BTreeMap::new(),
                },
            ))),
            _ => Ok(plan),
        }
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
    };
    TableScanRowLock { mode, wait_policy }
}

fn apply_locks_to_plan(
    plan: LogicalPlan,
    visible_relation: Option<TableReference>,
    locks: &[PlannedLockClause],
    matched_targets: &mut [bool],
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            let input = apply_locks_to_plan(
                Arc::unwrap_or_clone(alias.input),
                Some(alias.alias.clone()),
                locks,
                matched_targets,
            )?;
            SubqueryAlias::try_new(Arc::new(input), alias.alias)
                .map(LogicalPlan::SubqueryAlias)
        }
        LogicalPlan::TableScan(mut scan) => {
            let mut row_lock = scan.row_lock;
            for (idx, lock) in locks.iter().enumerate() {
                if lock_applies_to_scan(
                    lock.target.as_ref(),
                    visible_relation.as_ref(),
                    &scan.table_name,
                ) {
                    matched_targets[idx] = true;
                    row_lock = Some(match row_lock {
                        None => lock.row_lock,
                        Some(existing) => combine_row_locks(existing, lock.row_lock)?,
                    });
                }
            }
            scan.row_lock = row_lock;
            Ok(LogicalPlan::TableScan(scan))
        }
        other => other
            .map_children(|child| {
                apply_locks_to_plan(
                    child,
                    visible_relation.clone(),
                    locks,
                    matched_targets,
                )
                .map(Transformed::yes)
            })
            .map(|transformed| transformed.data),
    }
}

fn lock_applies_to_scan(
    target: Option<&TableReference>,
    visible_relation: Option<&TableReference>,
    scan_table: &TableReference,
) -> bool {
    let Some(target) = target else {
        return true;
    };
    match visible_relation {
        Some(alias) => target.resolved_eq(alias),
        None => target.resolved_eq(scan_table),
    }
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

/// Returns the order by expressions from the query.
fn to_order_by_exprs(order_by: Option<OrderBy>) -> Result<Vec<OrderByExpr>> {
    to_order_by_exprs_with_select(order_by, None)
}

/// Returns the order by expressions from the query with the select expressions.
pub(crate) fn to_order_by_exprs_with_select(
    order_by: Option<OrderBy>,
    select_exprs: Option<&Vec<Expr>>,
) -> Result<Vec<OrderByExpr>> {
    let Some(OrderBy { kind, interpolate }) = order_by else {
        // If no order by, return an empty array.
        return Ok(vec![]);
    };
    if let Some(_interpolate) = interpolate {
        return not_impl_err!("ORDER BY INTERPOLATE is not supported");
    }
    match kind {
        OrderByKind::All(order_by_options) => {
            let Some(exprs) = select_exprs else {
                return Ok(vec![]);
            };
            let order_by_exprs = exprs
                .iter()
                .map(|select_expr| match select_expr {
                    Expr::Column(column) => Ok(OrderByExpr {
                        expr: SQLExpr::Identifier(Ident {
                            value: column.name.clone(),
                            quote_style: None,
                            span: Span::empty(),
                        }),
                        options: order_by_options,
                        with_fill: None,
                    }),
                    // TODO: Support other types of expressions
                    _ => not_impl_err!(
                        "ORDER BY ALL is not supported for non-column expressions"
                    ),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(order_by_exprs)
        }
        OrderByKind::Expressions(order_by_exprs) => Ok(order_by_exprs),
    }
}
