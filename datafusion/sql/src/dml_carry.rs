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

//! Carried junk columns.
//!
//! A refreshed target row of an UPDATE or DELETE is requalified with every
//! other relation of the statement's input fixed at the row it joined, as
//! PostgreSQL's EvalPlanQual re-runs the plan. The statement's input therefore
//! carries, for each relation other than the target, the columns of its joined
//! row that the statement's WHERE clause and SET list read, plus a presence
//! column, as PostgreSQL's `preprocess_targetlist` adds its rowmark junk
//! columns. They are planned here, before logical optimization, so projection
//! pruning keeps them: the DML node reads every column of its input.
//!
//! A positive `IN` over an uncorrelated subquery and a correlated `EXISTS` of
//! the WHERE clause are pulled up into [`JoinType::LeftSemiCarry`] joins, the
//! sublinks PostgreSQL's `pull_up_sublinks` turns into semi joins, so the
//! partner row a target row matched first is carried with it. Every other
//! sublink stays a sub-plan the recheck evaluates again.
//!
//! Each locking query level (`SELECT ... FOR UPDATE OF t`, at the statement's
//! top or inside it) pulls up the same sublinks and carries the columns of
//! each relation it does not lock that a predicate linking it to a locked
//! relation reads, as junk columns its row-lock operator consumes and strips;
//! the level's own output does not show them.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::metadata::FieldMetadata;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    Column, DFSchemaRef, JUNK_COLUMN_METADATA_KEY, JUNK_RELATION_METADATA_KEY,
    JUNK_SOURCE_COLUMN_METADATA_KEY, JoinType, Result, TableReference, internal_err,
};
use datafusion_expr::expr::{AllExpr, AnyExpr, Exists, InSubquery, QuantifiedSource};
use datafusion_expr::utils::{conjunction, split_conjunction, split_conjunction_owned};
use datafusion_expr::{
    CarriedJunkColumns, Expr, Filter, Join, LogicalPlan, LogicalPlanBuilder, Projection,
    Subquery, SubqueryAlias, carried_junk_column_name, carried_presence_column_name, lit,
};

/// The column a pulled-up `IN` subquery's output is carried under.
const SEMI_IN_COLUMN: &str = "__c_in";

/// The metadata of a junk column carrying column `source_column` of
/// `relation`, or of `relation`'s presence column when `source_column` is
/// `None`.
pub(crate) fn junk_metadata(
    relation: &TableReference,
    source_column: Option<&str>,
) -> FieldMetadata {
    let mut metadata = HashMap::from([
        (JUNK_COLUMN_METADATA_KEY.to_string(), "true".to_string()),
        (JUNK_RELATION_METADATA_KEY.to_string(), relation.to_string()),
    ]);
    if let Some(column) = source_column {
        metadata.insert(
            JUNK_SOURCE_COLUMN_METADATA_KEY.to_string(),
            column.to_string(),
        );
    }
    FieldMetadata::from(metadata)
}

/// The carried part of a DML input: the input with its pulled-up semi joins,
/// the junk expressions its projection appends after the target's columns,
/// and the record of each carried relation.
pub(crate) struct CarriedDmlInput {
    pub(crate) source: LogicalPlan,
    pub(crate) junk: Vec<Expr>,
    pub(crate) carried: Vec<CarriedJunkColumns>,
}

/// Plan the carried junk columns of an UPDATE's or DELETE's input.
///
/// `source` is the statement's join tree, under its WHERE filter when it has
/// one; `target` is the relation the target's columns are named through;
/// `set_exprs` are the values the SET list assigns (empty for a DELETE). The
/// WHERE clause's pullable sublinks become [`JoinType::LeftSemiCarry`] joins,
/// and each relation of the result other than the target is carried with
/// the columns of it the WHERE clause and the SET list read.
pub(crate) fn carry_dml_input(
    source: LogicalPlan,
    target: &TableReference,
    set_exprs: &[Expr],
) -> Result<CarriedDmlInput> {
    let (source, filter, semi_conditions) = match source {
        LogicalPlan::Filter(filter) => {
            let pulled =
                pull_up_semi_joins(Arc::unwrap_or_clone(filter.input), filter.predicate)?;
            let source = match &pulled.filter {
                Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                    predicate.clone(),
                    Arc::new(pulled.plan),
                )?),
                None => pulled.plan,
            };
            (source, pulled.filter, pulled.conditions)
        }
        other => (other, None, Vec::new()),
    };

    let mut reads: Vec<Column> = Vec::new();
    for expr in set_exprs
        .iter()
        .chain(filter.iter())
        .chain(semi_conditions.iter())
    {
        collect_read_columns(expr, &mut reads)?;
    }

    let schema = Arc::clone(source.schema());
    let nullable = nullable_relations(&source)?;
    let mut junk = Vec::new();
    let mut carried = Vec::new();
    for relation in schema_relations(&schema) {
        if relation.resolved_eq(target) {
            continue;
        }
        let ordinal = carried.len();
        let columns = relation_columns_read(&schema, &relation, &reads);
        let mut junk_names = Vec::with_capacity(columns.len());
        for column in &columns {
            let name = carried_junk_column_name(ordinal, column);
            junk.push(
                Expr::Column(Column::new(Some(relation.clone()), column))
                    .alias_with_metadata(
                        name.clone(),
                        Some(junk_metadata(&relation, Some(column.as_str()))),
                    ),
            );
            junk_names.push(name);
        }
        let presence_name = carried_presence_column_name(ordinal);
        let presence = if nullable.iter().any(|candidate| candidate == &relation) {
            presence_of_nullable(&source, &schema, &relation)?
        } else {
            lit(true)
        };
        junk.push(presence.alias_with_metadata(
            presence_name.clone(),
            Some(junk_metadata(&relation, None)),
        ));
        carried.push(CarriedJunkColumns {
            relation,
            columns: junk_names,
            presence: presence_name,
        });
    }
    Ok(CarriedDmlInput {
        source,
        junk,
        carried,
    })
}

/// What pulling the WHERE clause's sublinks up produced.
struct PulledSemiJoins {
    plan: LogicalPlan,
    /// The conjuncts that stayed in the WHERE filter.
    filter: Option<Expr>,
    /// The condition of every semi join pulled up.
    conditions: Vec<Expr>,
}

fn pull_up_semi_joins(input: LogicalPlan, predicate: Expr) -> Result<PulledSemiJoins> {
    let mut plan = input;
    let mut remaining = Vec::new();
    let mut conditions = Vec::new();
    let mut ordinal = 0usize;
    for conjunct in split_conjunction_owned(predicate) {
        let alias = TableReference::bare(format!("__carried_semi_{ordinal}"));
        match semi_join_of(&conjunct, &alias)? {
            Some((inner, condition)) => {
                plan = LogicalPlanBuilder::from(plan)
                    .join_on(inner, JoinType::LeftSemiCarry, condition.iter().cloned())?
                    .build()?;
                conditions.extend(condition);
                ordinal += 1;
            }
            None => remaining.push(conjunct),
        }
    }
    Ok(PulledSemiJoins {
        plan,
        filter: conjunction(remaining),
        conditions,
    })
}

/// The carried semi join a WHERE conjunct is, aliased `alias`: its inner side
/// and its condition. `None` when the conjunct is not a sublink PostgreSQL
/// pulls up: a positive `IN` over an uncorrelated subquery, or a positive
/// `EXISTS` whose subquery's WHERE clause holds its only correlation.
fn semi_join_of(
    conjunct: &Expr,
    alias: &TableReference,
) -> Result<Option<(LogicalPlan, Vec<Expr>)>> {
    match conjunct {
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated: false,
        }) => {
            if plan_has_outer_reference(&subquery.subquery)? {
                return Ok(None);
            }
            let LogicalPlan::Projection(projection) = subquery.subquery.as_ref() else {
                return Ok(None);
            };
            let [output] = projection.expr.as_slice() else {
                return Ok(None);
            };
            let carried_output = output.clone().unalias().alias_with_metadata(
                SEMI_IN_COLUMN,
                Some(junk_metadata(alias, Some(SEMI_IN_COLUMN))),
            );
            let inner =
                carried_side(alias, vec![carried_output], Arc::clone(&projection.input))?;
            let condition = expr.as_ref().clone().eq(Expr::Column(Column::new(
                Some(alias.clone()),
                SEMI_IN_COLUMN,
            )));
            Ok(Some((inner, vec![condition])))
        }
        Expr::Exists(Exists {
            subquery,
            negated: false,
        }) => {
            let LogicalPlan::Projection(projection) = subquery.subquery.as_ref() else {
                return Ok(None);
            };
            for expr in &projection.expr {
                if expr_has_outer_reference(expr)? {
                    return Ok(None);
                }
            }
            let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
                return Ok(None);
            };
            let mut correlated = Vec::new();
            let mut local = Vec::new();
            for conjunct in split_conjunction(&filter.predicate) {
                if expr_has_outer_reference(conjunct)? {
                    correlated.push(conjunct.clone());
                } else {
                    local.push(conjunct.clone());
                }
            }
            if correlated.is_empty() {
                return Ok(None);
            }
            for conjunct in &correlated {
                if expr_has_subquery(conjunct)? {
                    return Ok(None);
                }
            }
            let rest = match conjunction(local) {
                Some(predicate) => Arc::new(LogicalPlan::Filter(Filter::try_new(
                    predicate,
                    Arc::clone(&filter.input),
                )?)),
                None => Arc::clone(&filter.input),
            };
            if plan_has_outer_reference(&rest)? {
                return Ok(None);
            }
            let mut inner_columns: Vec<Column> = Vec::new();
            for conjunct in &correlated {
                conjunct.apply(|expr| {
                    if let Expr::Column(column) = expr
                        && !inner_columns.contains(column)
                    {
                        inner_columns.push(column.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            if inner_columns.is_empty() {
                return Ok(None);
            }
            let carried_name = |position: usize| format!("__c{position}");
            let exprs = inner_columns
                .iter()
                .enumerate()
                .map(|(position, column)| {
                    let name = carried_name(position);
                    let metadata = junk_metadata(alias, Some(name.as_str()));
                    Expr::Column(column.clone()).alias_with_metadata(name, Some(metadata))
                })
                .collect();
            let inner = carried_side(alias, exprs, rest)?;
            let condition = correlated
                .into_iter()
                .map(|conjunct| {
                    conjunct
                        .transform(|expr| {
                            Ok(match expr {
                                Expr::Column(column) => {
                                    match inner_columns
                                        .iter()
                                        .position(|inner| inner == &column)
                                    {
                                        Some(position) => {
                                            Transformed::yes(Expr::Column(Column::new(
                                                Some(alias.clone()),
                                                carried_name(position),
                                            )))
                                        }
                                        None => Transformed::no(Expr::Column(column)),
                                    }
                                }
                                Expr::OuterReferenceColumn(_, column) => {
                                    Transformed::yes(Expr::Column(column))
                                }
                                other => Transformed::no(other),
                            })
                        })
                        .data()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some((inner, condition)))
        }
        _ => Ok(None),
    }
}

/// The inner side of a carried semi join: `exprs` over `input`, under `alias`.
fn carried_side(
    alias: &TableReference,
    exprs: Vec<Expr>,
    input: Arc<LogicalPlan>,
) -> Result<LogicalPlan> {
    let projection = LogicalPlan::Projection(Projection::try_new(exprs, input)?);
    SubqueryAlias::try_new(Arc::new(projection), alias.clone())
        .map(LogicalPlan::SubqueryAlias)
}

fn expr_has_outer_reference(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| Ok(matches!(expr, Expr::OuterReferenceColumn(_, _))))
}

fn expr_has_subquery(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| Ok(expr_subquery(expr).is_some()))
}

fn expr_subquery(expr: &Expr) -> Option<&Subquery> {
    match expr {
        Expr::Exists(Exists { subquery, .. })
        | Expr::InSubquery(InSubquery { subquery, .. })
        | Expr::ScalarSubquery(subquery)
        | Expr::AnyExpr(AnyExpr {
            source: QuantifiedSource::Subquery(subquery),
            ..
        })
        | Expr::AllExpr(AllExpr {
            source: QuantifiedSource::Subquery(subquery),
            ..
        }) => Some(subquery),
        _ => None,
    }
}

/// Whether any expression of `plan` or of a subquery inside it holds an
/// outer reference.
fn plan_has_outer_reference(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = expr_has_outer_reference(expr)?;
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
    })?;
    Ok(found)
}

/// Every column `expr` reads: its own columns and the outer references of
/// the subqueries inside it.
fn collect_read_columns(expr: &Expr, reads: &mut Vec<Column>) -> Result<()> {
    expr.apply(|expr| {
        match expr {
            Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => {
                if !reads.contains(column) {
                    reads.push(column.clone());
                }
            }
            other => {
                if let Some(subquery) = expr_subquery(other) {
                    subquery.subquery.apply_with_subqueries(|node| {
                        node.apply_expressions(|expr| {
                            expr.apply(|expr| {
                                if let Expr::OuterReferenceColumn(_, column) = expr
                                    && !reads.contains(column)
                                {
                                    reads.push(column.clone());
                                }
                                Ok(TreeNodeRecursion::Continue)
                            })
                        })
                    })?;
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// The qualifiers of `schema`'s fields, in order of first appearance.
fn schema_relations(schema: &DFSchemaRef) -> Vec<TableReference> {
    let mut relations: Vec<TableReference> = Vec::new();
    for (qualifier, _) in schema.iter() {
        if let Some(qualifier) = qualifier
            && !relations.contains(qualifier)
        {
            relations.push(qualifier.clone());
        }
    }
    relations
}

/// The columns of `relation` among `reads`, in `schema`'s order.
fn relation_columns_read(
    schema: &DFSchemaRef,
    relation: &TableReference,
    reads: &[Column],
) -> Vec<String> {
    schema
        .iter()
        .filter(|(qualifier, _)| *qualifier == Some(relation))
        .map(|(_, field)| field.name().clone())
        .filter(|name| {
            reads.iter().any(|read| {
                read.name == *name
                    && read
                        .relation
                        .as_ref()
                        .is_some_and(|read_relation| read_relation.resolved_eq(relation))
            })
        })
        .collect()
}

/// The relations an outer join of `plan` can null-extend.
fn nullable_relations(plan: &LogicalPlan) -> Result<Vec<TableReference>> {
    let mut nullable: Vec<TableReference> = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Join(Join {
            left,
            right,
            join_type,
            ..
        }) = node
        {
            let extended: Vec<&DFSchemaRef> = match join_type {
                JoinType::Left => vec![right.schema()],
                JoinType::Right => vec![left.schema()],
                JoinType::Full => vec![left.schema(), right.schema()],
                JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark
                | JoinType::LeftSemiCarry => Vec::new(),
            };
            for schema in extended {
                for relation in schema_relations(schema) {
                    if !nullable.contains(&relation) {
                        nullable.push(relation);
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(nullable)
}

/// The presence of a relation an outer join can null-extend: one of its
/// columns that its own rows never hold NULL in is non-NULL, or, when it has
/// none, not all of its columns are NULL. A null-extended row reads NULL in
/// every carried column either way.
fn presence_of_nullable(
    plan: &LogicalPlan,
    schema: &DFSchemaRef,
    relation: &TableReference,
) -> Result<Expr> {
    let own_schema = relation_own_schema(plan, relation)?;
    let required = own_schema.as_ref().and_then(|own| {
        own.iter()
            .find(|(qualifier, field)| {
                *qualifier == Some(relation) && !field.is_nullable()
            })
            .map(|(_, field)| field.name().clone())
    });
    if let Some(name) = required {
        return Ok(Expr::Column(Column::new(Some(relation.clone()), name)).is_not_null());
    }
    let all_null = schema
        .iter()
        .filter(|(qualifier, _)| *qualifier == Some(relation))
        .map(|(_, field)| {
            Expr::Column(Column::new(Some(relation.clone()), field.name())).is_null()
        })
        .reduce(Expr::and);
    Ok(match all_null {
        Some(all_null) => Expr::Not(Box::new(all_null)),
        None => lit(true),
    })
}

/// The schema of the FROM item `relation` names, before any join null-extends
/// it.
fn relation_own_schema(
    plan: &LogicalPlan,
    relation: &TableReference,
) -> Result<Option<DFSchemaRef>> {
    let mut found = None;
    plan.apply(|node| {
        let own = match node {
            LogicalPlan::SubqueryAlias(alias) if &alias.alias == relation => true,
            LogicalPlan::TableScan(scan) if scan.table_name.resolved_eq(relation) => true,
            _ => false,
        };
        if own {
            found = Some(Arc::clone(node.schema()));
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// Plan the carried junk columns of a locking query level (`SELECT ... FOR
/// UPDATE OF t`), as PostgreSQL's `preprocess_targetlist` adds its rowmark
/// junk columns, so the level's row-lock operator rechecks a refreshed row
/// with every relation it does not lock fixed at the row it joined.
///
/// The positive `IN` and `EXISTS` sublinks of the level's WHERE clause that
/// [`carry_dml_input`] pulls up are pulled up the same way, into
/// [`JoinType::LeftSemiCarry`] joins. Then the level's select list carries,
/// through its ORDER BY and LIMIT, the columns of each relation it does not
/// lock that a predicate linking it to a locked relation reads, with each
/// such relation's presence column, and a projection on top restores the
/// level's own output, so the query around the level never sees them. A
/// level whose relations are all locked, or that locks none, is returned
/// unchanged.
pub(crate) fn add_locking_junk_columns(plan: LogicalPlan) -> Result<LogicalPlan> {
    let Some(level) = locking_level_input(&plan) else {
        return Ok(plan);
    };
    let mut items: Vec<(TableReference, bool)> = Vec::new();
    let mut predicates: Vec<Expr> = Vec::new();
    collect_level(level, &mut items, &mut predicates)?;
    if !items.iter().any(|(_, locked)| *locked) {
        return Ok(plan);
    }
    let plan = match level {
        LogicalPlan::Filter(filter) => {
            let pulled = pull_up_semi_joins(
                filter.input.as_ref().clone(),
                filter.predicate.clone(),
            )?;
            if pulled.conditions.is_empty() {
                plan
            } else {
                let input = match pulled.filter {
                    Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(pulled.plan),
                    )?),
                    None => pulled.plan,
                };
                replace_level_input(&plan, input)?
            }
        }
        _ => plan,
    };
    let Some(level) = locking_level_input(&plan) else {
        return Ok(plan);
    };
    let mut items: Vec<(TableReference, bool)> = Vec::new();
    let mut predicates: Vec<Expr> = Vec::new();
    collect_level(level, &mut items, &mut predicates)?;
    let locked: Vec<&TableReference> = items
        .iter()
        .filter(|(_, locked)| *locked)
        .map(|(relation, _)| relation)
        .collect();
    let mut reads: Vec<Column> = Vec::new();
    for predicate in &predicates {
        let mut columns = Vec::new();
        collect_read_columns(predicate, &mut columns)?;
        let reads_locked = columns.iter().any(|column| {
            column.relation.as_ref().is_some_and(|relation| {
                locked.iter().any(|locked| relation.resolved_eq(locked))
            })
        });
        if reads_locked {
            for column in columns {
                if !reads.contains(&column) {
                    reads.push(column);
                }
            }
        }
    }
    let schema = Arc::clone(level.schema());
    let nullable = nullable_relations(level)?;
    let mut junk = Vec::new();
    let mut ordinal = 0usize;
    for (relation, is_locked) in &items {
        if *is_locked {
            continue;
        }
        let columns = relation_columns_read(&schema, relation, &reads);
        if columns.is_empty() {
            continue;
        }
        for column in &columns {
            junk.push(
                Expr::Column(Column::new(Some(relation.clone()), column))
                    .alias_with_metadata(
                        carried_junk_column_name(ordinal, column),
                        Some(junk_metadata(relation, Some(column.as_str()))),
                    ),
            );
        }
        let presence = if nullable.iter().any(|candidate| candidate == relation) {
            presence_of_nullable(level, &schema, relation)?
        } else {
            lit(true)
        };
        junk.push(presence.alias_with_metadata(
            carried_presence_column_name(ordinal),
            Some(junk_metadata(relation, None)),
        ));
        ordinal += 1;
    }
    if junk.is_empty() {
        return Ok(plan);
    }
    let Some(extended) = extend_level_output(&plan, &junk)? else {
        return Ok(plan);
    };
    // The level's own output, without the junk columns behind it.
    let output = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            Expr::Column(Column::new(qualifier.cloned(), field.name()))
        })
        .collect::<Vec<_>>();
    Projection::try_new(output, Arc::new(extended)).map(LogicalPlan::Projection)
}

/// `plan` with the input of its select-list projection replaced by `input`,
/// and the ORDER BY, LIMIT and projections above it rebuilt over it.
fn replace_level_input(plan: &LogicalPlan, input: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Limit(_) | LogicalPlan::Sort(_) => {
            let inputs = plan.inputs();
            let [below] = inputs.as_slice() else {
                return internal_err!("a LIMIT or ORDER BY has one input");
            };
            let below = replace_level_input(below, input)?;
            plan.with_new_exprs(plan.expressions(), vec![below])
        }
        LogicalPlan::Projection(projection) => {
            let below = match projection.input.as_ref() {
                LogicalPlan::Limit(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Projection(_) => {
                    replace_level_input(&projection.input, input)?
                }
                _ => input,
            };
            Projection::try_new(projection.expr.clone(), Arc::new(below))
                .map(LogicalPlan::Projection)
        }
        _ => internal_err!(
            "a locking query level has no select list to carry its junk columns"
        ),
    }
}

/// The input of a query level's select-list projection: below its LIMIT,
/// ORDER BY and select-list projections.
fn locking_level_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Limit(limit) => locking_level_input(&limit.input),
        LogicalPlan::Sort(sort) => locking_level_input(&sort.input),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Limit(_) | LogicalPlan::Sort(_) | LogicalPlan::Projection(_) => {
                locking_level_input(&projection.input)
            }
            other => Some(other),
        },
        _ => None,
    }
}

/// The FROM items of a query level, each with whether a lock reaches one of
/// its table scans, and the level's own predicates: its filters and join
/// conditions, not those inside a FROM item.
fn collect_level(
    plan: &LogicalPlan,
    items: &mut Vec<(TableReference, bool)>,
    predicates: &mut Vec<Expr>,
) -> Result<()> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            // A query level of its own locks its rows itself: to this level
            // it is a relation it does not lock.
            let locked = alias.is_table_alias()
                && alias.input.exists(|node| {
                    Ok(matches!(node, LogicalPlan::TableScan(scan) if scan.row_lock.is_some()))
                })?;
            items.push((alias.alias.clone(), locked));
        }
        LogicalPlan::TableScan(scan) => {
            items.push((scan.table_name.clone(), scan.row_lock.is_some()));
            predicates.extend(scan.filters.iter().cloned());
        }
        LogicalPlan::Filter(filter) => {
            predicates.extend(split_conjunction(&filter.predicate).into_iter().cloned());
            collect_level(&filter.input, items, predicates)?;
        }
        LogicalPlan::Join(join) => {
            for (left, right) in &join.on {
                predicates.push(left.clone().eq(right.clone()));
            }
            if let Some(filter) = &join.filter {
                predicates.extend(split_conjunction(filter).into_iter().cloned());
            }
            collect_level(&join.left, items, predicates)?;
            collect_level(&join.right, items, predicates)?;
        }
        other => {
            for input in other.inputs() {
                collect_level(input, items, predicates)?;
            }
        }
    }
    Ok(())
}

/// `plan` with `junk` appended to its select-list projection and carried
/// through the ORDER BY, LIMIT and projections above it; `None` when the
/// level's output is not a projection the junk columns can join.
fn extend_level_output(plan: &LogicalPlan, junk: &[Expr]) -> Result<Option<LogicalPlan>> {
    match plan {
        LogicalPlan::Limit(_) | LogicalPlan::Sort(_) => {
            let inputs = plan.inputs();
            let [input] = inputs.as_slice() else {
                return Ok(None);
            };
            let Some(input) = extend_level_output(input, junk)? else {
                return Ok(None);
            };
            plan.with_new_exprs(plan.expressions(), vec![input])
                .map(Some)
        }
        LogicalPlan::Projection(projection) => {
            let mut exprs = projection.expr.clone();
            let input = match projection.input.as_ref() {
                LogicalPlan::Limit(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Projection(_) => {
                    let Some(input) = extend_level_output(&projection.input, junk)?
                    else {
                        return Ok(None);
                    };
                    exprs.extend(junk.iter().map(|expr| {
                        let name = match expr {
                            Expr::Alias(alias) => alias.name.clone(),
                            other => other.schema_name().to_string(),
                        };
                        Expr::Column(Column::new_unqualified(name))
                    }));
                    Arc::new(input)
                }
                _ => {
                    exprs.extend(junk.iter().cloned());
                    Arc::clone(&projection.input)
                }
            };
            Projection::try_new(exprs, input)
                .map(LogicalPlan::Projection)
                .map(Some)
        }
        _ => Ok(None),
    }
}
