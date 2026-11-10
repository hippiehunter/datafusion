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

//! Whole-row values of relations an outer join null-extends.
//!
//! PostgreSQL's whole-row value of a relation is NULL on a row an outer join
//! null-extended, and a record otherwise, even a record whose every field is
//! NULL. The relation's columns alone cannot tell the two apart, so a
//! range variable that a query with an outer join may read as a whole row
//! carries a hidden column that is true on each of its own rows.
//! The join leaves that column NULL exactly on the rows it null-extends, and
//! the whole-row value reads it.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use arrow::datatypes::FieldRef;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{Column, DFSchema, Result, ScalarValue, TableReference};
use datafusion_expr::utils::SYSTEM_COLUMN_METADATA_KEY;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    Expr as SQLExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    JoinOperator, ObjectName, OrderBy, Select, TableFactor, TableWithJoins,
    visit_expressions,
};

use crate::planner::{PlannerContext, SqlToRel};

/// The name of the hidden column that is true on each row of `relation`.
fn row_presence_column(relation: &TableReference) -> String {
    format!("__row_present_{}", relation.table())
}

/// The hidden row-presence column of `relation` in `schema`, when the
/// relation carries one.
pub(crate) fn row_presence_field<'a>(
    schema: &'a DFSchema,
    relation: &TableReference,
) -> Option<(Option<&'a TableReference>, &'a FieldRef)> {
    let name = row_presence_column(relation);
    schema
        .iter()
        .find(|(qualifier, field)| *qualifier == Some(relation) && field.name() == &name)
}

/// The last name of `name`, the one a range variable takes.
fn range_variable(name: &ObjectName) -> Option<&Ident> {
    let ObjectName(parts) = name;
    parts.last().and_then(|part| part.as_ident())
}

/// Whether `table` joins anything with an outer join, at any nesting depth.
/// Derived tables are query levels of their own.
fn has_outer_join(table: &TableWithJoins) -> bool {
    let nested = |factor: &TableFactor| match factor {
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => has_outer_join(table_with_joins.as_ref()),
        _ => false,
    };
    nested(&table.relation)
        || table.joins.iter().any(|join| {
            matches!(
                join.join_operator,
                JoinOperator::Left(_)
                    | JoinOperator::LeftOuter(_)
                    | JoinOperator::Right(_)
                    | JoinOperator::RightOuter(_)
                    | JoinOperator::FullOuter(_)
            ) || nested(&join.relation)
        })
}

impl SqlToRel<'_> {
    /// The names an expression of `select`, or of a query nested in it, may
    /// read as the whole row of a range variable: a bare name, or `name.*`
    /// in value position. Empty when the FROM clause has no outer join, since
    /// nothing then null-extends a relation of this query level.
    ///
    /// Which relation is null-extended is not decided here: every range
    /// variable of one of these names, of whatever kind, carries the
    /// row-presence column, and the join structure decides by leaving the
    /// column nullable on the relations it can null-extend. A name that
    /// turns out to be a column, or a relation no join null-extends, costs
    /// one unread hidden column and changes no result.
    pub(crate) fn null_extended_whole_row_relations(
        &self,
        select: &Select,
        order_by: Option<&OrderBy>,
    ) -> Vec<String> {
        if !select.from.iter().any(has_outer_join) {
            return Vec::new();
        }
        let mut read_whole: HashSet<String> = HashSet::new();
        let mut note = |ident: Option<&Ident>| {
            if let Some(ident) = ident {
                read_whole.insert(self.ident_normalizer.normalize(ident.clone()));
            }
        };
        let mut visit = |expr: &SQLExpr| -> ControlFlow<()> {
            match expr {
                SQLExpr::Identifier(ident) => note(Some(ident)),
                SQLExpr::QualifiedWildcard(name, _) => note(range_variable(name)),
                SQLExpr::Function(function) => {
                    if let FunctionArguments::List(list) = &function.args {
                        for argument in &list.args {
                            if let FunctionArg::Unnamed(
                                FunctionArgExpr::QualifiedWildcard(name),
                            ) = argument
                            {
                                note(range_variable(name));
                            }
                        }
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        };
        let _ = visit_expressions(select, &mut visit);
        if let Some(order_by) = order_by {
            let _ = visit_expressions(order_by, &mut visit);
        }
        read_whole.into_iter().collect()
    }

    /// `plan`, one range variable of a FROM clause of any kind (a table, a
    /// view, a CTE reference, a derived table, a function relation, an
    /// aliased join), with its hidden row-presence column when the query may
    /// read it as a whole row beside an outer join. A definition planned to
    /// be printed keeps the relation as written.
    pub(crate) fn with_row_presence(
        &self,
        plan: LogicalPlan,
        planner_context: &PlannerContext,
    ) -> Result<LogicalPlan> {
        let Some(relation) = plan.schema().iter().find_map(|(qualifier, _)| qualifier)
        else {
            return Ok(plan);
        };
        let one_relation = plan
            .schema()
            .iter()
            .all(|(qualifier, _)| qualifier == Some(relation));
        if !one_relation
            || self.context_provider.plans_definitions_as_written()
            || !planner_context.reads_null_extended_whole_row(relation.table())
            || row_presence_field(plan.schema(), relation).is_some()
        {
            return Ok(plan);
        }
        let relation = relation.clone();
        let mut columns: Vec<Expr> = plan
            .schema()
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        let hidden = HashMap::from([(
            SYSTEM_COLUMN_METADATA_KEY.to_string(),
            "true".to_string(),
        )]);
        let name = row_presence_column(&relation);
        columns.push(
            Expr::Literal(ScalarValue::Boolean(Some(true)), None)
                .alias_qualified_with_metadata(
                    Some(relation),
                    name,
                    Some(FieldMetadata::from(hidden)),
                ),
        );
        LogicalPlanBuilder::from(plan).project(columns)?.build()
    }
}
