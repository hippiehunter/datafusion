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

//! Synchronous lowering of a view definition into the shallow semantic facts
//! needed for automatic DML retargeting.
//!
//! This module is intentionally in `datafusion-sql`: it consumes parser nodes
//! while the defining query and the real catalog/function provider are in
//! scope. Neither the resulting logical plan nor downstream catalog state
//! retains or reparses those nodes.

use crate::ast_walk::Walk as AstWalk;
use crate::planner::{PlannerContext, SqlToRel};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result, TableReference};
use datafusion_expr::{
    BoundSqlExpression, CreateViewColumn, CreateViewNotUpdatable, CreateViewUpdatability,
    Expr,
};
use sqlparser::ast::{
    Expr as SQLExpr, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SetExpr, TableFactor, Visitor,
};
use std::ops::ControlFlow;

/// Consume the parser-owned part of one view level into catalog-safe meaning.
///
/// `output_schema` is the schema of the already-planned defining query after
/// any explicit CREATE VIEW column aliases have been applied.
pub fn analyze_updatable_view(
    planner: &SqlToRel<'_>,
    query: &Query,
    declared_columns: &[Ident],
    output_schema: &DFSchemaRef,
) -> Result<CreateViewUpdatability> {
    let select = match analyze_query_shape(query) {
        Ok(select) => select,
        Err(reason) => return Ok(CreateViewUpdatability::NotUpdatable(reason)),
    };

    if select.from.len() != 1 {
        return Ok(CreateViewUpdatability::NotUpdatable(
            CreateViewNotUpdatable::Join,
        ));
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Ok(CreateViewUpdatability::NotUpdatable(
            CreateViewNotUpdatable::Join,
        ));
    }
    let (source_name, source_alias) = match &from.relation {
        TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } => (name, alias.as_ref().map(|alias| &alias.name)),
        _ => {
            return Ok(CreateViewUpdatability::NotUpdatable(
                CreateViewNotUpdatable::NotARelation,
            ));
        }
    };

    let source = planner.object_name_to_table_reference(source_name.clone())?;
    let table_source = planner.context_provider.get_table_source(source.clone())?;
    let qualifier = source_alias
        .map(|alias| TableReference::bare(alias.value.clone()))
        .unwrap_or_else(|| source.clone());
    let source_schema = DFSchema::try_from_qualified_schema(
        qualifier,
        table_source.schema().as_ref(),
    )?;

    let output_names = if declared_columns.is_empty() {
        output_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    } else {
        declared_columns
            .iter()
            .map(|column| column.value.clone())
            .collect::<Vec<_>>()
    };

    let columns = lower_projection_columns(planner, select, &source_schema, &output_names)?;
    if columns.len() != output_names.len() {
        return Ok(CreateViewUpdatability::NotUpdatable(
            CreateViewNotUpdatable::UnreadableDefinition,
        ));
    }
    if !columns.iter().any(|column| column.write_source.is_some()) {
        return Ok(CreateViewUpdatability::NotUpdatable(
            CreateViewNotUpdatable::NoUpdatableColumn,
        ));
    }

    let restriction = select
        .selection
        .as_deref()
        .map(|selection| {
            planner
                .sql_to_expr_ref(selection, &source_schema, &mut PlannerContext::new())
                .and_then(unqualify_expression)
                .map(BoundSqlExpression::new)
        })
        .transpose()?;

    Ok(CreateViewUpdatability::Updatable {
        source,
        columns,
        restriction,
    })
}

fn lower_projection_columns(
    planner: &SqlToRel<'_>,
    select: &Select,
    source_schema: &DFSchema,
    output_names: &[String],
) -> Result<Vec<CreateViewColumn>> {
    let expands_whole_relation = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    });
    if expands_whole_relation {
        if select.projection.len() != 1 || source_schema.fields().len() != output_names.len() {
            return Ok(Vec::new());
        }
        return Ok(output_names
            .iter()
            .zip(source_schema.fields())
            .map(|(output_name, field)| CreateViewColumn {
                name: output_name.clone(),
                write_source: Some(field.name().clone()),
                read_expression: BoundSqlExpression::new(Expr::Column(Column::new(
                    None::<TableReference>,
                    field.name(),
                ))),
            })
            .collect());
    }

    select
        .projection
        .iter()
        .zip(output_names)
        .map(|(item, output_name)| {
            let expression = match item {
                SelectItem::UnnamedExpr(expression)
                | SelectItem::ExprWithAlias { expr: expression, .. } => expression,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    return Err(DataFusionError::Internal(
                        "mixed wildcard view projection escaped shape validation".to_string(),
                    ));
                }
            };
            let write_source = referenced_column(expression);
            let read_expression = planner.sql_to_expr_ref(
                expression,
                source_schema,
                &mut PlannerContext::new(),
            )?;
            Ok(CreateViewColumn {
                name: output_name.clone(),
                write_source,
                read_expression: BoundSqlExpression::new(unqualify_expression(read_expression)?),
            })
        })
        .collect()
}

fn unqualify_expression(expression: Expr) -> Result<Expr> {
    expression
        .transform_down(|expression| match expression {
            Expr::Column(mut column) => {
                column.relation = None;
                Ok(Transformed::yes(Expr::Column(column)))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
}

fn analyze_query_shape(query: &Query) -> std::result::Result<&Select, CreateViewNotUpdatable> {
    if query.with.is_some() {
        return Err(CreateViewNotUpdatable::WithClause);
    }
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return Err(CreateViewNotUpdatable::LimitOffset);
    }
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => return Err(CreateViewNotUpdatable::SetOperation),
    };
    if select.distinct.is_some() {
        return Err(CreateViewNotUpdatable::Distinct);
    }
    if select.having.is_some() || !group_by_is_empty(&select.group_by) {
        return Err(CreateViewNotUpdatable::Grouping);
    }
    if select.qualify.is_some() {
        return Err(CreateViewNotUpdatable::WindowFunction);
    }
    for item in &select.projection {
        let expression = match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias { expr: expression, .. } => expression,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => continue,
        };
        if let Some(reason) = disqualifying_call(expression) {
            return Err(reason);
        }
    }
    Ok(select)
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            expressions.is_empty() && modifiers.is_empty()
        }
        GroupByExpr::All(_) | GroupByExpr::Quantified { .. } | GroupByExpr::OracleVector(_) => {
            false
        }
    }
}

fn disqualifying_call(expression: &SQLExpr) -> Option<CreateViewNotUpdatable> {
    struct CallVisitor {
        found: Option<CreateViewNotUpdatable>,
    }
    impl Visitor for CallVisitor {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &SQLExpr) -> ControlFlow<Self::Break> {
            let SQLExpr::Function(function) = expression else {
                return ControlFlow::Continue(());
            };
            if function.over.is_some() {
                self.found = Some(CreateViewNotUpdatable::WindowFunction);
                return ControlFlow::Break(());
            }
            if is_aggregate_name(&function.name) {
                self.found = Some(CreateViewNotUpdatable::Grouping);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = CallVisitor { found: None };
    let _ = AstWalk::walk(expression, &mut visitor);
    visitor.found
}

fn is_aggregate_name(name: &ObjectName) -> bool {
    let Some(last) = name.0.last().and_then(ObjectNamePart::as_ident) else {
        return false;
    };
    matches!(
        last.value.to_ascii_lowercase().as_str(),
        "any_value"
            | "array_agg"
            | "avg"
            | "bit_and"
            | "bit_or"
            | "bit_xor"
            | "bool_and"
            | "bool_or"
            | "corr"
            | "count"
            | "covar_pop"
            | "covar_samp"
            | "every"
            | "grouping"
            | "json_agg"
            | "json_object_agg"
            | "jsonb_agg"
            | "jsonb_object_agg"
            | "max"
            | "min"
            | "mode"
            | "percentile_cont"
            | "percentile_disc"
            | "regr_avgx"
            | "regr_avgy"
            | "regr_count"
            | "regr_intercept"
            | "regr_r2"
            | "regr_slope"
            | "regr_sxx"
            | "regr_sxy"
            | "regr_syy"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "string_agg"
            | "sum"
            | "var_pop"
            | "var_samp"
            | "variance"
            | "xmlagg"
    )
}

fn referenced_column(expression: &SQLExpr) -> Option<String> {
    match expression {
        SQLExpr::Identifier(identifier) => Some(identifier.value.clone()),
        SQLExpr::CompoundIdentifier(parts) => parts.last().map(|part| part.value.clone()),
        SQLExpr::Nested(inner) => referenced_column(inner),
        _ => None,
    }
}
