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

use std::sync::Arc;

use crate::planner::{
    ContextProvider, PlannerContext, SqlToRel, ValuesAssembly, ValuesDefault,
};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, Result, ScalarValue, UnnestOptions, not_impl_err,
    plan_err,
};
use datafusion_expr::{
    EmptyRelation, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
};
use sqlparser::ast::{
    Expr as SQLExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    Values as SQLValues,
};

/// One row of a values list: the relation it is evaluated over when a slot
/// holds a set-returning call, and the expression for each output column.
struct ValuesRow {
    input: Option<LogicalPlan>,
    exprs: Vec<Expr>,
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_values_to_plan_ref(
        &self,
        values: &SQLValues,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let empty_schema = Arc::new(DFSchema::empty());
        let defaults = planner_context.take_values_defaults();
        let assembly = planner_context.take_values_assembly();
        // The INSERT target schema describes this list and nothing below it: a
        // scalar subquery in a value slot may hold a VALUES of its own, whose
        // width has nothing to do with the target row's.
        let target_schema = planner_context.set_table_schema(None);

        // A row holding a set-returning call is every row the call produces,
        // and one holding a sub-select is computed rather than constant: such
        // rows are planned as relations of their own and unioned.
        let mut relational = false;
        let mut rows = Vec::with_capacity(values.rows.len());
        for row in &values.rows {
            let srf = self.values_row_set_returning_call(row, planner_context)?;
            let row_schema = match &srf {
                Some((_, scan)) => Arc::clone(scan.schema()),
                None => Arc::clone(&empty_schema),
            };
            let mut exprs = Vec::with_capacity(row.len());
            for (idx, value) in row.iter().enumerate() {
                if let Some((srf_slot, scan)) = &srf
                    && *srf_slot == idx
                {
                    exprs.push(Expr::Column(Column::from(
                        scan.schema().qualified_field(0),
                    )));
                    continue;
                }
                if let (Some(defaults), SQLExpr::Identifier(ident)) =
                    (defaults.as_ref().map(|defaults| &defaults.slots), value)
                    && is_default_identifier(ident)
                {
                    exprs.push(match defaults.get(idx) {
                        Some(ValuesDefault::Refused(message)) => {
                            return not_impl_err!("{message}");
                        }
                        Some(ValuesDefault::Column(Some(default))) => default.clone(),
                        _ => Expr::Literal(ScalarValue::Null, None),
                    });
                    continue;
                }
                exprs.push(self.sql_to_expr_ref(value, &row_schema, planner_context)?);
            }
            if let Some(defaults) = defaults
                .as_ref()
                .filter(|defaults| defaults.fill_omitted_trailing)
            {
                for default in defaults.slots.iter().skip(exprs.len()) {
                    exprs.push(match default {
                        ValuesDefault::Refused(message) => {
                            return not_impl_err!("{message}");
                        }
                        ValuesDefault::Column(Some(default)) => default.clone(),
                        ValuesDefault::Column(None) => {
                            Expr::Literal(ScalarValue::Null, None)
                        }
                    });
                }
            }
            let exprs = match &assembly {
                Some(assembly) => self.assemble_values_row(
                    exprs,
                    assembly,
                    &row_schema,
                    planner_context,
                )?,
                None => exprs,
            };
            // A row that reads the enclosing query — through a sub-select or
            // an outer reference — takes a value per outer row rather than
            // one for the statement.
            let computed = exprs.iter().try_fold(false, |found, expr| {
                Ok::<bool, datafusion_common::DataFusionError>(
                    found
                        || expr.exists(|node| {
                            Ok(matches!(
                                node,
                                Expr::ScalarSubquery(_)
                                    | Expr::InSubquery(_)
                                    | Expr::Exists(_)
                                    | Expr::OuterReferenceColumn(_, _)
                            ))
                        })?,
                )
            })?;
            relational |= srf.is_some() || computed;
            rows.push(ValuesRow {
                input: srf.map(|(_, scan)| scan),
                exprs,
            });
        }

        let schema = target_schema.unwrap_or_else(|| Arc::clone(&empty_schema));
        if relational {
            return self.values_rows_to_union(rows, &schema);
        }
        let values = rows
            .into_iter()
            .map(|row| {
                row.exprs
                    .into_iter()
                    .enumerate()
                    .map(|(index, expr)| {
                        let Some(target) = schema.fields().get(index) else {
                            return Ok(expr);
                        };
                        Ok(self
                            .context_provider
                            .plan_assignment_coercion(&expr, target, &empty_schema)?
                            .unwrap_or(expr))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        if schema.fields().is_empty() {
            LogicalPlanBuilder::values(values)?.build()
        } else {
            LogicalPlanBuilder::values_with_schema(values, &schema)?.build()
        }
    }

    /// The set-returning call in a values row, as the scan producing its
    /// rows, with the slot it occupies. A name the provider knows as a scalar
    /// function is never a set-returning call.
    fn values_row_set_returning_call(
        &self,
        row: &[SQLExpr],
        planner_context: &mut PlannerContext,
    ) -> Result<Option<(usize, LogicalPlan)>> {
        let mut found: Option<(usize, LogicalPlan)> = None;
        for (idx, value) in row.iter().enumerate() {
            let SQLExpr::Function(function) = value else {
                continue;
            };
            let Some(scan) = self.table_function_scan(function, planner_context)? else {
                continue;
            };
            if found.is_some() {
                return not_impl_err!(
                    "VALUES with more than one set-returning function in a row"
                );
            }
            found = Some((idx, scan));
        }
        Ok(found)
    }

    fn table_function_scan(
        &self,
        function: &Function,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        let name = function.name.to_string().to_ascii_lowercase();
        let is_set_returning = self.context_provider.is_set_returning_function(&name);
        if !is_set_returning && self.context_provider.get_function_meta(&name).is_some() {
            return Ok(None);
        }
        let schema = DFSchema::empty();
        let args = match &function.args {
            FunctionArguments::List(list) => {
                let mut args = Vec::with_capacity(list.args.len());
                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        } => {
                            args.push(self.sql_to_expr_ref(
                                expr,
                                &schema,
                                planner_context,
                            )?);
                        }
                        _ => return Ok(None),
                    }
                }
                args
            }
            FunctionArguments::None => Vec::new(),
            _ => return Ok(None),
        };
        if is_set_returning
            && let Some(expansion) = self
                .context_provider
                .plan_set_returning_function(&name, &args, &schema, None)?
        {
            let internal = (0..expansion.columns.len())
                .map(|index| format!("__values_srf_{index}"))
                .collect::<Vec<_>>();
            let lists = expansion
                .columns
                .iter()
                .zip(&internal)
                .map(|((_, expr), internal)| expr.clone().alias(internal))
                .collect::<Vec<_>>();
            let output = expansion
                .columns
                .iter()
                .zip(&internal)
                .map(|((name, _), internal)| {
                    Expr::Column(Column::from_name(internal)).alias(name)
                })
                .collect::<Vec<_>>();
            let plan = LogicalPlanBuilder::empty(true)
                .project(lists)?
                .unnest_columns_with_options(
                    internal.iter().map(Column::from_name).collect(),
                    UnnestOptions::new().with_preserve_nulls(false),
                )?
                .project(output)?
                .build()?;
            return Ok(Some(plan));
        }
        match self.context_provider.get_table_function_source(&name, args) {
            Ok(provider) => Ok(Some(
                LogicalPlanBuilder::scan(&name, provider, None)?.build()?,
            )),
            Err(_) => Ok(None),
        }
    }

    /// Turn the planned slots of one values row into the target table's row.
    fn assemble_values_row(
        &self,
        slots: Vec<Expr>,
        assembly: &ValuesAssembly,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
        assembly
            .fields
            .iter()
            .zip(&assembly.sources)
            .zip(&assembly.defaults)
            .map(|((field, sources), default)| {
                let slot = |index: usize| -> Result<Expr> {
                    slots.get(index).cloned().ok_or_else(|| {
                        datafusion_common::plan_datafusion_err!(
                            "Inconsistent data length across values list: got {} values in row but expected {}",
                            slots.len(),
                            index + 1
                        )
                    })
                };
                match sources.as_slice() {
                    [] => Ok(default
                        .clone()
                        .unwrap_or_else(|| Expr::Literal(ScalarValue::Null, None))),
                    [(index, path)] if path.is_empty() => slot(*index),
                    paths => {
                        // Each path applies to the result of the previous
                        // one, starting from a NULL of the column's type.
                        let mut base = Expr::Literal(ScalarValue::Null, None)
                            .cast_to(field.data_type(), &DFSchema::empty())?;
                        for (index, path) in paths {
                            base = self.plan_assignment_target(
                                base,
                                Arc::clone(field),
                                path,
                                slot(*index)?,
                                schema,
                                planner_context,
                            )?;
                        }
                        Ok(base)
                    }
                }
            })
            .collect()
    }

    /// Rows that are relations of their own — the output of a set-returning
    /// call, or computed through a sub-select — as a union of one projection
    /// per row, typed by the target schema or else by the first row.
    fn values_rows_to_union(
        &self,
        rows: Vec<ValuesRow>,
        schema: &DFSchemaRef,
    ) -> Result<LogicalPlan> {
        let target_fields = (!schema.fields().is_empty()).then(|| schema.fields().clone());
        let mut columns: Vec<(String, DataType)> = schema
            .fields()
            .iter()
            .map(|field| (field.name().clone(), field.data_type().clone()))
            .collect();
        let mut union: Option<LogicalPlan> = None;
        for row in rows {
            let input = match row.input {
                Some(scan) => scan,
                None => LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: true,
                    schema: Arc::new(DFSchema::empty()),
                }),
            };
            if columns.is_empty() {
                columns =
                    row.exprs
                        .iter()
                        .enumerate()
                        .map(|(idx, expr)| {
                            Ok((
                                format!("column{}", idx + 1),
                                expr.get_type(input.schema())?,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
            }
            if row.exprs.len() != columns.len() {
                return plan_err!(
                    "Inconsistent data length across values list: got {} values in row but expected {}",
                    row.exprs.len(),
                    columns.len()
                );
            }
            let projected = row
                .exprs
                .into_iter()
                .zip(&columns)
                .enumerate()
                .map(|(index, (expr, (name, data_type)))| {
                    let expr = match target_fields.as_ref().and_then(|fields| fields.get(index)) {
                        Some(target) => self
                            .context_provider
                            .plan_assignment_coercion(&expr, target, input.schema())?
                            .unwrap_or(expr),
                        None => expr,
                    };
                    let expr = if expr.get_type(input.schema())? == *data_type {
                        expr
                    } else {
                        expr.cast_to(data_type, input.schema())?
                    };
                    Ok(expr.alias(name))
                })
                .collect::<Result<Vec<_>>>()?;
            let plan = LogicalPlanBuilder::from(input)
                .project(projected)?
                .build()?;
            union = Some(match union {
                None => plan,
                Some(union) => LogicalPlanBuilder::from(union).union(plan)?.build()?,
            });
        }
        union.ok_or_else(|| {
            datafusion_common::plan_datafusion_err!("Values list cannot be empty")
        })
    }
}

/// `DEFAULT` in a value slot is the bare keyword, which the grammar carries as
/// an unquoted identifier. Quoting it makes it an ordinary column reference.
pub(crate) fn is_default_identifier(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("default")
}
