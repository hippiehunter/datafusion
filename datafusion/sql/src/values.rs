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
    Column, DFSchema, DFSchemaRef, Result, ScalarValue, not_impl_err, plan_err,
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
                    (defaults.as_ref(), value)
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
            let exprs = match &assembly {
                Some(assembly) => {
                    self.assemble_values_row(exprs, assembly, &row_schema, planner_context)?
                }
                None => exprs,
            };
            let computed = exprs.iter().try_fold(false, |found, expr| {
                Ok::<bool, datafusion_common::DataFusionError>(
                    found
                        || expr.exists(|node| {
                            Ok(matches!(
                                node,
                                Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
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

        let schema = target_schema.unwrap_or(empty_schema);
        if relational {
            return self.values_rows_to_union(rows, &schema);
        }
        let values = rows.into_iter().map(|row| row.exprs).collect();
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
        if self.context_provider.get_function_meta(&name).is_some() {
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
                            args.push(self.sql_to_expr_ref(expr, &schema, planner_context)?);
                        }
                        _ => return Ok(None),
                    }
                }
                args
            }
            FunctionArguments::None => Vec::new(),
            _ => return Ok(None),
        };
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
                columns = row
                    .exprs
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| {
                        Ok((format!("column{}", idx + 1), expr.get_type(input.schema())?))
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
                .map(|(expr, (name, data_type))| {
                    let expr = if expr.get_type(input.schema())? == *data_type {
                        expr
                    } else {
                        expr.cast_to(data_type, input.schema())?
                    };
                    Ok(expr.alias(name))
                })
                .collect::<Result<Vec<_>>>()?;
            let plan = LogicalPlanBuilder::from(input).project(projected)?.build()?;
            union = Some(match union {
                None => plan,
                Some(union) => LogicalPlanBuilder::from(union).union(plan)?.build()?,
            });
        }
        union.ok_or_else(|| datafusion_common::plan_datafusion_err!("Values list cannot be empty"))
    }
}

/// `DEFAULT` in a value slot is the bare keyword, which the grammar carries as
/// an unquoted identifier. Quoting it makes it an ordinary column reference.
pub(crate) fn is_default_identifier(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("default")
}
