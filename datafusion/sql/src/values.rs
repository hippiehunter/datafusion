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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{Expr as SQLExpr, Ident, Values as SQLValues};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_values_to_plan(
        &self,
        values: SQLValues,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let SQLValues {
            explicit_row: _,
            rows,
            ..
        } = values;

        let empty_schema = Arc::new(DFSchema::empty());
        let defaults = planner_context.take_values_defaults();
        let values = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        if let (Some(defaults), SQLExpr::Identifier(ident)) =
                            (defaults.as_ref(), &v)
                        {
                            if is_default_identifier(ident) {
                                let default_expr = defaults
                                    .get(idx)
                                    .and_then(|expr| expr.clone())
                                    .unwrap_or(Expr::Literal(ScalarValue::Null, None));
                                return Ok(default_expr);
                            }
                        }
                        self.sql_to_expr(v, &empty_schema, planner_context)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = planner_context.table_schema().unwrap_or(empty_schema);
        if schema.fields().is_empty() {
            LogicalPlanBuilder::values(values)?.build()
        } else {
            LogicalPlanBuilder::values_with_schema(values, &schema)?.build()
        }
    }
}

fn is_default_identifier(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("default")
}
