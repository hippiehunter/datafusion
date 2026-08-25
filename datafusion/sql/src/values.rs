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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel, ValuesDefault};
use datafusion_common::{DFSchema, Result, ScalarValue, not_impl_err};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{Expr as SQLExpr, Ident, Values as SQLValues};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_values_to_plan_ref(
        &self,
        values: &SQLValues,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let empty_schema = Arc::new(DFSchema::empty());
        let defaults = planner_context.take_values_defaults();
        // The INSERT target schema describes this list and nothing below it: a
        // scalar subquery in a value slot may hold a VALUES of its own, whose
        // width has nothing to do with the target row's.
        let target_schema = planner_context.set_table_schema(None);
        let values = values
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(idx, v)| {
                        if let (Some(defaults), SQLExpr::Identifier(ident)) =
                            (defaults.as_ref(), v)
                            && is_default_identifier(ident)
                        {
                            return match defaults.get(idx) {
                                Some(ValuesDefault::Refused(message)) => not_impl_err!("{message}"),
                                Some(ValuesDefault::Column(Some(default))) => Ok(default.clone()),
                                _ => Ok(Expr::Literal(ScalarValue::Null, None)),
                            };
                        }
                        self.sql_to_expr_ref(v, &empty_schema, planner_context)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = target_schema.unwrap_or(empty_schema);
        if schema.fields().is_empty() {
            LogicalPlanBuilder::values(values)?.build()
        } else {
            LogicalPlanBuilder::values_with_schema(values, &schema)?.build()
        }
    }
}

/// `DEFAULT` in a value slot is the bare keyword, which the grammar carries as
/// an unquoted identifier. Quoting it makes it an ordinary column reference.
pub(crate) fn is_default_identifier(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("default")
}
