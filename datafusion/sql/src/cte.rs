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

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{
    Result, not_impl_err, plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, TableSource};
use sqlparser::ast::{Ident, Query, SelectItem, SetExpr, SetOperator, With};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn plan_with_clause(
        &self,
        with: With,
        planner_context: &mut PlannerContext,
    ) -> Result<()> {
        let is_recursive = with.recursive;
        // Process CTEs from top to bottom
        for cte in with.cte_tables {
            // A `WITH` block can't use the same name more than once
            let cte_name = self.ident_normalizer.normalize(cte.alias.name.clone());
            if planner_context.contains_cte(&cte_name) {
                return plan_err!(
                    "WITH query name {cte_name:?} specified more than once"
                );
            }

            // Create a logical plan for the CTE
            // For recursive CTEs, we need to extract column aliases early and pass them
            // to recursive_cte() so the work table has the correct schema for self-references.
            let cte_plan = if is_recursive {
                // Extract column aliases from cte.alias.columns
                let column_aliases: Vec<Ident> =
                    cte.alias.columns.iter().map(|c| c.name.clone()).collect();
                self.recursive_cte(&cte_name, *cte.query, column_aliases, planner_context)?
            } else {
                self.non_recursive_cte(*cte.query, planner_context)?
            };

            // Each `WITH` block can change the column names in the last
            // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
            // For recursive CTEs, column aliases have already been applied within recursive_cte(),
            // but apply_table_alias will still apply the table name alias.
            let final_plan = self.apply_table_alias(cte_plan, cte.alias)?;
            // Export the CTE to the outer query
            planner_context.insert_cte(cte_name, final_plan);
        }
        Ok(())
    }

    fn non_recursive_cte(
        &self,
        cte_query: Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.query_to_plan(cte_query, planner_context)
    }

    fn recursive_cte(
        &self,
        cte_name: &str,
        mut cte_query: Query,
        column_aliases: Vec<Ident>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        if !self
            .context_provider
            .options()
            .execution
            .enable_recursive_ctes
        {
            return not_impl_err!("Recursive CTEs are not enabled");
        }

        let (left_expr, right_expr, set_quantifier) = match *cte_query.body {
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                set_quantifier,
            } => (left, right, set_quantifier),
            other => {
                // If the query is not a UNION, then it is not a recursive CTE
                *cte_query.body = other;
                return self.non_recursive_cte(cte_query, planner_context);
            }
        };

        // Each recursive CTE consists of two parts in the logical plan:
        //   1. A static term   (the left-hand side on the SQL, where the
        //                       referencing to the same CTE is not allowed)
        //
        //   2. A recursive term (the right hand side, and the recursive
        //                       part)

        // Since static term does not have any specific properties, it can
        // be compiled as if it was a regular expression. This will
        // allow us to infer the schema to be used in the recursive term.

        // ---------- Step 1: Compile the static term ------------------
        // If column aliases are provided, inject them into the AST before compiling.
        // This ensures columns get unique names even if the SELECT has duplicate literals
        // (e.g., SELECT 1, 0, 1 with aliases (n, a, b) becomes SELECT 1 AS n, 0 AS a, 1 AS b).
        let left_expr = if !column_aliases.is_empty() {
            self.inject_column_aliases_into_set_expr(*left_expr, &column_aliases)?
        } else {
            *left_expr
        };
        let static_plan = self.set_expr_to_plan(left_expr, planner_context)?;

        // Since the recursive CTEs include a component that references a
        // table with its name, like the example below:
        //
        // WITH RECURSIVE values(n) AS (
        //      SELECT 1 as n -- static term
        //    UNION ALL
        //      SELECT n + 1
        //      FROM values -- self reference
        //      WHERE n < 100
        // )
        //
        // We need a temporary 'relation' to be referenced and used. PostgreSQL
        // calls this a 'working table', but it is entirely an implementation
        // detail and a 'real' table with that name might not even exist (as
        // in the case of DataFusion).
        //
        // Since we can't simply register a table during planning stage (it is
        // an execution problem), we'll use a relation object that preserves the
        // schema of the input perfectly and also knows which recursive CTE it is
        // bound to.

        // ---------- Step 2: Create a temporary relation ------------------
        // Step 2.1: Create the schema for the work table
        // If column aliases are provided, we need to apply them to the schema
        // so that the recursive term can reference columns by their alias names.
        let work_table_schema: SchemaRef = if !column_aliases.is_empty() {
            // Create a new schema with aliased column names
            self.apply_column_aliases_to_schema(
                Arc::clone(static_plan.schema().inner()),
                &column_aliases,
            )?
        } else {
            Arc::clone(static_plan.schema().inner())
        };

        // Step 2.2: Create a table source for the temporary relation
        let work_table_source = self
            .context_provider
            .create_cte_work_table(cte_name, work_table_schema)?;

        // Step 2.3: Create a temporary relation logical plan that will be used
        // as the input to the recursive term
        let work_table_plan = LogicalPlanBuilder::scan(
            cte_name.to_string(),
            Arc::clone(&work_table_source),
            None,
        )?
        .build()?;

        let name = cte_name.to_string();

        // Step 2.4: Register the temporary relation in the planning context
        // For all the self references in the variadic term, we'll replace it
        // with the temporary relation we created above by temporarily registering
        // it as a CTE. This temporary relation in the planning context will be
        // replaced by the actual CTE plan once we're done with the planning.
        planner_context.insert_cte(cte_name.to_string(), work_table_plan);

        // ---------- Step 3: Compile the recursive term ------------------
        // this uses the named_relation we inserted above to resolve the
        // relation. This ensures that the recursive term uses the named relation logical plan
        // and thus the 'continuance' physical plan as its input and source
        let recursive_plan = self.set_expr_to_plan(*right_expr, planner_context)?;

        // Check if the recursive term references the CTE itself,
        // if not, it is a non-recursive CTE
        if !has_work_table_reference(&recursive_plan, &work_table_source) {
            // Remove the work table plan from the context
            planner_context.remove_cte(cte_name);
            // Compile it as a non-recursive CTE
            return self.set_operation_to_plan(
                SetOperator::Union,
                static_plan,
                recursive_plan,
                set_quantifier,
            );
        }

        // ---------- Step 4: Create the final plan ------------------
        let distinct = !Self::is_union_all(set_quantifier)?;
        LogicalPlanBuilder::from(static_plan)
            .to_recursive_query(name, recursive_plan, distinct)?
            .build()
    }

    /// Apply column aliases to a schema, returning a new schema with the aliased names
    fn apply_column_aliases_to_schema(
        &self,
        schema: SchemaRef,
        column_aliases: &[Ident],
    ) -> Result<SchemaRef> {
        let fields = schema.fields();
        if column_aliases.len() > fields.len() {
            return plan_err!(
                "Source table contains {} columns but {} names given as column alias",
                fields.len(),
                column_aliases.len()
            );
        }

        let new_fields: Vec<Field> = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if i < column_aliases.len() {
                    let new_name =
                        self.ident_normalizer.normalize(column_aliases[i].clone());
                    Field::new(new_name, field.data_type().clone(), field.is_nullable())
                        .with_metadata(field.metadata().clone())
                } else {
                    field.as_ref().clone()
                }
            })
            .collect();

        Ok(Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata().clone(),
        )))
    }

    /// Inject column aliases into a SetExpr's projection items.
    /// This modifies the AST so that SELECT items get unique names based on the provided aliases.
    /// For example, `SELECT 1, 0, 1` with aliases `(n, a, b)` becomes `SELECT 1 AS n, 0 AS a, 1 AS b`.
    fn inject_column_aliases_into_set_expr(
        &self,
        mut set_expr: SetExpr,
        column_aliases: &[Ident],
    ) -> Result<SetExpr> {
        match &mut set_expr {
            SetExpr::Select(select) => {
                let projection = &mut select.projection;
                // Apply aliases to projection items by position
                for (i, alias) in column_aliases.iter().enumerate() {
                    if i < projection.len() {
                        let item = &mut projection[i];
                        // Convert the item to have an alias
                        *item = match std::mem::replace(item, SelectItem::Wildcard(Default::default())) {
                            SelectItem::UnnamedExpr(expr) => {
                                SelectItem::ExprWithAlias {
                                    expr,
                                    alias: alias.clone(),
                                }
                            }
                            SelectItem::ExprWithAlias { expr, alias: _ } => {
                                // Replace existing alias with the CTE column alias
                                SelectItem::ExprWithAlias {
                                    expr,
                                    alias: alias.clone(),
                                }
                            }
                            other => other, // Keep wildcards and qualified wildcards as-is
                        };
                    }
                }
                Ok(set_expr)
            }
            SetExpr::Values(_) => {
                // For VALUES, we can't easily inject aliases at the AST level.
                // The caller will need to handle aliasing after compilation.
                Ok(set_expr)
            }
            _ => {
                // For other SetExpr types (nested set operations), return as-is
                Ok(set_expr)
            }
        }
    }
}

fn has_work_table_reference(
    plan: &LogicalPlan,
    work_table_source: &Arc<dyn TableSource>,
) -> bool {
    let mut has_reference = false;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && Arc::ptr_eq(&scan.source, work_table_source)
        {
            has_reference = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    // Closure always return Ok
    .unwrap();
    has_reference
}
