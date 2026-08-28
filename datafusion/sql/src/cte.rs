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

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use crate::planner::{ContextProvider, IdentNormalizer, PlannerContext, SqlToRel};

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{
    DFSchema, Result, not_impl_err, plan_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::{
    LogicalPlan, LogicalPlanBuilder, RecursiveSearch, RecursiveSearchOrder, TableSource,
};
use sqlparser::ast::{
    Cte, Ident, ObjectName, Query, SearchClause, SearchOrder, SelectItem, SetExpr,
    SetOperator, Visit, Visitor, With,
};

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn plan_with_clause_ref(
        &self,
        with: &With,
        planner_context: &mut PlannerContext,
    ) -> Result<()> {
        if !with.oracle_declarations.is_empty() {
            return not_impl_err!("Oracle PL/SQL declarations in WITH are not supported");
        }
        if with.cycle.is_some() {
            return not_impl_err!("CYCLE clauses of a recursive WITH are not supported");
        }
        let is_recursive = with.recursive;
        let cte_names: Vec<String> = with
            .cte_tables
            .iter()
            .map(|cte| self.ident_normalizer.normalize(cte.alias.name.clone()))
            .collect();
        // A `WITH` block can't use the same name more than once; a nested
        // WITH may shadow a name of an enclosing one.
        for (idx, cte_name) in cte_names.iter().enumerate() {
            if cte_names[..idx].contains(cte_name) {
                return plan_err!(
                    "WITH query name {cte_name:?} specified more than once"
                );
            }
        }
        // Every name of a recursive WITH list is in scope for every item, so
        // an item may reference one declared after it; items are planned in
        // dependency order. A plain WITH resolves names top to bottom, and a
        // forward reference there is a missing relation.
        let order = if is_recursive {
            with_list_dependency_order(
                &with.cte_tables,
                &cte_names,
                &self.ident_normalizer,
            )?
        } else {
            (0..with.cte_tables.len()).collect()
        };
        let search_target = with.cte_tables.len().checked_sub(1);
        for idx in order {
            let cte = &with.cte_tables[idx];
            let cte_name = cte_names[idx].clone();

            // Create a logical plan for the CTE
            // For recursive CTEs, we need to extract column aliases early and pass them
            // to recursive_cte() so the work table has the correct schema for self-references.
            let cte_plan = if is_recursive {
                // Extract column aliases from cte.alias.columns
                let column_aliases = cte
                    .alias
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>();
                self.recursive_cte_ref(
                    &cte_name,
                    cte.query.as_ref(),
                    &column_aliases,
                    (Some(idx) == search_target)
                        .then_some(with.search.as_ref())
                        .flatten(),
                    planner_context,
                )?
            } else {
                self.non_recursive_cte_ref(cte.query.as_ref(), planner_context)?
            };

            // Each `WITH` block can change the column names in the last
            // projection (e.g. "WITH table(t1, t2) AS SELECT 1, 2").
            // For recursive CTEs, column aliases have already been applied within recursive_cte(),
            // but apply_table_alias will still apply the table name alias.
            let final_plan = self.apply_table_alias(cte_plan, cte.alias.clone())?;
            // Export the CTE to the outer query
            planner_context.insert_cte(cte_name, final_plan);
        }
        Ok(())
    }

    fn non_recursive_cte_ref(
        &self,
        cte_query: &Query,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        self.query_to_plan_ref(cte_query, planner_context)
    }

    fn recursive_cte_ref(
        &self,
        cte_name: &str,
        cte_query: &Query,
        column_aliases: &[Ident],
        search: Option<&SearchClause>,
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

        // The CTE's own WITH list is its scope: visible to both of its terms
        // and to nothing outside the CTE.
        let mut cte_planner_context = planner_context.clone();
        let planner_context = &mut cte_planner_context;
        if let Some(with) = cte_query.with.as_deref() {
            self.plan_with_clause_ref(with, planner_context)?;
        }

        let (left_expr, right_expr, set_quantifier) = match cte_query.body.as_ref() {
            SetExpr::SetOperation {
                op: SetOperator::Union,
                left,
                right,
                set_quantifier,
            } => (left.as_ref(), right.as_ref(), set_quantifier),
            _ => {
                // If the query is not a UNION, then it is not a recursive CTE
            let plan = self.non_recursive_cte_ref(cte_query, planner_context)?;
            return if search.is_some() {
                plan_err!("SEARCH clause requires a recursive query")
            } else {
                Ok(plan)
            };
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
        let static_plan = if column_aliases.is_empty() {
            self.set_expr_to_plan_ref(left_expr, planner_context)?
        } else {
            let aliased = self
                .inject_column_aliases_into_set_expr(left_expr.clone(), column_aliases)?;
            self.set_expr_to_plan(aliased, planner_context)?
        };

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
        let recursive_plan = self.set_expr_to_plan_ref(right_expr, planner_context)?;

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
                set_quantifier.clone(),
            );
        }

        // ---------- Step 4: Create the final plan ------------------
        let distinct = !Self::is_union_all(set_quantifier.clone())?;
        let plan = LogicalPlanBuilder::from(static_plan)
            .to_recursive_query(name, recursive_plan, distinct)?
            .build()?;
        apply_recursive_search(plan, search, &self.ident_normalizer)
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
                        *item = match std::mem::replace(
                            item,
                            SelectItem::Wildcard(Default::default()),
                        ) {
                            SelectItem::UnnamedExpr(expr) => SelectItem::ExprWithAlias {
                                expr,
                                alias: alias.clone(),
                            },
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

fn apply_recursive_search(
    plan: LogicalPlan,
    search: Option<&SearchClause>,
    normalizer: &IdentNormalizer,
) -> Result<LogicalPlan> {
    let Some(search) = search else {
        return Ok(plan);
    };
    let LogicalPlan::RecursiveQuery(mut recursive) = plan else {
        return plan_err!("SEARCH clause requires a recursive query");
    };
    let source_schema = recursive.static_term.schema();
    let set_column = normalizer.normalize(search.set_column.clone());
    if source_schema
        .index_of_column_by_name(None, &set_column)
        .is_some()
    {
        return plan_err!("SEARCH sequence column {set_column:?} already exists");
    }
    let mut by_column_indices = Vec::with_capacity(search.by_columns.len());
    for ident in &search.by_columns {
        let name = normalizer.normalize(ident.clone());
        let Some(index) = source_schema.index_of_column_by_name(None, &name) else {
            return plan_err!("SEARCH BY column {name:?} does not exist");
        };
        by_column_indices.push(index);
    }
    if by_column_indices.is_empty() {
        return plan_err!("SEARCH BY requires at least one column");
    }

    let mut fields = source_schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect::<Vec<_>>();
    fields.push((
        None,
        Arc::new(Field::new(
            set_column.clone(),
            arrow::datatypes::DataType::Binary,
            false,
        )),
    ));
    recursive.schema = Arc::new(DFSchema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    )?);
    recursive.search = Some(RecursiveSearch {
        order: match search.order {
            SearchOrder::DepthFirst => RecursiveSearchOrder::DepthFirst,
            SearchOrder::BreadthFirst => RecursiveSearchOrder::BreadthFirst,
        },
        by_column_indices,
        set_column,
    });
    Ok(LogicalPlan::RecursiveQuery(recursive))
}

/// The order in which to plan the items of a recursive WITH list: each item
/// after every other item it references. An item's reference to itself is its
/// recursion and imposes no order; a reference cycle between distinct items is
/// mutual recursion, which PostgreSQL does not implement either.
fn with_list_dependency_order(
    ctes: &[Cte],
    cte_names: &[String],
    normalizer: &IdentNormalizer,
) -> Result<Vec<usize>> {
    let dependencies: Vec<HashSet<usize>> = ctes
        .iter()
        .enumerate()
        .map(|(idx, cte)| {
            let mut referenced = WithListReferences {
                names: cte_names,
                normalizer,
                shadowed: Vec::new(),
                referenced: HashSet::new(),
            };
            let _ = cte.query.visit(&mut referenced);
            referenced.referenced.remove(&idx);
            referenced.referenced
        })
        .collect();
    let mut order = Vec::with_capacity(ctes.len());
    let mut planned = vec![false; ctes.len()];
    while order.len() < ctes.len() {
        let next = (0..ctes.len()).find(|&idx| {
            !planned[idx] && dependencies[idx].iter().all(|&dep| planned[dep])
        });
        let Some(idx) = next else {
            return not_impl_err!(
                "mutual recursion between WITH items is not implemented"
            );
        };
        planned[idx] = true;
        order.push(idx);
    }
    Ok(order)
}

/// Collects which items of a WITH list a query references by their bare
/// name, ignoring names a WITH nested inside the query shadows.
struct WithListReferences<'a> {
    names: &'a [String],
    normalizer: &'a IdentNormalizer,
    shadowed: Vec<Vec<String>>,
    referenced: HashSet<usize>,
}

impl Visitor for WithListReferences<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        let inner_names = query
            .with
            .as_deref()
            .map(|with| {
                with.cte_tables
                    .iter()
                    .map(|cte| self.normalizer.normalize(cte.alias.name.clone()))
                    .collect()
            })
            .unwrap_or_default();
        self.shadowed.push(inner_names);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.shadowed.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        let [part] = relation.0.as_slice() else {
            return ControlFlow::Continue(());
        };
        let Some(ident) = part.as_ident() else {
            return ControlFlow::Continue(());
        };
        let name = self.normalizer.normalize(ident.clone());
        if self.shadowed.iter().any(|scope| scope.contains(&name)) {
            return ControlFlow::Continue(());
        }
        if let Some(idx) = self.names.iter().position(|cte_name| *cte_name == name) {
            self.referenced.insert(idx);
        }
        ControlFlow::Continue(())
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
