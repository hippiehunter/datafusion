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

use crate::ast_walk::Walk;
use crate::planner::{IdentNormalizer, PlannerContext, SqlToRel};

use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion_common::{
    Column, DFSchema, DataFusionError, Result, internal_err, not_impl_err, plan_err,
    sqlstate_datafusion_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_expr::expr::{Alias, Case, Cast};
use datafusion_expr::type_coercion::other::get_coerce_type_for_case_expression;
use datafusion_expr::utils::SYSTEM_COLUMN_METADATA_KEY;
use datafusion_expr::{
    Distinct, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection,
    TableSource, recursive_term_type_settles,
};
use sqlparser::ast::{
    AccessExpr, Array, AstBox as SQLBox, AttachedToken, BinaryOperator, CastKind, Cte,
    CycleClause, DataType as SQLDataType, Expr as SQLExpr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, Query,
    SearchClause, SearchOrder, Select, SelectItem, SetExpr, SetOperator, TableFactor,
    TableWithJoins, Value, Visitor, With,
};

impl SqlToRel<'_> {
    pub(super) fn plan_with_clause_ref(
        &self,
        with: &With,
        planner_context: &mut PlannerContext,
    ) -> Result<()> {
        if !with.oracle_declarations.is_empty() {
            return not_impl_err!("Oracle PL/SQL declarations in WITH are not supported");
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
        for idx in order {
            let cte = &with.cte_tables[idx];
            let cte_name = cte_names[idx].clone();

            let cte_plan = if is_recursive {
                self.recursive_cte_ref(&cte_name, cte, planner_context)?
            } else {
                let plan =
                    self.non_recursive_cte_ref(cte.query.as_ref(), planner_context)?;
                if cte.search.is_some() || cte.cycle.is_some() {
                    return Err(with_query_not_recursive());
                }
                plan
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
        cte: &Cte,
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
        let cte_query = cte.query.as_ref();
        let column_aliases = cte
            .alias
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();

        // The CTE's own WITH list is its scope: visible to both of its terms
        // and to nothing outside the CTE.
        let mut cte_planner_context = planner_context.clone();
        let planner_context = &mut cte_planner_context;
        if let Some(with) = cte_query.with.as_deref() {
            self.plan_with_clause_ref(with, planner_context)?;
        }

        // PostgreSQL types the cycle mark before it analyzes the item's query.
        let cycle_mark = cte
            .cycle
            .as_ref()
            .map(|cycle| self.cycle_mark(cycle, planner_context))
            .transpose()?;
        let search_or_cycle = cte.search.is_some() || cte.cycle.is_some();

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
                return if search_or_cycle {
                    Err(with_query_not_recursive())
                } else {
                    Ok(plan)
                };
            }
        };
        if search_or_cycle && is_set_operation(left_expr) {
            return Err(sqlstate_datafusion_err(
                "0A000",
                "with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT",
            ));
        }
        if search_or_cycle && is_set_operation(right_expr) {
            return Err(sqlstate_datafusion_err(
                "42601",
                "with a SEARCH or CYCLE clause, the right side of the UNION must be a SELECT",
            ));
        }

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
                .inject_column_aliases_into_set_expr(left_expr.clone(), &column_aliases)?;
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
        let static_columns: SchemaRef = if !column_aliases.is_empty() {
            // Create a new schema with aliased column names
            self.apply_column_aliases_to_schema(
                Arc::clone(static_plan.schema().inner()),
                &column_aliases,
            )?
        } else {
            Arc::clone(static_plan.schema().inner())
        };
        // A SEARCH or CYCLE clause adds its columns to the static term, and so
        // to the work table the recursive term reads.
        let search_cycle = if search_or_cycle {
            let column_names = static_columns
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            Some(SearchCycle::new(
                column_names,
                cte.search.as_ref(),
                cte.cycle.as_ref().zip(cycle_mark),
                &self.ident_normalizer,
            )?)
        } else {
            None
        };
        let (static_plan, work_table_schema) = match &search_cycle {
            Some(search_cycle) => {
                let static_plan =
                    self.extend_static_term(static_plan, search_cycle, planner_context)?;
                let work_table_schema =
                    search_cycle.work_table_schema(static_plan.schema());
                (static_plan, work_table_schema)
            }
            None => (static_plan, static_columns),
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
            if search_or_cycle {
                return Err(with_query_not_recursive());
            }
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
        let recursive_plan = match &search_cycle {
            Some(search_cycle) => {
                let reference = union_operand_select(right_expr)
                    .and_then(|select| self.work_table_reference(select, cte_name))
                    .ok_or_else(|| {
                        sqlstate_datafusion_err(
                            "0A000",
                            format!(
                                "with a SEARCH or CYCLE clause, the recursive reference to \
                                 WITH query \"{cte_name}\" must be at the top level of its \
                                 right-hand SELECT"
                            ),
                        )
                    })?;
                self.extend_recursive_term(
                    recursive_plan,
                    &reference,
                    static_plan.schema(),
                    search_cycle,
                    planner_context,
                )?
            }
            None => recursive_plan,
        };

        // ---------- Step 4: Create the final plan ------------------
        let distinct = !Self::is_union_all(set_quantifier.clone())?;
        LogicalPlanBuilder::from(static_plan)
            .to_recursive_query(name, recursive_plan, distinct)?
            .build()
    }

    /// The values a CYCLE clause marks rows with (`TRUE` and `FALSE` when it
    /// names none), converted to their common type, which is the mark
    /// column's type. As PostgreSQL's `select_common_type` resolves it, an
    /// untyped constant takes the other value's type and a pair of them is
    /// text.
    fn cycle_mark(
        &self,
        cycle: &CycleClause,
        planner_context: &mut PlannerContext,
    ) -> Result<CycleMark> {
        let (cycle_sql, non_cycle_sql) = match &cycle.mark_values {
            Some(mark_values) => (
                mark_values.cycle_value.clone(),
                mark_values.non_cycle_value.clone(),
            ),
            None => (
                SQLExpr::Value(Value::Boolean(true).with_empty_span()),
                SQLExpr::Value(Value::Boolean(false).with_empty_span()),
            ),
        };
        let constants = DFSchema::empty();
        let cycle_value = self.sql_to_expr_ref(&cycle_sql, &constants, planner_context)?;
        let non_cycle_value =
            self.sql_to_expr_ref(&non_cycle_sql, &constants, planner_context)?;
        let cycle_type = cycle_value.get_type(&constants)?;
        let non_cycle_type = non_cycle_value.get_type(&constants)?;
        let untyped = (
            is_untyped_constant(&cycle_sql),
            is_untyped_constant(&non_cycle_sql),
        );
        let mark_type = match untyped {
            (true, true) => DataType::Utf8,
            (true, false) => non_cycle_type,
            (false, true) => cycle_type,
            (false, false) => get_coerce_type_for_case_expression(
                std::slice::from_ref(&cycle_type),
                Some(&non_cycle_type),
            )
            .ok_or_else(|| {
                sqlstate_datafusion_err(
                    "42804",
                    format!(
                        "CYCLE types {cycle_type} and {non_cycle_type} cannot be matched"
                    ),
                )
            })?,
        };
        Ok(CycleMark {
            cycle: cycle_value.cast_to(&mark_type, &constants)?,
            non_cycle: non_cycle_value.cast_to(&mark_type, &constants)?,
        })
    }

    /// The non-recursive term with the columns a SEARCH and CYCLE clause add,
    /// as PostgreSQL's `rewriteSearchAndCycle` builds them: the search
    /// sequence (the record `ROW(0, search columns)` breadth first, the
    /// one-record path `ARRAY[ROW(search columns)]` depth first), the cycle
    /// mark's default and the one-record path of the cycle columns. Every
    /// column is named by the WITH item's column list.
    fn extend_static_term(
        &self,
        static_plan: LogicalPlan,
        search_cycle: &SearchCycle<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let static_schema = Arc::clone(static_plan.schema());
        let mut named = search_cycle
            .column_names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                Expr::Column(Column::from(static_schema.qualified_field(index)))
                    .alias(name)
            })
            .collect::<Vec<_>>();
        if let Some(search) = &search_cycle.search
            && matches!(search.clause.order, SearchOrder::BreadthFirst)
        {
            // The depth sits under the sequence column's name until the
            // sequence record is built from it.
            named.push(
                self.sql_to_expr_ref(&initial_depth(), &static_schema, planner_context)?
                    .alias(&search.sequence),
            );
        }
        let named = LogicalPlanBuilder::from(static_plan).project(named)?.build()?;
        let named_schema = Arc::clone(named.schema());

        let mut columns = (0..search_cycle.column_names.len())
            .map(|index| {
                Expr::Column(Column::from(named_schema.qualified_field(index)))
            })
            .collect::<Vec<_>>();
        if let Some(search) = &search_cycle.search {
            let search_columns = search.clause.by_columns.iter().map(identifier);
            let sequence = match search.clause.order {
                SearchOrder::BreadthFirst => row_constructor(
                    std::iter::once(identifier(&search.clause.set_column))
                        .chain(search_columns),
                ),
                SearchOrder::DepthFirst => {
                    one_element_array(row_constructor(search_columns))
                }
            };
            columns.push(
                self.sql_to_expr_ref(&sequence, &named_schema, planner_context)?
                    .alias(&search.sequence),
            );
        }
        if let Some(cycle) = &search_cycle.cycle {
            columns.push(cycle.values.non_cycle.clone().alias(&cycle.mark));
            let path = one_element_array(row_constructor(
                cycle.clause.columns.iter().map(identifier),
            ));
            columns.push(
                self.sql_to_expr_ref(&path, &named_schema, planner_context)?
                    .alias(&cycle.path),
            );
        }
        LogicalPlanBuilder::from(named).project(columns)?.build()
    }

    /// The recursive term as PostgreSQL's `rewriteSearchAndCycle` rewrites it:
    /// the SELECT also returns the added columns of the work table row each
    /// result row derives from; rows derived from a row that closed a cycle are
    /// dropped; the sequence and path grow by the row's search and cycle
    /// columns, and the mark records whether the row's cycle columns were
    /// already on the path.
    fn extend_recursive_term(
        &self,
        recursive_plan: LogicalPlan,
        reference: &Ident,
        static_schema: &DFSchema,
        search_cycle: &SearchCycle<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let carried = self.carry_work_table_columns(
            recursive_plan,
            reference,
            search_cycle,
            planner_context,
        )?;
        let carried = match &search_cycle.cycle {
            Some(cycle) => {
                let not_cycle = Expr::Column(Column::from_name(&cycle.mark))
                    .not_eq(cycle.values.cycle.clone());
                LogicalPlanBuilder::from(carried).filter(not_cycle)?.build()?
            }
            None => carried,
        };

        // The values the records and paths below are built from take the
        // types the non-recursive term built them from, so both terms build
        // records and paths of one type.
        let carried_schema = Arc::clone(carried.schema());
        let column_count = search_cycle.column_names.len();
        let mut aligned = Vec::with_capacity(carried_schema.fields().len());
        for (index, name) in search_cycle.column_names.iter().enumerate() {
            let column =
                Expr::Column(Column::from(carried_schema.qualified_field(index)));
            aligned.push(
                aligned_to(column, &carried_schema, static_schema.field(index))?
                    .alias(name),
            );
        }
        if let Some(search) = &search_cycle.search {
            aligned.push(match search.clause.order {
                SearchOrder::BreadthFirst => {
                    // The next depth sits under the sequence column's name
                    // until the sequence record is built from it.
                    let depth = self.sql_to_expr_ref(
                        &next_depth(&search.clause.set_column),
                        &carried_schema,
                        planner_context,
                    )?;
                    let depth_field =
                        breadth_first_depth_field(static_schema.field(column_count))?;
                    aligned_to(depth, &carried_schema, &depth_field)?
                        .alias(&search.sequence)
                }
                SearchOrder::DepthFirst => {
                    Expr::Column(Column::from_name(&search.sequence))
                }
            });
        }
        if let Some(cycle) = &search_cycle.cycle {
            aligned.push(Expr::Column(Column::from_name(&cycle.mark)));
            aligned.push(Expr::Column(Column::from_name(&cycle.path)));
        }
        let aligned = LogicalPlanBuilder::from(carried).project(aligned)?.build()?;
        let aligned_schema = Arc::clone(aligned.schema());

        let mut columns = (0..column_count)
            .map(|index| {
                Expr::Column(Column::from(aligned_schema.qualified_field(index)))
            })
            .collect::<Vec<_>>();
        if let Some(search) = &search_cycle.search {
            let search_columns = search.clause.by_columns.iter().map(identifier);
            let sequence = match search.clause.order {
                SearchOrder::BreadthFirst => row_constructor(
                    std::iter::once(identifier(&search.clause.set_column))
                        .chain(search_columns),
                ),
                SearchOrder::DepthFirst => concatenation(
                    identifier(&search.clause.set_column),
                    one_element_array(row_constructor(search_columns)),
                ),
            };
            columns.push(
                self.sql_to_expr_ref(&sequence, &aligned_schema, planner_context)?
                    .alias(&search.sequence),
            );
        }
        if let Some(cycle) = &search_cycle.cycle {
            let cycle_columns = || cycle.clause.columns.iter().map(identifier);
            let closes_cycle = SQLExpr::AnyOp {
                left: SQLBox::new(row_constructor(cycle_columns())),
                compare_op: BinaryOperator::Eq,
                right: SQLBox::new(identifier(cycle.path_ident)),
                is_some: false,
            };
            let closes_cycle =
                self.sql_to_expr_ref(&closes_cycle, &aligned_schema, planner_context)?;
            columns.push(
                Expr::Case(Case::new(
                    None,
                    vec![(Box::new(closes_cycle), Box::new(cycle.values.cycle.clone()))],
                    Some(Box::new(cycle.values.non_cycle.clone())),
                ))
                .alias(&cycle.mark),
            );
            let path = concatenation(
                identifier(cycle.path_ident),
                one_element_array(row_constructor(cycle_columns())),
            );
            columns.push(
                self.sql_to_expr_ref(&path, &aligned_schema, planner_context)?
                    .alias(&cycle.path),
            );
        }
        LogicalPlanBuilder::from(aligned).project(columns)?.build()
    }

    /// The recursive SELECT's plan returning its columns under the WITH
    /// item's column names followed by the SEARCH and CYCLE columns of the
    /// work table row it reads, as PostgreSQL's rewrite appends
    /// `ctename.sqc, ctename.cmc, ctename.cpa` to that SELECT's target list.
    fn carry_work_table_columns(
        &self,
        plan: LogicalPlan,
        reference: &Ident,
        search_cycle: &SearchCycle<'_>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection(projection) => {
                let column_count = search_cycle.column_names.len();
                if projection.expr.len() != column_count {
                    return plan_err!(
                        "Non-recursive term and recursive term must have the same number of columns ({} != {})",
                        column_count,
                        projection.expr.len()
                    );
                }
                let input_schema = Arc::clone(projection.input.schema());
                let mut exprs = projection
                    .expr
                    .into_iter()
                    .zip(&search_cycle.column_names)
                    .map(|(expr, name)| renamed(expr, name))
                    .collect::<Vec<_>>();
                for (column, name) in search_cycle.added_columns() {
                    let reference_column = SQLExpr::CompoundIdentifier(vec![
                        reference.clone(),
                        column.clone(),
                    ]);
                    exprs.push(
                        self.sql_to_expr_ref(
                            &reference_column,
                            &input_schema,
                            planner_context,
                        )?
                        .alias(name),
                    );
                }
                Ok(LogicalPlan::Projection(Projection::try_new(
                    exprs,
                    projection.input,
                )?))
            }
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let input = self.carry_work_table_columns(
                    Arc::unwrap_or_clone(input),
                    reference,
                    search_cycle,
                    planner_context,
                )?;
                Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(input))))
            }
            other => not_impl_err!(
                "SEARCH or CYCLE clause over a recursive term planned as {}",
                other.display()
            ),
        }
    }

    /// The name the recursive SELECT reads the work table under: the alias of
    /// its reference to the WITH item, or the item's own name. `None` when no
    /// reference sits at the top level of the SELECT's FROM list.
    fn work_table_reference(&self, select: &Select, cte_name: &str) -> Option<Ident> {
        select
            .from
            .iter()
            .find_map(|table| self.joined_work_table_reference(table, cte_name))
    }

    fn joined_work_table_reference(
        &self,
        table: &TableWithJoins,
        cte_name: &str,
    ) -> Option<Ident> {
        std::iter::once(&table.relation)
            .chain(table.joins.iter().map(|join| &join.relation))
            .find_map(|relation| match relation {
                TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } => {
                    let [part] = name.0.as_slice() else {
                        return None;
                    };
                    let ident = part.as_ident()?;
                    (self.ident_normalizer.normalize(ident.clone()) == cte_name).then(|| {
                        alias
                            .as_ref()
                            .map_or_else(|| ident.clone(), |alias| alias.name.clone())
                    })
                }
                TableFactor::NestedJoin {
                    table_with_joins,
                    alias,
                } => self
                    .joined_work_table_reference(table_with_joins, cte_name)
                    .map(|inner| {
                        alias.as_ref().map_or(inner, |alias| alias.name.clone())
                    }),
                _ => None,
            })
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

/// A recursive WITH item's SEARCH and CYCLE clauses, checked against the
/// item's column list as PostgreSQL's `analyzeCTE` checks them.
struct SearchCycle<'a> {
    /// The item's column names, in order
    column_names: Vec<String>,
    search: Option<Search<'a>>,
    cycle: Option<Cycle<'a>>,
}

struct Search<'a> {
    clause: &'a SearchClause,
    /// The sequence column's name
    sequence: String,
}

struct Cycle<'a> {
    clause: &'a CycleClause,
    /// `USING`: the path column as written
    path_ident: &'a Ident,
    /// The mark column's name
    mark: String,
    /// The path column's name
    path: String,
    values: CycleMark,
}

/// The values a CYCLE clause stores in its mark column, of the mark type.
struct CycleMark {
    /// `TO`: the mark of a row whose cycle columns were already on its path
    cycle: Expr,
    /// `DEFAULT`: the mark of every other row
    non_cycle: Expr,
}

impl<'a> SearchCycle<'a> {
    fn new(
        column_names: Vec<String>,
        search: Option<&'a SearchClause>,
        cycle: Option<(&'a CycleClause, CycleMark)>,
        normalizer: &IdentNormalizer,
    ) -> Result<Self> {
        let search = search
            .map(|clause| -> Result<Search<'a>> {
                check_clause_columns(
                    "search",
                    &clause.by_columns,
                    &column_names,
                    normalizer,
                )?;
                let sequence = normalizer.normalize(clause.set_column.clone());
                if column_names.contains(&sequence) {
                    return Err(sqlstate_datafusion_err(
                        "42601",
                        format!(
                            "search sequence column name \"{sequence}\" already used in WITH query column list"
                        ),
                    ));
                }
                Ok(Search { clause, sequence })
            })
            .transpose()?;
        let cycle = cycle
            .map(|(clause, values)| -> Result<Cycle<'a>> {
                check_clause_columns(
                    "cycle",
                    &clause.columns,
                    &column_names,
                    normalizer,
                )?;
                let Some(path_ident) = clause.using_column.as_ref() else {
                    return not_impl_err!("CYCLE clause without a USING path column");
                };
                let mark = normalizer.normalize(clause.set_column.clone());
                let path = normalizer.normalize(path_ident.clone());
                if column_names.contains(&mark) {
                    return Err(sqlstate_datafusion_err(
                        "42601",
                        format!(
                            "cycle mark column name \"{mark}\" already used in WITH query column list"
                        ),
                    ));
                }
                if column_names.contains(&path) {
                    return Err(sqlstate_datafusion_err(
                        "42601",
                        format!(
                            "cycle path column name \"{path}\" already used in WITH query column list"
                        ),
                    ));
                }
                if mark == path {
                    return Err(sqlstate_datafusion_err(
                        "42601",
                        "cycle mark column name and cycle path column name are the same",
                    ));
                }
                Ok(Cycle {
                    clause,
                    path_ident,
                    mark,
                    path,
                    values,
                })
            })
            .transpose()?;
        if let (Some(search), Some(cycle)) = (&search, &cycle) {
            if search.sequence == cycle.mark {
                return Err(sqlstate_datafusion_err(
                    "42601",
                    "search sequence column name and cycle mark column name are the same",
                ));
            }
            if search.sequence == cycle.path {
                return Err(sqlstate_datafusion_err(
                    "42601",
                    "search sequence column name and cycle path column name are the same",
                ));
            }
        }
        Ok(Self {
            column_names,
            search,
            cycle,
        })
    }

    /// The columns the clauses add after the item's own, in PostgreSQL's
    /// order (sequence, mark, path): each as written and by its name.
    fn added_columns(&self) -> Vec<(&Ident, &String)> {
        let mut added = Vec::with_capacity(3);
        if let Some(search) = &self.search {
            added.push((&search.clause.set_column, &search.sequence));
        }
        if let Some(cycle) = &self.cycle {
            added.push((&cycle.clause.set_column, &cycle.mark));
            added.push((cycle.path_ident, &cycle.path));
        }
        added
    }

    /// The work table's schema: the extended non-recursive term's columns,
    /// the added ones readable by name from the recursive term but, as in
    /// PostgreSQL, left out of its `*`.
    fn work_table_schema(&self, static_schema: &DFSchema) -> SchemaRef {
        let fields = static_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index < self.column_names.len() {
                    return Arc::clone(field);
                }
                let mut metadata = field.metadata().clone();
                metadata.insert(
                    SYSTEM_COLUMN_METADATA_KEY.to_string(),
                    "true".to_string(),
                );
                Arc::new(field.as_ref().clone().with_metadata(metadata))
            })
            .collect::<Vec<FieldRef>>();
        Arc::new(Schema::new_with_metadata(
            fields,
            static_schema.metadata().clone(),
        ))
    }
}

/// A SEARCH or CYCLE clause's column list names columns of the WITH item,
/// each once.
fn check_clause_columns(
    clause: &str,
    columns: &[Ident],
    column_names: &[String],
    normalizer: &IdentNormalizer,
) -> Result<()> {
    let mut seen = HashSet::with_capacity(columns.len());
    for column in columns {
        let name = normalizer.normalize(column.clone());
        if !column_names.contains(&name) {
            return Err(sqlstate_datafusion_err(
                "42601",
                format!("{clause} column \"{name}\" not in WITH query column list"),
            ));
        }
        if !seen.insert(name.clone()) {
            return Err(sqlstate_datafusion_err(
                "42701",
                format!("{clause} column \"{name}\" specified more than once"),
            ));
        }
    }
    Ok(())
}

/// Whether a CYCLE mark constant has PostgreSQL's `unknown` type: a string
/// literal no type names, or NULL.
fn is_untyped_constant(constant: &SQLExpr) -> bool {
    match constant {
        SQLExpr::Value(value) => matches!(
            value.value,
            Value::SingleQuotedString(_)
                | Value::DollarQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::UnicodeStringLiteral(_)
                | Value::NationalStringLiteral(_)
                | Value::Null
        ),
        _ => false,
    }
}

fn with_query_not_recursive() -> DataFusionError {
    sqlstate_datafusion_err("42601", "WITH query is not recursive")
}

/// Whether an operand of the UNION is a set operation rather than a single
/// query. A parenthesized set operation with its own ORDER BY, LIMIT, locking
/// or WITH is a subquery, as in PostgreSQL's set-operation tree.
fn is_set_operation(operand: &SetExpr) -> bool {
    match operand {
        SetExpr::SetOperation { .. } => true,
        SetExpr::Query(query) => {
            query_is_bare(query) && is_set_operation(query.body.as_ref())
        }
        _ => false,
    }
}

/// The SELECT a UNION operand is, through parentheses without clauses of
/// their own.
fn union_operand_select(operand: &SetExpr) -> Option<&Select> {
    match operand {
        SetExpr::Select(select) => Some(select.as_ref()),
        SetExpr::Query(query) if query_is_bare(query) => {
            union_operand_select(query.body.as_ref())
        }
        _ => None,
    }
}

fn query_is_bare(query: &Query) -> bool {
    query.with.is_none()
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && query.fetch.is_none()
        && query.locks.is_empty()
        && query.for_clause.is_none()
}

/// `expr` renamed `name`, keeping any metadata its alias carries.
fn renamed(expr: Expr, name: &str) -> Expr {
    match expr {
        Expr::Alias(alias) => Expr::Alias(
            Alias::new(*alias.expr, None::<&str>, name).with_metadata(alias.metadata),
        ),
        other => other.alias(name),
    }
}

/// `expr` yielding a value of `target`'s type and type metadata. A record or
/// path built over it then has the type the non-recursive term built. A value
/// whose type does not settle on the target is left for the recursive query's
/// own column type check to report.
fn aligned_to(expr: Expr, schema: &DFSchema, target: &FieldRef) -> Result<Expr> {
    let (_, field) = expr.to_field(schema)?;
    let same =
        field.data_type() == target.data_type() && field.metadata() == target.metadata();
    if same || !recursive_term_type_settles(target.data_type(), field.data_type()) {
        return Ok(expr);
    }
    Ok(Expr::Cast(Cast::new_from_field(Box::new(expr), Arc::clone(target))))
}

/// The depth element of a breadth-first search sequence record.
fn breadth_first_depth_field(sequence: &FieldRef) -> Result<FieldRef> {
    match sequence.data_type() {
        DataType::Struct(fields) if !fields.is_empty() => Ok(Arc::clone(&fields[0])),
        other => internal_err!("breadth-first search sequence planned as {other}"),
    }
}

fn identifier(ident: &Ident) -> SQLExpr {
    SQLExpr::Identifier(ident.clone())
}

/// `ROW(elements)`
fn row_constructor(elements: impl IntoIterator<Item = SQLExpr>) -> SQLExpr {
    SQLExpr::Function(Function {
        name: ObjectName::from(vec![Ident::new("row")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: elements
                .into_iter()
                .map(|element| FunctionArg::Unnamed(FunctionArgExpr::Expr(element)))
                .collect(),
            clauses: vec![],
            close_paren_token: AttachedToken::empty(),
        }),
        filter: None,
        null_treatment: None,
        nth_value_order: None,
        over: None,
        within_group: vec![],
    })
}

/// `ARRAY[element]`
fn one_element_array(element: SQLExpr) -> SQLExpr {
    SQLExpr::Array(Array {
        elem: vec![element],
        named: true,
    })
}

/// `left || right`
fn concatenation(left: SQLExpr, right: SQLExpr) -> SQLExpr {
    SQLExpr::BinaryOp {
        left: SQLBox::new(left),
        op: BinaryOperator::StringConcat,
        right: SQLBox::new(right),
    }
}

/// `CAST(0 AS BIGINT)`: the depth of the non-recursive term's rows.
fn initial_depth() -> SQLExpr {
    SQLExpr::Cast {
        kind: CastKind::Cast,
        expr: SQLBox::new(SQLExpr::Value(
            Value::Number("0".to_string(), false).with_empty_span(),
        )),
        data_type: SQLDataType::BigInt(None),
        format: None,
    }
}

/// `(sequence).f1 + 1`: one more than the depth a breadth-first sequence
/// record carries in its first field.
fn next_depth(sequence: &Ident) -> SQLExpr {
    SQLExpr::BinaryOp {
        left: SQLBox::new(SQLExpr::CompoundFieldAccess {
            root: SQLBox::new(identifier(sequence)),
            access_chain: vec![AccessExpr::Dot(SQLExpr::Identifier(Ident::new("f1")))],
        }),
        op: BinaryOperator::Plus,
        right: SQLBox::new(SQLExpr::Value(
            Value::Number("1".to_string(), false).with_empty_span(),
        )),
    }
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
            let _ = cte.query.walk(&mut referenced);
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
