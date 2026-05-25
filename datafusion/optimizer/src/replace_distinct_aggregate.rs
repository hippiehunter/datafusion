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

//! [`ReplaceDistinctWithAggregate`] replaces `DISTINCT ...` with `GROUP BY ...`

use crate::optimizer::{ApplyOrder, ApplyOrder::BottomUp};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::normalize_cols;
use datafusion_expr::utils::expand_wildcard;
use datafusion_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion_expr::{
    Aggregate, Distinct, DistinctOn, Expr, Limit, LogicalPlan, LogicalPlanBuilder, WindowFrame,
    WindowFunctionDefinition, col, lit,
};

/// Optimizer that replaces logical [[Distinct]] with a logical [[Aggregate]]
///
/// ```text
/// SELECT DISTINCT a, b FROM tab
/// ```
///
/// Into
/// ```text
/// SELECT a, b FROM tab GROUP BY a, b
/// ```
///
/// For a `DISTINCT ON` query the replacement uses a window function:
/// ```text
/// SELECT DISTINCT ON (a) b FROM tab ORDER BY a DESC, c
/// ```
///
/// becomes
/// ```text
/// SELECT b FROM (
///     SELECT b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY a DESC, c) AS __distinct_on_rn
///     FROM tab
/// ) WHERE __distinct_on_rn = 1
/// ORDER BY a DESC
/// ```
///
/// In case there are no columns, the [[Distinct]] is replaced by a [[Limit]]
///
/// ```text
/// SELECT DISTINCT * FROM empty_table
/// ```
///
/// Into
/// ```text
/// SELECT * FROM empty_table LIMIT 1
/// ```
#[derive(Default, Debug)]
pub struct ReplaceDistinctWithAggregate {}

impl ReplaceDistinctWithAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ReplaceDistinctWithAggregate {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let group_expr = expand_wildcard(input.schema(), &input, None)?;

                if group_expr.is_empty() {
                    // Special case: there are no columns to group by, so we can't replace it by a group by
                    // however, we can replace it by LIMIT 1 because there is either no output or a single empty row
                    return Ok(Transformed::yes(LogicalPlan::Limit(Limit {
                        skip: None,
                        fetch: Some(Box::new(lit(1i64))),
                        with_ties: false,
                        input,
                    })));
                }

                let field_count = input.schema().fields().len();
                for dep in input.schema().functional_dependencies().iter() {
                    // If distinct is exactly the same with a previous GROUP BY, we can
                    // simply remove it:
                    if dep.source_indices.len() >= field_count
                        && dep.source_indices[..field_count]
                            .iter()
                            .enumerate()
                            .all(|(idx, f_idx)| idx == *f_idx)
                    {
                        return Ok(Transformed::yes(input.as_ref().clone()));
                    }
                }

                // Replace with aggregation:
                let aggr_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    input,
                    group_expr,
                    vec![],
                )?);
                Ok(Transformed::yes(aggr_plan))
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                select_expr,
                on_expr,
                sort_expr,
                input,
                schema,
            })) => {
                let expr_cnt = on_expr.len();

                let registry = config.function_registry().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "DISTINCT ON requires a function registry (for row_number UDWF)".to_string(),
                    )
                })?;
                let row_number_udwf = registry.udwf("row_number")?;

                let order_by = sort_expr.clone().unwrap_or_default();
                let partition_by = normalize_cols(on_expr.clone(), input.as_ref())?;

                let rn_expr = Expr::WindowFunction(Box::new(WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(row_number_udwf),
                    params: WindowFunctionParams {
                        args: vec![],
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new(None),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }));

                let rn_col = "__distinct_on_rn";
                let window_plan = LogicalPlanBuilder::from(input.as_ref().clone())
                    .window(vec![rn_expr.alias(rn_col)])?
                    .filter(col(rn_col).eq(lit(1i64)))?
                    .build()?;

                let project_exprs = select_expr
                    .into_iter()
                    .zip(schema.iter())
                    .map(|(expr, (qualifier, field))| {
                        expr.alias_qualified(qualifier.cloned(), field.name())
                    })
                    .collect::<Vec<Expr>>();

                let plan = LogicalPlanBuilder::from(window_plan)
                    .project(project_exprs)?
                    .build()?;

                let plan = if let Some(mut sort_expr) = sort_expr {
                    sort_expr.truncate(expr_cnt);
                    LogicalPlanBuilder::from(plan)
                        .sort(sort_expr)?
                        .build()?
                } else {
                    plan
                };

                Ok(Transformed::yes(plan))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "replace_distinct_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
    use crate::test::*;
    use arrow::datatypes::{Fields, Schema};
    use std::sync::Arc;

    use crate::OptimizerContext;
    use datafusion_common::Result;
    use datafusion_expr::{
        Expr, col, logical_plan::builder::LogicalPlanBuilder, table_scan,
    };
    use datafusion_functions_aggregate::sum::sum;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(ReplaceDistinctWithAggregate::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn eliminate_redundant_distinct_simple() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], Vec::<Expr>::new())?
            .project(vec![col("c")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.c
          Aggregate: groupBy=[[test.c]], aggr=[[]]
            TableScan: test
        ")
    }

    #[test]
    fn eliminate_redundant_distinct_pair() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("b")], Vec::<Expr>::new())?
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.b
          Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]
            TableScan: test
        ")
    }

    #[test]
    fn do_not_eliminate_distinct() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]
          Projection: test.a, test.b
            TableScan: test
        ")
    }

    #[test]
    fn do_not_eliminate_distinct_with_aggr() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("b"), col("c")], vec![sum(col("c"))])?
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]
          Projection: test.a, test.b
            Aggregate: groupBy=[[test.a, test.b, test.c]], aggr=[[sum(test.c)]]
              TableScan: test
        ")
    }

    #[test]
    fn use_limit_1_when_no_columns() -> Result<()> {
        let plan = table_scan(Some("test"), &Schema::new(Fields::empty()), None)?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Limit: skip=0, fetch=1
          TableScan: test
        ")
    }
}
