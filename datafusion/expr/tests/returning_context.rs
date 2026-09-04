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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::logical_plan::builder::LogicalTableSource;
use datafusion_expr::{
    DmlStatement, EmptyRelation, Expr, InsertOp, LogicalPlan, Merge, ReturningContext,
    WriteOp,
};

fn schema() -> Arc<DFSchema> {
    Arc::new(
        DFSchema::try_from(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
            .expect("test schema"),
    )
}

fn empty(schema: Arc<DFSchema>) -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema,
    })
}

#[test]
fn rebuilding_dml_preserves_dual_image_returning_context() {
    let table_schema = schema();
    let target = Arc::new(LogicalTableSource::new(Arc::new(
        table_schema.as_arrow().clone(),
    )));
    let mut dml = DmlStatement::new(
        TableReference::bare("t"),
        target,
        WriteOp::Insert(InsertOp::Append),
        Arc::new(empty(Arc::clone(&table_schema))),
    );
    let context = ReturningContext::DualImage {
        eval_schema: Arc::clone(&table_schema),
    };
    dml.returning_context = Some(context.clone());

    let rebuilt = LogicalPlan::Dml(dml)
        .with_new_exprs(vec![], vec![empty(Arc::clone(&table_schema))])
        .expect("rebuild DML");
    let LogicalPlan::Dml(rebuilt) = rebuilt else {
        panic!("expected DML")
    };
    assert_eq!(rebuilt.returning_context, Some(context));
}

#[test]
fn rebuilding_merge_preserves_applied_image_returning_context() {
    let table_schema = schema();
    let target = Arc::new(empty(Arc::clone(&table_schema)));
    let source = Arc::new(empty(Arc::clone(&table_schema)));
    let mut merge = Merge::new(
        TableReference::bare("t"),
        Arc::clone(&target),
        Arc::clone(&source),
        Expr::Literal(true.into(), None),
        vec![],
    );
    let context = ReturningContext::MergeApplied {
        eval_schema: Arc::clone(&table_schema),
        action_column: 0,
    };
    merge.returning_context = Some(context.clone());

    let rebuilt = LogicalPlan::Merge(merge)
        .with_new_exprs(
            vec![Expr::Literal(true.into(), None)],
            vec![Arc::unwrap_or_clone(target), Arc::unwrap_or_clone(source)],
        )
        .expect("rebuild MERGE");
    let LogicalPlan::Merge(rebuilt) = rebuilt else {
        panic!("expected MERGE")
    };
    assert_eq!(rebuilt.returning_context, Some(context));
}
