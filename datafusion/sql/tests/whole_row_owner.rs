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

#[expect(
    dead_code,
    reason = "Shared integration helpers expose registrations unused by this target"
)]
mod common;

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use common::{MockContextProvider, MockSessionState};
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{DFSchema, Result, ScalarValue, TableReference, plan_err};
use datafusion_expr::{
    AggregateUDF, ColumnarValue, Expr, ExprSchemable, LogicalPlanBuilder,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableSource, Volatility,
    WindowUDF,
};
use datafusion_sql::parser::{DFParser, Statement as DfStatement};
use datafusion_sql::planner::{
    ContextProvider, ExprPlanner, PlannedRelation, PlannerResult, RelationPlanner,
    RelationPlannerContext, RelationPlanning, SqlToRel,
};
use sqlparser::ast::{SetExpr, Statement as SqlStatement, TableFactor};
use sqlparser::dialect::PostgreSqlDialect;

#[derive(Debug, PartialEq, Eq, Hash)]
struct NamedStruct(Signature, bool);

impl ScalarUDFImpl for NamedStruct {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        if self.1 { "named_struct" } else { "row" }
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(
            args.iter()
                .enumerate()
                .filter(|(index, _)| !self.1 || index % 2 == 1)
                .map(|(_, ty)| ty)
                .enumerate()
                .map(|(index, ty)| Field::new(format!("f{index}"), ty.clone(), true))
                .collect::<Vec<_>>()
                .into(),
        ))
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!("whole-row owner tests only plan expressions")
    }
}

#[derive(Debug)]
struct RowPlanner(Arc<ScalarUDF>);
impl ExprPlanner for RowPlanner {
    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        named: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        let args = if named {
            args
        } else {
            args.into_iter()
                .enumerate()
                .flat_map(|(index, expr)| {
                    [
                        Expr::Literal(ScalarValue::Utf8(Some(format!("f{index}"))), None),
                        expr,
                    ]
                })
                .collect()
        };
        Ok(PlannerResult::Planned(self.0.call(args)))
    }
}

struct SystemColumnSource(Arc<arrow::datatypes::Schema>);
impl TableSource for SystemColumnSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        Arc::clone(&self.0)
    }
}

type Observed = Vec<(String, Option<String>)>;
struct OwnerContext {
    inner: MockContextProvider,
    record: Arc<ScalarUDF>,
    planners: Vec<Arc<dyn ExprPlanner>>,
    relation_planners: Vec<Arc<dyn RelationPlanner>>,
    calls: RefCell<Observed>,
    reject: bool,
}
impl OwnerContext {
    fn new() -> Self {
        let record = Arc::new(ScalarUDF::new_from_impl(NamedStruct(
            Signature::variadic_any(Volatility::Immutable),
            true,
        )));
        Self {
            inner: MockContextProvider {
                state: MockSessionState::default().with_scalar_function(Arc::new(
                    ScalarUDF::new_from_impl(NamedStruct(
                        Signature::variadic_any(Volatility::Immutable),
                        false,
                    )),
                )),
            },
            record: Arc::clone(&record),
            planners: vec![Arc::new(RowPlanner(record))],
            relation_planners: Vec::new(),
            calls: RefCell::new(Vec::new()),
            reject: false,
        }
    }
    fn plan(&self, sql: &str) -> Result<datafusion_expr::LogicalPlan> {
        let statement = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {})?
            .pop_front()
            .unwrap();
        SqlToRel::new(self).statement_to_plan(statement)
    }
}
impl ContextProvider for OwnerContext {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if name.table() == "system_columns" {
            let source = self
                .inner
                .get_table_source(TableReference::bare("typed_columns"))?;
            let mut fields = source.schema().fields().to_vec();
            fields.push(Arc::new(
                Field::new("rowid", DataType::Int64, false).with_metadata(HashMap::from(
                    [(
                        datafusion_expr::utils::SYSTEM_COLUMN_METADATA_KEY.to_string(),
                        "true".to_string(),
                    )],
                )),
            ));
            return Ok(Arc::new(SystemColumnSource(Arc::new(
                arrow::datatypes::Schema::new(fields),
            ))));
        }
        self.inner.get_table_source(name)
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        if name == "named_struct" {
            Some(Arc::clone(&self.record))
        } else {
            self.inner.get_function_meta(name)
        }
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.inner.get_aggregate_meta(name)
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.inner.get_window_meta(name)
    }
    fn get_variable_type(&self, names: &[String]) -> Option<DataType> {
        self.inner.get_variable_type(names)
    }
    fn options(&self) -> &datafusion_common::config::ConfigOptions {
        self.inner.options()
    }
    fn udf_names(&self) -> Vec<String> {
        self.inner.udf_names()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.inner.udaf_names()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.inner.udwf_names()
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.planners
    }
    fn get_relation_planners(&self) -> &[Arc<dyn RelationPlanner>] {
        &self.relation_planners
    }
    fn plan_whole_row_reference(
        &self,
        relation: &TableReference,
        source: Option<&TableReference>,
        record: Expr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        if self.reject {
            return plan_err!("owned whole-row rejection");
        }
        self.calls
            .borrow_mut()
            .push((relation.to_string(), source.map(ToString::to_string)));
        let (_, field) = record.to_field(schema)?;
        let name = field.name().clone();
        Ok(record.alias_with_metadata(
            name,
            Some(FieldMetadata::from(HashMap::from([(
                "owner".to_string(),
                source.map_or_else(|| "anonymous".to_string(), ToString::to_string),
            )]))),
        ))
    }
}

#[test]
fn whole_row_owner_survives_table_and_column_aliases() {
    let context = OwnerContext::new();
    let plan = context
        .plan("SELECT r FROM typed_columns AS r(x,y)")
        .unwrap();
    assert_eq!(
        *context.calls.borrow(),
        vec![("r".into(), Some("typed_columns".into()))]
    );
    let field = &plan.schema().fields()[0];
    assert_eq!(
        field.metadata().get("owner").map(String::as_str),
        Some("typed_columns")
    );
    let DataType::Struct(fields) = field.data_type() else {
        panic!("whole row keeps its typed members")
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].data_type(), &DataType::Int32);
    assert_eq!(fields[1].data_type(), &DataType::Utf8);
}

#[test]
fn whole_row_derived_cte_and_join_aliases_have_no_base_owner() {
    for sql in [
        "SELECT r FROM (SELECT * FROM typed_columns) r",
        "WITH r AS (SELECT * FROM typed_columns) SELECT r FROM r",
        "SELECT r FROM (typed_columns a CROSS JOIN typed_columns b) AS r(a,b,c,d)",
    ] {
        let context = OwnerContext::new();
        context
            .plan(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(*context.calls.borrow(), vec![("r".into(), None)], "{sql}");
    }
}

#[test]
fn whole_row_nested_shadow_does_not_change_the_outer_owner() {
    let context = OwnerContext::new();
    context.plan("SELECT r, (SELECT r FROM (SELECT * FROM typed_columns) r), r FROM typed_columns r").unwrap();
    assert_eq!(
        *context.calls.borrow(),
        vec![
            ("r".into(), Some("typed_columns".into())),
            ("r".into(), None),
            ("r".into(), Some("typed_columns".into())),
        ]
    );
}

#[test]
fn explicit_row_constructor_does_not_call_the_whole_row_owner() {
    let context = OwnerContext::new();
    context
        .plan("SELECT ROW(r.id,r.j) FROM typed_columns r")
        .unwrap();
    assert!(context.calls.borrow().is_empty());
}

#[test]
fn whole_row_owner_error_is_not_a_column_fallback() {
    let mut context = OwnerContext::new();
    context.reject = true;
    let error = context.plan("SELECT r FROM typed_columns r").unwrap_err();
    assert!(error.to_string().contains("owned whole-row rejection"));
}

#[test]
fn whole_row_qualified_source_does_not_merge_equal_bare_names() {
    for (sql, source) in [
        (
            "SELECT ROW(s1.typed_columns.*) FROM s1.typed_columns CROSS JOIN s2.typed_columns",
            "s1.typed_columns",
        ),
        (
            "SELECT named_struct('value',s2.typed_columns.*) FROM s1.typed_columns CROSS JOIN s2.typed_columns",
            "s2.typed_columns",
        ),
    ] {
        let context = OwnerContext::new();
        context
            .plan(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(
            *context.calls.borrow(),
            vec![(source.into(), Some(source.into()))]
        );
    }
    let context = OwnerContext::new();
    let error = context
        .plan("SELECT typed_columns FROM s1.typed_columns CROSS JOIN s2.typed_columns")
        .unwrap_err();
    assert!(error.to_string().contains("ambiguous"));
    assert!(context.calls.borrow().is_empty());
}

#[derive(Debug)]
struct AnonymousWholeRowExtension {
    replacement: Option<TableFactor>,
}

impl RelationPlanner for AnonymousWholeRowExtension {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = &relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        if context
            .object_name_to_table_reference(name.clone())?
            .table()
            != "anonymous_source"
        {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        if let Some(replacement) = &self.replacement {
            let mut replacement = replacement.clone();
            let TableFactor::Derived {
                alias: replacement_alias,
                ..
            } = &mut replacement
            else {
                panic!("the replacement is a derived relation")
            };
            *replacement_alias = alias.clone();
            return Ok(RelationPlanning::Original(Box::new(replacement)));
        }
        let source = context
            .context_provider()
            .get_table_source(TableReference::bare("typed_columns"))?;
        let plan = LogicalPlanBuilder::scan(
            TableReference::bare("anonymous_source"),
            source,
            None,
        )?
        .build()?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan,
            alias.clone(),
        ))))
    }
}

#[test]
fn whole_row_extension_shadow_never_inherits_an_outer_base_owner() {
    let DfStatement::Statement(statement) = DFParser::parse_sql_with_dialect(
        "SELECT * FROM (SELECT * FROM typed_columns)",
        &PostgreSqlDialect {},
    )
    .unwrap()
    .pop_front()
    .unwrap() else {
        panic!("the replacement SQL is an ordinary statement")
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        panic!("the replacement SQL is a query")
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("the replacement query is a SELECT")
    };
    let derived = select.from[0].relation.clone();
    for replacement in [None, Some(derived)] {
        for inner in ["anonymous_source r", "anonymous_source r(x,y)"] {
            let mut context = OwnerContext::new();
            context
                .relation_planners
                .push(Arc::new(AnonymousWholeRowExtension {
                    replacement: replacement.clone(),
                }));
            let sql =
                format!("SELECT r, (SELECT r FROM {inner}), r FROM typed_columns r");
            context
                .plan(&sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(
                *context.calls.borrow(),
                vec![
                    ("r".into(), Some("typed_columns".into())),
                    ("r".into(), None),
                    ("r".into(), Some("typed_columns".into())),
                ],
                "{sql} with replacement {replacement:?}",
            );
        }
    }
}

#[test]
fn whole_row_using_alias_shadows_only_its_query_scope() {
    let context = OwnerContext::new();
    context.plan(
        "SELECT r, (SELECT r FROM typed_columns a JOIN typed_columns b USING(id) AS r), r FROM typed_columns r"
    ).unwrap();
    assert_eq!(
        *context.calls.borrow(),
        vec![
            ("r".into(), Some("typed_columns".into())),
            ("r".into(), None),
            ("r".into(), Some("typed_columns".into())),
        ],
    );
}

#[test]
fn whole_row_excludes_internal_system_columns() {
    let context = OwnerContext::new();
    let plan = context.plan("SELECT r FROM system_columns r").unwrap();
    let DataType::Struct(fields) = plan.schema().field(0).data_type() else {
        panic!("whole row must be a record")
    };
    assert_eq!(fields.len(), 2);
    assert!(fields.iter().all(|field| field.name() != "rowid"));
    assert_eq!(
        *context.calls.borrow(),
        vec![("r".into(), Some("system_columns".into()))]
    );
}
