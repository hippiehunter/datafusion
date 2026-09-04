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

use std::any::Any;
#[cfg(test)]
use std::collections::HashMap;
use std::fmt::Display;
use std::{sync::Arc, vec};

use arrow::datatypes::*;
use datafusion_common::config::ConfigOptions;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{
    Constraint, Constraints, GetExt, NullsDistinct, Result, ScalarValue, TableReference,
    plan_err,
};
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::planner::{
    ContextProvider, CreateTableLikeOptions, CreateTableLikeSource, ExprPlanner,
    TypePlanner,
};

// Note: make_array from datafusion_functions_nested was removed

struct MockCsvType {}

impl GetExt for MockCsvType {
    fn get_ext(&self) -> String {
        "csv".to_string()
    }
}

impl FileType for MockCsvType {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for MockCsvType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_ext())
    }
}

#[derive(Default)]
pub(crate) struct MockSessionState {
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    type_planner: Option<Arc<dyn TypePlanner>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    pub config_options: ConfigOptions,
}

impl MockSessionState {
    pub fn with_scalar_function(mut self, scalar_function: Arc<ScalarUDF>) -> Self {
        self.scalar_functions
            .insert(scalar_function.name().to_string(), scalar_function);
        self
    }

    pub fn with_aggregate_function(
        mut self,
        aggregate_function: Arc<AggregateUDF>,
    ) -> Self {
        // TODO: change to to_string() if all the function name is converted to lowercase
        self.aggregate_functions.insert(
            aggregate_function.name().to_string().to_lowercase(),
            aggregate_function,
        );
        self
    }

    #[allow(dead_code)] // Window function crate was pruned but keep method for potential future use
    pub fn with_window_function(mut self, window_function: Arc<WindowUDF>) -> Self {
        self.window_functions
            .insert(window_function.name().to_string(), window_function);
        self
    }
}

pub(crate) struct MockContextProvider {
    pub(crate) state: MockSessionState,
}

impl ContextProvider for MockContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let schema = match name.table() {
            "test" => Ok(Schema::new(vec![
                Field::new("t_date32", DataType::Date32, false),
                Field::new("t_date64", DataType::Date64, false),
            ])),
            "j1" => Ok(Schema::new(vec![
                Field::new("j1_id", DataType::Int32, false),
                Field::new("j1_string", DataType::Utf8, false),
            ])),
            "j2" => Ok(Schema::new(vec![
                Field::new("j2_id", DataType::Int32, false),
                Field::new("j2_string", DataType::Utf8, false),
            ])),
            "j3" => Ok(Schema::new(vec![
                Field::new("j3_id", DataType::Int32, false),
                Field::new("j3_string", DataType::Utf8, false),
            ])),
            // A table whose columns carry declared defaults, for the planner
            // paths that resolve DEFAULT against the catalog. Kept separate
            // from `person` so those defaults do not rewrite every INSERT
            // snapshot that table appears in.
            "column_defaults" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("tag", DataType::Utf8, true),
                Field::new("age", DataType::Int32, true),
            ])),
            "test_decimal" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("price", DataType::Decimal128(10, 2), false),
            ])),
            "person" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("salary", DataType::Float64, false),
                Field::new(
                    "birth_date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("😀", DataType::Int32, false),
            ])),
            "person_quoted_cols" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("First Name", DataType::Utf8, false),
                Field::new("Last Name", DataType::Utf8, false),
                Field::new("Age", DataType::Int32, false),
                Field::new("State", DataType::Utf8, false),
                Field::new("Salary", DataType::Float64, false),
                Field::new(
                    "Birth Date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("😀", DataType::Int32, false),
            ])),
            "person_with_uuid_extension" => Ok(Schema::new(vec![
                Field::new("id", DataType::FixedSizeBinary(16), false).with_metadata(
                    [("ARROW:extension:name".to_string(), "arrow.uuid".to_string())]
                        .into(),
                ),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
            ])),
            "orders" => Ok(Schema::new(vec![
                Field::new("order_id", DataType::UInt32, false),
                Field::new("customer_id", DataType::UInt32, false),
                Field::new("o_item_id", DataType::Utf8, false),
                Field::new("qty", DataType::Int32, false),
                Field::new("price", DataType::Float64, false),
                Field::new("delivered", DataType::Boolean, false),
            ])),
            "array" => Ok(Schema::new(vec![
                Field::new(
                    "left",
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::Int64,
                        true,
                    ))),
                    false,
                ),
                Field::new(
                    "right",
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::Int64,
                        true,
                    ))),
                    false,
                ),
            ])),
            "lineitem" => Ok(Schema::new(vec![
                Field::new("l_item_id", DataType::UInt32, false),
                Field::new("l_description", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
            ])),
            "aggregate_test_100" => Ok(Schema::new(vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::UInt32, false),
                Field::new("c3", DataType::Int8, false),
                Field::new("c4", DataType::Int16, false),
                Field::new("c5", DataType::Int32, false),
                Field::new("c6", DataType::Int64, false),
                Field::new("c7", DataType::UInt8, false),
                Field::new("c8", DataType::UInt16, false),
                Field::new("c9", DataType::UInt32, false),
                Field::new("c10", DataType::UInt64, false),
                Field::new("c11", DataType::Float32, false),
                Field::new("c12", DataType::Float64, false),
                Field::new("c13", DataType::Utf8, false),
            ])),
            "UPPERCASE_test" => Ok(Schema::new(vec![
                Field::new("Id", DataType::UInt32, false),
                Field::new("lower", DataType::UInt32, false),
            ])),
            "unnest_table" => Ok(Schema::new(vec![
                Field::new(
                    "array_col",
                    DataType::List(Arc::new(Field::new_list_field(
                        DataType::Int64,
                        true,
                    ))),
                    false,
                ),
                Field::new(
                    "struct_col",
                    DataType::Struct(Fields::from(vec![
                        Field::new("field1", DataType::Int64, true),
                        Field::new("field2", DataType::Utf8, true),
                    ])),
                    false,
                ),
            ])),
            _ => plan_err!("No table named: {} found", name.table()),
        };

        match schema {
            Ok(t) => Ok(Arc::new(
                EmptyTable::new(Arc::new(t))
                    .with_column_defaults(column_defaults(name.table())),
            )),
            Err(e) => Err(e),
        }
    }

    fn get_create_table_like_source(
        &self,
        name: TableReference,
        options: CreateTableLikeOptions,
    ) -> Result<CreateTableLikeSource> {
        let names: &[(&str, DataType)] = match name.table() {
            "like_left" => &[
                ("left_key", DataType::Int32),
                ("left_value", DataType::Utf8),
            ],
            "like_right" => &[
                ("right_value", DataType::Boolean),
                ("right_key", DataType::Int64),
            ],
            _ => return plan_err!("No CREATE TABLE LIKE source named: {}", name.table()),
        };

        // Encode the received option set into otherwise opaque Arrow metadata.
        // The integration tests can then prove that ordered SQL options were
        // collapsed before the provider call without exposing parser types in
        // this interface.
        let option_metadata = [
            ("like.defaults", options.defaults),
            ("like.constraints", options.constraints),
            ("like.indexes", options.indexes),
            ("like.identity", options.identity),
            ("like.generated", options.generated),
            ("like.comments", options.comments),
            ("like.storage", options.storage),
        ]
        .into_iter()
        .map(|(name, enabled)| (name.to_string(), enabled.to_string()))
        .collect::<HashMap<_, _>>();
        let fields = names
            .iter()
            .map(|(name, data_type)| {
                Arc::new(
                    Field::new(*name, data_type.clone(), false)
                        .with_metadata(option_metadata.clone()),
                )
            })
            .collect::<Vec<_>>();

        let mut constraints = Vec::new();
        if options.constraints {
            constraints.push(Constraint::Check {
                name: Some(format!("{}_check", name.table())),
                expr: format!("{} IS NOT NULL", names[0].0),
                referenced_columns: vec![names[0].0.to_string()],
                enforced: Some(true),
            });
        }
        if options.indexes {
            constraints.push(Constraint::PrimaryKey(vec![0]));
            constraints.push(Constraint::Unique {
                columns: vec![1],
                nulls_distinct: NullsDistinct::Distinct,
            });
        }
        let default_value = match names[1].1 {
            DataType::Utf8 => ScalarValue::Utf8(Some("copied".to_string())),
            DataType::Int64 => ScalarValue::Int64(Some(42)),
            ref data_type => {
                unreachable!("test LIKE source has no default for {data_type}")
            }
        };
        let column_defaults = options
            .defaults
            .then(|| vec![(names[1].0.to_string(), Expr::Literal(default_value, None))])
            .unwrap_or_default();

        Ok(CreateTableLikeSource {
            schema: Arc::new(Schema::new(fields)),
            constraints: Constraints::new_unverified(constraints),
            column_defaults,
            check_expressions: options
                .constraints
                .then(|| {
                    vec![datafusion_expr::BoundSqlExpression::new(
                        datafusion_expr::col(names[0].0).is_not_null(),
                    )]
                })
                .unwrap_or_default(),
            generated_expressions: Vec::new(),
        })
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions.get(name).cloned()
    }

    fn options(&self) -> &ConfigOptions {
        &self.state.config_options
    }

    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        Ok(Arc::new(MockCsvType {}))
    }

    fn create_cte_work_table(
        &self,
        _name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(EmptyTable::new(schema)))
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.state.expr_planners
    }

    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        if let Some(type_planner) = &self.state.type_planner {
            Some(Arc::clone(type_planner))
        } else {
            None
        }
    }
}

/// Declared column defaults the planner resolves `DEFAULT` against, for the
/// tables whose tests need one.
fn column_defaults(table: &str) -> HashMap<String, Expr> {
    match table {
        "column_defaults" => HashMap::from([(
            "age".to_string(),
            Expr::Literal(ScalarValue::Int32(Some(42)), None),
        )]),
        _ => HashMap::new(),
    }
}

struct EmptyTable {
    table_schema: SchemaRef,
    column_defaults: HashMap<String, Expr>,
}

impl EmptyTable {
    fn new(table_schema: SchemaRef) -> Self {
        Self {
            table_schema,
            column_defaults: HashMap::new(),
        }
    }

    fn with_column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.column_defaults = column_defaults;
        self
    }
}

impl TableSource for EmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}
