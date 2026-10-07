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

//! Plan nodes that keep what a query wrote where execution planning keeps
//! only its effect.
//!
//! Execution planning inlines a common table expression into every
//! reference to it, which leaves nothing to tell a CTE reference from a
//! derived table and drops a CTE nothing references, and it renames the
//! columns of a `FROM` item written with a column alias list by a projection
//! no different from a derived table's own. A provider that plans a stored
//! definition to print it back as SQL
//! ([`ContextProvider::plans_definitions_as_written`]) gets these nodes
//! instead: [`WithQuery`] over the query that owns a `WITH` list, a
//! [`CteReference`] leaf wherever the query reads one of its items, and an
//! [`AliasedRelation`] over a `FROM` item with a column alias list, and a
//! [`FunctionRelation`] over table functions before their expansion as lists
//! and unnesting loses their SQL identities. The plan is for reading; nothing
//! executes it.
//!
//! [`ContextProvider::plans_definitions_as_written`]: crate::planner::ContextProvider::plans_definitions_as_written

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, FieldRef, Schema};
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference, internal_err};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};

/// A `SEARCH` clause of a recursive `WITH` item.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct CteSearch {
    pub breadth_first: bool,
    pub columns: Vec<String>,
    pub sequence_column: String,
}

/// A `CYCLE` clause of a recursive `WITH` item. The mark values are the
/// constants the query wrote (or `TRUE`/`FALSE` when it wrote none),
/// converted to the mark column's type.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct CteCycle {
    pub columns: Vec<String>,
    pub mark_column: String,
    pub mark_value: Expr,
    pub mark_default: Expr,
    pub path_column: String,
}

/// One item of a `WITH` list: its name and the clauses around its query.
/// An item whose query reads itself has the `UNION` of its non-recursive and
/// recursive terms as its query, each term as written.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct CteItem {
    pub name: String,
    /// The column names written after the item's name; empty when none were.
    pub column_names: Vec<String>,
    /// `Some(true)` for `AS MATERIALIZED`, `Some(false)` for
    /// `AS NOT MATERIALIZED`.
    pub materialized: Option<bool>,
    pub search: Option<CteSearch>,
    pub cycle: Option<CteCycle>,
}

/// A query with its `WITH` list. The items are in the order PostgreSQL keeps
/// them: as written, except that a `WITH RECURSIVE` list puts each item after
/// the items it reads. The inputs are the query, then each item's query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WithQuery {
    /// Whether the list was written `WITH RECURSIVE`.
    pub recursive: bool,
    pub items: Vec<CteItem>,
    inputs: Vec<LogicalPlan>,
}

impl WithQuery {
    /// `query` under the list `items`, each with its query.
    pub fn new(
        recursive: bool,
        items: Vec<(CteItem, LogicalPlan)>,
        query: LogicalPlan,
    ) -> Self {
        let mut inputs = Vec::with_capacity(items.len() + 1);
        inputs.push(query);
        let items = items
            .into_iter()
            .map(|(item, plan)| {
                inputs.push(plan);
                item
            })
            .collect();
        Self {
            recursive,
            items,
            inputs,
        }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// The query that owns the list.
    pub fn query(&self) -> &LogicalPlan {
        &self.inputs[0]
    }

    /// Each item with its query.
    pub fn items_with_plans(&self) -> impl Iterator<Item = (&CteItem, &LogicalPlan)> {
        self.items.iter().zip(&self.inputs[1..])
    }

    fn mark_exprs(&self) -> Vec<Expr> {
        self.items
            .iter()
            .filter_map(|item| item.cycle.as_ref())
            .flat_map(|cycle| [cycle.mark_value.clone(), cycle.mark_default.clone()])
            .collect()
    }
}

impl PartialOrd for WithQuery {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.recursive, &self.items)
            .partial_cmp(&(other.recursive, &other.items))
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for WithQuery {
    fn name(&self) -> &str {
        "WithQuery"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.query().schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.mark_exprs()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let names = self
            .items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>();
        write!(
            f,
            "WithQuery: {}{}",
            if self.recursive { "RECURSIVE " } else { "" },
            names.join(", ")
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != self.inputs.len() {
            return internal_err!(
                "WithQuery has {} inputs, not {}",
                self.inputs.len(),
                inputs.len()
            );
        }
        let mut marks = exprs.into_iter();
        let mut items = self.items.clone();
        for cycle in items.iter_mut().filter_map(|item| item.cycle.as_mut()) {
            let (Some(mark_value), Some(mark_default)) = (marks.next(), marks.next())
            else {
                return internal_err!("WithQuery lost a CYCLE clause's mark values");
            };
            cycle.mark_value = mark_value;
            cycle.mark_default = mark_default;
        }
        Ok(Self {
            recursive: self.recursive,
            items,
            inputs,
        })
    }
}

/// A read of the `WITH` item `name`, whose columns the schema names as the
/// item's columns qualified by the item's name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CteReference {
    pub name: String,
    schema: DFSchemaRef,
}

impl CteReference {
    pub fn new(name: String, schema: DFSchemaRef) -> Self {
        Self { name, schema }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl PartialOrd for CteReference {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.name
            .partial_cmp(&other.name)
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for CteReference {
    fn name(&self) -> &str {
        "CteReference"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CteReference: {}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return internal_err!("CteReference is a leaf without expressions");
        }
        Ok(self.clone())
    }
}

/// A `FROM` item written `input AS alias(column, ...)`. Its schema names the
/// input's columns by the list, the columns past its end by their own names
/// (made distinct from the listed ones), all qualified by the alias.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AliasedRelation {
    pub alias: TableReference,
    input: LogicalPlan,
    schema: DFSchemaRef,
}

impl AliasedRelation {
    /// `input` under `alias`, its columns named `column_names` in order.
    pub fn try_new(
        alias: TableReference,
        input: LogicalPlan,
        column_names: Vec<String>,
    ) -> Result<Self> {
        let schema = renamed_schema(&alias, input.schema(), column_names)?;
        Ok(Self {
            alias,
            input,
            schema,
        })
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// The relation the alias names.
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
}

fn renamed_schema(
    alias: &TableReference,
    input: &DFSchema,
    column_names: Vec<String>,
) -> Result<DFSchemaRef> {
    if column_names.len() != input.fields().len() {
        return internal_err!(
            "a relation of {} columns named by {} names",
            input.fields().len(),
            column_names.len()
        );
    }
    let fields = input
        .fields()
        .iter()
        .zip(column_names)
        .map(|(field, name)| Field::clone(field).with_name(name))
        .collect::<Vec<_>>();
    let schema = DFSchema::try_from_qualified_schema(
        alias.clone(),
        &Schema::new_with_metadata(fields, input.metadata().clone()),
    )?
    .with_functional_dependencies(input.functional_dependencies().clone())?;
    Ok(Arc::new(schema))
}

impl PartialOrd for AliasedRelation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.alias, &self.input)
            .partial_cmp(&(&other.alias, &other.input))
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for AliasedRelation {
    fn name(&self) -> &str {
        "AliasedRelation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let names = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        write!(f, "AliasedRelation: {}({})", self.alias, names.join(", "))
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true, true) =
            (inputs.pop(), inputs.is_empty(), exprs.is_empty())
        else {
            return internal_err!("AliasedRelation has one input and no expressions");
        };
        let column_names = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        Self::try_new(self.alias.clone(), input, column_names)
    }
}

/// One bound table-function call, before its execution expansion as lists.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionRelationCall {
    pub name: String,
    pub args: Vec<Expr>,
    pub column_definitions: Vec<FieldRef>,
}

/// The calls of one function FROM item. The input supplies the analyzed
/// output schema; the calls retain the SQL identities that list expansion
/// and a shared Unnest cannot recover. Calls of ROWS FROM must remain one
/// item so their outputs are zipped and padded, rather than cross joined.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionRelation {
    pub calls: Vec<FunctionRelationCall>,
    pub with_ordinality: bool,
    input: LogicalPlan,
}

impl FunctionRelation {
    pub fn new(
        input: LogicalPlan,
        calls: Vec<FunctionRelationCall>,
        with_ordinality: bool,
    ) -> Self {
        Self {
            calls,
            with_ordinality,
            input,
        }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    fn call_arguments(&self) -> Vec<Expr> {
        self.calls
            .iter()
            .flat_map(|call| call.args.iter().cloned())
            .collect()
    }
}

impl PartialOrd for FunctionRelation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let names = self.calls.iter().map(|call| &call.name).collect::<Vec<_>>();
        let other_names = other.calls.iter().map(|call| &call.name).collect::<Vec<_>>();
        (self.with_ordinality, names, self.call_arguments(), &self.input)
            .partial_cmp(&(
                other.with_ordinality,
                other_names,
                other.call_arguments(),
                &other.input,
            ))
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for FunctionRelation {
    fn name(&self) -> &str {
        "FunctionRelation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.call_arguments()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let names = self.calls.iter().map(|call| call.name.as_str()).collect::<Vec<_>>();
        write!(f, "FunctionRelation: {}", names.join(", "))
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return internal_err!("FunctionRelation has one input");
        };
        let argument_count = self.calls.iter().map(|call| call.args.len()).sum::<usize>();
        if exprs.len() != argument_count {
            return internal_err!(
                "FunctionRelation has {} arguments, not {}",
                argument_count,
                exprs.len()
            );
        }
        let mut calls = self.calls.clone();
        for (argument, expr) in calls.iter_mut().flat_map(|call| &mut call.args).zip(exprs) {
            *argument = expr;
        }
        Ok(Self::new(input, calls, self.with_ordinality))
    }
}

/// The node `plan` is, when it is one of this module's.
pub fn as_definition_node<T: 'static>(plan: &LogicalPlan) -> Option<&T> {
    match plan {
        LogicalPlan::Extension(extension) => {
            let node: &dyn UserDefinedLogicalNode = extension.node.as_ref();
            node.as_any().downcast_ref::<T>()
        }
        _ => None,
    }
}
