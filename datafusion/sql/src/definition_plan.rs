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

//! Plan nodes that keep a query's `WITH` list as the query wrote it.
//!
//! Execution planning inlines a common table expression into every
//! reference to it, which leaves nothing to tell a CTE reference from a
//! derived table and drops a CTE nothing references. A provider that plans a
//! stored definition to print it back as SQL
//! ([`ContextProvider::plans_definitions_as_written`]) gets these nodes
//! instead: [`WithQuery`] over the query that owns the list, and a
//! [`CteReference`] leaf wherever the query reads one of its items. The plan
//! is for reading; nothing executes it.
//!
//! [`ContextProvider::plans_definitions_as_written`]: crate::planner::ContextProvider::plans_definitions_as_written

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result, internal_err};
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
