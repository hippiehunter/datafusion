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
//! [`AliasedRelation`] over a `FROM` item with a column alias list, a
//! [`FunctionRelation`] over table functions before their expansion as lists
//! and unnesting loses their SQL identities, and a [`SampledRelation`] over
//! a table read under `TABLESAMPLE`, where execution planning keeps only the
//! provider's sampling predicate, and a [`WindowClause`] under the windows of
//! a query level that writes a WINDOW clause, where execution planning keeps
//! only the windows its calls run over. A set-returning function called in an
//! expression is a [`SetReturningCall`], where execution planning expands it
//! into the lists an `Unnest` walks. The plan is for reading; nothing
//! executes it.
//!
//! [`ContextProvider::plans_definitions_as_written`]: crate::planner::ContextProvider::plans_definitions_as_written

use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::{DFSchema, DFSchemaRef, Result, TableReference, internal_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    ColumnarValue, Expr, Extension, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, SortExpr, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore, Volatility, WindowFrame,
};

use crate::planner::TableSampleMethod;

/// Metadata on a definition-only window expression: `between` or `short`.
pub const WINDOW_FRAME_SYNTAX_METADATA_KEY: &str = "datafusion.window_frame_syntax";

/// The explicit empty GROUP BY clause, which has the same grouping effect
/// as an ungrouped aggregate but must remain visible in a stored definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct EmptyGrouping {
    input: LogicalPlan,
}
impl EmptyGrouping {
    pub fn new(input: LogicalPlan) -> Self {
        Self { input }
    }
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}
impl UserDefinedLogicalNodeCore for EmptyGrouping {
    fn name(&self) -> &str {
        "EmptyGrouping"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EmptyGrouping")
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return internal_err!("EmptyGrouping requires no expressions and one input");
        }
        Ok(Self::new(inputs.remove(0)))
    }
}

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
        let other_names = other
            .calls
            .iter()
            .map(|call| &call.name)
            .collect::<Vec<_>>();
        (
            self.with_ordinality,
            names,
            self.call_arguments(),
            &self.input,
        )
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
        let names = self
            .calls
            .iter()
            .map(|call| call.name.as_str())
            .collect::<Vec<_>>();
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
        for (argument, expr) in
            calls.iter_mut().flat_map(|call| &mut call.args).zip(exprs)
        {
            *argument = expr;
        }
        Ok(Self::new(input, calls, self.with_ordinality))
    }
}

/// A table read under `TABLESAMPLE method (percentage) [REPEATABLE (seed)]`:
/// the scan of the table, the method, and the argument and seed as the query
/// wrote them. Its rows are the scan's, so its schema is the scan's.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SampledRelation {
    pub method: TableSampleMethod,
    pub percentage: Expr,
    pub repeatable: Option<Expr>,
    input: LogicalPlan,
}

impl SampledRelation {
    pub fn new(
        input: LogicalPlan,
        method: TableSampleMethod,
        percentage: Expr,
        repeatable: Option<Expr>,
    ) -> Self {
        Self {
            method,
            percentage,
            repeatable,
            input,
        }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// The scan of the sampled table.
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }
}

impl PartialOrd for SampledRelation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.method, &self.percentage, &self.repeatable, &self.input)
            .partial_cmp(&(
                other.method,
                &other.percentage,
                &other.repeatable,
                &other.input,
            ))
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for SampledRelation {
    fn name(&self) -> &str {
        "SampledRelation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![self.percentage.clone()];
        exprs.extend(self.repeatable.clone());
        exprs
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SampledRelation: {:?}", self.method)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return internal_err!("SampledRelation has one input");
        };
        let expected = 1 + usize::from(self.repeatable.is_some());
        if exprs.len() != expected {
            return internal_err!(
                "SampledRelation has {} expressions, not {}",
                expected,
                exprs.len()
            );
        }
        let mut exprs = exprs.into_iter();
        let (Some(percentage), repeatable) = (exprs.next(), exprs.next()) else {
            return internal_err!("SampledRelation lost its percentage");
        };
        Ok(Self::new(input, self.method, percentage, repeatable))
    }
}

/// One window of a query level's WINDOW clause, as the clause defines it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct WindowDefinition {
    /// The window's name.
    pub name: String,
    /// The earlier window `name AS (base ...)` copies.
    pub base: Option<String>,
    /// The PARTITION BY keys: the base window's when the definition copies
    /// one, which may not write its own.
    pub partition_by: Vec<Expr>,
    /// The ORDER BY keys, the base window's when `order_copied`.
    pub order_by: Vec<SortExpr>,
    /// Whether the ORDER BY is the one the base window wrote, which a
    /// definition that writes none copies.
    pub order_copied: bool,
    /// The frame clause the definition wrote, `None` where it wrote none.
    pub frame: Option<WindowFrame>,
    /// Whether the written frame used BETWEEN rather than the short form.
    pub frame_between: bool,
}

/// A query level's WINDOW clause over the rows its windows read: the rows
/// after grouping and HAVING. Execution planning keeps only the windows a
/// call runs over, folded into each call; this keeps every window the
/// clause defines, used or not, and what each wrote. Its rows are its
/// input's.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowClause {
    pub windows: Vec<WindowDefinition>,
    input: LogicalPlan,
}

impl WindowClause {
    pub fn new(input: LogicalPlan, windows: Vec<WindowDefinition>) -> Self {
        Self { windows, input }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    /// The rows the windows read.
    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// The window the clause names `name`.
    pub fn window(&self, name: &str) -> Option<&WindowDefinition> {
        self.windows.iter().find(|window| window.name == name)
    }
}

impl PartialOrd for WindowClause {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.windows, &self.input)
            .partial_cmp(&(&other.windows, &other.input))
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for WindowClause {
    fn name(&self) -> &str {
        "WindowClause"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    /// Each window's PARTITION BY keys, then its ORDER BY keys.
    fn expressions(&self) -> Vec<Expr> {
        self.windows
            .iter()
            .flat_map(|window| {
                window
                    .partition_by
                    .iter()
                    .cloned()
                    .chain(window.order_by.iter().map(|sort| sort.expr.clone()))
            })
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let names = self
            .windows
            .iter()
            .map(|window| window.name.as_str())
            .collect::<Vec<_>>();
        write!(f, "WindowClause: {}", names.join(", "))
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let (Some(input), true) = (inputs.pop(), inputs.is_empty()) else {
            return internal_err!("WindowClause has one input");
        };
        let expected = self
            .windows
            .iter()
            .map(|window| window.partition_by.len() + window.order_by.len())
            .sum::<usize>();
        if exprs.len() != expected {
            return internal_err!(
                "WindowClause has {} expressions, not {}",
                expected,
                exprs.len()
            );
        }
        let mut exprs = exprs.into_iter();
        let windows = self
            .windows
            .iter()
            .map(|window| WindowDefinition {
                partition_by: exprs.by_ref().take(window.partition_by.len()).collect(),
                order_by: window
                    .order_by
                    .iter()
                    .zip(exprs.by_ref())
                    .map(|(sort, expr)| sort.with_expr(expr))
                    .collect(),
                ..window.clone()
            })
            .collect();
        Ok(Self::new(input, windows))
    }
}

/// A call of the set-returning function `name` in an expression, as the
/// query wrote it: the function's name and its arguments before the
/// provider expands the call into lists, typed as one row of the function's
/// result. Its arguments take no coercion here; they are the call's as
/// written. It is never evaluated.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetReturningCall {
    name: String,
    row: FieldRef,
    signature: Signature,
}

impl SetReturningCall {
    /// The call of `name` whose rows are each a value of `row`.
    pub fn new(name: String, row: FieldRef) -> Self {
        Self {
            name,
            row,
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }

    /// The call over `args`.
    pub fn call(self, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ScalarUDF::new_from_impl(self)),
            args,
        ))
    }

    /// The written call `function` is, when it is one.
    pub fn of(function: &ScalarFunction) -> Option<&Self> {
        function.func.inner().as_any().downcast_ref::<Self>()
    }
}

impl ScalarUDFImpl for SetReturningCall {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.row.data_type().clone())
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(self.row.as_ref().clone().with_name(&self.name)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "the set-returning call {} of a plan planned as written is not evaluated",
            self.name
        )
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
