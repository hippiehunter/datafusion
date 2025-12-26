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

use crate::logical_plan::psm::{ProcedureArg, PsmBlock};
use crate::{Expr, LogicalPlan, SortExpr, Volatility};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::expr::Sort;
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNodeContainer, TreeNodeRecursion};
use datafusion_common::{
    Constraints, DFSchema, DFSchemaRef, Result, SchemaReference, TableReference,
};
pub use sqlparser::ast::{AlterTable, CreateDomain, DropDomain};
use sqlparser::ast::{Ident, ObjectName};

static DDL_EMPTY_SCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| Arc::new(DFSchema::empty()));

/// Various types of DDL  (CREATE / DROP) catalog manipulation
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum DdlStatement {
    /// Creates an external table.
    CreateExternalTable(CreateExternalTable),
    /// Creates an in memory table.
    CreateMemoryTable(CreateMemoryTable),
    /// Creates a new view.
    CreateView(CreateView),
    /// Creates a new catalog schema.
    CreateCatalogSchema(CreateCatalogSchema),
    /// Creates a new catalog (aka "Database").
    CreateCatalog(CreateCatalog),
    /// Creates a new index.
    CreateIndex(CreateIndex),
    /// Drops a table.
    DropTable(DropTable),
    /// Drops a view.
    DropView(DropView),
    /// Drops a catalog schema
    DropCatalogSchema(DropCatalogSchema),
    /// Create function statement
    CreateFunction(CreateFunction),
    /// Drop function statement
    DropFunction(DropFunction),
    /// ALTER TABLE
    AlterTable(AlterTable),
    /// CREATE DOMAIN
    CreateDomain(CreateDomain),
    /// DROP DOMAIN
    DropDomain(DropDomain),
    /// DROP SEQUENCE
    DropSequence(DropSequence),
    /// CREATE PROCEDURE (SQL:2016 Part 4 - PSM)
    CreateProcedure(CreateProcedure),
    /// DROP PROCEDURE (SQL:2016 Part 4 - PSM)
    DropProcedure(DropProcedure),
    /// CREATE ROLE
    CreateRole(CreateRole),
    /// DROP ROLE
    DropRole(DropRole),
}

impl DdlStatement {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            DdlStatement::CreateExternalTable(CreateExternalTable { schema, .. }) => {
                schema
            }
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. })
            | DdlStatement::CreateView(CreateView { input, .. }) => input.schema(),
            DdlStatement::CreateCatalogSchema(CreateCatalogSchema { schema, .. }) => {
                schema
            }
            DdlStatement::CreateCatalog(CreateCatalog { schema, .. }) => schema,
            DdlStatement::CreateIndex(CreateIndex { schema, .. }) => schema,
            DdlStatement::DropTable(DropTable { schema, .. }) => schema,
            DdlStatement::DropView(DropView { schema, .. }) => schema,
            DdlStatement::DropCatalogSchema(DropCatalogSchema { schema, .. }) => schema,
            DdlStatement::CreateFunction(CreateFunction { schema, .. }) => schema,
            DdlStatement::DropFunction(DropFunction { schema, .. }) => schema,
            DdlStatement::AlterTable(_)
            | DdlStatement::CreateDomain(_)
            | DdlStatement::DropDomain(_)
            | DdlStatement::DropSequence(_)
            | DdlStatement::CreateProcedure(_)
            | DdlStatement::DropProcedure(_)
            | DdlStatement::CreateRole(_)
            | DdlStatement::DropRole(_) => &DDL_EMPTY_SCHEMA,
        }
    }

    /// Return a descriptive string describing the type of this
    /// [`DdlStatement`]
    pub fn name(&self) -> &str {
        match self {
            DdlStatement::CreateExternalTable(_) => "CreateExternalTable",
            DdlStatement::CreateMemoryTable(_) => "CreateMemoryTable",
            DdlStatement::CreateView(_) => "CreateView",
            DdlStatement::CreateCatalogSchema(_) => "CreateCatalogSchema",
            DdlStatement::CreateCatalog(_) => "CreateCatalog",
            DdlStatement::CreateIndex(_) => "CreateIndex",
            DdlStatement::DropTable(_) => "DropTable",
            DdlStatement::DropView(_) => "DropView",
            DdlStatement::DropCatalogSchema(_) => "DropCatalogSchema",
            DdlStatement::CreateFunction(_) => "CreateFunction",
            DdlStatement::DropFunction(_) => "DropFunction",
            DdlStatement::AlterTable(_) => "AlterTable",
            DdlStatement::CreateDomain(_) => "CreateDomain",
            DdlStatement::DropDomain(_) => "DropDomain",
            DdlStatement::DropSequence(_) => "DropSequence",
            DdlStatement::CreateProcedure(_) => "CreateProcedure",
            DdlStatement::DropProcedure(_) => "DropProcedure",
            DdlStatement::CreateRole(_) => "CreateRole",
            DdlStatement::DropRole(_) => "DropRole",
        }
    }

    /// Return all inputs for this plan
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            DdlStatement::CreateExternalTable(_) => vec![],
            DdlStatement::CreateCatalogSchema(_) => vec![],
            DdlStatement::CreateCatalog(_) => vec![],
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. }) => {
                vec![input]
            }
            DdlStatement::CreateView(CreateView { input, .. }) => vec![input],
            DdlStatement::CreateIndex(_) => vec![],
            DdlStatement::DropTable(_) => vec![],
            DdlStatement::DropView(_) => vec![],
            DdlStatement::DropCatalogSchema(_) => vec![],
            DdlStatement::CreateFunction(_) => vec![],
            DdlStatement::DropFunction(_) => vec![],
            DdlStatement::AlterTable(_) => vec![],
            DdlStatement::CreateDomain(_) => vec![],
            DdlStatement::DropDomain(_) => vec![],
            DdlStatement::DropSequence(_) => vec![],
            DdlStatement::CreateProcedure(_) => vec![],
            DdlStatement::DropProcedure(_) => vec![],
            DdlStatement::CreateRole(_) => vec![],
            DdlStatement::DropRole(_) => vec![],
        }
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    ///
    /// See [crate::LogicalPlan::display] for an example
    pub fn display(&self) -> impl Display + '_ {
        struct Wrapper<'a>(&'a DdlStatement);
        impl Display for Wrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    DdlStatement::CreateExternalTable(CreateExternalTable {
                        name,
                        constraints,
                        ..
                    }) => {
                        if constraints.is_empty() {
                            write!(f, "CreateExternalTable: {name:?}")
                        } else {
                            write!(f, "CreateExternalTable: {name:?} {constraints}")
                        }
                    }
                    DdlStatement::CreateMemoryTable(CreateMemoryTable {
                        name,
                        constraints,
                        ..
                    }) => {
                        if constraints.is_empty() {
                            write!(f, "CreateMemoryTable: {name:?}")
                        } else {
                            write!(f, "CreateMemoryTable: {name:?} {constraints}")
                        }
                    }
                    DdlStatement::CreateView(CreateView { name, .. }) => {
                        write!(f, "CreateView: {name:?}")
                    }
                    DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
                        schema_name,
                        ..
                    }) => {
                        write!(f, "CreateCatalogSchema: {schema_name:?}")
                    }
                    DdlStatement::CreateCatalog(CreateCatalog {
                        catalog_name, ..
                    }) => {
                        write!(f, "CreateCatalog: {catalog_name:?}")
                    }
                    DdlStatement::CreateIndex(CreateIndex { name, .. }) => {
                        write!(f, "CreateIndex: {name:?}")
                    }
                    DdlStatement::DropTable(DropTable {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropTable: {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::DropView(DropView {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropView: {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::DropCatalogSchema(DropCatalogSchema {
                        name,
                        if_exists,
                        cascade,
                        ..
                    }) => {
                        write!(
                            f,
                            "DropCatalogSchema: {name:?} if not exist:={if_exists} cascade:={cascade}"
                        )
                    }
                    DdlStatement::CreateFunction(CreateFunction { name, .. }) => {
                        write!(f, "CreateFunction: name {name:?}")
                    }
                    DdlStatement::DropFunction(DropFunction { name, .. }) => {
                        write!(f, "DropFunction: name {name:?}")
                    }
                    DdlStatement::AlterTable(alter_table) => {
                        write!(f, "AlterTable: {alter_table}")
                    }
                    DdlStatement::CreateDomain(create_domain) => {
                        write!(f, "CreateDomain: {create_domain}")
                    }
                    DdlStatement::DropDomain(DropDomain {
                        if_exists,
                        name,
                        drop_behavior,
                    }) => {
                        write!(
                            f,
                            "DropDomain: {name:?} if not exist:={if_exists} drop_behavior:={drop_behavior:?}"
                        )
                    }
                    DdlStatement::DropSequence(DropSequence { name, if_exists, .. }) => {
                        write!(
                            f,
                            "DropSequence: {name:?} if not exist:={if_exists}"
                        )
                    }
                    DdlStatement::CreateProcedure(CreateProcedure { name, .. }) => {
                        write!(f, "CreateProcedure: name {name:?}")
                    }
                    DdlStatement::DropProcedure(DropProcedure { name, if_exists, .. }) => {
                        write!(f, "DropProcedure: name {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::CreateRole(CreateRole {
                        name,
                        if_not_exists,
                    }) => {
                        write!(
                            f,
                            "CreateRole: {name:?} if not exist:={if_not_exists}"
                        )
                    }
                    DdlStatement::DropRole(DropRole {
                        name,
                        if_exists,
                        cascade,
                    }) => {
                        write!(
                            f,
                            "DropRole: {name:?} if not exist:={if_exists} cascade:={cascade}"
                        )
                    }
                }
            }
        }
        Wrapper(self)
    }
}

/// Creates an external table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: TableReference,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: String,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
    /// Whether the table is a temporary table
    pub temporary: bool,
    /// SQL used to create the table, if available
    pub definition: Option<String>,
    /// Order expressions supplied by user
    pub order_exprs: Vec<Vec<Sort>>,
    /// Whether the table is an infinite streams
    pub unbounded: bool,
    /// Table(provider) specific options
    pub options: HashMap<String, String>,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
    /// Default values for columns
    pub column_defaults: HashMap<String, Expr>,
}

impl CreateExternalTable {
    /// Creates a builder for [`CreateExternalTable`] with required fields.
    ///
    /// # Arguments
    /// * `name` - The table name
    /// * `location` - The physical location of the table files
    /// * `file_type` - The file type (e.g., "parquet", "csv", "json")
    /// * `schema` - The table schema
    ///
    /// # Example
    /// ```
    /// # use datafusion_expr::CreateExternalTable;
    /// # use datafusion_common::{DFSchema, TableReference};
    /// # use std::sync::Arc;
    /// let table = CreateExternalTable::builder(
    ///     TableReference::bare("my_table"),
    ///     "/path/to/data",
    ///     "parquet",
    ///     Arc::new(DFSchema::empty())
    /// ).build();
    /// ```
    pub fn builder(
        name: impl Into<TableReference>,
        location: impl Into<String>,
        file_type: impl Into<String>,
        schema: DFSchemaRef,
    ) -> CreateExternalTableBuilder {
        CreateExternalTableBuilder {
            name: name.into(),
            location: location.into(),
            file_type: file_type.into(),
            schema,
            table_partition_cols: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Default::default(),
            column_defaults: HashMap::new(),
        }
    }
}

/// Builder for [`CreateExternalTable`] that provides a fluent API for construction.
///
/// Created via [`CreateExternalTable::builder`].
#[derive(Debug, Clone)]
pub struct CreateExternalTableBuilder {
    name: TableReference,
    location: String,
    file_type: String,
    schema: DFSchemaRef,
    table_partition_cols: Vec<String>,
    if_not_exists: bool,
    or_replace: bool,
    temporary: bool,
    definition: Option<String>,
    order_exprs: Vec<Vec<Sort>>,
    unbounded: bool,
    options: HashMap<String, String>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
}

impl CreateExternalTableBuilder {
    /// Set the partition columns
    pub fn with_partition_cols(mut self, cols: Vec<String>) -> Self {
        self.table_partition_cols = cols;
        self
    }

    /// Set the if_not_exists flag
    pub fn with_if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    /// Set the or_replace flag
    pub fn with_or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    /// Set the temporary flag
    pub fn with_temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    /// Set the SQL definition
    pub fn with_definition(mut self, definition: Option<String>) -> Self {
        self.definition = definition;
        self
    }

    /// Set the order expressions
    pub fn with_order_exprs(mut self, order_exprs: Vec<Vec<Sort>>) -> Self {
        self.order_exprs = order_exprs;
        self
    }

    /// Set the unbounded flag
    pub fn with_unbounded(mut self, unbounded: bool) -> Self {
        self.unbounded = unbounded;
        self
    }

    /// Set the table options
    pub fn with_options(mut self, options: HashMap<String, String>) -> Self {
        self.options = options;
        self
    }

    /// Set the table constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Set the column defaults
    pub fn with_column_defaults(
        mut self,
        column_defaults: HashMap<String, Expr>,
    ) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    /// Build the [`CreateExternalTable`]
    pub fn build(self) -> CreateExternalTable {
        CreateExternalTable {
            schema: self.schema,
            name: self.name,
            location: self.location,
            file_type: self.file_type,
            table_partition_cols: self.table_partition_cols,
            if_not_exists: self.if_not_exists,
            or_replace: self.or_replace,
            temporary: self.temporary,
            definition: self.definition,
            order_exprs: self.order_exprs,
            unbounded: self.unbounded,
            options: self.options,
            constraints: self.constraints,
            column_defaults: self.column_defaults,
        }
    }
}

// Hashing refers to a subset of fields considered in PartialEq.
impl Hash for CreateExternalTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
        self.location.hash(state);
        self.file_type.hash(state);
        self.table_partition_cols.hash(state);
        self.if_not_exists.hash(state);
        self.definition.hash(state);
        self.order_exprs.hash(state);
        self.unbounded.hash(state);
        self.options.len().hash(state); // HashMap is not hashable
    }
}

// Manual implementation needed because of `schema`, `options`, and `column_defaults` fields.
// Comparison excludes these fields.
impl PartialOrd for CreateExternalTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableCreateExternalTable<'a> {
            /// The table name
            pub name: &'a TableReference,
            /// The physical location
            pub location: &'a String,
            /// The file type of physical file
            pub file_type: &'a String,
            /// Partition Columns
            pub table_partition_cols: &'a Vec<String>,
            /// Option to not error if table already exists
            pub if_not_exists: &'a bool,
            /// SQL used to create the table, if available
            pub definition: &'a Option<String>,
            /// Order expressions supplied by user
            pub order_exprs: &'a Vec<Vec<Sort>>,
            /// Whether the table is an infinite streams
            pub unbounded: &'a bool,
            /// The list of constraints in the schema, such as primary key, unique, etc.
            pub constraints: &'a Constraints,
        }
        let comparable_self = ComparableCreateExternalTable {
            name: &self.name,
            location: &self.location,
            file_type: &self.file_type,
            table_partition_cols: &self.table_partition_cols,
            if_not_exists: &self.if_not_exists,
            definition: &self.definition,
            order_exprs: &self.order_exprs,
            unbounded: &self.unbounded,
            constraints: &self.constraints,
        };
        let comparable_other = ComparableCreateExternalTable {
            name: &other.name,
            location: &other.location,
            file_type: &other.file_type,
            table_partition_cols: &other.table_partition_cols,
            if_not_exists: &other.if_not_exists,
            definition: &other.definition,
            order_exprs: &other.order_exprs,
            unbounded: &other.unbounded,
            constraints: &other.constraints,
        };
        comparable_self
            .partial_cmp(&comparable_other)
            // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Creates an in memory table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateMemoryTable {
    /// The table name
    pub name: TableReference,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
    /// Default values for columns
    pub column_defaults: Vec<(String, Expr)>,
    /// Whether the table is `TableType::Temporary`
    pub temporary: bool,
    /// Storage parameters supplied via CREATE TABLE WITH (...)
    pub storage_parameters: BTreeMap<String, String>,
}

/// Creates a view.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct CreateView {
    /// The table name
    pub name: TableReference,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
    /// Whether the view is ephemeral
    pub temporary: bool,
}

/// Creates a catalog (aka "Database").
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalog {
    /// The catalog name
    pub catalog_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateCatalog {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.catalog_name.partial_cmp(&other.catalog_name) {
            Some(Ordering::Equal) => self.if_not_exists.partial_cmp(&other.if_not_exists),
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Creates a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalogSchema {
    /// The table schema
    pub schema_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateCatalogSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.schema_name.partial_cmp(&other.schema_name) {
            Some(Ordering::Equal) => self.if_not_exists.partial_cmp(&other.if_not_exists),
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Drops a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropTable {
    /// The table name
    pub name: TableReference,
    /// If the table exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DropTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.if_exists.partial_cmp(&other.if_exists),
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Drops a view.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropView {
    /// The view name
    pub name: TableReference,
    /// If the view exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DropView {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.if_exists.partial_cmp(&other.if_exists),
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Drops a sequence.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct DropSequence {
    /// The sequence name
    pub name: ObjectName,
    /// If the sequence exists
    pub if_exists: bool,
    /// Whether drop should cascade
    pub cascade: bool,
    /// Whether drop should restrict
    pub restrict: bool,
    /// Whether drop should purge
    pub purge: bool,
    /// Whether drop should use TEMPORARY
    pub temporary: bool,
    /// Optional table qualifier (dialect-specific)
    pub table: Option<ObjectName>,
}

/// Drops a schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropCatalogSchema {
    /// The schema name
    pub name: SchemaReference,
    /// If the schema exists
    pub if_exists: bool,
    /// Whether drop should cascade
    pub cascade: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for DropCatalogSchema {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => match self.if_exists.partial_cmp(&other.if_exists) {
                Some(Ordering::Equal) => self.cascade.partial_cmp(&other.cascade),
                cmp => cmp,
            },
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Arguments passed to the `CREATE FUNCTION` statement
///
/// These statements are turned into executable functions using [`FunctionFactory`]
///
/// # Notes
///
/// This structure purposely mirrors the structure in sqlparser's
/// [`sqlparser::ast::Statement::CreateFunction`], but does not use it directly
/// to avoid a dependency on sqlparser in the core crate.
///
///
/// [`FunctionFactory`]: https://docs.rs/datafusion/latest/datafusion/execution/context/trait.FunctionFactory.html
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CreateFunction {
    pub or_replace: bool,
    pub temporary: bool,
    pub name: String,
    pub args: Option<Vec<OperateFunctionArg>>,
    pub return_type: Option<DataType>,
    pub params: CreateFunctionBody,
    /// PSM body (BEGIN/END block) for SQL:2016 procedural functions.
    /// Mutually exclusive with `params.function_body`.
    pub psm_body: Option<PsmBlock>,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateFunction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableCreateFunction<'a> {
            pub or_replace: &'a bool,
            pub temporary: &'a bool,
            pub name: &'a String,
            pub args: &'a Option<Vec<OperateFunctionArg>>,
            pub return_type: &'a Option<DataType>,
            pub params: &'a CreateFunctionBody,
        }
        let comparable_self = ComparableCreateFunction {
            or_replace: &self.or_replace,
            temporary: &self.temporary,
            name: &self.name,
            args: &self.args,
            return_type: &self.return_type,
            params: &self.params,
        };
        let comparable_other = ComparableCreateFunction {
            or_replace: &other.or_replace,
            temporary: &other.temporary,
            name: &other.name,
            args: &other.args,
            return_type: &other.return_type,
            params: &other.params,
        };
        comparable_self
            .partial_cmp(&comparable_other)
            // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// Part of the `CREATE FUNCTION` statement
///
/// See [`CreateFunction`] for details
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct OperateFunctionArg {
    // TODO: figure out how to support mode
    // pub mode: Option<ArgMode>,
    pub name: Option<Ident>,
    pub data_type: DataType,
    pub default_expr: Option<Expr>,
}

impl<'a> TreeNodeContainer<'a, Expr> for OperateFunctionArg {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.default_expr.apply_elements(f)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.default_expr.map_elements(f)?.map_data(|default_expr| {
            Ok(Self {
                default_expr,
                ..self
            })
        })
    }
}

/// Part of the `CREATE FUNCTION` statement
///
/// See [`CreateFunction`] for details
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct CreateFunctionBody {
    /// LANGUAGE lang_name
    pub language: Option<Ident>,
    /// IMMUTABLE | STABLE | VOLATILE
    pub behavior: Option<Volatility>,
    /// RETURN or AS function body
    pub function_body: Option<Expr>,
}

impl<'a> TreeNodeContainer<'a, Expr> for CreateFunctionBody {
    fn apply_elements<F: FnMut(&'a Expr) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.function_body.apply_elements(f)
    }

    fn map_elements<F: FnMut(Expr) -> Result<Transformed<Expr>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.function_body
            .map_elements(f)?
            .map_data(|function_body| {
                Ok(Self {
                    function_body,
                    ..self
                })
            })
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct DropFunction {
    pub name: String,
    pub if_exists: bool,
    pub schema: DFSchemaRef,
}

impl PartialOrd for DropFunction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.if_exists.partial_cmp(&other.if_exists),
            cmp => cmp,
        }
        // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// CREATE PROCEDURE statement (SQL:2016 Part 4 - PSM).
///
/// Procedures differ from functions in that:
/// - They do not have a return type (but may have OUT/INOUT parameters)
/// - They are invoked with CALL, not in expressions
/// - They may modify database state via DML
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CreateProcedure {
    /// Whether to replace an existing procedure with the same name.
    pub or_replace: bool,
    /// The procedure name.
    pub name: String,
    /// The procedure arguments (may include IN, OUT, INOUT parameters).
    pub args: Option<Vec<ProcedureArg>>,
    /// The procedure body as a PSM block.
    pub body: PsmBlock,
}

// Manual implementation needed because PsmBlock doesn't implement PartialOrd.
// Comparison is based on name and or_replace only.
impl PartialOrd for CreateProcedure {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(Ordering::Equal) => self.or_replace.partial_cmp(&other.or_replace),
            cmp => cmp,
        }
        .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

/// DROP PROCEDURE statement (SQL:2016 Part 4 - PSM).
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct DropProcedure {
    /// The procedure name.
    pub name: String,
    /// IF EXISTS clause.
    pub if_exists: bool,
}

/// CREATE ROLE statement.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct CreateRole {
    /// The role name.
    pub name: String,
    /// IF NOT EXISTS clause.
    pub if_not_exists: bool,
}

/// DROP ROLE statement.
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub struct DropRole {
    /// The role name.
    pub name: String,
    /// IF EXISTS clause.
    pub if_exists: bool,
    /// CASCADE option.
    pub cascade: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CreateIndex {
    pub name: Option<String>,
    pub table: TableReference,
    pub using: Option<String>,
    pub columns: Vec<SortExpr>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub schema: DFSchemaRef,
}

// Manual implementation needed because of `schema` field. Comparison excludes this field.
impl PartialOrd for CreateIndex {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        #[derive(PartialEq, PartialOrd)]
        struct ComparableCreateIndex<'a> {
            pub name: &'a Option<String>,
            pub table: &'a TableReference,
            pub using: &'a Option<String>,
            pub columns: &'a Vec<SortExpr>,
            pub unique: &'a bool,
            pub if_not_exists: &'a bool,
        }
        let comparable_self = ComparableCreateIndex {
            name: &self.name,
            table: &self.table,
            using: &self.using,
            columns: &self.columns,
            unique: &self.unique,
            if_not_exists: &self.if_not_exists,
        };
        let comparable_other = ComparableCreateIndex {
            name: &other.name,
            table: &other.table,
            using: &other.using,
            columns: &other.columns,
            unique: &other.unique,
            if_not_exists: &other.if_not_exists,
        };
        comparable_self
            .partial_cmp(&comparable_other)
            // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

#[cfg(test)]
mod test {
    use crate::{CreateCatalog, DdlStatement, DropView};
    use datafusion_common::{DFSchema, DFSchemaRef, TableReference};
    use std::cmp::Ordering;

    #[test]
    fn test_partial_ord() {
        let catalog = DdlStatement::CreateCatalog(CreateCatalog {
            catalog_name: "name".to_string(),
            if_not_exists: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });
        let catalog_2 = DdlStatement::CreateCatalog(CreateCatalog {
            catalog_name: "name".to_string(),
            if_not_exists: true,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        assert_eq!(catalog.partial_cmp(&catalog_2), Some(Ordering::Less));

        let drop_view = DdlStatement::DropView(DropView {
            name: TableReference::from("table"),
            if_exists: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        assert_eq!(drop_view.partial_cmp(&catalog), Some(Ordering::Greater));
    }
}
