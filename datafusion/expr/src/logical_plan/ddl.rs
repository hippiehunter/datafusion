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

use crate::{BoundSqlExpression, Expr, LogicalPlan, SortExpr};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
};

use crate::expr::Sort;
use arrow::datatypes::DataType;
use datafusion_common::{Constraints, DFSchemaRef, DataFusionError, Result, TableReference};

/// Various types of DDL  (CREATE / DROP) catalog manipulation
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum DdlStatement {
    /// Creates an external table.
    CreateExternalTable(CreateExternalTable),
    /// Creates an in memory table.
    CreateMemoryTable(CreateMemoryTable),
    /// Creates a new view.
    CreateView(CreateView),
    /// Creates a new index.
    CreateIndex(CreateIndex),
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
            DdlStatement::CreateIndex(CreateIndex { schema, .. }) => schema,
        }
    }

    /// Return a descriptive string describing the type of this
    /// [`DdlStatement`]
    pub fn name(&self) -> &str {
        match self {
            DdlStatement::CreateExternalTable(_) => "CreateExternalTable",
            DdlStatement::CreateMemoryTable(_) => "CreateMemoryTable",
            DdlStatement::CreateView(_) => "CreateView",
            DdlStatement::CreateIndex(_) => "CreateIndex",
        }
    }

    /// Return all inputs for this plan
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            DdlStatement::CreateExternalTable(_) => vec![],
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. }) => {
                vec![input]
            }
            DdlStatement::CreateView(CreateView { input, .. }) => vec![input],
            DdlStatement::CreateIndex(_) => vec![],
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
                        spec, ..
                    }) => {
                        let CreateMemoryTableSpec {
                            name, constraints, ..
                        } = spec;
                        if constraints.is_empty() {
                            write!(f, "CreateMemoryTable: {name:?}")
                        } else {
                            write!(f, "CreateMemoryTable: {name:?} {constraints}")
                        }
                    }
                    DdlStatement::CreateView(CreateView { spec, .. }) => {
                        let CreateViewSpec { name, .. } = spec;
                        write!(f, "CreateView: {name:?}")
                    }
                    DdlStatement::CreateIndex(CreateIndex { name, .. }) => {
                        write!(f, "CreateIndex: {name:?}")
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

/// Parser-free catalog portion of a `CREATE TABLE` plan.
///
/// The relational input is intentionally not part of this value. Consumers
/// that compile CTAS can move this header into a command or physical factory
/// while carrying the SELECT plan as a separate child. This is also the
/// neutral hand-off point for downstream engines: it contains only DataFusion
/// semantic values and does not reference a downstream crate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateMemoryTableSpec {
    /// The table name
    pub name: TableReference,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
    /// Default values for columns
    pub column_defaults: Vec<(String, Expr)>,
    /// Bound CHECK predicates, in the same order as the CHECK entries in
    /// [`Self::constraints`]. The durable constraint source remains useful
    /// for catalog display, but downstream planners and executors must consume
    /// these semantic expressions instead of parsing that source again.
    pub check_expressions: Vec<BoundSqlExpression>,
    /// Generated-column expressions bound while the CREATE statement's AST
    /// and final table schema are both in scope. Durable SQL spelling remains
    /// catalog metadata, but later planners and executors consume this
    /// semantic form and never reparse that metadata.
    pub generated_expressions: Vec<(String, BoundSqlExpression)>,
    /// Whether the table is `TableType::Temporary`
    pub temporary: bool,
    /// Storage parameters supplied via CREATE TABLE WITH (...)
    pub storage_parameters: BTreeMap<String, String>,
    /// Typed declarative partition key planned against the table schema.
    /// Consumers must not reconstruct this information from SQL display text.
    pub partitioning: Option<CreateTablePartitioning>,
    /// Typed `PARTITION OF` declaration. The parent identity and bound remain
    /// semantic plan data; consumers never reconstruct them from storage
    /// parameter strings or rendered SQL.
    pub partition_of: Option<CreateTablePartitionOf>,
    /// Parent relations named by `INHERITS`, in declaration order.
    pub inherits: Vec<TableReference>,
}

impl CreateMemoryTableSpec {
    /// Every semantic expression owned directly by this DDL header, in the
    /// stable order used by [`Self::with_new_expressions`]. Exposing these to
    /// normal logical-plan expression traversal is important: analyzer rules
    /// must see catalog expressions just as they see predicates and
    /// projections in the relational child.
    pub(crate) fn expressions(&self) -> Vec<&Expr> {
        fn collect_bound<'a>(bound: &'a CreateTablePartitionBound, out: &mut Vec<&'a Expr>) {
            match bound {
                CreateTablePartitionBound::Range { lower, upper } => {
                    for value in lower.iter().chain(upper) {
                        if let CreateTablePartitionBoundValue::Expr(expression) = value {
                            out.push(expression);
                        }
                    }
                }
                CreateTablePartitionBound::List { values } => {
                    out.extend(values.iter().flatten());
                }
                CreateTablePartitionBound::Hash { .. }
                | CreateTablePartitionBound::Default => {}
            }
        }

        let mut expressions = Vec::new();
        expressions.extend(self.column_defaults.iter().map(|(_, expression)| expression));
        expressions.extend(
            self.check_expressions
                .iter()
                .map(BoundSqlExpression::expression),
        );
        expressions.extend(
            self.generated_expressions
                .iter()
                .map(|(_, expression)| expression.expression()),
        );
        if let Some(partitioning) = &self.partitioning {
            expressions.extend(partitioning.keys.iter().map(|key| &key.expr));
        }
        if let Some(partition_of) = &self.partition_of {
            collect_bound(&partition_of.bound, &mut expressions);
        }
        expressions
    }

    /// Rebuild this header with the expressions returned by
    /// [`Self::expressions`]. This keeps analyzer rewrites inside the semantic
    /// plan rather than forcing a later catalog consumer to rebind SQL text.
    pub(crate) fn with_new_expressions(&self, expressions: Vec<Expr>) -> Result<Self> {
        fn missing() -> DataFusionError {
            DataFusionError::Internal(
                "CreateMemoryTableSpec expression rewrite supplied too few expressions"
                    .to_string(),
            )
        }

        fn rewrite_bound(
            bound: &mut CreateTablePartitionBound,
            expressions: &mut impl Iterator<Item = Expr>,
        ) -> Result<()> {
            match bound {
                CreateTablePartitionBound::Range { lower, upper } => {
                    for value in lower.iter_mut().chain(upper) {
                        if let CreateTablePartitionBoundValue::Expr(expression) = value {
                            *expression = expressions.next().ok_or_else(missing)?;
                        }
                    }
                }
                CreateTablePartitionBound::List { values } => {
                    for expression in values.iter_mut().flatten() {
                        *expression = expressions.next().ok_or_else(missing)?;
                    }
                }
                CreateTablePartitionBound::Hash { .. }
                | CreateTablePartitionBound::Default => {}
            }
            Ok(())
        }

        let mut rewritten = self.clone();
        let mut expressions = expressions.into_iter();
        for (_, expression) in &mut rewritten.column_defaults {
            *expression = expressions.next().ok_or_else(missing)?;
        }
        for expression in &mut rewritten.check_expressions {
            *expression = BoundSqlExpression::new(expressions.next().ok_or_else(missing)?);
        }
        for (_, expression) in &mut rewritten.generated_expressions {
            *expression = BoundSqlExpression::new(expressions.next().ok_or_else(missing)?);
        }
        if let Some(partitioning) = &mut rewritten.partitioning {
            for key in &mut partitioning.keys {
                key.expr = expressions.next().ok_or_else(missing)?;
            }
        }
        if let Some(partition_of) = &mut rewritten.partition_of {
            rewrite_bound(&mut partition_of.bound, &mut expressions)?;
        }
        if expressions.next().is_some() {
            return Err(DataFusionError::Internal(
                "CreateMemoryTableSpec expression rewrite supplied too many expressions"
                    .to_string(),
            ));
        }
        Ok(rewritten)
    }
}

/// Creates an in memory table.
///
/// SQL lowering returns the catalog header and relational input together as a
/// `LogicalPlan` node so ordinary DataFusion tree transforms remain usable.
/// Runtime consumers should split it with [`CreateMemoryTable::into_parts`]
/// at their logical-plan boundary rather than retaining this wrapper.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateMemoryTable {
    /// Parser-free catalog command header.
    pub spec: CreateMemoryTableSpec,
    /// Relational child. `EmptyRelation` represents a CREATE without AS.
    pub input: Arc<LogicalPlan>,
}

impl CreateMemoryTable {
    pub fn new(spec: CreateMemoryTableSpec, input: Arc<LogicalPlan>) -> Self {
        Self { spec, input }
    }

    pub fn into_parts(self) -> (CreateMemoryTableSpec, Arc<LogicalPlan>) {
        (self.spec, self.input)
    }
}

impl Deref for CreateMemoryTable {
    type Target = CreateMemoryTableSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl DerefMut for CreateMemoryTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateTablePartitionOf {
    pub parent: TableReference,
    pub bound: CreateTablePartitionBound,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum CreateTablePartitionBound {
    Range {
        lower: Vec<CreateTablePartitionBoundValue>,
        upper: Vec<CreateTablePartitionBoundValue>,
    },
    /// Each entry is one partition key row. A scalar list item has one
    /// expression; a tuple item has one expression per partition key column.
    List {
        values: Vec<Vec<Expr>>,
    },
    Hash {
        modulus: u64,
        remainder: u64,
    },
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum CreateTablePartitionBoundValue {
    MinValue,
    MaxValue,
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateTablePartitioning {
    pub strategy: CreateTablePartitioningStrategy,
    pub keys: Vec<CreateTablePartitionKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateTablePartitioningStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateTablePartitionKey {
    /// `Some` only for a plain column key. Expression keys retain the planned
    /// expression in `expr` and deliberately have no surrogate column.
    pub column_name: Option<String>,
    pub expr: Expr,
    pub result_type: DataType,
    pub opclass: Option<String>,
    pub collation: Option<String>,
}

/// Creates a view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum CreateViewCheckOption {
    #[default]
    None,
    Local,
    Cascaded,
}

/// Why a defining query cannot be represented as an automatically updatable
/// single-relation view. The SQL crate derives this while parser syntax is in
/// scope; downstream catalog and DML code retain only this shallow semantic
/// result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CreateViewNotUpdatable {
    UnreadableDefinition,
    WithClause,
    SetOperation,
    Distinct,
    Grouping,
    WindowFunction,
    LimitOffset,
    Join,
    NotARelation,
    NoUpdatableColumn,
}

/// One output column of an automatically updatable view.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateViewColumn {
    /// Catalog-visible output name.
    pub name: String,
    /// Direct source column accepting writes, or `None` for a computed output.
    pub write_source: Option<String>,
    /// The value shown by the view, bound against the immediately underlying
    /// relation. Keeping this for computed columns lets stacked-view filters
    /// substitute their real expression instead of treating them as NULL.
    pub read_expression: BoundSqlExpression,
}

/// Parser-free meaning of the part of a view definition needed by automatic
/// DML retargeting and information-schema updatability reporting.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum CreateViewUpdatability {
    Updatable {
        source: TableReference,
        columns: Vec<CreateViewColumn>,
        restriction: Option<BoundSqlExpression>,
    },
    NotUpdatable(CreateViewNotUpdatable),
}

/// Parser-free catalog portion of a `CREATE VIEW` plan.
///
/// The defining query is deliberately excluded. SQL lowering can pair this
/// neutral header with a relational input, while downstream consumers split
/// the two at their logical-plan boundary and retain only the semantic facts
/// they need from that input.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct CreateViewSpec {
    /// The table name
    pub name: TableReference,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// Option to not error if view already exists (IF NOT EXISTS clause)
    pub if_not_exists: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
    /// SQL for the defining query only, without the surrounding `CREATE VIEW`
    /// statement. Catalog consumers need this representation, but must not
    /// recover it later by reparsing `definition`.
    pub query_definition: Option<String>,
    /// Whether the view is ephemeral
    pub temporary: bool,
    /// Semantic scope of `WITH CHECK OPTION`, lowered at the SQL boundary.
    pub check_option: CreateViewCheckOption,
    /// Automatic-update analysis captured while the defining query's AST and
    /// the full catalog/type/function provider are still available.
    pub updatability: CreateViewUpdatability,
}

/// Creates a view.
///
/// This wrapper exists inside DataFusion's logical-plan layer so ordinary tree
/// transforms can still visit the defining query. Runtime consumers should
/// split it with [`CreateView::into_parts`] rather than treating DDL as a leaf
/// that secretly retains a relational tree.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct CreateView {
    /// Parser-free catalog command header.
    pub spec: CreateViewSpec,
    /// Defining relational query.
    pub input: Arc<LogicalPlan>,
}

impl CreateView {
    pub fn new(spec: CreateViewSpec, input: Arc<LogicalPlan>) -> Self {
        Self { spec, input }
    }

    pub fn into_parts(self) -> (CreateViewSpec, Arc<LogicalPlan>) {
        (self.spec, self.input)
    }
}

impl Deref for CreateView {
    type Target = CreateViewSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl DerefMut for CreateView {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
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
