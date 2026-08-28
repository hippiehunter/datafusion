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

pub mod builder;
mod ddl;
pub mod display;
pub mod dml;
mod extension;
pub(crate) mod invariants;
mod merge;
pub mod psm;
pub use invariants::{InvariantLevel, assert_expected_schema, check_subquery_expr};
mod plan;
mod statement;
pub mod tree_node;

pub use builder::{
    LogicalPlanBuilder, LogicalPlanBuilderOptions, LogicalTableSource, UNNAMED_TABLE,
    build_join_schema, requalify_sides_if_needed, table_scan, union,
    wrap_projection_for_join_if_necessary,
};
pub use ddl::{
    // SQL/MED (Management of External Data) types
    AlterForeignDataWrapperOperation,
    AlterForeignDataWrapperStatement,
    AlterForeignTableOperation,
    AlterForeignTableStatement,
    AlterMaterializedView,
    AlterMaterializedViewOperation,
    AlterSequence,
    AlterServerOperation,
    AlterServerStatement,
    AlterTable,
    AlterUserMappingStatement,
    CreateAssertion,
    CreateCatalog,
    CreateCatalogSchema,
    CreateDomain,
    CreateExternalTable,
    CreateForeignDataWrapperStatement,
    CreateForeignTableStatement,
    CreateFunction,
    CreateFunctionBody,
    CreateIndex,
    CreateMaterializedView,
    CreateMemoryTable,
    CreateTablePartitionBound,
    CreateTablePartitionBoundValue,
    CreateTablePartitionKey,
    CreateTablePartitionOf,
    CreateTablePartitioning,
    CreateTablePartitioningStrategy,
    CreateProcedure,
    CreatePropertyGraph,
    CreateRole,
    CreateSequence,
    CreateServerOption,
    CreateServerStatement,
    CreateUserMappingStatement,
    CreateView,
    DdlStatement,
    DropAssertion,
    DropBehavior,
    DropCatalogSchema,
    DropDomain,
    DropForeignDataWrapperStatement,
    DropForeignTableStatement,
    DropFunction,
    DropIndex,
    DropMaterializedView,
    DropProcedure,
    DropPropertyGraph,
    DropRole,
    DropSequence,
    DropServerStatement,
    DropTable,
    DropUserMappingStatement,
    DropView,
    GraphEdgeEndpoint,
    GraphEdgeTableDefinition,
    GraphKeyClause,
    GraphPropertiesClause,
    GraphPropertyDefinition,
    GraphVertexTableDefinition,
    ImportForeignSchemaLimitType,
    ImportForeignSchemaStatement,
    MaterializedViewRefreshMethod,
    OperateFunctionArg,
    RefreshMaterializedView,
    SequenceOptions,
    UserMappingUser,
};
pub use dml::{
    ConflictAssignment, ConflictTarget, DmlStatement, DoUpdateAction, InsertOp,
    OnConflict, OnConflictAction, WriteOp,
};
pub use merge::{
    Merge, MergeAction, MergeAssignment, MergeClause, MergeInsertExpr, MergeInsertKind,
    MergeUpdateExpr,
};
pub use plan::{
    AfterMatchSkipOption, Aggregate, Analyze, ColumnUnnestList, DescribeTable, Distinct,
    DistinctOn, EdgeDirection, EdgePattern, EmptyMatchesMode, EmptyRelation, Explain,
    ExplainOption, Extension, FetchType, Filter, GraphColumn, GraphPattern,
    GraphPatternElement, GraphPatternExpr, GraphTable, Join, JoinConstraint, JoinType,
    JsonTable, JsonTableColumnDef, JsonTableErrorHandling, LabelExpression, Limit,
    LogicalPlan, MatchRecognize, MeasureExpr, NodePattern, Partitioning, PathFinding,
    PathMode, Pattern, PatternSymbol, PlanType, Projection, RecursiveQuery, Repartition,
    RepetitionQuantifier, RowLimiting, RowsPerMatchOption, SkipType, Sort,
    StringifiedPlan, Subquery, SubqueryAlias, SubsetDef, SymbolDef, TableScan,
    TableScanRowLock, TableScanRowLockMode, TableScanRowLockWaitPolicy,
    ToStringifiedPlan, Union, Unnest, Values, Window, projection_schema,
};
pub use psm::{
    HandlerCondition, HandlerType, ParameterMode, ProcedureArg, PsmBlock, PsmCase,
    PsmElseIf, PsmFor, PsmHandler, PsmIf, PsmLoop, PsmRepeat, PsmResignal, PsmReturn,
    PsmSelectInto, PsmSetVariable, PsmSignal, PsmStatement, PsmStatementKind,
    PsmVariable, PsmWhen, PsmWhile, RegionInfo,
};
pub use statement::{
    AnalyzeTable, Call, Deallocate, Execute, Grant, GrantRole, Prepare, ReleaseSavepoint,
    ResetVariable, Revoke, RevokeRole, RollbackToSavepoint, Savepoint, SetTransaction,
    SetVariable, Statement, TransactionAccessMode, TransactionConclusion, TransactionEnd,
    TransactionIsolationLevel, TransactionStart, TruncateTable, UseDatabase, Vacuum,
};

pub use datafusion_common::format::ExplainFormat;

pub use display::display_schema;

pub use extension::{UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
