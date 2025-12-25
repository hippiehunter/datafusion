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

//! SQL:2016 Part 4 (PSM) - Persistent Stored Modules
//!
//! This module provides logical plan representations for procedural SQL constructs
//! including stored functions and procedures with compound statements (BEGIN/END blocks),
//! control flow (IF, WHILE, LOOP, etc.), and exception handling (SIGNAL, HANDLER).
//!
//! # Design for Optimization
//!
//! The representation includes [`RegionInfo`] metadata on each statement and block
//! to support UDF optimization techniques like "outlining" as described in the
//! [CMU PRISM paper](https://www.vldb.org/pvldb/vol18/p1-arch.pdf).
//!
//! Key insight: Regions that do NOT contain relational operations (SELECT, INSERT, etc.)
//! can be "outlined" (extracted to opaque functions hidden from the query optimizer),
//! while regions WITH relational operations should remain visible for query optimization.
//!
//! # Planning Only
//!
//! This module provides planning infrastructure only. Execution and function/procedure
//! registration are downstream responsibilities.

use crate::{Expr, LogicalPlan};
use arrow::datatypes::DataType;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};

#[cfg(feature = "sql")]
use sqlparser::ast::Ident;

#[cfg(not(feature = "sql"))]
use crate::expr::Ident;

// ============================================================================
// Region Metadata for Optimization
// ============================================================================

/// Metadata for UDF optimization (outlining vs inlining decisions).
///
/// Per the [CMU PRISM paper](https://www.vldb.org/pvldb/vol18/p1-arch.pdf),
/// regions without relational operations can be "outlined" (extracted to opaque
/// functions), while regions with SELECT statements should remain visible for
/// query optimization.
///
/// This metadata is computed bottom-up during planning and aggregated at each
/// level of the PSM statement hierarchy.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub struct RegionInfo {
    /// True if this region contains SELECT, INSERT, UPDATE, DELETE, MERGE, or other DML/DDL.
    pub contains_relational: bool,

    /// True if any expression contains a scalar subquery (hidden relational op).
    pub contains_scalar_subquery: bool,

    /// Count of relational operations in this region (for cost-based decisions).
    pub relational_op_count: u32,

    /// True if this is a pure expression (e.g., RETURN expr with no side effects).
    /// Enables "subquery elision" optimization.
    pub is_pure_expression: bool,
}

impl RegionInfo {
    /// Creates a new RegionInfo indicating a relational operation.
    pub fn relational() -> Self {
        Self {
            contains_relational: true,
            relational_op_count: 1,
            ..Default::default()
        }
    }

    /// Creates a new RegionInfo indicating a scalar subquery.
    pub fn scalar_subquery() -> Self {
        Self {
            contains_scalar_subquery: true,
            ..Default::default()
        }
    }

    /// Creates a new RegionInfo for a pure expression.
    pub fn pure_expression() -> Self {
        Self {
            is_pure_expression: true,
            ..Default::default()
        }
    }

    /// Can this region be outlined (hidden from query optimizer)?
    ///
    /// Returns true if the region contains no relational operations and no
    /// scalar subqueries. Such regions can be compiled to opaque functions
    /// that the query optimizer treats as black boxes.
    pub fn can_outline(&self) -> bool {
        !self.contains_relational && !self.contains_scalar_subquery
    }

    /// Merge info from a child region into this region.
    ///
    /// Used during bottom-up aggregation of region metadata.
    pub fn merge(&mut self, child: &RegionInfo) {
        self.contains_relational |= child.contains_relational;
        self.contains_scalar_subquery |= child.contains_scalar_subquery;
        self.relational_op_count += child.relational_op_count;
        // is_pure_expression is not merged - it's only set on leaf expressions
    }
}

impl Display for RegionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.contains_relational {
            parts.push(format!("relational_ops={}", self.relational_op_count));
        }
        if self.contains_scalar_subquery {
            parts.push("has_subquery".to_string());
        }
        if self.is_pure_expression {
            parts.push("pure".to_string());
        }
        if parts.is_empty() {
            write!(f, "procedural")
        } else {
            write!(f, "{}", parts.join(", "))
        }
    }
}

// ============================================================================
// PSM Statement Types
// ============================================================================

/// A procedural statement in a stored function/procedure body.
///
/// Each statement carries [`RegionInfo`] metadata for optimization decisions.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PsmStatement {
    /// The kind of statement.
    pub kind: PsmStatementKind,
    /// Optimization metadata for this statement.
    pub info: RegionInfo,
}

impl PsmStatement {
    /// Creates a new PSM statement with the given kind and computed region info.
    pub fn new(kind: PsmStatementKind, info: RegionInfo) -> Self {
        Self { kind, info }
    }

    /// Creates a new PSM statement with default (procedural-only) region info.
    pub fn procedural(kind: PsmStatementKind) -> Self {
        Self {
            kind,
            info: RegionInfo::default(),
        }
    }
}

impl Display for PsmStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

/// The kind of PSM statement.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum PsmStatementKind {
    /// Compound statement: BEGIN ... END
    Block(PsmBlock),

    /// IF condition THEN ... [ELSEIF ...] [ELSE ...] END IF
    If(PsmIf),

    /// WHILE condition DO ... END WHILE
    While(PsmWhile),

    /// REPEAT ... UNTIL condition END REPEAT
    Repeat(PsmRepeat),

    /// LOOP ... END LOOP (with LEAVE/ITERATE)
    Loop(PsmLoop),

    /// FOR cursor AS (SELECT ...) DO ... END FOR
    For(PsmFor),

    /// Procedural CASE statement
    Case(PsmCase),

    /// RETURN [expression]
    Return(PsmReturn),

    /// DECLARE variable TYPE [DEFAULT value]
    DeclareVariable(PsmVariable),

    /// SET variable = expression
    SetVariable(PsmSetVariable),

    /// SELECT ... INTO variable (always relational)
    SelectInto(PsmSelectInto),

    /// Embedded SQL statement (DML/DDL - always relational)
    Sql(LogicalPlan),

    /// DECLARE HANDLER
    DeclareHandler(PsmHandler),

    /// SIGNAL SQLSTATE
    Signal(PsmSignal),

    /// RESIGNAL
    Resignal(PsmResignal),

    /// LEAVE label
    Leave(Ident),

    /// ITERATE label
    Iterate(Ident),
}

impl Display for PsmStatementKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PsmStatementKind::Block(block) => write!(f, "{}", block),
            PsmStatementKind::If(if_stmt) => write!(f, "{}", if_stmt),
            PsmStatementKind::While(while_stmt) => write!(f, "{}", while_stmt),
            PsmStatementKind::Repeat(repeat) => write!(f, "{}", repeat),
            PsmStatementKind::Loop(loop_stmt) => write!(f, "{}", loop_stmt),
            PsmStatementKind::For(for_stmt) => write!(f, "{}", for_stmt),
            PsmStatementKind::Case(case) => write!(f, "{}", case),
            PsmStatementKind::Return(ret) => write!(f, "{}", ret),
            PsmStatementKind::DeclareVariable(var) => write!(f, "{}", var),
            PsmStatementKind::SetVariable(set) => write!(f, "{}", set),
            PsmStatementKind::SelectInto(sel) => write!(f, "{}", sel),
            PsmStatementKind::Sql(plan) => write!(f, "SQL: {}", plan.display()),
            PsmStatementKind::DeclareHandler(handler) => write!(f, "{}", handler),
            PsmStatementKind::Signal(signal) => write!(f, "{}", signal),
            PsmStatementKind::Resignal(resignal) => write!(f, "{}", resignal),
            PsmStatementKind::Leave(label) => write!(f, "LEAVE {}", label),
            PsmStatementKind::Iterate(label) => write!(f, "ITERATE {}", label),
        }
    }
}

// ============================================================================
// Compound Statement (Block)
// ============================================================================

/// Compound statement block (BEGIN ... END) with optimization metadata.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmBlock {
    /// Optional label for the block (for LEAVE/ITERATE).
    pub label: Option<Ident>,
    /// Statements in the block.
    pub statements: Vec<PsmStatement>,
    /// Aggregated region info from all statements.
    pub info: RegionInfo,
}

impl PsmBlock {
    /// Creates a new block with statements and computed region info.
    pub fn new(
        label: Option<Ident>,
        statements: Vec<PsmStatement>,
    ) -> Self {
        let mut info = RegionInfo::default();
        for stmt in &statements {
            info.merge(&stmt.info);
        }
        Self {
            label,
            statements,
            info,
        }
    }
}

impl Hash for PsmBlock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.statements.hash(state);
        self.info.hash(state);
    }
}

impl Display for PsmBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(label) = &self.label {
            write!(f, "{}: ", label)?;
        }
        write!(f, "BEGIN ")?;
        for (i, stmt) in self.statements.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{}", stmt)?;
        }
        write!(f, " END")
    }
}

// ============================================================================
// Control Flow Statements
// ============================================================================

/// IF statement with per-branch region info.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmIf {
    /// The condition expression.
    pub condition: Expr,
    /// True if the condition contains a scalar subquery.
    pub condition_has_subquery: bool,
    /// The THEN branch statements.
    pub then_body: Vec<PsmStatement>,
    /// Region info for the THEN branch.
    pub then_info: RegionInfo,
    /// Optional ELSEIF clauses.
    pub elseif_clauses: Vec<PsmElseIf>,
    /// Optional ELSE branch statements.
    pub else_body: Option<Vec<PsmStatement>>,
    /// Region info for the ELSE branch.
    pub else_info: Option<RegionInfo>,
}

impl Hash for PsmIf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.condition.hash(state);
        self.condition_has_subquery.hash(state);
        self.then_body.hash(state);
        self.then_info.hash(state);
        self.elseif_clauses.hash(state);
        self.else_body.hash(state);
        self.else_info.hash(state);
    }
}

impl Display for PsmIf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IF {} THEN ...", self.condition)?;
        if !self.elseif_clauses.is_empty() {
            write!(f, " ELSEIF ...")?;
        }
        if self.else_body.is_some() {
            write!(f, " ELSE ...")?;
        }
        write!(f, " END IF")
    }
}

/// ELSEIF clause.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmElseIf {
    /// The condition expression.
    pub condition: Expr,
    /// True if the condition contains a scalar subquery.
    pub condition_has_subquery: bool,
    /// The body statements.
    pub body: Vec<PsmStatement>,
    /// Region info for this branch.
    pub info: RegionInfo,
}

impl Hash for PsmElseIf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.condition.hash(state);
        self.condition_has_subquery.hash(state);
        self.body.hash(state);
        self.info.hash(state);
    }
}

/// WHILE loop.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmWhile {
    /// Optional label for the loop.
    pub label: Option<Ident>,
    /// The condition expression.
    pub condition: Expr,
    /// True if the condition contains a scalar subquery.
    pub condition_has_subquery: bool,
    /// The loop body statements.
    pub body: Vec<PsmStatement>,
    /// Region info for the body.
    pub body_info: RegionInfo,
}

impl Hash for PsmWhile {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.condition.hash(state);
        self.condition_has_subquery.hash(state);
        self.body.hash(state);
        self.body_info.hash(state);
    }
}

impl Display for PsmWhile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(label) = &self.label {
            write!(f, "{}: ", label)?;
        }
        write!(f, "WHILE {} DO ... END WHILE", self.condition)
    }
}

/// REPEAT loop.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmRepeat {
    /// Optional label for the loop.
    pub label: Option<Ident>,
    /// The loop body statements.
    pub body: Vec<PsmStatement>,
    /// Region info for the body.
    pub body_info: RegionInfo,
    /// The UNTIL condition expression.
    pub until_condition: Expr,
    /// True if the condition contains a scalar subquery.
    pub condition_has_subquery: bool,
}

impl Hash for PsmRepeat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.body.hash(state);
        self.body_info.hash(state);
        self.until_condition.hash(state);
        self.condition_has_subquery.hash(state);
    }
}

impl Display for PsmRepeat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(label) = &self.label {
            write!(f, "{}: ", label)?;
        }
        write!(f, "REPEAT ... UNTIL {} END REPEAT", self.until_condition)
    }
}

/// Simple LOOP statement.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmLoop {
    /// Optional label for the loop.
    pub label: Option<Ident>,
    /// The loop body statements.
    pub body: Vec<PsmStatement>,
    /// Region info for the body.
    pub body_info: RegionInfo,
}

impl Hash for PsmLoop {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.body.hash(state);
        self.body_info.hash(state);
    }
}

impl Display for PsmLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(label) = &self.label {
            write!(f, "{}: ", label)?;
        }
        write!(f, "LOOP ... END LOOP")
    }
}

/// FOR cursor loop (always contains relational - the query).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmFor {
    /// Optional label for the loop.
    pub label: Option<Ident>,
    /// The cursor name.
    pub cursor_name: Ident,
    /// The query providing rows for the cursor.
    pub query: Box<LogicalPlan>,
    /// The loop body statements.
    pub body: Vec<PsmStatement>,
    /// Region info for the body.
    pub body_info: RegionInfo,
}

impl Hash for PsmFor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label.hash(state);
        self.cursor_name.hash(state);
        self.query.hash(state);
        self.body.hash(state);
        self.body_info.hash(state);
    }
}

impl Display for PsmFor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(label) = &self.label {
            write!(f, "{}: ", label)?;
        }
        write!(f, "FOR {} AS (...) DO ... END FOR", self.cursor_name)
    }
}

/// Procedural CASE statement.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmCase {
    /// Optional operand for simple CASE.
    pub operand: Option<Expr>,
    /// True if the operand contains a scalar subquery.
    pub operand_has_subquery: bool,
    /// The WHEN clauses.
    pub when_clauses: Vec<PsmWhen>,
    /// Optional ELSE clause.
    pub else_clause: Option<Vec<PsmStatement>>,
    /// Region info for the ELSE clause.
    pub else_info: Option<RegionInfo>,
}

impl Hash for PsmCase {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operand.hash(state);
        self.operand_has_subquery.hash(state);
        self.when_clauses.hash(state);
        self.else_clause.hash(state);
        self.else_info.hash(state);
    }
}

impl Display for PsmCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE")?;
        if let Some(operand) = &self.operand {
            write!(f, " {}", operand)?;
        }
        write!(f, " WHEN ... END CASE")
    }
}

/// WHEN clause in a procedural CASE statement.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmWhen {
    /// The condition expression.
    pub condition: Expr,
    /// True if the condition contains a scalar subquery.
    pub condition_has_subquery: bool,
    /// The body statements.
    pub body: Vec<PsmStatement>,
    /// Region info for this branch.
    pub info: RegionInfo,
}

impl Hash for PsmWhen {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.condition.hash(state);
        self.condition_has_subquery.hash(state);
        self.body.hash(state);
        self.info.hash(state);
    }
}

// ============================================================================
// Variable and Assignment Statements
// ============================================================================

/// Variable declaration (DECLARE variable TYPE [DEFAULT value]).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmVariable {
    /// The variable name.
    pub name: Ident,
    /// The variable's data type.
    pub data_type: DataType,
    /// Optional default value.
    pub default: Option<Expr>,
    /// True if the default expression contains a scalar subquery.
    pub default_has_subquery: bool,
}

impl Hash for PsmVariable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.default.hash(state);
        self.default_has_subquery.hash(state);
    }
}

impl Display for PsmVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DECLARE {} {}", self.name, self.data_type)?;
        if let Some(default) = &self.default {
            write!(f, " DEFAULT {}", default)?;
        }
        Ok(())
    }
}

/// RETURN statement.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmReturn {
    /// Optional return value.
    pub value: Option<Expr>,
    /// True if the value contains a scalar subquery.
    pub has_subquery: bool,
}

impl Hash for PsmReturn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.has_subquery.hash(state);
    }
}

impl Display for PsmReturn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RETURN")?;
        if let Some(value) = &self.value {
            write!(f, " {}", value)?;
        }
        Ok(())
    }
}

/// SET variable = expression.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmSetVariable {
    /// The target variable(s).
    pub targets: Vec<Ident>,
    /// The value expression.
    pub value: Expr,
    /// True if the value contains a scalar subquery.
    pub has_subquery: bool,
}

impl Hash for PsmSetVariable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.targets.hash(state);
        self.value.hash(state);
        self.has_subquery.hash(state);
    }
}

impl Display for PsmSetVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SET ")?;
        for (i, target) in self.targets.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", target)?;
        }
        write!(f, " = {}", self.value)
    }
}

/// SELECT ... INTO variable (inherently relational).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmSelectInto {
    /// The query providing the values.
    pub query: Box<LogicalPlan>,
    /// The target variables.
    pub targets: Vec<Ident>,
}

impl Hash for PsmSelectInto {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.query.hash(state);
        self.targets.hash(state);
    }
}

impl Display for PsmSelectInto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ... INTO ")?;
        for (i, target) in self.targets.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", target)?;
        }
        Ok(())
    }
}

// ============================================================================
// Exception Handling
// ============================================================================

/// Exception handler (DECLARE HANDLER).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmHandler {
    /// The handler type (CONTINUE or EXIT).
    pub handler_type: HandlerType,
    /// The condition that triggers this handler.
    pub condition: HandlerCondition,
    /// The handler action statement.
    pub statement: Box<PsmStatement>,
}

impl Hash for PsmHandler {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handler_type.hash(state);
        self.condition.hash(state);
        self.statement.hash(state);
    }
}

impl Display for PsmHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DECLARE {} HANDLER FOR {} {}",
            self.handler_type, self.condition, self.statement
        )
    }
}

/// Handler type.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum HandlerType {
    /// Continue execution after handling.
    Continue,
    /// Exit the block after handling.
    Exit,
}

impl Display for HandlerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerType::Continue => write!(f, "CONTINUE"),
            HandlerType::Exit => write!(f, "EXIT"),
        }
    }
}

/// Condition that triggers a handler.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum HandlerCondition {
    /// Specific SQLSTATE value.
    SqlState(String),
    /// SQLEXCEPTION condition.
    SqlException,
    /// NOT FOUND condition.
    NotFound,
    /// SQLWARNING condition.
    SqlWarning,
    /// Named condition.
    ConditionName(Ident),
}

impl Display for HandlerCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerCondition::SqlState(state) => write!(f, "SQLSTATE '{}'", state),
            HandlerCondition::SqlException => write!(f, "SQLEXCEPTION"),
            HandlerCondition::NotFound => write!(f, "NOT FOUND"),
            HandlerCondition::SqlWarning => write!(f, "SQLWARNING"),
            HandlerCondition::ConditionName(name) => write!(f, "{}", name),
        }
    }
}

/// SIGNAL statement.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmSignal {
    /// The SQLSTATE value to signal.
    pub sqlstate: String,
    /// Optional SET items (MESSAGE_TEXT, etc.).
    pub set_items: Vec<(Ident, Expr)>,
}

impl Hash for PsmSignal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sqlstate.hash(state);
        self.set_items.hash(state);
    }
}

impl Display for PsmSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SIGNAL SQLSTATE '{}'", self.sqlstate)?;
        if !self.set_items.is_empty() {
            write!(f, " SET ...")?;
        }
        Ok(())
    }
}

/// RESIGNAL statement.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PsmResignal {
    /// Optional SQLSTATE value (re-raise current if None).
    pub sqlstate: Option<String>,
    /// Optional SET items (MESSAGE_TEXT, etc.).
    pub set_items: Vec<(Ident, Expr)>,
}

impl Hash for PsmResignal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sqlstate.hash(state);
        self.set_items.hash(state);
    }
}

impl Display for PsmResignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RESIGNAL")?;
        if let Some(state) = &self.sqlstate {
            write!(f, " SQLSTATE '{}'", state)?;
        }
        if !self.set_items.is_empty() {
            write!(f, " SET ...")?;
        }
        Ok(())
    }
}

// ============================================================================
// Procedure Parameters
// ============================================================================

/// Parameter mode for procedures (IN, OUT, INOUT).
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash, Debug)]
pub enum ParameterMode {
    /// Input parameter (default).
    In,
    /// Output parameter.
    Out,
    /// Input/output parameter.
    InOut,
}

impl Display for ParameterMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParameterMode::In => write!(f, "IN"),
            ParameterMode::Out => write!(f, "OUT"),
            ParameterMode::InOut => write!(f, "INOUT"),
        }
    }
}

/// Procedure argument definition.
#[derive(Clone, PartialEq, Eq, PartialOrd, Debug)]
pub struct ProcedureArg {
    /// The parameter mode.
    pub mode: ParameterMode,
    /// Optional parameter name.
    pub name: Option<Ident>,
    /// The parameter data type.
    pub data_type: DataType,
    /// Optional default value.
    pub default: Option<Expr>,
}

impl Hash for ProcedureArg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.mode.hash(state);
        self.name.hash(state);
        self.data_type.hash(state);
        self.default.hash(state);
    }
}

impl Display for ProcedureArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.mode)?;
        if let Some(name) = &self.name {
            write!(f, " {}", name)?;
        }
        write!(f, " {}", self.data_type)?;
        if let Some(default) = &self.default {
            write!(f, " DEFAULT {}", default)?;
        }
        Ok(())
    }
}
