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

//! SQL:2016 Part 4 (PSM) Planning
//!
//! This module converts PSM (Persistent Stored Modules) AST nodes into
//! DataFusion logical plan representations.

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{not_impl_err, tree_node::TreeNode, DFSchema, Result};
use datafusion_expr::logical_plan::psm::{
    PsmBlock, PsmCase, PsmElseIf, PsmIf, PsmReturn, PsmSetVariable, PsmStatement,
    PsmStatementKind, PsmVariable, PsmWhen, PsmWhile, RegionInfo,
};
use datafusion_expr::Expr;
use sqlparser::ast::{
    self, ConditionalStatements, DeclareAssignment, Ident, ReturnStatementValue, Statement,
};

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Plan PSM body from ConditionalStatements (used by CREATE PROCEDURE).
    ///
    /// ConditionalStatements can be either:
    /// - Sequence { statements } - plain sequence of statements
    /// - BeginEnd(BeginEndStatements) - BEGIN/END block
    pub fn plan_psm_block_from_conditional(
        &self,
        body: &ConditionalStatements,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmBlock> {
        match body {
            ConditionalStatements::BeginEnd(begin_end) => {
                // Delegate to existing BEGIN/END handler
                self.plan_psm_block(begin_end, planner_context)
            }
            ConditionalStatements::Sequence { statements } => {
                // Plan sequence of statements as a block
                let mut planned_statements = Vec::new();
                let mut info = RegionInfo::default();

                for stmt in statements {
                    let planned = self.plan_psm_statement(stmt, planner_context)?;
                    info.merge(&planned.info);
                    planned_statements.push(planned);
                }

                Ok(PsmBlock {
                    label: None,
                    statements: planned_statements,
                    info,
                })
            }
        }
    }
    /// Plan a PSM compound statement (BEGIN/END block).
    pub fn plan_psm_block(
        &self,
        block: &ast::BeginEndStatements,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmBlock> {
        let mut statements = Vec::new();
        let mut info = RegionInfo::default();

        for stmt in &block.statements {
            let planned = self.plan_psm_statement(stmt, planner_context)?;
            info.merge(&planned.info);
            statements.push(planned);
        }

        Ok(PsmBlock {
            label: None, // BeginEndStatements doesn't have a label in sqlparser
            statements,
            info,
        })
    }

    /// Plan a single PSM statement.
    fn plan_psm_statement(
        &self,
        stmt: &Statement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        match stmt {
            // Variable declaration: DECLARE var TYPE [DEFAULT expr]
            Statement::Declare { stmts, .. } => {
                self.plan_psm_declare(stmts, planner_context)
            }

            // SET variable = expression
            Statement::Set(set) => self.plan_psm_set(set, planner_context),

            // RETURN [expression]
            Statement::Return(ret) => self.plan_psm_return(ret, planner_context),

            // IF condition THEN ... [ELSEIF ...] [ELSE ...] END IF
            Statement::If(if_stmt) => self.plan_psm_if(if_stmt, planner_context),

            // WHILE condition DO ... END WHILE
            Statement::While(while_stmt) => {
                self.plan_psm_while(while_stmt, planner_context)
            }

            // CASE ... WHEN ... END CASE
            Statement::Case(case_stmt) => {
                self.plan_psm_case(case_stmt, planner_context)
            }

            // RAISE statement (exception/error signaling)
            Statement::Raise(raise_stmt) => {
                self.plan_psm_raise(raise_stmt, planner_context)
            }

            // Embedded SQL - plan as regular statement
            other => {
                let plan = self.sql_statement_to_plan(other.clone())?;
                let info = RegionInfo::relational();
                Ok(PsmStatement::new(PsmStatementKind::Sql(plan), info))
            }
        }
    }

    /// Plan DECLARE variable statements.
    fn plan_psm_declare(
        &self,
        stmts: &[ast::Declare],
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        // Handle the first declaration (PSM typically has one per DECLARE)
        let decl = stmts.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(
                "DECLARE statement must have at least one declaration".to_string(),
            )
        })?;

        // Check if this is a variable declaration (not cursor, handler, etc.)
        if decl.declare_type.is_some() {
            return not_impl_err!(
                "DECLARE {:?} not yet supported",
                decl.declare_type
            );
        }

        let schema = DFSchema::empty();

        // Get the data type
        let data_type = decl.data_type.as_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(
                "Variable declaration must have a data type".to_string(),
            )
        })?;

        // Convert data type using the SQL planner's convert method
        let field = self.convert_data_type_to_field(data_type)?;
        let arrow_type = field.data_type().clone();

        // Plan default expression if present
        let (default_expr, has_subquery) = if let Some(assignment) = &decl.assignment {
            let expr = match assignment {
                DeclareAssignment::Expr(e) => e.as_ref().clone(),
                DeclareAssignment::Default(e) => e.as_ref().clone(),
                DeclareAssignment::DuckAssignment(e) => e.as_ref().clone(),
                DeclareAssignment::MsSqlAssignment(e) => e.as_ref().clone(),
                DeclareAssignment::For(_) => {
                    return not_impl_err!("FOR assignment in DECLARE not supported")
                }
            };
            let planned = self.sql_to_expr(expr, &schema, planner_context)?;
            let has_subquery = Self::expr_contains_subquery(&planned);
            (Some(planned), has_subquery)
        } else {
            (None, false)
        };

        let info = if has_subquery {
            RegionInfo::scalar_subquery()
        } else {
            RegionInfo::default()
        };

        // Get the variable name(s)
        let names = &decl.names;
        if names.len() != 1 {
            return not_impl_err!(
                "Multiple variable names in single DECLARE not yet supported"
            );
        }

        let var = PsmVariable {
            name: names[0].clone(),
            data_type: arrow_type,
            default: default_expr,
            default_has_subquery: has_subquery,
        };

        Ok(PsmStatement::new(
            PsmStatementKind::DeclareVariable(var),
            info,
        ))
    }

    /// Plan SET statement (variable assignment).
    fn plan_psm_set(
        &self,
        set: &ast::Set,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        match set {
            ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let schema = DFSchema::empty();

                // Convert variable name to identifier
                let targets: Vec<Ident> = variable
                    .0
                    .iter()
                    .filter_map(|part| part.as_ident().cloned())
                    .collect();

                // Get the first value
                let value = values
                    .first()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan(
                        "SET statement must have a value".to_string(),
                    ))?;

                // Plan the value expression
                let value_expr =
                    self.sql_to_expr(value.clone(), &schema, planner_context)?;
                let has_subquery = Self::expr_contains_subquery(&value_expr);

                let info = if has_subquery {
                    RegionInfo::scalar_subquery()
                } else {
                    RegionInfo::default()
                };

                Ok(PsmStatement::new(
                    PsmStatementKind::SetVariable(PsmSetVariable {
                        targets,
                        value: value_expr,
                        has_subquery,
                    }),
                    info,
                ))
            }
            ast::Set::ParenthesizedAssignments { variables, values } => {
                let schema = DFSchema::empty();

                // Convert variable names to identifiers
                let targets: Vec<Ident> = variables
                    .iter()
                    .filter_map(|v| v.0.last().and_then(|p| p.as_ident().cloned()))
                    .collect();

                // For now, only support single value
                let value = values
                    .first()
                    .ok_or_else(|| datafusion_common::DataFusionError::Plan(
                        "SET statement must have a value".to_string(),
                    ))?;

                let value_expr =
                    self.sql_to_expr(value.clone(), &schema, planner_context)?;
                let has_subquery = Self::expr_contains_subquery(&value_expr);

                let info = if has_subquery {
                    RegionInfo::scalar_subquery()
                } else {
                    RegionInfo::default()
                };

                Ok(PsmStatement::new(
                    PsmStatementKind::SetVariable(PsmSetVariable {
                        targets,
                        value: value_expr,
                        has_subquery,
                    }),
                    info,
                ))
            }
            _ => not_impl_err!("Unsupported SET variant"),
        }
    }

    /// Plan RETURN [expression].
    fn plan_psm_return(
        &self,
        ret: &ast::ReturnStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        let schema = DFSchema::empty();

        let (value, has_subquery) = match &ret.value {
            Some(ReturnStatementValue::Expr(expr)) => {
                let planned =
                    self.sql_to_expr(expr.clone(), &schema, planner_context)?;
                let has_subquery = Self::expr_contains_subquery(&planned);
                (Some(planned), has_subquery)
            }
            None => (None, false),
        };

        let mut info = if has_subquery {
            RegionInfo::scalar_subquery()
        } else {
            RegionInfo::default()
        };

        // Mark as pure expression if it's just a simple return
        if value.is_some() && !has_subquery {
            info.is_pure_expression = true;
        }

        Ok(PsmStatement::new(
            PsmStatementKind::Return(PsmReturn { value, has_subquery }),
            info,
        ))
    }

    /// Plan IF statement.
    fn plan_psm_if(
        &self,
        if_stmt: &ast::IfStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        let schema = DFSchema::empty();

        // Plan condition - required for IF
        let condition_expr = if_stmt.if_block.condition.clone().ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(
                "IF statement must have a condition".to_string(),
            )
        })?;
        let condition = self.sql_to_expr(condition_expr, &schema, planner_context)?;
        let condition_has_subquery = Self::expr_contains_subquery(&condition);

        // Plan THEN body
        let mut then_info = RegionInfo::default();
        let then_body: Vec<PsmStatement> = if_stmt
            .if_block
            .conditional_statements
            .statements()
            .iter()
            .map(|s| {
                let stmt = self.plan_psm_statement(s, planner_context)?;
                then_info.merge(&stmt.info);
                Ok(stmt)
            })
            .collect::<Result<Vec<_>>>()?;

        // Plan ELSEIF clauses
        let elseif_clauses: Vec<PsmElseIf> = if_stmt
            .elseif_blocks
            .iter()
            .map(|elseif| {
                let cond_expr = elseif.condition.clone().ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(
                        "ELSEIF clause must have a condition".to_string(),
                    )
                })?;
                let cond = self.sql_to_expr(cond_expr, &schema, planner_context)?;
                let cond_has_subquery = Self::expr_contains_subquery(&cond);

                let mut info = RegionInfo::default();
                let body: Vec<PsmStatement> = elseif
                    .conditional_statements
                    .statements()
                    .iter()
                    .map(|s| {
                        let stmt = self.plan_psm_statement(s, planner_context)?;
                        info.merge(&stmt.info);
                        Ok(stmt)
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(PsmElseIf {
                    condition: cond,
                    condition_has_subquery: cond_has_subquery,
                    body,
                    info,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Plan ELSE body
        let (else_body, else_info) = if let Some(else_block) = &if_stmt.else_block {
            let mut info = RegionInfo::default();
            let body: Vec<PsmStatement> = else_block
                .conditional_statements
                .statements()
                .iter()
                .map(|s| {
                    let stmt = self.plan_psm_statement(s, planner_context)?;
                    info.merge(&stmt.info);
                    Ok(stmt)
                })
                .collect::<Result<Vec<_>>>()?;
            (Some(body), Some(info))
        } else {
            (None, None)
        };

        // Aggregate info for the entire IF statement
        let mut info = RegionInfo::default();
        if condition_has_subquery {
            info.contains_scalar_subquery = true;
        }
        info.merge(&then_info);
        for elseif in &elseif_clauses {
            info.merge(&elseif.info);
            if elseif.condition_has_subquery {
                info.contains_scalar_subquery = true;
            }
        }
        if let Some(ref ei) = else_info {
            info.merge(ei);
        }

        Ok(PsmStatement::new(
            PsmStatementKind::If(PsmIf {
                condition,
                condition_has_subquery,
                then_body,
                then_info,
                elseif_clauses,
                else_body,
                else_info,
            }),
            info,
        ))
    }

    /// Plan WHILE loop.
    fn plan_psm_while(
        &self,
        while_stmt: &ast::WhileStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        let schema = DFSchema::empty();

        // Plan condition - required for WHILE
        let condition_expr =
            while_stmt.condition.clone().ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(
                    "WHILE statement must have a condition".to_string(),
                )
            })?;
        let condition = self.sql_to_expr(condition_expr, &schema, planner_context)?;
        let condition_has_subquery = Self::expr_contains_subquery(&condition);

        let mut body_info = RegionInfo::default();
        let body: Vec<PsmStatement> = while_stmt
            .body
            .statements()
            .iter()
            .map(|s| {
                let stmt = self.plan_psm_statement(s, planner_context)?;
                body_info.merge(&stmt.info);
                Ok(stmt)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut info = body_info.clone();
        if condition_has_subquery {
            info.contains_scalar_subquery = true;
        }

        Ok(PsmStatement::new(
            PsmStatementKind::While(PsmWhile {
                label: None, // WhileStatement doesn't have a label in sqlparser
                condition,
                condition_has_subquery,
                body,
                body_info,
            }),
            info,
        ))
    }

    /// Plan CASE statement.
    fn plan_psm_case(
        &self,
        case_stmt: &ast::CaseStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        let schema = DFSchema::empty();

        // Plan operand if present (simple CASE)
        let (operand, operand_has_subquery) = if let Some(op) = &case_stmt.match_expr {
            let planned = self.sql_to_expr(op.clone(), &schema, planner_context)?;
            let has_subquery = Self::expr_contains_subquery(&planned);
            (Some(planned), has_subquery)
        } else {
            (None, false)
        };

        // Plan WHEN clauses
        let when_clauses: Vec<PsmWhen> = case_stmt
            .when_blocks
            .iter()
            .map(|when| {
                // Plan condition - get the condition from the ConditionalStatementBlock
                let cond_expr = when.condition.clone().ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(
                        "WHEN clause must have a condition".to_string(),
                    )
                })?;
                let cond = self.sql_to_expr(cond_expr, &schema, planner_context)?;
                let cond_has_subquery = Self::expr_contains_subquery(&cond);

                let mut info = RegionInfo::default();
                let body: Vec<PsmStatement> = when
                    .conditional_statements
                    .statements()
                    .iter()
                    .map(|s| {
                        let stmt = self.plan_psm_statement(s, planner_context)?;
                        info.merge(&stmt.info);
                        Ok(stmt)
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(PsmWhen {
                    condition: cond,
                    condition_has_subquery: cond_has_subquery,
                    body,
                    info,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Plan ELSE clause
        let (else_clause, else_info) = if let Some(else_block) = &case_stmt.else_block {
            let mut info = RegionInfo::default();
            let body: Vec<PsmStatement> = else_block
                .conditional_statements
                .statements()
                .iter()
                .map(|s| {
                    let stmt = self.plan_psm_statement(s, planner_context)?;
                    info.merge(&stmt.info);
                    Ok(stmt)
                })
                .collect::<Result<Vec<_>>>()?;
            (Some(body), Some(info))
        } else {
            (None, None)
        };

        // Aggregate info
        let mut info = RegionInfo::default();
        if operand_has_subquery {
            info.contains_scalar_subquery = true;
        }
        for when in &when_clauses {
            info.merge(&when.info);
            if when.condition_has_subquery {
                info.contains_scalar_subquery = true;
            }
        }
        if let Some(ref ei) = else_info {
            info.merge(ei);
        }

        Ok(PsmStatement::new(
            PsmStatementKind::Case(PsmCase {
                operand,
                operand_has_subquery,
                when_clauses,
                else_clause,
                else_info,
            }),
            info,
        ))
    }

    /// Plan RAISE statement (exception signaling).
    /// Maps to PsmSignal for SQL standard SIGNAL semantics.
    fn plan_psm_raise(
        &self,
        raise_stmt: &ast::RaiseStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        use datafusion_expr::logical_plan::psm::PsmSignal;

        let schema = DFSchema::empty();

        // Convert RAISE to a signal-like structure
        let (sqlstate, set_items) = match &raise_stmt.value {
            Some(ast::RaiseStatementValue::Expr(expr)) => {
                // RAISE expr - treat expr as message
                let planned = self.sql_to_expr(expr.clone(), &schema, planner_context)?;
                let message_ident = Ident::new("MESSAGE_TEXT");
                ("45000".to_string(), vec![(message_ident, planned)])
            }
            Some(ast::RaiseStatementValue::UsingMessage(expr)) => {
                // RAISE USING MESSAGE = expr
                let planned = self.sql_to_expr(expr.clone(), &schema, planner_context)?;
                let message_ident = Ident::new("MESSAGE_TEXT");
                ("45000".to_string(), vec![(message_ident, planned)])
            }
            None => {
                // RAISE without value - generic exception
                ("45000".to_string(), vec![])
            }
        };

        Ok(PsmStatement::procedural(PsmStatementKind::Signal(
            PsmSignal { sqlstate, set_items },
        )))
    }

    /// Check if an expression contains a scalar subquery.
    fn expr_contains_subquery(expr: &Expr) -> bool {
        use datafusion_expr::expr::Exists;

        match expr {
            Expr::ScalarSubquery(_) => true,
            Expr::InSubquery(_) => true,
            Expr::Exists(Exists { .. }) => true,
            _ => {
                // Check nested expressions
                let mut contains = false;
                let _ = expr.apply(|e| {
                    if matches!(
                        e,
                        Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)
                    ) {
                        contains = true;
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                });
                contains
            }
        }
    }
}
