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

use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{not_impl_err, tree_node::TreeNode, Result, ScalarValue};
use datafusion_expr::logical_plan::psm::{
    HandlerCondition, HandlerType, PsmBlock, PsmCase, PsmElseIf, PsmHandler, PsmIf,
    PsmReturn, PsmSetVariable, PsmStatement, PsmStatementKind, PsmVariable, PsmWhen,
    PsmWhile, RegionInfo,
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
            Statement::Set(set) => self.plan_psm_set(&set.inner, planner_context),

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

            // SIGNAL statement (SQL standard exception signaling)
            Statement::Signal(signal_stmt) => {
                self.plan_psm_signal(signal_stmt, planner_context)
            }

            // RESIGNAL statement (SQL standard exception re-signaling)
            Statement::Resignal(resignal_stmt) => {
                self.plan_psm_resignal(resignal_stmt, planner_context)
            }

            // LOOP ... END LOOP
            Statement::Loop(loop_stmt) => self.plan_psm_loop(loop_stmt, planner_context),

            // REPEAT ... UNTIL condition END REPEAT
            Statement::Repeat(repeat_stmt) => {
                self.plan_psm_repeat(repeat_stmt, planner_context)
            }

            // LEAVE label (exit loop)
            Statement::Leave(leave_stmt) => {
                let label = leave_stmt.label.clone().unwrap_or_else(|| Ident::new(""));
                Ok(PsmStatement::procedural(PsmStatementKind::Leave(label)))
            }

            // ITERATE label (continue loop)
            Statement::Iterate(iterate_stmt) => {
                let label = iterate_stmt.label.clone().unwrap_or_else(|| Ident::new(""));
                Ok(PsmStatement::procedural(PsmStatementKind::Iterate(label)))
            }

            // Labeled block: label: BEGIN ... END
            Statement::LabeledBlock(labeled) => {
                self.plan_psm_labeled_block(labeled, planner_context)
            }

            // Embedded SQL - plan as regular statement
            other => {
                // Merge PSM schema into outer query schema so PSM variables/parameters
                // are visible when resolving column references in the SQL statement
                let psm_schema = planner_context.psm_schema();

                // Build a new outer schema that includes PSM variables
                let new_outer = if !psm_schema.fields().is_empty() {
                    match planner_context.outer_query_schema() {
                        Some(outer) => {
                            let mut merged = psm_schema.as_ref().clone();
                            merged.merge(outer);
                            Some(Arc::new(merged))
                        }
                        None => Some(psm_schema),
                    }
                } else {
                    None
                };

                // Set the new outer schema if we created one
                let old_outer = if let Some(new) = new_outer {
                    planner_context.set_outer_query_schema(Some(new))
                } else {
                    None
                };

                let plan = self.sql_statement_to_plan_with_context(other.clone(), planner_context)?;

                // Restore the old outer query schema
                planner_context.set_outer_query_schema(old_outer);

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

        // Check if this is a handler declaration
        if let Some(ref declare_type) = decl.declare_type {
            // Pattern match on the DeclareType
            // Based on the error message, it's structured as Handler { handler_type: ... }
            return self.plan_psm_handler(declare_type, decl, planner_context);
        }

        let schema = planner_context.psm_schema();

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
            data_type: arrow_type.clone(),
            default: default_expr,
            default_has_subquery: has_subquery,
        };

        // Add the declared variable to the PSM schema so it can be referenced later
        planner_context.add_psm_variable(&names[0].value, arrow_type)?;

        Ok(PsmStatement::new(
            PsmStatementKind::DeclareVariable(var),
            info,
        ))
    }

    /// Plan DECLARE HANDLER statement.
    fn plan_psm_handler(
        &self,
        declare_type: &ast::DeclareType,
        decl: &ast::Declare,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        // Extract handler information from declare_type
        // Pattern match on the DeclareType enum
        let (handler_type, condition, statement) = match declare_type {
            ast::DeclareType::Handler { handler_type: sql_handler_type } => {
                // Map sqlparser handler type to DataFusion HandlerType
                let handler_type = match sql_handler_type {
                    ast::DeclareHandlerType::Continue => HandlerType::Continue,
                    ast::DeclareHandlerType::Exit => HandlerType::Exit,
                    ast::DeclareHandlerType::Undo => {
                        // UNDO is not supported in most SQL implementations
                        return not_impl_err!("UNDO handler type not supported");
                    }
                };

                // The handler condition is stored in the names field
                // It's one of: SQLEXCEPTION, SQLWARNING, NOT FOUND, SQLSTATE '...', or condition name
                let condition = if let Some(first_name) = decl.names.first() {
                    let cond_str = first_name.value.as_str();
                    match cond_str {
                        "SQLEXCEPTION" => HandlerCondition::SqlException,
                        "SQLWARNING" => HandlerCondition::SqlWarning,
                        "NOT FOUND" => HandlerCondition::NotFound,
                        // Check if it's SQLSTATE (would be parsed with the state value)
                        // For SQLSTATE '23000', the name will contain just the identifier
                        // and we need to look at the pattern differently
                        _ => {
                            // Assume it's either a condition name or we need to handle SQLSTATE differently
                            // For now, treat as condition name
                            HandlerCondition::ConditionName(first_name.clone())
                        }
                    }
                } else {
                    return Err(datafusion_common::DataFusionError::Plan(
                        "Handler must have a condition".to_string(),
                    ));
                };

                // Plan the handler body statement (stored in handler_body field)
                let statement = if let Some(ref stmt) = decl.handler_body {
                    Box::new(self.plan_psm_statement(stmt, planner_context)?)
                } else {
                    return Err(datafusion_common::DataFusionError::Plan(
                        "Handler must have a body statement".to_string(),
                    ));
                };

                (handler_type, condition, statement)
            }
            other => {
                return not_impl_err!("DECLARE {:?} not yet supported", other);
            }
        };

        Ok(PsmStatement::procedural(PsmStatementKind::DeclareHandler(
            PsmHandler {
                handler_type,
                condition,
                statement,
            },
        )))
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
                let schema = planner_context.psm_schema();

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
                let schema = planner_context.psm_schema();

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
        let schema = planner_context.psm_schema();

        let (value, has_subquery) = match &ret.value {
            Some(ReturnStatementValue::Expr(expr)) => {
                let planned =
                    self.sql_to_expr(expr.clone(), &schema, planner_context)?;
                let has_subquery = Self::expr_contains_subquery(&planned);
                (Some(planned), has_subquery)
            }
            Some(ReturnStatementValue::Next(_)) => {
                // RETURN NEXT is PostgreSQL-specific, not supported yet
                return not_impl_err!("RETURN NEXT is not supported");
            }
            Some(ReturnStatementValue::Query(_)) => {
                // RETURN QUERY is PostgreSQL-specific, not supported yet
                return not_impl_err!("RETURN QUERY is not supported");
            }
            Some(ReturnStatementValue::QueryExecute { .. }) => {
                // RETURN QUERY EXECUTE is PostgreSQL-specific, not supported yet
                return not_impl_err!("RETURN QUERY EXECUTE is not supported");
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
        let schema = planner_context.psm_schema();

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
        let schema = planner_context.psm_schema();

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
        let schema = planner_context.psm_schema();

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

        let schema = planner_context.psm_schema();

        // Extract SQLSTATE from message if it's a Sqlstate, otherwise use default
        let sqlstate = match &raise_stmt.message {
            Some(ast::RaiseMessage::Sqlstate(state)) => state.clone(),
            _ => "45000".to_string(),
        };

        // Convert USING items to set_items
        let mut set_items = Vec::new();

        // If there's a format string message, add it as MESSAGE_TEXT
        if let Some(ast::RaiseMessage::FormatString(msg)) = &raise_stmt.message {
            let message_ident = Ident::new("MESSAGE_TEXT");
            let planned = Expr::Literal(ScalarValue::Utf8(Some(msg.clone())), None);
            set_items.push((message_ident, planned));
        }

        // Process USING clause items
        for item in &raise_stmt.using {
            let planned = self.sql_to_expr(item.value.clone(), &schema, planner_context)?;
            let option_ident = Ident::new(item.option.to_string());
            set_items.push((option_ident, planned));
        }

        Ok(PsmStatement::procedural(PsmStatementKind::Signal(
            PsmSignal { sqlstate, set_items },
        )))
    }

    /// Plan SIGNAL statement (SQL standard exception signaling).
    fn plan_psm_signal(
        &self,
        signal_stmt: &ast::SignalStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        use datafusion_expr::logical_plan::psm::PsmSignal;

        let schema = planner_context.psm_schema();

        // Extract SQLSTATE value
        let sqlstate = signal_stmt.sqlstate.clone();

        // Process SET items
        let mut set_items = Vec::new();
        for item in &signal_stmt.set_items {
            let planned = self.sql_to_expr(item.value.clone(), &schema, planner_context)?;
            set_items.push((item.name.clone(), planned));
        }

        Ok(PsmStatement::procedural(PsmStatementKind::Signal(
            PsmSignal { sqlstate, set_items },
        )))
    }

    /// Plan RESIGNAL statement (SQL standard exception re-signaling).
    fn plan_psm_resignal(
        &self,
        resignal_stmt: &ast::ResignalStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        use datafusion_expr::logical_plan::psm::PsmResignal;

        let schema = planner_context.psm_schema();

        // Extract optional SQLSTATE value
        let sqlstate = resignal_stmt.sqlstate.clone();

        // Process SET items
        let mut set_items = Vec::new();
        for item in &resignal_stmt.set_items {
            let planned = self.sql_to_expr(item.value.clone(), &schema, planner_context)?;
            set_items.push((item.name.clone(), planned));
        }

        Ok(PsmStatement::procedural(PsmStatementKind::Resignal(
            PsmResignal { sqlstate, set_items },
        )))
    }

    /// Plan LOOP statement.
    fn plan_psm_loop(
        &self,
        loop_stmt: &ast::LoopStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        use datafusion_expr::logical_plan::psm::PsmLoop;

        let mut body_info = RegionInfo::default();
        let body: Vec<PsmStatement> = loop_stmt
            .body
            .statements()
            .iter()
            .map(|s| {
                let stmt = self.plan_psm_statement(s, planner_context)?;
                body_info.merge(&stmt.info);
                Ok(stmt)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PsmStatement::new(
            PsmStatementKind::Loop(PsmLoop {
                label: loop_stmt.label.clone(),
                body,
                body_info: body_info.clone(),
            }),
            body_info,
        ))
    }

    /// Plan REPEAT statement.
    fn plan_psm_repeat(
        &self,
        repeat_stmt: &ast::RepeatStatement,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        use datafusion_expr::logical_plan::psm::PsmRepeat;

        let schema = planner_context.psm_schema();

        let mut body_info = RegionInfo::default();
        let body: Vec<PsmStatement> = repeat_stmt
            .body
            .statements()
            .iter()
            .map(|s| {
                let stmt = self.plan_psm_statement(s, planner_context)?;
                body_info.merge(&stmt.info);
                Ok(stmt)
            })
            .collect::<Result<Vec<_>>>()?;

        // Plan UNTIL condition (until is Expr directly, not Option)
        let until_condition =
            self.sql_to_expr(repeat_stmt.until.clone(), &schema, planner_context)?;
        let condition_has_subquery = Self::expr_contains_subquery(&until_condition);

        let mut info = body_info.clone();
        if condition_has_subquery {
            info.contains_scalar_subquery = true;
        }

        Ok(PsmStatement::new(
            PsmStatementKind::Repeat(PsmRepeat {
                label: repeat_stmt.label.clone(),
                body,
                body_info,
                until_condition,
                condition_has_subquery,
            }),
            info,
        ))
    }

    /// Plan labeled block (label: BEGIN ... END).
    fn plan_psm_labeled_block(
        &self,
        labeled: &ast::LabeledBlock,
        planner_context: &mut PlannerContext,
    ) -> Result<PsmStatement> {
        let mut info = RegionInfo::default();
        let statements: Vec<PsmStatement> = labeled
            .statements
            .iter()
            .map(|s| {
                let stmt = self.plan_psm_statement(s, planner_context)?;
                info.merge(&stmt.info);
                Ok(stmt)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PsmStatement::new(
            PsmStatementKind::Block(PsmBlock {
                label: labeled.label.clone(),
                statements,
                info: info.clone(),
            }),
            info,
        ))
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
