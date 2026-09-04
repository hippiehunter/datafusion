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

//! Parser-free semantic expressions produced by SQL lowering.

use std::ops::Deref;
use std::sync::Arc;

use crate::Expr;

/// An SQL expression after parsing, name resolution, and type/function
/// binding.
///
/// The newtype makes the SQL/frontend boundary explicit without retaining a
/// parser node or depending on an embedding engine's catalog types. Source
/// text, dialect identity, and catalog-specific policy belong to the embedding
/// catalog; runtime consumers should receive this semantic value instead of
/// reconstructing it from source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct BoundSqlExpression {
    expression: Arc<Expr>,
}

impl BoundSqlExpression {
    /// Mark an expression produced by the SQL binder as safe to cross the
    /// parser boundary.
    pub fn new(expression: Expr) -> Self {
        Self {
            expression: Arc::new(expression),
        }
    }

    /// Borrow the bound expression.
    pub fn expression(&self) -> &Expr {
        self.expression.as_ref()
    }

    /// Recover an owned expression without cloning when this is the last
    /// reference, or clone the expression otherwise.
    pub fn into_expression(self) -> Expr {
        Arc::unwrap_or_clone(self.expression)
    }
}

impl From<Expr> for BoundSqlExpression {
    fn from(expression: Expr) -> Self {
        Self::new(expression)
    }
}

impl Deref for BoundSqlExpression {
    type Target = Expr;

    fn deref(&self) -> &Self::Target {
        self.expression()
    }
}
