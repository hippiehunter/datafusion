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

//! `INTERSECT` and `EXCEPT` as a query wrote them.
//!
//! Execution planning turns both into joins over their inputs, which leaves
//! nothing to tell a written `INTERSECT` from a semi join, or an `EXCEPT ALL`
//! from the duplicate numbering that keeps its multiplicity. A planner that
//! plans a stored definition to print it back as SQL keeps the operation as
//! this node instead. The plan is for reading; nothing executes it.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, Result, internal_err};

use crate::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};

/// Which of the two operations a [`SetOperation`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub enum SetOperationKind {
    Intersect,
    Except,
}

impl fmt::Display for SetOperationKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            SetOperationKind::Intersect => "INTERSECT",
            SetOperationKind::Except => "EXCEPT",
        })
    }
}

/// `left INTERSECT [ALL] right` or `left EXCEPT [ALL] right`. The inputs are
/// the two queries as written, after their columns were given the types the
/// operation compares them as; the output is the left input's columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetOperation {
    pub op: SetOperationKind,
    /// Whether the operation keeps duplicate rows (`ALL`).
    pub all: bool,
    inputs: [LogicalPlan; 2],
}

impl SetOperation {
    pub fn new(
        op: SetOperationKind,
        all: bool,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> Self {
        Self {
            op,
            all,
            inputs: [left, right],
        }
    }

    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }

    pub fn left(&self) -> &LogicalPlan {
        &self.inputs[0]
    }

    pub fn right(&self) -> &LogicalPlan {
        &self.inputs[1]
    }
}

impl PartialOrd for SetOperation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.op, self.all)
            .partial_cmp(&(other.op, other.all))
            .filter(|cmp| *cmp != Ordering::Equal || self == other)
    }
}

impl UserDefinedLogicalNodeCore for SetOperation {
    fn name(&self) -> &str {
        "SetOperation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.left().schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SetOperation: {}{}",
            self.op,
            if self.all { " ALL" } else { "" }
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() {
            return internal_err!("SetOperation has no expressions");
        }
        let Ok([left, right]) = <[LogicalPlan; 2]>::try_from(inputs) else {
            return internal_err!("SetOperation has two inputs");
        };
        Ok(Self::new(self.op, self.all, left, right))
    }
}
