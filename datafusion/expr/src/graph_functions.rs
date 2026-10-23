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

//! Functions that only have meaning inside a GRAPH_TABLE's COLUMNS or WHERE
//! clause, where they describe a match rather than a row: `MATCHNUM()`
//! numbers the matches and `ELEMENT_NUMBER(v)` gives the position of a
//! `ONE ROW PER VERTEX` or `ONE ROW PER STEP` iterator variable in its path.
//!
//! The SQL planner resolves these names only while it plans a GRAPH_TABLE.
//! Compiling the graph table replaces every call; a call that reaches
//! evaluation is a planning bug.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::{Result, internal_err};

use crate::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

/// A graph-only function of a GRAPH_TABLE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GraphFunction {
    /// `MATCHNUM()`
    MatchNumber,
    /// `ELEMENT_NUMBER(v)`
    ElementNumber,
}

impl GraphFunction {
    /// The graph function a call name denotes inside a GRAPH_TABLE.
    pub fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("matchnum") {
            Some(Self::MatchNumber)
        } else if name.eq_ignore_ascii_case("element_number") {
            Some(Self::ElementNumber)
        } else {
            None
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::MatchNumber => "matchnum",
            Self::ElementNumber => "element_number",
        }
    }

    /// The graph function an expression calls, if it calls one.
    pub fn of_expr(expr: &Expr) -> Option<Self> {
        let Expr::ScalarFunction(call) = expr else {
            return None;
        };
        call.func
            .inner()
            .as_any()
            .downcast_ref::<GraphFunctionUdf>()
            .map(|udf| udf.function)
    }

    /// The function as a scalar UDF the SQL planner can place in a plan.
    pub fn udf(self) -> Arc<ScalarUDF> {
        let signature = match self {
            Self::MatchNumber => Signature::nullary(Volatility::Volatile),
            Self::ElementNumber => Signature::any(1, Volatility::Volatile),
        };
        Arc::new(ScalarUDF::new_from_impl(GraphFunctionUdf {
            function: self,
            signature,
        }))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct GraphFunctionUdf {
    function: GraphFunction,
    signature: Signature,
}

impl ScalarUDFImpl for GraphFunctionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.function.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "{}() was evaluated outside the GRAPH_TABLE that names it",
            self.function.name()
        )
    }
}
