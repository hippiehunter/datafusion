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

use crate::planner::{ContextProvider, SqlToRel};
use datafusion_common::{Result, not_impl_err};
use datafusion_expr::Operator;
use sqlparser::ast::BinaryOperator;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn parse_sql_binary_op(&self, op: &BinaryOperator) -> Result<Operator> {
        match *op {
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Modulo => Ok(Operator::Modulo),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
            BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
            BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
            BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
            BinaryOperator::PGLikeMatch => Ok(Operator::LikeMatch),
            BinaryOperator::PGILikeMatch => Ok(Operator::ILikeMatch),
            BinaryOperator::PGNotLikeMatch => Ok(Operator::NotLikeMatch),
            BinaryOperator::PGNotILikeMatch => Ok(Operator::NotILikeMatch),
            BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
            BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
            BinaryOperator::Xor => Ok(Operator::BitwiseXor),
            BinaryOperator::BitwiseXor => Ok(Operator::BitwiseXor),
            BinaryOperator::PGBitwiseXor => Ok(Operator::BitwiseXor),
            BinaryOperator::PGBitwiseShiftRight => Ok(Operator::BitwiseShiftRight),
            BinaryOperator::PGBitwiseShiftLeft => Ok(Operator::BitwiseShiftLeft),
            BinaryOperator::StringConcat => Ok(Operator::StringConcat),
            BinaryOperator::ArrowAt => Ok(Operator::ArrowAt),
            BinaryOperator::AtArrow => Ok(Operator::AtArrow),
            BinaryOperator::Arrow => Ok(Operator::Arrow),
            BinaryOperator::LongArrow => Ok(Operator::LongArrow),
            BinaryOperator::HashArrow => Ok(Operator::HashArrow),
            BinaryOperator::HashLongArrow => Ok(Operator::HashLongArrow),
            BinaryOperator::AtAt => Ok(Operator::AtAt),
            BinaryOperator::Spaceship => Ok(Operator::IsNotDistinctFrom),
            BinaryOperator::MyIntegerDivide => Ok(Operator::IntegerDivide),
            BinaryOperator::HashMinus => Ok(Operator::HashMinus),
            BinaryOperator::AtQuestion => Ok(Operator::AtQuestion),
            BinaryOperator::Question => Ok(Operator::Question),
            BinaryOperator::QuestionAnd => Ok(Operator::QuestionAnd),
            BinaryOperator::QuestionPipe => Ok(Operator::QuestionPipe),
            BinaryOperator::PGOverlap => Ok(Operator::ArrayOverlap),
            BinaryOperator::PGCustomBinaryOperator(ref names) => {
                let op_name = names.last().map(String::as_str).unwrap_or("");
                match op_name {
                    "=" => Ok(Operator::Eq),
                    "<>" | "!=" => Ok(Operator::NotEq),
                    "<" => Ok(Operator::Lt),
                    ">" => Ok(Operator::Gt),
                    "<=" => Ok(Operator::LtEq),
                    ">=" => Ok(Operator::GtEq),
                    "~" => Ok(Operator::RegexMatch),
                    "~*" => Ok(Operator::RegexIMatch),
                    "!~" => Ok(Operator::RegexNotMatch),
                    "!~*" => Ok(Operator::RegexNotIMatch),
                    "~~" => Ok(Operator::LikeMatch),
                    "~~*" => Ok(Operator::ILikeMatch),
                    "!~~" => Ok(Operator::NotLikeMatch),
                    "!~~*" => Ok(Operator::NotILikeMatch),
                    "||" => Ok(Operator::StringConcat),
                    "<<" => Ok(Operator::BitwiseShiftLeft),
                    ">>" => Ok(Operator::BitwiseShiftRight),
                    "#" => Ok(Operator::BitwiseXor),
                    "&" => Ok(Operator::BitwiseAnd),
                    "|" => Ok(Operator::BitwiseOr),
                    "&&" => Ok(Operator::ArrayOverlap),
                    "@>" => Ok(Operator::AtArrow),
                    "<@" => Ok(Operator::ArrowAt),
                    "->" => Ok(Operator::Arrow),
                    "->>" => Ok(Operator::LongArrow),
                    "#>" => Ok(Operator::HashArrow),
                    "#>>" => Ok(Operator::HashLongArrow),
                    "#-" => Ok(Operator::HashMinus),
                    "@@" => Ok(Operator::AtAt),
                    "?" => Ok(Operator::Question),
                    "?&" => Ok(Operator::QuestionAnd),
                    "?|" => Ok(Operator::QuestionPipe),
                    _ => not_impl_err!(
                        "Unsupported qualified operator: OPERATOR({})",
                        names.join(".")
                    ),
                }
            }
            _ => not_impl_err!("Unsupported binary operator: {:?}", op),
        }
    }
}
