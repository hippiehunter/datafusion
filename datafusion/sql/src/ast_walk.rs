//! DataFusion SQL's unit-break facade over the private sqlparser fork's
//! intrinsically erased recursive AST traversal.

use std::ops::ControlFlow;

use sqlparser::ast::{AstBox, Cte, Expr, Query, Statement, VisitErased, Visitor};

type AstVisitor<'a> = dyn Visitor<Break = ()> + 'a;

/// An AST root that drives the derived traversal through one erased visitor.
pub(crate) trait Walk {
    fn walk(&self, visitor: &mut AstVisitor<'_>) -> ControlFlow<()>;
}

macro_rules! walk_roots {
    ($($root:ty),* $(,)?) => {
        $(
            impl Walk for $root {
                fn walk(&self, visitor: &mut AstVisitor<'_>) -> ControlFlow<()> {
                    VisitErased::visit_erased(self, visitor)
                }
            }
        )*
    };
}

walk_roots!(Statement, Query, Cte, Expr);

impl<T: Walk + ?Sized> Walk for Box<T> {
    fn walk(&self, visitor: &mut AstVisitor<'_>) -> ControlFlow<()> {
        (**self).walk(visitor)
    }
}

impl<T: Walk> Walk for AstBox<T> {
    fn walk(&self, visitor: &mut AstVisitor<'_>) -> ControlFlow<()> {
        (**self).walk(visitor)
    }
}
