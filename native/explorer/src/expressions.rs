// The idea of this file is to have functions that
// transform the expressions from the Elixir side
// to the Rust side. It's a conversion of Elixir tuples
// in the following format: `{:operation, args}` to
// the Polars expressions.

use polars::prelude::{col, DataFrame, IntoLazy};
use polars::prelude::{Expr, Literal};

use crate::{ExDataFrame, ExExpr};

#[rustler::nif]
pub fn expr_integer(number: i64) -> ExExpr {
    let expr = number.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_float(number: f64) -> ExExpr {
    let expr = number.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_column(name: &str) -> ExExpr {
    let expr = col(name);
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_equal(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.eq(right_expr))
}

#[rustler::nif]
pub fn expr_describe_filter_plan(data: ExDataFrame, expr: ExExpr) -> String {
    let df: DataFrame = data.resource.0.clone();
    let expressions: Expr = expr.resource.0.clone();
    df.lazy().filter(expressions).describe_plan()
}
