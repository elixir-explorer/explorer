// The idea of this file is to have functions that
// transform the expressions from the Elixir side
// to the Rust side. Each function receives a basic type
// or an expression and returns an expression that is
// wrapped in an Elixir struct.

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::{col, DataFrame, IntoLazy};
use polars::prelude::{Expr, Literal};

use crate::datatypes::{ExDate, ExDateTime};
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
pub fn expr_string(string: String) -> ExExpr {
    let expr = string.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_boolean(boolean: bool) -> ExExpr {
    let expr = boolean.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_date(date: ExDate) -> ExExpr {
    let naive_date = NaiveDate::from(date);
    let expr = naive_date.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_datetime(datetime: ExDateTime) -> ExExpr {
    let naive_datetime = NaiveDateTime::from(datetime);
    let expr = naive_datetime.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_column(name: &str) -> ExExpr {
    let expr = col(name);
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_eq(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.eq(right_expr))
}

#[rustler::nif]
pub fn expr_neq(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.neq(right_expr))
}

#[rustler::nif]
pub fn expr_gt(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.gt(right_expr))
}

#[rustler::nif]
pub fn expr_gt_eq(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.gt_eq(right_expr))
}

#[rustler::nif]
pub fn expr_lt(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.lt(right_expr))
}

#[rustler::nif]
pub fn expr_lt_eq(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.lt_eq(right_expr))
}

#[rustler::nif]
pub fn expr_binary_and(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.and(right_expr))
}

#[rustler::nif]
pub fn expr_binary_or(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.or(right_expr))
}

#[rustler::nif]
pub fn expr_is_nil(expr: ExExpr) -> ExExpr {
    let expr: Expr = expr.resource.0.clone();

    ExExpr::new(expr.is_null())
}

#[rustler::nif]
pub fn expr_is_not_nil(expr: ExExpr) -> ExExpr {
    let expr: Expr = expr.resource.0.clone();

    ExExpr::new(expr.is_not_null())
}

#[rustler::nif]
pub fn expr_add(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr + right_expr)
}

#[rustler::nif]
pub fn expr_subtract(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr - right_expr)
}

#[rustler::nif]
pub fn expr_divide(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr / right_expr)
}

#[rustler::nif]
pub fn expr_pow(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr: Expr = left.resource.0.clone();
    let right_expr: Expr = right.resource.0.clone();

    ExExpr::new(left_expr.pow(right_expr))
}

#[rustler::nif]
pub fn expr_describe_filter_plan(data: ExDataFrame, expr: ExExpr) -> String {
    let df: DataFrame = data.resource.0.clone();
    let expressions: Expr = expr.resource.0.clone();
    df.lazy().filter(expressions).describe_plan()
}
