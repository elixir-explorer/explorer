// The idea of this file is to have functions that
// transform the expressions from the Elixir side
// to the Rust side. Each function receives a basic type
// or an expression and returns an expression that is
// wrapped in an Elixir struct.

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::{
    col, concat_str, cov, pearson_corr, when, IntoLazy, LiteralValue, SortOptions,
};
use polars::prelude::{DataType, Expr, Literal, StrptimeOptions, TimeUnit};

use crate::atoms::{microsecond, millisecond, nanosecond};
use crate::datatypes::{ExDate, ExDateTime, ExDuration, ExSeriesDtype};
use crate::series::{cast_str_to_f64, ewm_opts, rolling_opts};
use crate::{ExDataFrame, ExExpr, ExSeries};

// Useful to get an ExExpr vec into a vec of expressions.
pub fn ex_expr_to_exprs(ex_exprs: Vec<ExExpr>) -> Vec<Expr> {
    ex_exprs
        .iter()
        .map(|ex_expr| ex_expr.clone_inner())
        .collect()
}

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
pub fn expr_atom(atom: &str) -> ExExpr {
    let expr = cast_str_to_f64(atom).lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_date(date: ExDate) -> ExExpr {
    let naive_date = NaiveDate::from(date);
    let expr = naive_date.lit().dt().date();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_datetime(datetime: ExDateTime) -> ExExpr {
    let naive_datetime = NaiveDateTime::from(datetime);
    let expr = naive_datetime.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_duration(duration: ExDuration) -> ExExpr {
    // Note: it's tempting to use `.lit()` on a `chrono::Duration` struct in this function, but
    // doing so will lose precision information as `chrono::Duration`s have no time units.
    let time_unit = time_unit_of_ex_duration(duration);
    let expr = Expr::Literal(LiteralValue::Duration(duration.value, time_unit));
    ExExpr::new(expr)
}

fn time_unit_of_ex_duration(duration: ExDuration) -> TimeUnit {
    let precision = duration.precision;
    if precision == millisecond() {
        TimeUnit::Milliseconds
    } else if precision == microsecond() {
        TimeUnit::Microseconds
    } else if precision == nanosecond() {
        TimeUnit::Nanoseconds
    } else {
        panic!("unrecognized precision: {precision:?}")
    }
}

#[rustler::nif]
pub fn expr_series(series: ExSeries) -> ExExpr {
    let series = series.clone_inner();
    let expr = series.lit();
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_cast(data: ExExpr, to_dtype: ExSeriesDtype) -> ExExpr {
    let expr = data.clone_inner();
    let to_dtype = DataType::try_from(&to_dtype).expect("dtype is not valid");

    ExExpr::new(expr.cast(to_dtype))
}

#[rustler::nif]
pub fn expr_column(name: &str) -> ExExpr {
    let expr = col(name);
    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_equal(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.eq(right_expr))
}

#[rustler::nif]
pub fn expr_not_equal(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.neq(right_expr))
}

#[rustler::nif]
pub fn expr_greater(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.gt(right_expr))
}

#[rustler::nif]
pub fn expr_greater_equal(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.gt_eq(right_expr))
}

#[rustler::nif]
pub fn expr_less(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.lt(right_expr))
}

#[rustler::nif]
pub fn expr_less_equal(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.lt_eq(right_expr))
}

#[rustler::nif]
pub fn expr_binary_and(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.and(right_expr))
}

#[rustler::nif]
pub fn expr_binary_or(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.or(right_expr))
}

#[rustler::nif]
pub fn expr_binary_in(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.is_in(right_expr))
}

#[rustler::nif]
pub fn expr_is_nil(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.is_null())
}

#[rustler::nif]
pub fn expr_is_not_nil(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.is_not_null())
}

#[rustler::nif]
pub fn expr_is_finite(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.is_finite())
}

#[rustler::nif]
pub fn expr_is_infinite(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.is_infinite())
}

#[rustler::nif]
pub fn expr_is_nan(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.is_nan())
}

#[rustler::nif]
pub fn expr_all_equal(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();
    // TODO: add this option as an argument.
    let drop_nulls = false;

    ExExpr::new(left_expr.eq(right_expr).all(drop_nulls))
}

#[rustler::nif]
pub fn expr_slice(expr: ExExpr, offset: i64, length: u32) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.slice(offset, length))
}

#[rustler::nif]
pub fn expr_slice_by_indices(expr: ExExpr, indices_expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.take(indices_expr.clone_inner()))
}

#[rustler::nif]
pub fn expr_head(expr: ExExpr, length: usize) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.head(Some(length)))
}

#[rustler::nif]
pub fn expr_tail(expr: ExExpr, length: usize) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.tail(Some(length)))
}

#[rustler::nif]
pub fn expr_shift(expr: ExExpr, offset: i64, _default: Option<ExExpr>) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.shift(offset))
}

#[rustler::nif]
pub fn expr_sample_n(
    expr: ExExpr,
    n: u64,
    with_replacement: bool,
    shuffle: bool,
    seed: Option<u64>,
) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.sample_n(n.lit(), with_replacement, shuffle, seed))
}

#[rustler::nif]
pub fn expr_sample_frac(
    expr: ExExpr,
    frac: f64,
    with_replacement: bool,
    shuffle: bool,
    seed: Option<u64>,
) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.sample_frac(frac.lit(), with_replacement, shuffle, seed))
}

#[rustler::nif]
pub fn expr_rank(expr: ExExpr, method: &str, descending: bool, seed: Option<u64>) -> ExExpr {
    let expr = expr.clone_inner();
    let rank_options = crate::parse_rank_method_options(method, descending);

    ExExpr::new(expr.rank(rank_options, seed).cast(DataType::Float64))
}

#[rustler::nif]
pub fn expr_peaks(data: ExExpr, min_or_max: &str) -> ExExpr {
    let expr = data.clone_inner();
    let type_expr = if min_or_max == "min" {
        expr.min()
    } else {
        expr.max()
    };

    ExExpr::new(data.clone_inner().eq(type_expr))
}

#[rustler::nif]
pub fn expr_fill_missing_with_strategy(data: ExExpr, strategy: &str) -> ExExpr {
    let expr = data.clone_inner();
    let result_expr = match strategy {
        "backward" => expr.backward_fill(None),
        "forward" => expr.forward_fill(None),
        "min" => expr.clone().fill_null(expr.min()),
        "max" => expr.clone().fill_null(expr.max()),
        "mean" => expr.clone().fill_null(expr.mean()),
        _other => panic!("unknown strategy {strategy:?}"),
    };
    ExExpr::new(result_expr)
}

#[rustler::nif]
pub fn expr_fill_missing_with_value(data: ExExpr, value: ExExpr) -> ExExpr {
    let expr = data.clone_inner();
    let value = value.clone_inner();
    ExExpr::new(expr.fill_null(value))
}

#[rustler::nif]
pub fn expr_add(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr + right_expr)
}

#[rustler::nif]
pub fn expr_subtract(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr - right_expr)
}

#[rustler::nif]
pub fn expr_divide(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner().cast(DataType::Float64);
    let right_expr = right.clone_inner().cast(DataType::Float64);

    ExExpr::new(left_expr / right_expr)
}

#[rustler::nif]
pub fn expr_quotient(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    let quotient = left_expr
        / when(right_expr.clone().eq(0))
            .then(Expr::Literal(LiteralValue::Null))
            .otherwise(right_expr);

    ExExpr::new(quotient)
}

#[rustler::nif]
pub fn expr_remainder(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    let quotient = left_expr.clone()
        / when(right_expr.clone().eq(0))
            .then(Expr::Literal(LiteralValue::Null))
            .otherwise(right_expr.clone());

    let mult = right_expr * quotient;
    let result = left_expr - mult;

    ExExpr::new(result)
}

#[rustler::nif]
pub fn expr_multiply(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr * right_expr)
}

#[rustler::nif]
pub fn expr_pow(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    ExExpr::new(left_expr.pow(right_expr))
}

#[rustler::nif]
pub fn expr_log(left: ExExpr, base: f64) -> ExExpr {
    let left_expr = left.clone_inner();

    ExExpr::new(left_expr.log(base))
}

#[rustler::nif]
pub fn expr_log_natural(left: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner();

    ExExpr::new(left_expr.log(std::f64::consts::E))
}

#[rustler::nif]
pub fn expr_exp(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.exp())
}

#[rustler::nif]
pub fn expr_sum(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.sum())
}

#[rustler::nif]
pub fn expr_min(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.min())
}

#[rustler::nif]
pub fn expr_max(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.max())
}

#[rustler::nif]
pub fn expr_argmax(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.arg_max().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_argmin(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.arg_min().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_mean(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.mean())
}

#[rustler::nif]
pub fn expr_median(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.median())
}

#[rustler::nif]
pub fn expr_mode(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.mode())
}

#[rustler::nif]
pub fn expr_product(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.product())
}

#[rustler::nif]
pub fn expr_abs(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.abs())
}

#[rustler::nif]
pub fn expr_variance(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.var(1))
}

#[rustler::nif]
pub fn expr_standard_deviation(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.std(1))
}

#[rustler::nif]
pub fn expr_quantile(expr: ExExpr, quantile: f64) -> ExExpr {
    let expr = expr.clone_inner();
    let strategy = crate::parse_quantile_interpol_options("nearest");
    ExExpr::new(expr.quantile(quantile.into(), strategy))
}

#[rustler::nif]
pub fn expr_skew(data: ExExpr, bias: bool) -> ExExpr {
    let expr = data.clone_inner();
    ExExpr::new(expr.skew(bias))
}

#[rustler::nif]
pub fn expr_correlation(left: ExExpr, right: ExExpr, ddof: u8) -> ExExpr {
    let left_expr = left.clone_inner().cast(DataType::Float64);
    let right_expr = right.clone_inner().cast(DataType::Float64);
    ExExpr::new(pearson_corr(left_expr, right_expr, ddof))
}

#[rustler::nif]
pub fn expr_covariance(left: ExExpr, right: ExExpr) -> ExExpr {
    let left_expr = left.clone_inner().cast(DataType::Float64);
    let right_expr = right.clone_inner().cast(DataType::Float64);
    ExExpr::new(cov(left_expr, right_expr))
}

#[rustler::nif]
pub fn expr_alias(expr: ExExpr, name: &str) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.alias(name))
}

#[rustler::nif]
pub fn expr_count(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    // We need to add zero to work around a Polars bug
    // where casting a count returns the wrong result
    ExExpr::new((expr.count() + 0.lit()).cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_nil_count(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.null_count().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_n_distinct(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.n_unique().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_first(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.first())
}

#[rustler::nif]
pub fn expr_last(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.last())
}

#[rustler::nif]
pub fn expr_format(exprs: Vec<ExExpr>) -> ExExpr {
    ExExpr::new(concat_str(ex_expr_to_exprs(exprs), ""))
}

#[rustler::nif]
pub fn expr_concat(exprs: Vec<ExExpr>) -> ExExpr {
    let mut iter = exprs.iter();
    let mut result = iter.next().unwrap().clone_inner();

    for expr in iter {
        result = result.append(expr.clone_inner(), false);
    }

    ExExpr::new(result)
}

#[rustler::nif]
pub fn expr_coalesce(left: ExExpr, right: ExExpr) -> ExExpr {
    let predicate = left.clone_inner().is_not_null();
    let left_expr = left.clone_inner();
    let right_expr = right.clone_inner();

    let condition = when(predicate).then(left_expr).otherwise(right_expr);

    ExExpr::new(condition)
}

#[rustler::nif]
pub fn expr_select(predicate: ExExpr, on_true: ExExpr, on_false: ExExpr) -> ExExpr {
    let predicate_expr = predicate.clone_inner();
    let on_true_expr = on_true.clone_inner();
    let on_false_expr = on_false.clone_inner();

    let condition = when(predicate_expr)
        .then(on_true_expr)
        .otherwise(on_false_expr);

    ExExpr::new(condition)
}

// window functions
macro_rules! init_window_expr_fun {
    ($name:ident, $fun:ident) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(
            data: ExExpr,
            window_size: usize,
            weights: Option<Vec<f64>>,
            min_periods: Option<usize>,
            center: bool,
        ) -> ExExpr {
            let expr = data.clone_inner();
            let opts = rolling_opts(window_size, weights, min_periods, center);
            ExExpr::new(expr.$fun(opts))
        }
    };
}

init_window_expr_fun!(expr_window_max, rolling_max);
init_window_expr_fun!(expr_window_min, rolling_min);
init_window_expr_fun!(expr_window_sum, rolling_sum);
init_window_expr_fun!(expr_window_mean, rolling_mean);
init_window_expr_fun!(expr_window_median, rolling_median);

#[rustler::nif(schedule = "DirtyCpu")]
pub fn expr_window_standard_deviation(
    data: ExExpr,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> ExExpr {
    let expr = data.clone_inner();
    let opts = rolling_opts(window_size, weights, min_periods, center);
    ExExpr::new(expr.rolling_std(opts).cast(DataType::Float64))
}

#[rustler::nif]
pub fn expr_cumulative_min(data: ExExpr, reverse: bool) -> ExExpr {
    let expr = data.clone_inner();
    ExExpr::new(expr.cummin(reverse))
}

#[rustler::nif]
pub fn expr_cumulative_max(data: ExExpr, reverse: bool) -> ExExpr {
    let expr = data.clone_inner();
    ExExpr::new(expr.cummax(reverse))
}

#[rustler::nif]
pub fn expr_cumulative_sum(data: ExExpr, reverse: bool) -> ExExpr {
    let expr = data.clone_inner();
    ExExpr::new(expr.cumsum(reverse))
}

#[rustler::nif]
pub fn expr_cumulative_product(data: ExExpr, reverse: bool) -> ExExpr {
    let expr = data.clone_inner();
    ExExpr::new(expr.cumprod(reverse))
}

#[rustler::nif]
pub fn expr_ewm_mean(
    data: ExExpr,
    alpha: f64,
    adjust: bool,
    min_periods: usize,
    ignore_nulls: bool,
) -> ExExpr {
    let expr = data.clone_inner();
    let opts = ewm_opts(alpha, adjust, min_periods, ignore_nulls);
    ExExpr::new(expr.ewm_mean(opts))
}

#[rustler::nif]
pub fn expr_reverse(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.reverse())
}

#[rustler::nif]
pub fn expr_sort(expr: ExExpr, descending: bool, nulls_last: bool) -> ExExpr {
    let expr = expr.clone_inner();

    // TODO: make these bools options.
    let multithreaded = false;
    let maintain_order = true;

    let opts = SortOptions {
        descending,
        nulls_last,
        multithreaded,
        maintain_order,
    };

    ExExpr::new(expr.sort_with(opts))
}

#[rustler::nif]
pub fn expr_argsort(expr: ExExpr, descending: bool, nulls_last: bool) -> ExExpr {
    let expr = expr.clone_inner();

    // TODO: make these bools options.
    let multithreaded = false;
    let maintain_order = true;

    let opts = SortOptions {
        descending,
        nulls_last,
        multithreaded,
        maintain_order,
    };

    ExExpr::new(expr.arg_sort(opts).cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_distinct(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.unique_stable())
}

#[rustler::nif]
pub fn expr_unordered_distinct(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.unique())
}

#[rustler::nif]
pub fn expr_unary_not(expr: ExExpr) -> ExExpr {
    let predicate = expr.clone_inner();
    ExExpr::new(predicate.not())
}

#[rustler::nif]
pub fn expr_describe_filter_plan(data: ExDataFrame, expr: ExExpr) -> String {
    let df = data.clone();
    let expressions = expr.clone_inner();
    df.lazy().filter(expressions).describe_plan()
}

#[rustler::nif]
pub fn expr_contains(expr: ExExpr, pattern: &str) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.str().contains_literal(pattern.lit()))
}

#[rustler::nif]
pub fn expr_upcase(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.str().to_uppercase())
}

#[rustler::nif]
pub fn expr_downcase(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.str().to_lowercase())
}

#[rustler::nif]
pub fn expr_strip(expr: ExExpr, string: Option<String>) -> ExExpr {
    let expr = expr.clone_inner();
    let matches_expr = match string {
        Some(string) => string.lit(),
        None => Expr::Literal(LiteralValue::Null),
    };
    ExExpr::new(expr.str().strip_chars(matches_expr))
}

#[rustler::nif]
pub fn expr_lstrip(expr: ExExpr, string: Option<String>) -> ExExpr {
    let expr = expr.clone_inner();
    let matches_expr = match string {
        Some(string) => string.lit(),
        None => Expr::Literal(LiteralValue::Null),
    };
    ExExpr::new(expr.str().strip_chars_start(matches_expr))
}

#[rustler::nif]
pub fn expr_rstrip(expr: ExExpr, string: Option<String>) -> ExExpr {
    let expr = expr.clone_inner();
    let matches_expr = match string {
        Some(string) => string.lit(),
        None => Expr::Literal(LiteralValue::Null),
    };
    ExExpr::new(expr.str().strip_chars_end(matches_expr))
}

#[rustler::nif]
pub fn expr_substring(expr: ExExpr, offset: i64, length: Option<u64>) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.str().slice(offset, length))
}

#[rustler::nif]
pub fn expr_replace(expr: ExExpr, pat: String, value: String) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.str().replace_all(
        Expr::Literal(LiteralValue::Utf8(pat)),
        Expr::Literal(LiteralValue::Utf8(value)),
        true,
    ))
}

#[rustler::nif]
pub fn expr_round(expr: ExExpr, decimals: u32) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.round(decimals))
}

#[rustler::nif]
pub fn expr_floor(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.floor())
}

#[rustler::nif]
pub fn expr_ceil(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();
    ExExpr::new(expr.ceil())
}

#[rustler::nif]
pub fn expr_sin(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.sin())
}

#[rustler::nif]
pub fn expr_cos(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.cos())
}

#[rustler::nif]
pub fn expr_tan(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.tan())
}

#[rustler::nif]
pub fn expr_asin(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.arcsin())
}

#[rustler::nif]
pub fn expr_acos(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.arccos())
}

#[rustler::nif]
pub fn expr_atan(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.arctan())
}

#[rustler::nif]
pub fn expr_strptime(expr: ExExpr, format_string: &str) -> ExExpr {
    let options = StrptimeOptions {
        format: Some(format_string.to_string()),
        strict: false,
        exact: true,
        cache: true,
    };
    ExExpr::new(expr.clone_inner().str().to_datetime(
        Some(TimeUnit::Microseconds),
        None,
        options,
        "earliest".lit(),
    ))
}

#[rustler::nif]
pub fn expr_strftime(expr: ExExpr, format_string: &str) -> ExExpr {
    ExExpr::new(expr.clone_inner().dt().strftime(format_string))
}

#[rustler::nif]
pub fn expr_clip_integer(expr: ExExpr, min: i64, max: i64) -> ExExpr {
    let expr = expr.clone_inner().clip(min.lit(), max.lit());

    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_clip_float(expr: ExExpr, min: f64, max: f64) -> ExExpr {
    let expr = expr
        .clone_inner()
        .cast(DataType::Float64)
        .clip(min.lit(), max.lit());

    ExExpr::new(expr)
}

#[rustler::nif]
pub fn expr_day_of_week(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().weekday().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_day_of_year(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().ordinal_day().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_week_of_year(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().week().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_month(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().month().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_year(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().year().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_hour(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().hour().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_minute(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().minute().cast(DataType::Int64))
}

#[rustler::nif]
pub fn expr_second(expr: ExExpr) -> ExExpr {
    let expr = expr.clone_inner();

    ExExpr::new(expr.dt().second().cast(DataType::Int64))
}
