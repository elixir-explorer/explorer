use crate::{
    datatypes::{
        ex_naive_datetime_to_timestamp, ExCorrelationMethod, ExDate, ExDecimal, ExNaiveDateTime,
        ExRankMethod, ExSeriesDtype, ExTime, ExTimeUnit, ExValidValue,
    },
    encoding, ExDataFrame, ExSeries, ExplorerError,
};

use encoding::encode_naive_datetime;

use polars::prelude::*;
use polars_ops::chunked_array::cov::{cov, pearson_corr};
use polars_ops::prelude::peaks::*;
use rustler::{Binary, Encoder, Env, Term};

pub mod from_list;
pub mod log;

#[rustler::nif]
pub fn s_as_str(data: ExSeries) -> Result<String, ExplorerError> {
    Ok(format!("{:?}", data.resource.0))
}

#[rustler::nif]
pub fn s_name(data: ExSeries) -> Result<String, ExplorerError> {
    Ok(data.name().to_string())
}

#[rustler::nif]
pub fn s_rename(data: ExSeries, name: &str) -> Result<ExSeries, ExplorerError> {
    let mut s = data.clone_inner();
    s.rename(name.into());
    Ok(ExSeries::new(s))
}

#[rustler::nif]
pub fn s_dtype(data: ExSeries) -> Result<ExSeriesDtype, ExplorerError> {
    ExSeriesDtype::try_from(data.dtype())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_slice(series: ExSeries, offset: i64, length: usize) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.slice(offset, length)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_concat(series_vec: Vec<ExSeries>) -> Result<ExSeries, ExplorerError> {
    let mut iter = series_vec.iter();
    let mut series = iter.next().unwrap().clone_inner();

    for s in iter {
        series.append(s)?;
    }

    Ok(ExSeries::new(series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_mask(series: ExSeries, filter: ExSeries) -> Result<ExSeries, ExplorerError> {
    if let Ok(ca) = filter.bool() {
        let series = series.filter(ca)?;
        Ok(ExSeries::new(series))
    } else {
        Err(ExplorerError::Other("Expected a boolean mask".into()))
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_add(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = other.clone_inner();
    let result = s + s1;
    Ok(ExSeries::new(result?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_subtract(lhs: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let left = lhs.clone_inner();
    let right = rhs.clone_inner();
    let result = left - right;
    Ok(ExSeries::new(result?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_multiply(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = other.clone_inner();
    let result = s * s1;
    Ok(ExSeries::new(result?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_divide(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner().cast(&DataType::Float64)?;
    let s1 = other.clone_inner().cast(&DataType::Float64)?;
    let result = s / s1;
    Ok(ExSeries::new(result?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_quotient(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(checked_div(data, other)?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_remainder(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = other.clone_inner();
    let div = checked_div(data, other)?;
    let mult = s1 * div;
    let result = s - mult?;

    Ok(ExSeries::new(result?))
}

// There is a bug in Polars where broadcast is not applied to checked_div
// and instead it discards values.
fn checked_div(data: ExSeries, other: ExSeries) -> Result<Series, ExplorerError> {
    match data.len() {
        1 => {
            let num = data.i64()?.get(0).unwrap();
            Ok(Series::new(
                data.name().clone(),
                other.i64()?.apply(|v| v.and_then(|v| num.checked_div(v))),
            ))
        }
        _ => match other.len() {
            1 => Ok(data.checked_div_num(other.i64()?.get(0).unwrap())?),
            _ => Ok(data.checked_div(&other)?),
        },
    }
}

#[rustler::nif]
pub fn s_head(series: ExSeries, length: Option<usize>) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.head(length)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_tail(series: ExSeries, length: Option<usize>) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.tail(length)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_shift(series: ExSeries, offset: i64) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.shift(offset)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sort(
    series: ExSeries,
    descending: bool,
    maintain_order: bool,
    multithreaded: bool,
    nulls_last: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = SortOptions {
        descending,
        maintain_order,
        multithreaded,
        nulls_last,
        limit: None,
    };
    Ok(ExSeries::new(series.sort_with(opts)?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_argsort(
    series: ExSeries,
    descending: bool,
    maintain_order: bool,
    multithreaded: bool,
    nulls_last: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = SortOptions {
        descending,
        maintain_order,
        multithreaded,
        nulls_last,
        limit: None,
    };
    let indices = series.arg_sort(opts).into_series();
    Ok(ExSeries::new(indices))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_distinct(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    let unique = series.take(&series.arg_unique()?)?;
    Ok(ExSeries::new(unique))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_unordered_distinct(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    let unique = series.unique()?;
    Ok(ExSeries::new(unique))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_frequencies(series: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let df = series.value_counts(true, true, "counts".into(), false)?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cut(
    series: ExSeries,
    bins: Vec<f64>,
    labels: Option<Vec<String>>,
    break_point_label: Option<&str>,
    category_label: Option<&str>,
    left_close: bool,
    include_breaks: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let series = series.clone_inner();

    // Cut is going to return a Series of a Struct. We need to convert it to a DF.
    let cut_series = cut(
        &series,
        bins,
        labels.map(|vec| vec.iter().map(|label| label.into()).collect()),
        left_close,
        include_breaks,
    )?;

    if include_breaks {
        let mut cut_df = cut_series.struct_()?.clone().unnest();

        let cut_df = cut_df.insert_column(0, series)?;

        cut_df.set_column_names([
            "values",
            break_point_label.unwrap_or("break_point"),
            category_label.unwrap_or("category"),
        ])?;

        Ok(ExDataFrame::new(cut_df.clone()))
    } else {
        let mut cut_df = DataFrame::new(vec![Column::from(series), Column::from(cut_series)])?;
        cut_df.set_column_names(["values", category_label.unwrap_or("category")])?;

        Ok(ExDataFrame::new(cut_df.clone()))
    }
}

#[allow(clippy::too_many_arguments)]
#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_qcut(
    series: ExSeries,
    quantiles: Vec<f64>,
    labels: Option<Vec<String>>,
    break_point_label: Option<&str>,
    category_label: Option<&str>,
    allow_duplicates: bool,
    left_close: bool,
    include_breaks: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let series = series.clone_inner();

    let qcut_series: Series = qcut(
        &series,
        quantiles,
        labels.map(|vec| vec.iter().map(|label| label.into()).collect()),
        left_close,
        allow_duplicates,
        include_breaks,
    )?;

    if include_breaks {
        let mut qcut_df = qcut_series.struct_()?.clone().unnest();
        let qcut_df = qcut_df.insert_column(0, series)?;

        qcut_df.set_column_names([
            "values",
            break_point_label.unwrap_or("break_point"),
            category_label.unwrap_or("category"),
        ])?;

        Ok(ExDataFrame::new(qcut_df.clone()))
    } else {
        let mut qcut_df = DataFrame::new(vec![Column::from(series), Column::from(qcut_series)])?;
        qcut_df.set_column_names(["values", category_label.unwrap_or("category")])?;

        Ok(ExDataFrame::new(qcut_df.clone()))
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_slice_by_indices(series: ExSeries, indices: Vec<u32>) -> Result<ExSeries, ExplorerError> {
    let idx = UInt32Chunked::from_vec("idx".into(), indices);
    let s1 = series.take(&idx)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_slice_by_series(series: ExSeries, indices: ExSeries) -> Result<ExSeries, ExplorerError> {
    match indices.strict_cast(&DataType::UInt32) {
        Ok(casted) => {
            let idx = casted.u32()?;
            match series.take(idx) {
                Ok(s1) => Ok(ExSeries::new(s1)),
                Err(_) => Err(ExplorerError::Other(
                    "slice/2 cannot select from indices that are out-of-bounds".into(),
                )),
            }
        }
        Err(_) => Err(ExplorerError::Other(
            "slice/2 expects a series of positive integers".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_null(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.is_null().into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_not_null(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.is_not_null().into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_finite(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.is_finite()?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_infinite(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.is_infinite()?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_nan(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.is_nan()?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_at_every(series: ExSeries, n: usize) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(series.gather_every(n, 0)?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_series_equal(
    series: ExSeries,
    other: ExSeries,
    null_equal: bool,
) -> Result<bool, ExplorerError> {
    let result = if null_equal {
        series.equals_missing(&other)
    } else {
        series.equals(&other)
    };

    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_equal(lhs: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(
        lhs.clone_inner().equal(&rhs.clone_inner())?.into_series(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_not_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = rhs.clone_inner();
    Ok(ExSeries::new(s.not_equal(&s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_greater(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = rhs.clone_inner();
    Ok(ExSeries::new(s.gt(&s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_greater_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = rhs.clone_inner();
    Ok(ExSeries::new(s.gt_eq(&s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_less(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = rhs.clone_inner();
    Ok(ExSeries::new(s.lt(&s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_less_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = data.clone_inner();
    let s1 = rhs.clone_inner();
    Ok(ExSeries::new(s.lt_eq(&s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_in(s: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = match s.dtype() {
        DataType::Categorical(_, _) => is_in(&s, &rhs.implode()?.into(), false)?,
        _ => is_in(&s, &rhs.cast(s.dtype())?.implode()?.into(), false)?,
    };

    Ok(ExSeries::new(s.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_and(lhs: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let and = lhs.bool()? & rhs.bool()?;
    Ok(ExSeries::new(and.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_or(lhs: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let or = lhs.bool()? | rhs.bool()?;
    Ok(ExSeries::new(or.into_series()))
}

#[rustler::nif]
pub fn s_size(series: ExSeries) -> Result<usize, ExplorerError> {
    Ok(series.len())
}

#[rustler::nif]
pub fn s_nil_count(series: ExSeries) -> Result<usize, ExplorerError> {
    Ok(series.null_count())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_strategy(
    series: ExSeries,
    strategy: &str,
) -> Result<ExSeries, ExplorerError> {
    let strat = match strategy {
        "backward" => FillNullStrategy::Backward(None),
        "forward" => FillNullStrategy::Forward(None),
        "min" => FillNullStrategy::Min,
        "max" => FillNullStrategy::Max,
        "mean" => FillNullStrategy::Mean,
        s => return Err(ExplorerError::Other(format!("Strategy {s} not supported"))),
    };

    Ok(ExSeries::new(series.fill_null(strat)?))
}

// Used for the non-finite atoms: [`:nan`, `:infinity`, `:neg_infinity`].
#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_atom(
    ex_series: ExSeries,
    atom: &str,
) -> Result<ExSeries, ExplorerError> {
    let filled_series = match ex_series.dtype() {
        DataType::Float32 => ex_series
            .f32()?
            .fill_null_with_values(cast_str_to_f32(atom))?
            .into_series(),
        DataType::Float64 => ex_series
            .f64()?
            .fill_null_with_values(cast_str_to_f64(atom))?
            .into_series(),
        // We shouldn't ever call `Native.s_fill_missing_with_atom/2` from the
        // Elixir side with a non-float series.
        other => {
            panic!("s_fill_missing_with_atom/2 implemented for float types, found: {other:?}")
        }
    };

    Ok(ExSeries::new(filled_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_int(
    ex_series: ExSeries,
    integer: i64,
) -> Result<ExSeries, ExplorerError> {
    let filled_series = match ex_series.dtype() {
        DataType::Int8 => ex_series
            .i8()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        DataType::Int16 => ex_series
            .i16()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        DataType::Int32 => ex_series
            .i32()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        DataType::Int64 => ex_series
            .i64()?
            .fill_null_with_values(integer)?
            .into_series(),
        DataType::UInt8 => ex_series
            .u8()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        DataType::UInt16 => ex_series
            .u16()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        DataType::UInt32 => ex_series
            .u32()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        DataType::UInt64 => ex_series
            .u64()?
            .fill_null_with_values(integer.try_into()?)?
            .into_series(),
        // We shouldn't ever call `Native.s_fill_missing_with_int/2` from the
        // Elixir side with a non-integer series.
        other => {
            panic!("s_fill_missing_with_int/2 implemented for integer types, found: {other:?}")
        }
    };

    Ok(ExSeries::new(filled_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_float(
    ex_series: ExSeries,
    float64: f64,
) -> Result<ExSeries, ExplorerError> {
    let filled_series = match ex_series.dtype() {
        DataType::Float32 => ex_series
            .f32()?
            .fill_null_with_values(f64_to_f32(float64))?
            .into_series(),
        DataType::Float64 => ex_series
            .f64()?
            .fill_null_with_values(float64)?
            .into_series(),
        // We shouldn't ever call `Native.s_fill_missing_with_float/2` from the
        // Elixir side with a non-float series.
        other => {
            panic!("s_fill_missing_with_float/2 implemented for float types, found: {other:?}")
        }
    };

    Ok(ExSeries::new(filled_series))
}

// TryFrom is not implemented for f64 -> f32. This is a work around.
// Adapted from: https://stackoverflow.com/a/72247742/5932228
fn f64_to_f32(float64: f64) -> f32 {
    let float32 = float64 as f32;
    assert_eq!(
        float64.is_finite(),
        float32.is_finite(),
        "f32 overflow during conversion"
    );
    float32
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_bin(
    series: ExSeries,
    binary: Binary,
) -> Result<ExSeries, ExplorerError> {
    let s = match series.dtype() {
        DataType::String => {
            if let Ok(_string) = std::str::from_utf8(&binary) {
                // This casting is necessary just because it's not possible to fill UTF8 series.
                unsafe {
                    series
                        .cast_unchecked(&DataType::Binary)?
                        .binary()?
                        .fill_null_with_values(&binary)?
                        .cast_unchecked(&DataType::String)?
                }
            } else {
                return Err(ExplorerError::Other("cannot cast to string".into()));
            }
        }
        DataType::Binary => series
            .binary()?
            .fill_null_with_values(&binary)?
            .into_series(),
        dt => panic!("fill_missing/2 not implemented for {dt:?}"),
    };
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_date(series: ExSeries, date: ExDate) -> Result<ExSeries, ExplorerError> {
    let s = series
        .date()?
        .fill_null_with_values(date.into())?
        .cast(&DataType::Date)?
        .into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_datetime(
    series: ExSeries,
    ex_naive_datetime: ExNaiveDateTime,
) -> Result<ExSeries, ExplorerError> {
    let time_unit = match series._dtype() {
        DataType::Datetime(time_unit, _) => *time_unit,
        _ => TimeUnit::Microseconds,
    };

    let timestamp = ex_naive_datetime_to_timestamp(ex_naive_datetime, time_unit)?;

    let s = series
        .datetime()?
        .fill_null_with_values(timestamp)?
        .cast(series.dtype())?
        .into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_boolean(
    series: ExSeries,
    boolean: bool,
) -> Result<ExSeries, ExplorerError> {
    let s = series.bool()?.fill_null_with_values(boolean)?.into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_decimal(
    _series: ExSeries,
    _decimal: ExDecimal,
) -> Result<ExSeries, ExplorerError> {
    // We need to make sure that the series dtype is aligned with the decimal's scale.
    // let s = series
    //     .decimal()?
    //     .fill_null_with_values(decimal.signed_coef())?
    //     .into_series();
    // Ok(ExSeries::new(s))
    Err(ExplorerError::Other(
        "fill_missing/2 with decimals is not supported yet by Polars".to_string(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_sum(
    series: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = rolling_opts_fixed_window(window_size, weights, min_periods, center);
    let s1 = series.rolling_sum(opts)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_mean(
    series: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = rolling_opts_fixed_window(window_size, weights, min_periods, center);
    let s1 = series.rolling_mean(opts)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_median(
    series: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = rolling_opts_fixed_window(window_size, weights, min_periods, center);
    let s1 = series
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(series.name().clone()).rolling_median(opts)])
        .collect()?
        .column(series.name())?
        .as_materialized_series()
        .clone();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_max(
    series: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = rolling_opts_fixed_window(window_size, weights, min_periods, center);
    let s1 = series.rolling_max(opts)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_min(
    series: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = rolling_opts_fixed_window(window_size, weights, min_periods, center);
    let s1 = series.rolling_min(opts)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_standard_deviation(
    series: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = rolling_opts_fixed_window(window_size, weights, min_periods, center);
    let s1 = series.rolling_std(opts)?;
    Ok(ExSeries::new(s1))
}

// Used for rolling functions - also see "expressions" module
pub fn rolling_opts_fixed_window(
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> RollingOptionsFixedWindow {
    let min_periods: usize = if let Some(mp) = min_periods {
        mp
    } else {
        window_size
    };
    // let window_size_duration = Duration::new(window_size as i64);

    RollingOptionsFixedWindow {
        window_size,
        weights,
        min_periods,
        center,
        ..Default::default()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_ewm_mean(
    series: ExSeries,
    alpha: f64,
    adjust: bool,
    min_periods: usize,
    ignore_nulls: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = ewm_opts(alpha, adjust, min_periods, ignore_nulls);
    let s1 = polars_ops::prelude::ewm_mean(&series, opts)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_ewm_standard_deviation(
    series: ExSeries,
    alpha: f64,
    adjust: bool,
    bias: bool,
    min_periods: usize,
    ignore_nulls: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = EWMOptions {
        alpha,
        adjust,
        bias,
        min_periods,
        ignore_nulls,
    };
    let s1 = polars_ops::prelude::ewm_std(&series, opts)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_ewm_variance(
    series: ExSeries,
    alpha: f64,
    adjust: bool,
    bias: bool,
    min_periods: usize,
    ignore_nulls: bool,
) -> Result<ExSeries, ExplorerError> {
    let opts = EWMOptions {
        alpha,
        adjust,
        bias,
        min_periods,
        ignore_nulls,
    };
    let s1 = polars_ops::prelude::ewm_var(&series, opts)?;
    Ok(ExSeries::new(s1))
}

pub fn ewm_opts(alpha: f64, adjust: bool, min_periods: usize, ignore_nulls: bool) -> EWMOptions {
    EWMOptions {
        alpha,
        adjust,
        min_periods,
        ignore_nulls,
        ..Default::default()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_to_list(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    encoding::list_from_series(data, env)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_to_iovec(env: Env, series: ExSeries) -> Result<Term, ExplorerError> {
    if series.null_count() != 0 {
        Err(ExplorerError::Other(
            "cannot invoke to_iovec on series with nils".into(),
        ))
    } else {
        encoding::iovec_from_series(series, env)
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sum(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    match s.dtype() {
        DataType::Boolean => Ok(s.sum::<u32>()?.encode(env)),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Ok(s.sum::<i64>()?.encode(env))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Ok(s.sum::<u64>()?.encode(env))
        }
        DataType::Float32 | DataType::Float64 => {
            Ok(encoding::term_from_float64(s.sum::<f64>()?, env))
        }
        dt => panic!("sum/1 not implemented for {dt:?}"),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_min(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    match s.dtype() {
        DataType::Int8 => Ok(s.min::<i8>()?.encode(env)),
        DataType::Int16 => Ok(s.min::<i16>()?.encode(env)),
        DataType::Int32 => Ok(s.min::<i32>()?.encode(env)),
        DataType::Int64 => Ok(s.min::<i64>()?.encode(env)),
        DataType::UInt8 => Ok(s.min::<u8>()?.encode(env)),
        DataType::UInt16 => Ok(s.min::<u16>()?.encode(env)),
        DataType::UInt32 => Ok(s.min::<u32>()?.encode(env)),
        DataType::UInt64 => Ok(s.min::<u64>()?.encode(env)),
        DataType::Float32 | DataType::Float64 | DataType::Decimal(_, _) => {
            Ok(term_from_optional_float(s.min::<f64>()?, env))
        }
        DataType::Date => Ok(s.min::<i32>()?.map(ExDate::from).encode(env)),
        DataType::Time => Ok(s.min::<i64>()?.map(ExTime::from).encode(env)),
        DataType::Datetime(unit, _) => Ok(s
            .min::<i64>()?
            .map(|v| encode_naive_datetime(v, *unit, env).unwrap())
            .encode(env)),
        dt => panic!("min/1 not implemented for {dt:?}"),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_max(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    match s.dtype() {
        DataType::Int8 => Ok(s.max::<i8>()?.encode(env)),
        DataType::Int16 => Ok(s.max::<i16>()?.encode(env)),
        DataType::Int32 => Ok(s.max::<i32>()?.encode(env)),
        DataType::Int64 => Ok(s.max::<i64>()?.encode(env)),
        DataType::UInt8 => Ok(s.max::<u8>()?.encode(env)),
        DataType::UInt16 => Ok(s.max::<u16>()?.encode(env)),
        DataType::UInt32 => Ok(s.max::<u32>()?.encode(env)),
        DataType::UInt64 => Ok(s.max::<u64>()?.encode(env)),
        DataType::Float32 | DataType::Float64 | DataType::Decimal(_, _) => {
            Ok(term_from_optional_float(s.max::<f64>()?, env))
        }
        DataType::Date => Ok(s.max::<i32>()?.map(ExDate::from).encode(env)),
        DataType::Time => Ok(s.max::<i64>()?.map(ExTime::from).encode(env)),
        DataType::Datetime(unit, _) => Ok(s
            .max::<i64>()?
            .map(|v| encode_naive_datetime(v, *unit, env).unwrap())
            .encode(env)),
        dt => panic!("max/1 not implemented for {dt:?}"),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_argmax(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    Ok(s.arg_max().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_argmin(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    Ok(s.arg_min().encode(env))
}

fn is_numeric(dtype: &DataType) -> bool {
    dtype.is_primitive_numeric() || matches!(dtype, DataType::Decimal(_, _))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_mean(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    if is_numeric(s.dtype()) {
        Ok(term_from_optional_float(s.mean(), env))
    } else {
        panic!("mean/1 not implemented for {:?}", &s.dtype())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_median(env: Env, s: ExSeries) -> Result<Term, ExplorerError> {
    if is_numeric(s.dtype()) {
        Ok(term_from_optional_float(s.median(), env))
    } else {
        panic!("median/1 not implemented for {:?}", &s.dtype())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_mode(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match mode::mode(&s) {
        Ok(s) => Ok(ExSeries::new(s)),
        Err(e) => Err(e.into()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_product(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    if is_numeric(s.dtype()) {
        let series = s
            .clone_inner()
            .into_frame()
            .lazy()
            .select([col(s.name().clone()).product()])
            .collect()?
            .column(s.name())?
            .as_materialized_series()
            .clone();

        Ok(ExSeries::new(series))
    } else {
        panic!("product/1 not implemented for {:?}", &s.dtype())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_variance(s: ExSeries, ddof: u8) -> Result<ExSeries, ExplorerError> {
    if is_numeric(s.dtype()) {
        let var_series = s
            .clone_inner()
            .into_frame()
            .lazy()
            .select([col(s.name().clone()).var(ddof)])
            .collect()?
            .column(s.name())?
            .as_materialized_series()
            .clone();

        Ok(ExSeries::new(var_series))
    } else {
        panic!("variance/2 not implemented for {:?}", &s.dtype())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_standard_deviation(s: ExSeries, ddof: u8) -> Result<ExSeries, ExplorerError> {
    if is_numeric(s.dtype()) {
        let std_series = s
            .clone_inner()
            .into_frame()
            .lazy()
            .select([col(s.name().clone()).std(ddof)])
            .collect()?
            .column(s.name())?
            .as_materialized_series()
            .clone();

        Ok(ExSeries::new(std_series))
    } else {
        panic!("standard_deviation/2 not implemented for {:?}", &s.dtype())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_skew(env: Env, s: ExSeries, bias: bool) -> Result<Term, ExplorerError> {
    if is_numeric(s.dtype()) {
        Ok(term_from_optional_float(s.skew(bias)?, env))
    } else {
        panic!("skew/2 not implemented for {:?}", &s.dtype())
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_correlation(
    env: Env,
    s1: ExSeries,
    s2: ExSeries,
    method: ExCorrelationMethod,
) -> Result<Term, ExplorerError> {
    let s1 = s1.clone_inner().cast(&DataType::Float64)?;
    let s2 = s2.clone_inner().cast(&DataType::Float64)?;

    let corr = match method {
        ExCorrelationMethod::Pearson => pearson_corr(s1.f64()?, s2.f64()?),
        ExCorrelationMethod::Spearman => {
            let df = df!("s1" => s1, "s2" => s2)?
                .lazy()
                .with_column(spearman_rank_corr(col("s1"), col("s2"), true).alias("corr"))
                .collect()?;
            match df.column("corr")?.get(0)? {
                AnyValue::Float64(x) => Some(x),
                _ => None,
            }
        }
    };
    Ok(term_from_optional_float(corr, env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_covariance(env: Env, s1: ExSeries, s2: ExSeries, ddof: u8) -> Result<Term, ExplorerError> {
    let s1 = s1.clone_inner().cast(&DataType::Float64)?;
    let s2 = s2.clone_inner().cast(&DataType::Float64)?;
    let cov = cov(s1.f64()?, s2.f64()?, ddof);
    Ok(term_from_optional_float(cov, env))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_all(s: ExSeries) -> Result<bool, ExplorerError> {
    let s = s.clone_inner();

    Ok(s.bool()?.all())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_any(s: ExSeries) -> Result<bool, ExplorerError> {
    let s = s.clone_inner();

    Ok(s.bool()?.any())
}

fn term_from_optional_float(option: Option<f64>, env: Env<'_>) -> Term<'_> {
    match option {
        Some(float) => encoding::term_from_float64(float, env),
        None => rustler::types::atom::nil().to_term(env),
    }
}

#[rustler::nif]
pub fn s_at(env: Env, series: ExSeries, idx: usize) -> Result<Term, ExplorerError> {
    encoding::resource_term_from_value(&series.resource, series.get(idx)?, env)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_sum(series: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let new_series = polars_ops::prelude::cum_sum(&series, reverse)?;
    Ok(ExSeries::new(new_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_count(series: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let new_series = polars_ops::prelude::cum_count(&series, reverse)?;
    Ok(ExSeries::new(new_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_max(series: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let new_series = polars_ops::prelude::cum_max(&series, reverse)?;
    Ok(ExSeries::new(new_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_min(series: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let new_series = polars_ops::prelude::cum_min(&series, reverse)?;
    Ok(ExSeries::new(new_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_product(series: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let new_series = polars_ops::prelude::cum_prod(&series, reverse)?;
    Ok(ExSeries::new(new_series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_quantile<'a>(
    env: Env<'a>,
    s: ExSeries,
    quantile: f64,
    strategy: &str,
) -> Result<Term<'a>, ExplorerError> {
    let dtype = s.dtype();
    let strategy = parse_quantile_interpol_options(strategy);
    match dtype {
        DataType::Date => match s.date()?.quantile(quantile, strategy)? {
            None => Ok(None::<ExDate>.encode(env)),
            Some(days) => Ok(ExDate::from(days as i32).encode(env)),
        },
        DataType::Time => match s.time()?.quantile(quantile, strategy)? {
            None => Ok(None::<ExTime>.encode(env)),
            Some(microseconds) => Ok(ExTime::from(microseconds as i64).encode(env)),
        },
        DataType::Datetime(unit, None) => match s.datetime()?.quantile(quantile, strategy)? {
            None => Ok(None::<ExNaiveDateTime>.encode(env)),
            Some(time) => Ok(encode_naive_datetime(time as i64, *unit, env)
                .unwrap()
                .encode(env)),
        },
        _ => encoding::resource_term_from_value(
            &s.resource,
            s.quantile_reduce(quantile, strategy)?
                .into_series("quantile".into())
                .cast(dtype)?
                .get(0)?,
            env,
        ),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_peak_max(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let ca = match s.dtype() {
        DataType::Int8 => peak_max(s.i8()?),
        DataType::Int16 => peak_max(s.i16()?),
        DataType::Int32 => peak_max(s.i32()?),
        DataType::Int64 => peak_max(s.i64()?),

        DataType::UInt8 => peak_max(s.u8()?),
        DataType::UInt16 => peak_max(s.u16()?),
        DataType::UInt32 => peak_max(s.u32()?),
        DataType::UInt64 => peak_max(s.u64()?),

        DataType::Float32 => peak_max(s.f32()?),
        DataType::Float64 => peak_max(s.f64()?),
        DataType::Decimal(_, _) => peak_max(s.decimal()?),

        DataType::Date => peak_max(s.date()?),
        DataType::Time => peak_max(s.time()?),
        DataType::Datetime(_unit, None) => peak_max(s.datetime()?),
        DataType::Duration(_unit) => peak_max(s.duration()?),
        dt => panic!("peak_max/1 not implemented for {dt:?}"),
    };

    Ok(ExSeries::new(ca.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_peak_min(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let ca = match s.dtype() {
        DataType::Int8 => peak_min(s.i8()?),
        DataType::Int16 => peak_min(s.i16()?),
        DataType::Int32 => peak_min(s.i32()?),
        DataType::Int64 => peak_min(s.i64()?),

        DataType::UInt8 => peak_min(s.u8()?),
        DataType::UInt16 => peak_min(s.u16()?),
        DataType::UInt32 => peak_min(s.u32()?),
        DataType::UInt64 => peak_min(s.u64()?),

        DataType::Float32 => peak_min(s.f32()?),
        DataType::Float64 => peak_min(s.f64()?),
        DataType::Decimal(_, _) => peak_min(s.decimal()?),

        DataType::Date => peak_min(s.date()?),
        DataType::Time => peak_min(s.time()?),
        DataType::Datetime(_unit, None) => peak_min(s.datetime()?),
        DataType::Duration(_unit) => peak_min(s.duration()?),
        dt => panic!("peak_min/1 not implemented for {dt:?}"),
    };

    Ok(ExSeries::new(ca.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_reverse(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s.reverse()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_n_distinct(s: ExSeries) -> Result<usize, ExplorerError> {
    Ok(s.n_unique()?)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cast(s: ExSeries, to_type: ExSeriesDtype) -> Result<ExSeries, ExplorerError> {
    let dtype = DataType::try_from(&to_type)?;
    Ok(ExSeries::new(s.cast(&dtype)?))
}

pub fn cast_str_to_f32(atom: &str) -> f32 {
    match atom {
        "nan" => f32::NAN,
        "infinity" => f32::INFINITY,
        "neg_infinity" => f32::NEG_INFINITY,
        _ => panic!("unknown literal {atom:?}"),
    }
}

pub fn cast_str_to_f64(atom: &str) -> f64 {
    match atom {
        "nan" => f64::NAN,
        "infinity" => f64::INFINITY,
        "neg_infinity" => f64::NEG_INFINITY,
        _ => panic!("unknown literal {atom:?}"),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_categories(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Categorical(Some(mapping), _) => {
            let size = mapping.len() as u32;
            let categories: Vec<&str> = (0..size).map(|id| mapping.get(id)).collect();
            let series = Series::new("categories".into(), &categories);
            Ok(ExSeries::new(series))
        }
        _ => panic!("Cannot get categories from non categorical series"),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_categorise(s: ExSeries, cat: ExSeries) -> Result<ExSeries, ExplorerError> {
    match cat.dtype() {
        DataType::Categorical(Some(mapping), _) => {
            let categories = mapping.get_categories();
            let m = categories.len();

            let chunks: ChunkedArray<UInt32Type> = if s.dtype() == &DataType::String {
                let ca = s.str()?;
                if m >= 10 {
                    let hash_map: std::collections::HashMap<&str, u32> = categories
                        .values_iter()
                        .enumerate()
                        .map(|(i, v)| (v, i as u32))
                        .collect();
                    ca.into_iter()
                        .map(|opt| opt.and_then(|slice| hash_map.get(slice).copied()))
                        .collect()
                } else {
                    ca.into_iter()
                        .map(|opt| opt.and_then(|slice| mapping.find(slice)))
                        .collect()
                }
            } else {
                s.cast(&DataType::UInt32)?.u32()?.clone()
            };

            let categorical_chunks = unsafe {
                CategoricalChunked::from_cats_and_rev_map_unchecked(
                    chunks,
                    mapping.clone(),
                    false,
                    CategoricalOrdering::default(),
                )
            };
            Ok(ExSeries::new(categorical_chunks.into_series()))
        }
        _ => panic!("Cannot get categories from non categorical or string series"),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sample_n(
    series: ExSeries,
    n: usize,
    replace: bool,
    shuffle: bool,
    seed: Option<u64>,
) -> Result<ExSeries, ExplorerError> {
    let new_s = series.sample_n(n, replace, shuffle, seed)?;

    Ok(ExSeries::new(new_s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sample_frac(
    series: ExSeries,
    frac: f64,
    replace: bool,
    shuffle: bool,
    seed: Option<u64>,
) -> Result<ExSeries, ExplorerError> {
    let new_s = series.sample_frac(frac, replace, shuffle, seed)?;

    Ok(ExSeries::new(new_s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_rank(
    series: ExSeries,
    method: ExRankMethod,
    descending: bool,
    seed: Option<u64>,
) -> Result<ExSeries, ExplorerError> {
    let rank_method = parse_rank_method_options(method, descending);
    let rank_data_type = match rank_method.method {
        RankMethod::Average => DataType::Float64,
        RankMethod::Ordinal => DataType::UInt32,
        _ => DataType::Int64,
    };

    let new_s = series
        .rank(rank_method, seed)
        .cast(&rank_data_type)?
        .into_series();

    Ok(ExSeries::new(new_s))
}

pub fn parse_rank_method_options(strategy: ExRankMethod, descending: bool) -> RankOptions {
    match strategy {
        ExRankMethod::Ordinal => RankOptions {
            method: RankMethod::Ordinal,
            descending,
        },
        ExRankMethod::Random => RankOptions {
            method: RankMethod::Random,
            descending,
        },
        ExRankMethod::Average => RankOptions {
            method: RankMethod::Average,
            descending,
        },
        ExRankMethod::Min => RankOptions {
            method: RankMethod::Min,
            descending,
        },
        ExRankMethod::Max => RankOptions {
            method: RankMethod::Max,
            descending,
        },
        ExRankMethod::Dense => RankOptions {
            method: RankMethod::Dense,
            descending,
        },
    }
}

pub fn parse_quantile_interpol_options(strategy: &str) -> QuantileMethod {
    match strategy {
        "nearest" => QuantileMethod::Nearest,
        "lower" => QuantileMethod::Lower,
        "higher" => QuantileMethod::Higher,
        "midpoint" => QuantileMethod::Midpoint,
        "linear" => QuantileMethod::Linear,
        _ => QuantileMethod::Nearest,
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_coalesce(s1: ExSeries, s2: ExSeries) -> Result<ExSeries, ExplorerError> {
    let coalesced = s1.zip_with(&s1.is_not_null(), &s2)?;
    Ok(ExSeries::new(coalesced))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_select(
    pred: ExSeries,
    on_true: ExSeries,
    on_false: ExSeries,
) -> Result<ExSeries, ExplorerError> {
    match pred.len() {
        1 => match pred.bool().unwrap().get(0).unwrap() {
            true => Ok(on_true),
            false => Ok(on_false),
        },
        _ => {
            let selected = on_true.zip_with(pred.bool().unwrap(), &on_false)?;
            Ok(ExSeries::new(selected))
        }
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_not(s1: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s2 = s1
        .bool()?
        .into_iter()
        .map(|opt_v| opt_v.map(|v| !v))
        .collect();

    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_contains(s1: ExSeries, pattern: &str, literal: bool) -> Result<ExSeries, ExplorerError> {
    let chunked_array = if literal {
        s1.str()?.contains_literal(pattern)?
    } else {
        s1.str()?.contains(pattern, true)?
    };
    Ok(ExSeries::new(chunked_array.into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_upcase(s1: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s1.str()?.to_uppercase().into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_downcase(s1: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s1.str()?.to_lowercase().into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_replace(
    s1: ExSeries,
    pattern: &str,
    replacement: &str,
    literal: bool,
) -> Result<ExSeries, ExplorerError> {
    let chunked_array = if literal {
        s1.str()?.replace_literal_all(pattern, replacement)?
    } else {
        s1.str()?.replace_all(pattern, replacement)?
    };
    Ok(ExSeries::new(chunked_array.into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_strip(s1: ExSeries, pattern: Option<&str>) -> Result<ExSeries, ExplorerError> {
    // There are no eager strip functions.
    let pattern = match pattern {
        None => String::from(r"^[ \s]+|[ \s]+$"),
        Some(string) => format!(r#"^[{}]+|[{}]+$"#, &string, &string),
    };

    // replace only replaces the leftmost match, so we need to call it twice.
    Ok(ExSeries::new(
        s1.str()?
            .replace(pattern.as_str(), "")?
            .replace(pattern.as_str(), "")?
            .into(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_lstrip(s1: ExSeries, pattern: Option<&str>) -> Result<ExSeries, ExplorerError> {
    // There are no eager strip functions.
    let pattern = match pattern {
        None => String::from(r"^[ \s]+"),
        Some(string) => format!(r#"^[{}]+"#, &string),
    };

    Ok(ExSeries::new(
        s1.str()?.replace(pattern.as_str(), "")?.into(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_rstrip(s1: ExSeries, pattern: Option<&str>) -> Result<ExSeries, ExplorerError> {
    // There are no eager strip functions.
    let pattern = match pattern {
        None => String::from(r"[ \s]+$"),
        Some(string) => format!(r#"[{}]+$"#, &string),
    };

    Ok(ExSeries::new(
        s1.str()?.replace(pattern.as_str(), "")?.into(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_substring(
    s: ExSeries,
    offset: i64,
    length: Option<u64>,
) -> Result<ExSeries, ExplorerError> {
    let length = match length {
        Some(l) => l.lit(),
        None => Expr::Literal(LiteralValue::Scalar(Scalar::null(DataType::Null))),
    };
    let s2 = s
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(s.name().clone()).str().slice(offset.lit(), length)])
        .collect()?
        .column(s.name())?
        .as_materialized_series()
        .clone();
    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_split(s1: ExSeries, by: &str) -> Result<ExSeries, ExplorerError> {
    let s2 = s1
        .str()?
        .split(&ChunkedArray::new("a".into(), &[by]))?
        .into_series();

    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_split_into(s1: ExSeries, by: &str, names: Vec<String>) -> Result<ExSeries, ExplorerError> {
    let s2 = s1
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(s1.name().clone())
            .str()
            .splitn(by.lit(), names.len())
            .struct_()
            .rename_fields(names)
            .alias(s1.name().clone())])
        .collect()?
        .column(s1.name())?
        .as_materialized_series()
        .clone();

    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_round(s: ExSeries, decimals: u32) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(
        s.round(decimals, RoundMode::HalfAwayFromZero)?
            .into_series(),
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_floor(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s.floor()?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_ceil(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s.ceil()?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_abs(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(abs(&s)?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_day_of_week(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.weekday()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_day_of_year(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.ordinal_day()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_week_of_year(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.week()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_month(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.month()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_year(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.year()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_hour(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.hour()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_minute(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.minute()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_second(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = s.second()?.into_series();

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_strptime(
    s: ExSeries,
    format_string: Option<&str>,
    precision: Option<ExTimeUnit>,
) -> Result<ExSeries, ExplorerError> {
    let timeunit = match precision {
        None => TimeUnit::Microseconds,
        Some(precision) => TimeUnit::try_from(&precision).unwrap(),
    };

    let s1 = s
        .str()?
        .as_datetime(
            format_string,
            timeunit,
            true,
            false,
            None,
            &StringChunked::from_iter(std::iter::once("earliest")),
        )?
        .into_series();
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_strftime(s: ExSeries, format_string: &str) -> Result<ExSeries, ExplorerError> {
    let s1 = s.strftime(format_string)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_clip_integer(s: ExSeries, min: i64, max: i64) -> Result<ExSeries, ExplorerError> {
    let s1 = clip(
        &s,
        &Series::new("min_clip".into(), &[min]),
        &Series::new("max_clip".into(), &[max]),
    )?;

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_clip_float(s: ExSeries, min: f64, max: f64) -> Result<ExSeries, ExplorerError> {
    let s1 = clip(
        &s,
        &Series::new("min_clip".into(), &[min]),
        &Series::new("max_clip".into(), &[max]),
    )?;

    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sin(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.sin()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.sin())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cos(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.cos()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.cos())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_tan(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.tan()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.tan())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_asin(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.asin()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.asin())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_acos(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.acos()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.acos())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_atan(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.atan()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.atan())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_degrees(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.to_degrees()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.to_degrees())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_radians(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    match s.dtype() {
        DataType::Float64 => {
            let s1 = s.f64()?.apply_values(|o| o.to_radians()).into();
            Ok(ExSeries::new(s1))
        }
        DataType::Float32 => {
            let s1 = s
                .f32()?
                .cast(&DataType::Float64)?
                .f64()?
                .apply_values(|o| o.to_radians())
                .into();
            Ok(ExSeries::new(s1))
        }
        _ => Err(ExplorerError::Other(
            "Only f32 and f64 dtypes are supported".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_join(s1: ExSeries, separator: &str) -> Result<ExSeries, ExplorerError> {
    let s2 = s1
        .list()?
        .lst_join(&ChunkedArray::new("a".into(), &[separator]), true)?
        .into_series();

    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_lengths(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s2 = s.list()?.lst_lengths().into_series();

    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn s_member(
    s: ExSeries,
    value: ExValidValue,
    inner_dtype: ExSeriesDtype,
) -> Result<ExSeries, ExplorerError> {
    let inner_dtype = DataType::try_from(&inner_dtype)?;
    let value_expr = value.lit_with_matching_precision(&inner_dtype);

    let s2 = s
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(s.name().clone()).list().contains(value_expr, false)])
        .collect()?
        .column(s.name())?
        .as_materialized_series()
        .clone();

    Ok(ExSeries::new(s2))
}

#[rustler::nif]
pub fn s_field(s: ExSeries, name: &str) -> Result<ExSeries, ExplorerError> {
    let s2 = s
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(s.name().clone())
            .struct_()
            .field_by_name(name)
            .alias(name)])
        .collect()?
        .column(name)?
        .as_materialized_series()
        .clone();
    Ok(ExSeries::new(s2))
}

#[rustler::nif]
pub fn s_json_decode(s: ExSeries, ex_dtype: ExSeriesDtype) -> Result<ExSeries, ExplorerError> {
    let dtype = DataType::try_from(&ex_dtype).unwrap();
    let s2 = s
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(s.name().clone())
            .str()
            .json_decode(Some(dtype), None)
            .alias(s.name().clone())])
        .collect()?
        .column(s.name())?
        .as_materialized_series()
        .clone();

    Ok(ExSeries::new(s2))
}

#[rustler::nif]
pub fn s_json_path_match(s: ExSeries, json_path: String) -> Result<ExSeries, ExplorerError> {
    let var_series = s
        .clone_inner()
        .into_frame()
        .lazy()
        .select([col(s.name().clone()).str().json_path_match(json_path.lit())])
        .collect()?
        .column(s.name())?
        .as_materialized_series()
        .clone();

    Ok(ExSeries::new(var_series))
}

#[rustler::nif]
pub fn s_row_index(series: ExSeries) -> Result<ExSeries, ExplorerError> {
    let len = u32::try_from(series.len())?;
    let s = Series::new("row_index".into(), 0..len);
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_count_matches(
    s1: ExSeries,
    pattern: &str,
    literal: bool,
) -> Result<ExSeries, ExplorerError> {
    let chunked_array = if literal {
        s1.str()?.count_matches(pattern, true)?
    } else {
        s1.str()?.count_matches(pattern, false)?
    };
    Ok(ExSeries::new(chunked_array.into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_re_scan(s1: ExSeries, pattern: &str) -> Result<ExSeries, ExplorerError> {
    let chunked_array = s1.str()?.extract_all(pattern)?;
    Ok(ExSeries::new(chunked_array.into()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_re_named_captures(s1: ExSeries, pattern: &str) -> Result<ExSeries, ExplorerError> {
    let s2 = s1
        .clone_inner()
        .into_frame()
        .lazy()
        .with_column(
            col(s1.name().clone())
                .str()
                .extract_groups(pattern)?
                .alias(s1.name().clone()),
        )
        .collect()?
        .column(s1.name())?
        .as_materialized_series()
        .clone();

    Ok(ExSeries::new(s2))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_index_of<'a>(
    env: Env<'a>,
    series: ExSeries,
    value: ExValidValue<'a>,
) -> Result<Term<'a>, ExplorerError> {
    let dtype = series.dtype().clone();
    let needle = value.into_scalar(dtype)?;
    let idx_value = match index_of(&series, needle)? {
        None => AnyValue::Null,
        Some(idx) => AnyValue::UInt64(idx as u64),
    };

    encoding::resource_term_from_value(&series.resource, idx_value, env)
}
