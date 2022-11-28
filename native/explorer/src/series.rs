use crate::{
    datatypes::{ExDate, ExDateTime},
    encoding, ExDataFrame, ExSeries, ExplorerError,
};

use polars::prelude::*;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use rand_pcg::Pcg64;
use rustler::{Binary, Encoder, Env, Term};
use std::{result::Result, slice};

#[rustler::nif]
pub fn s_as_str(data: ExSeries) -> Result<String, ExplorerError> {
    Ok(format!("{:?}", data.resource.0))
}

macro_rules! from_list {
    ($name:ident, $type:ty) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(name: &str, val: Vec<Option<$type>>) -> ExSeries {
            ExSeries::new(Series::new(name, val.as_slice()))
        }
    };
}

from_list!(s_from_list_i64, i64);
from_list!(s_from_list_u32, u32);
from_list!(s_from_list_bool, bool);
from_list!(s_from_list_f64, f64);
from_list!(s_from_list_str, String);

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_date(name: &str, val: Vec<Option<ExDate>>) -> ExSeries {
    ExSeries::new(
        Series::new(
            name,
            val.iter()
                .map(|d| d.map(|d| d.into()))
                .collect::<Vec<Option<i32>>>(),
        )
        .cast(&DataType::Date)
        .unwrap(),
    )
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_datetime(name: &str, val: Vec<Option<ExDateTime>>) -> ExSeries {
    ExSeries::new(
        Series::new(
            name,
            val.iter()
                .map(|dt| dt.map(|dt| dt.into()))
                .collect::<Vec<Option<i64>>>(),
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap(),
    )
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_binary(name: &str, val: Vec<Option<Binary>>) -> ExSeries {
    ExSeries::new(Series::new(
        name,
        val.iter()
            .map(|bin| bin.map(|bin| bin.as_slice()))
            .collect::<Vec<Option<&[u8]>>>(),
    ))
}

macro_rules! from_binary {
    ($name:ident, $type:ty, $bytes:expr) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(name: &str, val: Binary) -> ExSeries {
            let slice = val.as_slice();
            let transmuted = unsafe {
                slice::from_raw_parts(slice.as_ptr() as *const $type, slice.len() / $bytes)
            };
            ExSeries::new(Series::new(name, transmuted))
        }
    };
}

from_binary!(s_from_binary_f64, f64, 8);
from_binary!(s_from_binary_i32, i32, 4);
from_binary!(s_from_binary_i64, i64, 8);
from_binary!(s_from_binary_u8, u8, 1);

#[rustler::nif]
pub fn s_name(data: ExSeries) -> Result<String, ExplorerError> {
    Ok(data.resource.0.name().to_string())
}

#[rustler::nif]
pub fn s_rename(data: ExSeries, name: &str) -> Result<ExSeries, ExplorerError> {
    let mut s = data.resource.0.clone();
    s.rename(name);
    Ok(ExSeries::new(s))
}

#[rustler::nif]
pub fn s_dtype(data: ExSeries) -> Result<String, ExplorerError> {
    let s = &data.resource.0;
    let dt = s.dtype().to_string();
    Ok(dt)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_slice(data: ExSeries, offset: i64, length: usize) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let series = s.slice(offset, length);
    Ok(ExSeries::new(series))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_concat(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let mut s = data.resource.0.clone();
    let s1 = &other.resource.0;
    s.append(s1)?;
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_mask(data: ExSeries, filter: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &filter.resource.0;
    if let Ok(ca) = s1.bool() {
        let series = s.filter(ca)?;
        Ok(ExSeries::new(series))
    } else {
        Err(ExplorerError::Other("Expected a boolean mask".into()))
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_add(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    Ok(ExSeries::new(s + s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_subtract(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    Ok(ExSeries::new(s - s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_multiply(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    Ok(ExSeries::new(s * s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_divide(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    Ok(ExSeries::new(s / s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_quotient(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    let div = s.checked_div(s1)?;

    Ok(ExSeries::new(div))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_remainder(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    let div = s.checked_div(s1)?;
    let mult = s1 * &div;
    let result = s - &mult;

    Ok(ExSeries::new(result))
}

#[rustler::nif]
pub fn s_head(data: ExSeries, length: Option<usize>) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.head(length)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_tail(data: ExSeries, length: Option<usize>) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.tail(length)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_shift(data: ExSeries, offset: i64) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.shift(offset)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sort(
    data: ExSeries,
    descending: bool,
    nulls_last: bool,
) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let opts = SortOptions {
        descending,
        nulls_last,
    };
    Ok(ExSeries::new(s.sort_with(opts)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_argsort(
    data: ExSeries,
    descending: bool,
    nulls_last: bool,
) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let opts = SortOptions {
        descending,
        nulls_last,
    };
    let indices = s.argsort(opts).cast(&DataType::Int64)?;
    Ok(ExSeries::new(indices))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_distinct(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let unique = s.take(&s.arg_unique()?)?;
    Ok(ExSeries::new(unique))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_unordered_distinct(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let unique = s.unique()?;
    Ok(ExSeries::new(unique))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_frequencies(data: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let s: &Series = &data.resource.0;
    let mut df = s.value_counts(true, true)?;
    let df = df
        .try_apply("counts", |s: &Series| s.cast(&DataType::Int64))?
        .clone();
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_slice_by_indices(data: ExSeries, indices: Vec<u32>) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let idx = UInt32Chunked::from_vec("idx", indices);
    let s1 = s.take(&idx)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_null(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.is_null().into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_is_not_null(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.is_not_null().into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_at_every(data: ExSeries, n: usize) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = s.take_every(n);
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_series_equal(
    data: ExSeries,
    other: ExSeries,
    null_equal: bool,
) -> Result<bool, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &other.resource.0;
    let result = if null_equal {
        s.series_equal_missing(s1)
    } else {
        s.series_equal(s1)
    };
    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &rhs.resource.0;
    Ok(ExSeries::new(s.equal(s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_not_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &rhs.resource.0;
    Ok(ExSeries::new(s.not_equal(s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_greater(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &rhs.resource.0;
    Ok(ExSeries::new(s.gt(s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_greater_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &rhs.resource.0;
    Ok(ExSeries::new(s.gt_eq(s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_less(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &rhs.resource.0;
    Ok(ExSeries::new(s.lt(s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_less_equal(data: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s1 = &rhs.resource.0;
    Ok(ExSeries::new(s.lt_eq(s1)?.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_and(lhs: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &lhs.resource.0;
    let s1 = &rhs.resource.0;
    let and = s.bool()? & s1.bool()?;
    Ok(ExSeries::new(and.into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_or(lhs: ExSeries, rhs: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &lhs.resource.0;
    let s1 = &rhs.resource.0;
    let or = s.bool()? | s1.bool()?;
    Ok(ExSeries::new(or.into_series()))
}

#[rustler::nif]
pub fn s_size(data: ExSeries) -> Result<usize, ExplorerError> {
    let s = &data.resource.0;
    Ok(s.len())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing(data: ExSeries, strategy: &str) -> Result<ExSeries, ExplorerError> {
    let strat = match strategy {
        "backward" => FillNullStrategy::Backward(None),
        "forward" => FillNullStrategy::Forward(None),
        "min" => FillNullStrategy::Min,
        "max" => FillNullStrategy::Max,
        "mean" => FillNullStrategy::Mean,
        s => {
            return Err(ExplorerError::Other(format!(
                "Strategy {} not supported",
                s
            )))
        }
    };

    let s = &data.resource.0;
    let s1 = s.fill_null(strat)?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_int(data: ExSeries, strategy: i64) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s.i64()?.fill_null_with_values(strategy)?.into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_float(data: ExSeries, strategy: f64) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s.f64()?.fill_null_with_values(strategy)?.into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_bin(data: ExSeries, strategy: &str) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s.utf8()?.fill_null_with_values(strategy)?.into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_fill_missing_with_boolean(
    data: ExSeries,
    strategy: bool,
) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s.bool()?.fill_null_with_values(strategy)?.into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_sum(
    data: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let s: &Series = &data.resource.0;
    let opts = rolling_opts(window_size, weights, min_periods, center);
    let s1 = s.rolling_sum(opts.into())?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_mean(
    data: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let s: &Series = &data.resource.0;
    let opts = rolling_opts(window_size, weights, min_periods, center);
    let s1 = s.rolling_mean(opts.into())?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_max(
    data: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let s: &Series = &data.resource.0;
    let opts = rolling_opts(window_size, weights, min_periods, center);
    let s1 = s.rolling_max(opts.into())?;
    Ok(ExSeries::new(s1))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_window_min(
    data: ExSeries,
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> Result<ExSeries, ExplorerError> {
    let s: &Series = &data.resource.0;
    let opts = rolling_opts(window_size, weights, min_periods, center);
    let s1 = s.rolling_min(opts.into())?;
    Ok(ExSeries::new(s1))
}

// Used for rolling functions - also see "expressions" module
pub fn rolling_opts(
    window_size: usize,
    weights: Option<Vec<f64>>,
    min_periods: Option<usize>,
    center: bool,
) -> RollingOptions {
    let min_periods = if let Some(mp) = min_periods {
        mp
    } else {
        window_size
    };
    let window_size_duration = Duration::new(window_size as i64);

    RollingOptions {
        window_size: window_size_duration,
        weights,
        min_periods,
        center,
        ..Default::default()
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_to_list(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    encoding::list_from_series(data, env)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_to_iovec(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    if data.resource.0.null_count() != 0 {
        Err(ExplorerError::Other(String::from(
            "cannot invoke to_iovec on series with nils",
        )))
    } else {
        encoding::iovec_from_series(data, env)
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_sum(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    match s.dtype() {
        DataType::Boolean => Ok(s.sum::<i64>().encode(env)),
        DataType::UInt32 | DataType::Int64 => Ok(s.sum::<i64>().encode(env)),
        DataType::Float64 => Ok(s.sum::<f64>().encode(env)),
        dt => panic!("sum/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_min(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s: &Series = &data.resource.0;
    match s.dtype() {
        DataType::UInt32 | DataType::Int64 => Ok(s.min::<i64>().encode(env)),
        DataType::Float64 => Ok(s.min::<f64>().encode(env)),
        DataType::Date => Ok(s.min::<i32>().map(ExDate::from).encode(env)),
        DataType::Datetime(TimeUnit::Microseconds, None) => {
            Ok(s.min::<i64>().map(ExDateTime::from).encode(env))
        }
        dt => panic!("min/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_max(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s: &Series = &data.resource.0;
    match s.dtype() {
        DataType::UInt32 | DataType::Int64 => Ok(s.max::<i64>().encode(env)),
        DataType::Float64 => Ok(s.max::<f64>().encode(env)),
        DataType::Date => Ok(s.max::<i32>().map(ExDate::from).encode(env)),
        DataType::Datetime(TimeUnit::Microseconds, None) => {
            Ok(s.max::<i64>().map(ExDateTime::from).encode(env))
        }
        dt => panic!("max/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_mean(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    match s.dtype() {
        DataType::Boolean => Ok(s.mean().encode(env)),
        DataType::UInt32 | DataType::Int64 | DataType::Float64 => Ok(s.mean().encode(env)),
        dt => panic!("mean/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_median(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    match s.dtype() {
        DataType::UInt32 | DataType::Int64 | DataType::Float64 => Ok(s.median().encode(env)),
        dt => panic!("median/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_variance(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    match s.dtype() {
        DataType::UInt32 | DataType::Int64 => Ok(s.i64()?.var(1).encode(env)),
        DataType::Float64 => Ok(s.f64()?.var(1).encode(env)),
        dt => panic!("var/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_standard_deviation(env: Env, data: ExSeries) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    match s.dtype() {
        DataType::UInt32 | DataType::Int64 => Ok(s.i64()?.std(1).encode(env)),
        DataType::Float64 => Ok(s.f64()?.std(1).encode(env)),
        dt => panic!("std/1 not implemented for {:?}", dt),
    }
}

#[rustler::nif]
pub fn s_at(env: Env, data: ExSeries, idx: usize) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    encoding::resource_term_from_value(&data.resource, s.get(idx), env)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_sum(data: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.cumsum(reverse)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_max(data: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.cummax(reverse)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cumulative_min(data: ExSeries, reverse: bool) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.cummin(reverse)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_quantile<'a>(
    env: Env<'a>,
    data: ExSeries,
    quantile: f64,
    strategy: &str,
) -> Result<Term<'a>, ExplorerError> {
    let s = &data.resource.0;
    let dtype = s.dtype();
    let strategy = parse_quantile_interpol_options(strategy);
    match dtype {
        DataType::Date => match s.date()?.quantile(quantile, strategy)? {
            None => Ok(None::<ExDate>.encode(env)),
            Some(days) => Ok(ExDate::from(days as i32).encode(env)),
        },
        DataType::Datetime(TimeUnit::Microseconds, None) => {
            match s.datetime()?.quantile(quantile, strategy)? {
                None => Ok(None::<ExDateTime>.encode(env)),
                Some(microseconds) => Ok(ExDateTime::from(microseconds as i64).encode(env)),
            }
        }
        _ => encoding::term_from_value(
            s.quantile_as_series(quantile, strategy)?
                .cast(dtype)?
                .get(0),
            env,
        ),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_peak_max(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.peak_max().into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_peak_min(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.peak_min().into_series()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_reverse(data: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    Ok(ExSeries::new(s.reverse()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_n_distinct(data: ExSeries) -> Result<usize, ExplorerError> {
    let s = &data.resource.0;
    Ok(s.n_unique()?)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_pow_f_rhs(data: ExSeries, exponent: f64) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s
        .cast(&DataType::Float64)?
        .f64()?
        .apply(|v| v.powf(exponent))
        .into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_pow_f_lhs(data: ExSeries, exponent: f64) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s
        .cast(&DataType::Float64)?
        .f64()?
        .apply(|v| exponent.powf(v))
        .into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_pow_i_rhs(data: ExSeries, exponent: u32) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;
    let s = s.i64()?.apply(|v| v.pow(exponent)).into_series();
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_pow_i_lhs(data: ExSeries, base: u32) -> Result<ExSeries, ExplorerError> {
    let s = &data.resource.0;

    let s = s
        .i64()?
        .try_apply(|v| match u32::try_from(v) {
            Ok(v) => Ok(base.pow(v).into()),
            Err(_) => Err(PolarsError::ComputeError(
                "negative exponent with an integer base".into(),
            )),
        })?
        .into_series();

    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_cast(data: ExSeries, to_type: &str) -> Result<ExSeries, ExplorerError> {
    let s: &Series = &data.resource.0;
    let dtype = cast_str_to_dtype(to_type)?;
    Ok(ExSeries::new(s.cast(&dtype)?))
}

pub fn cast_str_to_dtype(str_type: &str) -> Result<DataType, ExplorerError> {
    match str_type {
        "float" => Ok(DataType::Float64),
        "integer" => Ok(DataType::Int64),
        "date" => Ok(DataType::Date),
        "datetime" => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        "boolean" => Ok(DataType::Boolean),
        "string" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        _ => Err(ExplorerError::Other(String::from("Cannot cast to type"))),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_seedable_random_indices(
    length: usize,
    n_samples: usize,
    replacement: bool,
    seed: u64,
) -> Vec<usize> {
    let mut rng: Pcg64 = SeedableRng::seed_from_u64(seed);
    let range: Vec<usize> = (0..length).collect();
    if replacement {
        (0..n_samples).map(|_| rng.gen_range(0..length)).collect()
    } else {
        range
            .iter()
            .choose_multiple(&mut rng, n_samples)
            .iter()
            .map(|x| **x)
            .collect()
    }
}

pub fn parse_quantile_interpol_options(strategy: &str) -> QuantileInterpolOptions {
    match strategy {
        "nearest" => QuantileInterpolOptions::Nearest,
        "lower" => QuantileInterpolOptions::Lower,
        "higher" => QuantileInterpolOptions::Higher,
        "midpoint" => QuantileInterpolOptions::Midpoint,
        "linear" => QuantileInterpolOptions::Linear,
        _ => QuantileInterpolOptions::Nearest,
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_coalesce(data: ExSeries, other: ExSeries) -> Result<ExSeries, ExplorerError> {
    let s1 = &data.resource.0;
    let s2 = &other.resource.0;
    let coalesced = s1.zip_with(&s1.is_not_null(), s2)?;
    Ok(ExSeries::new(coalesced))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_select(
    pred: ExSeries,
    on_true: ExSeries,
    on_false: ExSeries,
) -> Result<ExSeries, ExplorerError> {
    let s1 = &pred.resource.0;
    let s2 = &on_true.resource.0;
    let s3 = &on_false.resource.0;

    if let Ok(ca) = s1.bool() {
        let selected = s2.zip_with(ca, s3)?;
        Ok(ExSeries::new(selected))
    } else {
        Err(ExplorerError::Other("Expected a boolean mask".into()))
    }
}
