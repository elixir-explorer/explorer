use chrono::prelude::*;
use polars::prelude::*;
use rustler::{Encoder, Env, OwnedBinary, ResourceArc, Term};
use std::collections::HashMap;
use std::{mem, slice};

use chrono_tz::OffsetComponents;
use chrono_tz::OffsetName;
use chrono_tz::Tz;

use crate::atoms::{
    self, calendar, day, hour, infinity, microsecond, millisecond, minute, month, nan, nanosecond,
    neg_infinity, precision, second, std_offset, time_zone, utc_offset, value, year, zone_abbr,
};
use crate::datatypes::{
    days_to_date, time64ns_to_time, timestamp_to_datetime, timestamp_to_naive_datetime,
    ExMicrosecondTuple, ExSeries, ExSeriesRef,
};
use crate::ExplorerError;

use rustler::types::atom;

// Encoding helpers

macro_rules! iterator_series_to_list {
    ($env: ident, $iterator: expr) => {{
        $iterator.rfold(Term::list_new_empty($env), |acc, term| {
            acc.list_prepend(term)
        })
    }};
}

macro_rules! encode_chunked_array {
    ($chunked_array: expr, $env: ident, $encode_fun: expr) => {{
        $chunked_array.physical().downcast_iter().flat_map(|iter| {
            iter.into_iter()
                .map(|opt_v| opt_v.copied().map($encode_fun).encode($env))
        })
    }};
}

macro_rules! encode_date_struct {
    ($v: ident, $date_struct_keys: ident, $calendar_iso_module: ident, $date_module: ident, $env: ident) => {{
        let dt = days_to_date($v);

        Term::map_from_term_arrays(
            $env,
            $date_struct_keys,
            &[
                $date_module,
                $calendar_iso_module,
                dt.day().encode($env),
                dt.month().encode($env),
                dt.year().encode($env),
            ],
        )
        .unwrap()
    }};
}

// Here we build the Date struct manually, as it's much faster than using Date NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
#[inline]
fn date_struct_keys<'a>(env: Env<'a>) -> [Term<'a>; 5] {
    [
        atom::__struct__().encode(env),
        calendar().encode(env),
        day().encode(env),
        month().encode(env),
        year().encode(env),
    ]
}

#[inline]
fn encode_date(v: i32, env: Env) -> Result<Term, ExplorerError> {
    let date_struct_keys = &date_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let date_module = atoms::date_module().encode(env);
    Ok(encode_date_struct!(
        v,
        date_struct_keys,
        calendar_iso_module,
        date_module,
        env
    ))
}

#[inline]
fn date_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let date_struct_keys = &date_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let date_module = atoms::date_module().encode(env);

    Ok(iterator_series_to_list!(
        env,
        encode_chunked_array!(s.date()?, env, |date| encode_date_struct!(
            date,
            date_struct_keys,
            calendar_iso_module,
            date_module,
            env
        ))
    ))
}

macro_rules! encode_naive_datetime_struct {
    (
        $timestamp: expr,
        $time_unit: expr,
        $naive_datetime_struct_keys: ident,
        $calendar_iso_module: ident,
        $naive_datetime_module: ident,
        $env: ident
    ) => {{
        let ndt = timestamp_to_naive_datetime($timestamp, $time_unit);

        Term::map_from_term_arrays(
            $env,
            $naive_datetime_struct_keys,
            &[
                $naive_datetime_module,
                $calendar_iso_module,
                ndt.day().encode($env),
                ndt.month().encode($env),
                ndt.year().encode($env),
                ndt.hour().encode($env),
                ndt.minute().encode($env),
                ndt.second().encode($env),
                ndt.microsecond_tuple_tu($time_unit).encode($env),
            ],
        )
        .unwrap()
    }};
}

// Here we build the NaiveDateTime struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn naive_datetime_struct_keys<'a>(env: Env<'a>) -> [Term<'a>; 9] {
    [
        atom::__struct__().encode(env),
        calendar().encode(env),
        day().encode(env),
        month().encode(env),
        year().encode(env),
        hour().encode(env),
        minute().encode(env),
        second().encode(env),
        microsecond().encode(env),
    ]
}

#[inline]
pub fn encode_naive_datetime(
    timestamp: i64,
    time_unit: TimeUnit,
    env: Env,
) -> Result<Term, ExplorerError> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let naive_datetime_module = atoms::naive_datetime_module().encode(env);

    Ok(encode_naive_datetime_struct!(
        timestamp,
        time_unit,
        naive_datetime_struct_keys,
        calendar_iso_module,
        naive_datetime_module,
        env
    ))
}

#[inline]
fn naive_datetime_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let naive_datetime_module = atoms::naive_datetime_module().encode(env);
    let time_unit = match s.dtype() {
        DataType::Datetime(time_unit, None) => *time_unit,
        _ => panic!("should only use this function for naive datetimes"),
    };

    Ok(iterator_series_to_list!(
        env,
        encode_chunked_array!(
            s.datetime()?,
            env,
            |timestamp| encode_naive_datetime_struct!(
                timestamp,
                time_unit,
                naive_datetime_struct_keys,
                calendar_iso_module,
                naive_datetime_module,
                env
            )
        )
    ))
}

macro_rules! encode_datetime_struct {
    (
        $timestamp: expr,
        $time_unit: expr,
        $time_zone: expr,
        $datetime_struct_keys: ident,
        $calendar_iso_module: ident,
        $datetime_module: ident,
        $env: ident
    ) => {{
        let dt_tz = timestamp_to_datetime($timestamp, $time_unit, $time_zone);
        let tz_offset = dt_tz.offset();

        Term::map_from_term_arrays(
            $env,
            $datetime_struct_keys,
            &[
                $datetime_module,
                $calendar_iso_module,
                dt_tz.day().encode($env),
                dt_tz.hour().encode($env),
                dt_tz.microsecond_tuple_tu($time_unit).encode($env),
                dt_tz.minute().encode($env),
                dt_tz.month().encode($env),
                dt_tz.second().encode($env),
                tz_offset.dst_offset().num_seconds().encode($env),
                $time_zone.to_string().encode($env),
                tz_offset.base_utc_offset().num_seconds().encode($env),
                dt_tz.year().encode($env),
                tz_offset.abbreviation().encode($env),
            ],
        )
        .unwrap()
    }};
}

// Here we build the DateTime struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn datetime_struct_keys<'a>(env: Env<'a>) -> [Term<'a>; 13] {
    [
        atom::__struct__().encode(env),
        calendar().encode(env),
        day().encode(env),
        hour().encode(env),
        microsecond().encode(env),
        minute().encode(env),
        month().encode(env),
        second().encode(env),
        std_offset().encode(env),
        time_zone().encode(env),
        utc_offset().encode(env),
        year().encode(env),
        zone_abbr().encode(env),
    ]
}

#[inline]
pub fn encode_datetime(
    timestamp: i64,
    time_unit: TimeUnit,
    time_zone: Tz,
    env: Env,
) -> Result<Term, ExplorerError> {
    let datetime_struct_keys = &datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let datetime_module = atoms::datetime_module().encode(env);

    Ok(encode_datetime_struct!(
        timestamp,
        time_unit,
        time_zone,
        datetime_struct_keys,
        calendar_iso_module,
        datetime_module,
        env
    ))
}

#[inline]
fn datetime_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let datetime_struct_keys = &datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let datetime_module = atoms::datetime_module().encode(env);
    let time_unit = match s.dtype() {
        DataType::Datetime(time_unit, Some(_)) => *time_unit,
        _ => panic!("datetime_series_to_list called on series with wrong type"),
    };
    let time_zone = match s.dtype() {
        DataType::Datetime(_, Some(time_zone)) => time_zone.parse::<Tz>().unwrap(),
        _ => panic!("datetime_series_to_list called on series with wrong type"),
    };

    Ok(iterator_series_to_list!(
        env,
        encode_chunked_array!(s.datetime()?, env, |timestamp| encode_datetime_struct!(
            timestamp,
            time_unit,
            time_zone,
            datetime_struct_keys,
            calendar_iso_module,
            datetime_module,
            env
        ))
    ))
}

fn time_unit_to_atom(time_unit: TimeUnit) -> atom::Atom {
    match time_unit {
        TimeUnit::Milliseconds => millisecond(),
        TimeUnit::Microseconds => microsecond(),
        TimeUnit::Nanoseconds => nanosecond(),
    }
}
// ######### Duration ##########
macro_rules! encode_duration_struct {
    ($v: expr, $time_unit: expr, $duration_struct_keys: ident, $duration_module: ident, $env: ident) => {{
        let value = $v;
        let precision = time_unit_to_atom($time_unit);

        Term::map_from_term_arrays(
            $env,
            $duration_struct_keys,
            &[$duration_module, value.encode($env), precision.encode($env)],
        )
        .unwrap()
    }};
}

// Here we build the Explorer.Duration struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn duration_struct_keys<'a>(env: Env<'a>) -> [Term<'a>; 3] {
    [
        atom::__struct__().encode(env),
        value().encode(env),
        precision().encode(env),
    ]
}

#[inline]
pub fn encode_duration(v: i64, time_unit: TimeUnit, env: Env) -> Result<Term, ExplorerError> {
    let duration_struct_keys = &duration_struct_keys(env);
    let duration_module = atoms::duration_module().encode(env);

    Ok(encode_duration_struct!(
        v,
        time_unit,
        duration_struct_keys,
        duration_module,
        env
    ))
}

#[inline]
fn duration_series_to_list<'b>(
    s: &Series,
    time_unit: TimeUnit,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError> {
    let duration_struct_keys = &duration_struct_keys(env);
    let duration_module = atoms::duration_module().encode(env);

    Ok(iterator_series_to_list!(
        env,
        encode_chunked_array!(s.duration()?, env, |duration| encode_duration_struct!(
            duration,
            time_unit,
            duration_struct_keys,
            duration_module,
            env
        ))
    ))
}

// ######### End of Duration ##########

// ######### Decimal ##########
macro_rules! encode_decimal_struct {
    ($v: expr, $scale: expr, $decimal_struct_keys: ident, $decimal_module: ident, $env: ident) => {{
        let coef = $v.abs();
        let scale = -($scale as isize);
        let sign = $v.signum();
        // Elixir's Decimal has only 1 or -1. We need to treat positive zero as positive - 1.
        let sign = if sign == 0 { 1 } else { sign };

        Term::map_from_term_arrays(
            $env,
            $decimal_struct_keys,
            &[
                $decimal_module,
                coef.encode($env),
                scale.encode($env),
                sign.encode($env),
            ],
        )
        .unwrap()
    }};
}

// Here we build the Decimal struct manually, as it's much faster than using NifStruct
fn decimal_struct_keys<'a>(env: Env<'a>) -> [Term<'a>; 4] {
    [
        atom::__struct__().encode(env),
        atoms::coef().encode(env),
        atoms::exp().encode(env),
        atoms::sign().encode(env),
    ]
}

#[inline]
pub fn encode_decimal(v: i128, scale: usize, env: Env) -> Result<Term, ExplorerError> {
    let struct_keys = &decimal_struct_keys(env);
    let module_atom = atoms::decimal_module().encode(env);

    Ok(encode_decimal_struct!(
        v,
        scale,
        struct_keys,
        module_atom,
        env
    ))
}

#[inline]
fn decimal_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let struct_keys = &decimal_struct_keys(env);
    let module_atom = atoms::decimal_module().encode(env);
    let decimal_chunked = s.decimal()?;
    let scale = decimal_chunked.scale();

    Ok(iterator_series_to_list!(
        env,
        encode_chunked_array!(decimal_chunked, env, |decimal| encode_decimal_struct!(
            decimal,
            scale,
            struct_keys,
            module_atom,
            env
        ))
    ))
}

// ######### End of Decimal ##########

macro_rules! encode_time_struct {
    ($v: expr, $naive_time_struct_keys: ident, $calendar_iso_module: ident, $time_module: ident, $env: ident) => {{
        let t = time64ns_to_time($v);
        let microseconds = t.nanosecond() / 1_000;

        // Limit the number of digits in the microsecond part of a timestamp to 6.
        // This is necessary because the microsecond part of Elixir is only 6 digits.
        let limited_ms = if microseconds > 999_999 {
            999_999
        } else {
            microseconds
        };

        Term::map_from_term_arrays(
            $env,
            $naive_time_struct_keys,
            &[
                $time_module,
                $calendar_iso_module,
                t.hour().encode($env),
                t.minute().encode($env),
                t.second().encode($env),
                (limited_ms, 6).encode($env),
            ],
        )
        .unwrap()
    }};
}

// Here we build the NaiveTime struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn naive_time_struct_keys<'a>(env: Env<'a>) -> [Term<'a>; 6] {
    [
        atom::__struct__().encode(env),
        calendar().encode(env),
        hour().encode(env),
        minute().encode(env),
        second().encode(env),
        microsecond().encode(env),
    ]
}

#[inline]
fn encode_time(v: i64, env: Env) -> Result<Term, ExplorerError> {
    let naive_time_struct_keys = &naive_time_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let time_module = atoms::time_module().encode(env);

    Ok(encode_time_struct!(
        v,
        naive_time_struct_keys,
        calendar_iso_module,
        time_module,
        env
    ))
}

#[inline]
fn time_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let naive_time_struct_keys = &naive_time_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env);
    let time_module = atoms::time_module().encode(env);

    Ok(iterator_series_to_list!(
        env,
        encode_chunked_array!(s.time()?, env, |time| encode_time_struct!(
            time,
            naive_time_struct_keys,
            calendar_iso_module,
            time_module,
            env
        ))
    ))
}

fn generic_string_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    Ok(iterator_series_to_list!(
        env,
        s.str()?.into_iter().map(|option| option.encode(env))
    ))
}

fn generic_binary_series_to_list<'b>(
    resource: &ResourceArc<ExSeriesRef>,
    s: &Series,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError> {
    let nil = atom::nil().to_term(env);
    let acc = Term::list_new_empty(env);
    let list = s.binary()?.downcast_iter().rfold(acc, |acc, array| {
        array.iter().rfold(acc, |acc, v| {
            let term = match v {
                Some(values) => {
                    unsafe { resource.make_binary_unsafe(env, |_| values) }.to_term(env)
                }
                None => nil,
            };
            acc.list_prepend(term)
        })
    });
    Ok(list)
}

// HELP WANTED: Make this more efficient.
fn categorical_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    generic_string_series_to_list(&s.cast(&DataType::String).unwrap(), env)
}

// Convert f32 and f64 series taking into account NaN and Infinity floats (they are encoded as atoms).
macro_rules! float_series_to_list {
    ($name:ident, $convert_function:ident) => {
        #[inline]
        fn $name<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
            let nan_atom = nan().encode(env);
            let neg_infinity_atom = neg_infinity().encode(env);
            let infinity_atom = infinity().encode(env);
            let nil_atom = atom::nil().encode(env);

            Ok(iterator_series_to_list!(
                env,
                s.$convert_function()?.into_iter().map(|option| {
                    match option {
                        Some(x) => {
                            if x.is_finite() {
                                x.encode(env)
                            } else {
                                match (x.is_nan(), x.is_sign_negative()) {
                                    (true, _) => nan_atom,
                                    (false, true) => neg_infinity_atom,
                                    (false, false) => infinity_atom,
                                }
                            }
                        }
                        None => nil_atom,
                    }
                })
            ))
        }
    };
}

float_series_to_list!(float64_series_to_list, f64);
float_series_to_list!(float32_series_to_list, f32);

macro_rules! series_to_list {
    ($s:ident, $env:ident, $convert_function:ident) => {
        Ok(iterator_series_to_list!(
            $env,
            $s.$convert_function()?
                .into_iter()
                .map(|option| option.encode($env))
        ))
    };
}

#[inline]
fn null_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let nil = atom::nil().to_term(env);
    let mut list = Term::list_new_empty(env);
    for _n in 0..s.len() {
        list = list.list_prepend(nil);
    }
    Ok(list)
}

macro_rules! series_to_iovec {
    ($resource:ident, $v:expr, $env:ident, $in_type:ty) => {{
        Ok(iterator_series_to_list!(
            $env,
            $v.downcast_iter().map(|array| {
                let slice: &[$in_type] = array.values().as_slice();

                let aligned_slice = unsafe {
                    slice::from_raw_parts(
                        slice.as_ptr() as *const u8,
                        slice.len() * mem::size_of::<$in_type>(),
                    )
                };

                unsafe { $resource.make_binary_unsafe($env, |_| aligned_slice) }.to_term($env)
            })
        ))
    }};
}

// API

pub fn resource_term_from_value<'b>(
    resource: &ResourceArc<ExSeriesRef>,
    v: AnyValue,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError> {
    match v {
        AnyValue::Binary(v) => unsafe {
            Ok(Some(resource.make_binary_unsafe(env, |_| v)).encode(env))
        },
        AnyValue::Null => Ok(atom::nil().to_term(env)),
        AnyValue::Boolean(v) => Ok(v.encode(env)),
        AnyValue::String(v) => Ok(v.encode(env)),
        AnyValue::Int8(v) => Ok(v.encode(env)),
        AnyValue::Int16(v) => Ok(v.encode(env)),
        AnyValue::Int32(v) => Ok(v.encode(env)),
        AnyValue::Int64(v) => Ok(v.encode(env)),
        AnyValue::UInt8(v) => Ok(v.encode(env)),
        AnyValue::UInt16(v) => Ok(v.encode(env)),
        AnyValue::UInt32(v) => Ok(v.encode(env)),
        AnyValue::UInt64(v) => Ok(v.encode(env)),
        AnyValue::Float32(v) => Ok(term_from_float32(v, env)),
        AnyValue::Float64(v) => Ok(term_from_float64(v, env)),
        AnyValue::Date(v) => encode_date(v, env),
        AnyValue::Time(v) => encode_time(v, env),
        AnyValue::Datetime(v, time_unit, None) => encode_naive_datetime(v, time_unit, env),
        AnyValue::Datetime(v, time_unit, Some(time_zone)) => {
            encode_datetime(v, time_unit, time_zone.parse::<Tz>().unwrap(), env)
        }
        AnyValue::Duration(v, time_unit) => encode_duration(v, time_unit, env),
        AnyValue::Categorical(idx, mapping) => Ok(mapping.cat_to_str(idx).encode(env)),
        AnyValue::List(series) => list_from_series(ExSeries::new(series), env),
        AnyValue::Struct(_, _, fields) => v
            ._iter_struct_av()
            .zip(fields)
            .map(|(value, field)| {
                Ok((
                    field.name.as_str(),
                    resource_term_from_value(resource, value, env)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, ExplorerError>>()
            .map(|map| map.encode(env)),
        AnyValue::Decimal(value, _precision, scale) => encode_decimal(value, scale, env),
        dt => panic!("cannot encode value {dt:?} to term"),
    }
}

// Macro for decoding both f32 and f64 to term.
macro_rules! term_from_float {
    ($name:ident, $type:ty) => {
        pub fn $name(float: $type, env: Env<'_>) -> Term<'_> {
            if float.is_finite() {
                float.encode(env)
            } else {
                match (float.is_nan(), float.is_sign_negative()) {
                    (true, _) => nan().encode(env),
                    (false, true) => neg_infinity().encode(env),
                    (false, false) => infinity().encode(env),
                }
            }
        }
    };
}

term_from_float!(term_from_float64, f64);
term_from_float!(term_from_float32, f32);

pub fn list_from_series(s: ExSeries, env: Env) -> Result<Term, ExplorerError> {
    match s.dtype() {
        DataType::Null => null_series_to_list(&s, env),
        DataType::Boolean => series_to_list!(s, env, bool),

        DataType::Int8 => series_to_list!(s, env, i8),
        DataType::Int16 => series_to_list!(s, env, i16),
        DataType::Int32 => series_to_list!(s, env, i32),
        DataType::Int64 => series_to_list!(s, env, i64),

        DataType::UInt8 => series_to_list!(s, env, u8),
        DataType::UInt16 => series_to_list!(s, env, u16),
        DataType::UInt32 => series_to_list!(s, env, u32),
        DataType::UInt64 => series_to_list!(s, env, u64),

        DataType::Float32 => float32_series_to_list(&s, env),
        DataType::Float64 => float64_series_to_list(&s, env),

        DataType::Date => date_series_to_list(&s, env),
        DataType::Time => time_series_to_list(&s, env),
        DataType::Datetime(_, None) => naive_datetime_series_to_list(&s, env),
        DataType::Datetime(_, Some(_)) => datetime_series_to_list(&s, env),
        DataType::Duration(time_unit) => duration_series_to_list(&s, *time_unit, env),

        DataType::Binary => generic_binary_series_to_list(&s.resource, &s, env),
        DataType::String => generic_string_series_to_list(&s, env),
        DataType::Categorical(_, _) => categorical_series_to_list(&s, env),

        DataType::List(_inner_dtype) => s
            .list()?
            .into_iter()
            .map(|item| match item {
                Some(list) => list_from_series(ExSeries::new(list), env),
                None => Ok(None::<bool>.encode(env)),
            })
            .collect::<Result<Vec<Term>, ExplorerError>>()
            .map(|lists| lists.encode(env)),
        DataType::Struct(_fields) => s
            .iter()
            .map(|value| resource_term_from_value(&s.resource, value, env))
            .collect::<Result<Vec<_>, ExplorerError>>()
            .map(|values| values.encode(env)),
        DataType::Decimal(_precision, _scale) => decimal_series_to_list(&s, env),
        dt => panic!("to_list/1 not implemented for {dt:?}"),
    }
}

#[allow(clippy::size_of_in_element_count)]
pub fn iovec_from_series(s: ExSeries, env: Env) -> Result<Term, ExplorerError> {
    let resource = &s.resource;

    match s.dtype() {
        DataType::Boolean => {
            let mut bin = OwnedBinary::new(s.len()).unwrap();
            let slice = bin.as_mut_slice();
            for (i, v) in s.bool()?.into_iter().enumerate() {
                slice[i] = v.unwrap() as u8;
            }
            Ok([bin.release(env)].encode(env))
        }
        DataType::Int8 => series_to_iovec!(resource, s.i8()?, env, i8),
        DataType::Int16 => series_to_iovec!(resource, s.i16()?, env, i16),
        DataType::Int32 => series_to_iovec!(resource, s.i32()?, env, i32),
        DataType::Int64 => series_to_iovec!(resource, s.i64()?, env, i64),
        DataType::UInt8 => series_to_iovec!(resource, s.u8()?, env, u8),
        DataType::UInt16 => series_to_iovec!(resource, s.u16()?, env, u16),
        DataType::UInt32 => series_to_iovec!(resource, s.u32()?, env, u32),
        DataType::UInt64 => series_to_iovec!(resource, s.u64()?, env, u64),
        DataType::Float32 => series_to_iovec!(resource, s.f32()?, env, f32),
        DataType::Float64 => series_to_iovec!(resource, s.f64()?, env, f64),
        DataType::Date => series_to_iovec!(resource, s.date()?.physical(), env, i32),
        DataType::Time => series_to_iovec!(resource, s.time()?.physical(), env, i64),
        DataType::Datetime(_, None) => {
            series_to_iovec!(resource, s.datetime()?.physical(), env, i64)
        }
        DataType::Duration(_) => {
            series_to_iovec!(resource, s.duration()?.physical(), env, i64)
        }
        DataType::Categorical(_, _) => {
            series_to_iovec!(resource, s.cast(&DataType::UInt32)?.u32()?, env, u32)
        }
        dt => panic!("to_iovec/1 not implemented for {dt:?}"),
    }
}
