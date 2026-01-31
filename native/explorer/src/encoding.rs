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
use rustler::wrapper::{list, map, NIF_TERM};

// Encoding helpers

macro_rules! unsafe_iterator_series_to_list {
    ($env: ident, $iterator: expr) => {{
        let env_as_c_arg = $env.as_c_arg();
        let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

        let list = $iterator.rfold(acc, |acc, term| unsafe {
            list::make_list_cell(env_as_c_arg, term.as_c_arg(), acc)
        });

        unsafe { Term::new($env, list) }
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

macro_rules! unsafe_encode_date {
    ($v: ident, $date_struct_keys: ident, $calendar_iso_module: ident, $date_module: ident, $env: ident) => {{
        let dt = days_to_date($v);

        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $date_struct_keys,
                    &[
                        $date_module,
                        $calendar_iso_module,
                        dt.day().encode($env).as_c_arg(),
                        dt.month().encode($env).as_c_arg(),
                        dt.year().encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    }};
}

// Here we build the Date struct manually, as it's much faster than using Date NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
#[inline]
fn date_struct_keys(env: Env) -> [NIF_TERM; 5] {
    [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
    ]
}

#[inline]
fn encode_date(v: i32, env: Env) -> Result<Term, ExplorerError> {
    let date_struct_keys = &date_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let date_module = atoms::date_module().encode(env).as_c_arg();
    Ok(unsafe_encode_date!(
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
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let date_module = atoms::date_module().encode(env).as_c_arg();

    Ok(unsafe_iterator_series_to_list!(
        env,
        encode_chunked_array!(s.date()?, env, |date| unsafe_encode_date!(
            date,
            date_struct_keys,
            calendar_iso_module,
            date_module,
            env
        ))
    ))
}

macro_rules! unsafe_encode_naive_datetime {
    (
        $timestamp: expr,
        $time_unit: expr,
        $naive_datetime_struct_keys: ident,
        $calendar_iso_module: ident,
        $naive_datetime_module: ident,
        $env: ident
    ) => {{
        let ndt = timestamp_to_naive_datetime($timestamp, $time_unit);

        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $naive_datetime_struct_keys,
                    &[
                        $naive_datetime_module,
                        $calendar_iso_module,
                        ndt.day().encode($env).as_c_arg(),
                        ndt.month().encode($env).as_c_arg(),
                        ndt.year().encode($env).as_c_arg(),
                        ndt.hour().encode($env).as_c_arg(),
                        ndt.minute().encode($env).as_c_arg(),
                        ndt.second().encode($env).as_c_arg(),
                        ndt.microsecond_tuple_tu($time_unit).encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    }};
}

// Here we build the NaiveDateTime struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn naive_datetime_struct_keys(env: Env) -> [NIF_TERM; 9] {
    [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
        hour().encode(env).as_c_arg(),
        minute().encode(env).as_c_arg(),
        second().encode(env).as_c_arg(),
        microsecond().encode(env).as_c_arg(),
    ]
}

#[inline]
pub fn encode_naive_datetime(
    timestamp: i64,
    time_unit: TimeUnit,
    env: Env,
) -> Result<Term, ExplorerError> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();

    Ok(unsafe_encode_naive_datetime!(
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
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();
    let time_unit = match s.dtype() {
        DataType::Datetime(time_unit, None) => *time_unit,
        _ => panic!("should only use this function for naive datetimes"),
    };

    Ok(unsafe_iterator_series_to_list!(
        env,
        encode_chunked_array!(
            s.datetime()?,
            env,
            |timestamp| unsafe_encode_naive_datetime!(
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

macro_rules! unsafe_encode_datetime {
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

        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $datetime_struct_keys,
                    &[
                        $datetime_module,
                        $calendar_iso_module,
                        dt_tz.day().encode($env).as_c_arg(),
                        dt_tz.hour().encode($env).as_c_arg(),
                        dt_tz
                            .microsecond_tuple_tu($time_unit)
                            .encode($env)
                            .as_c_arg(),
                        dt_tz.minute().encode($env).as_c_arg(),
                        dt_tz.month().encode($env).as_c_arg(),
                        dt_tz.second().encode($env).as_c_arg(),
                        tz_offset.dst_offset().num_seconds().encode($env).as_c_arg(),
                        $time_zone.to_string().encode($env).as_c_arg(),
                        tz_offset
                            .base_utc_offset()
                            .num_seconds()
                            .encode($env)
                            .as_c_arg(),
                        dt_tz.year().encode($env).as_c_arg(),
                        tz_offset.abbreviation().encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    }};
}

// Here we build the DateTime struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn datetime_struct_keys(env: Env) -> [NIF_TERM; 13] {
    [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        hour().encode(env).as_c_arg(),
        microsecond().encode(env).as_c_arg(),
        minute().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        second().encode(env).as_c_arg(),
        std_offset().encode(env).as_c_arg(),
        time_zone().encode(env).as_c_arg(),
        utc_offset().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
        zone_abbr().encode(env).as_c_arg(),
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
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let datetime_module = atoms::datetime_module().encode(env).as_c_arg();

    Ok(unsafe_encode_datetime!(
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
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let datetime_module = atoms::datetime_module().encode(env).as_c_arg();
    let time_unit = match s.dtype() {
        DataType::Datetime(time_unit, Some(_)) => *time_unit,
        _ => panic!("datetime_series_to_list called on series with wrong type"),
    };
    let time_zone = match s.dtype() {
        DataType::Datetime(_, Some(time_zone)) => time_zone.parse::<Tz>().unwrap(),
        _ => panic!("datetime_series_to_list called on series with wrong type"),
    };

    Ok(unsafe_iterator_series_to_list!(
        env,
        encode_chunked_array!(s.datetime()?, env, |timestamp| unsafe_encode_datetime!(
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
macro_rules! unsafe_encode_duration {
    ($v: expr, $time_unit: expr, $duration_struct_keys: ident, $duration_module: ident, $env: ident) => {{
        let value = $v;
        let precision = time_unit_to_atom($time_unit);

        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $duration_struct_keys,
                    &[
                        $duration_module,
                        value.encode($env).as_c_arg(),
                        precision.encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    }};
}

// Here we build the Explorer.Duration struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn duration_struct_keys(env: Env) -> [NIF_TERM; 3] {
    [
        atom::__struct__().encode(env).as_c_arg(),
        value().encode(env).as_c_arg(),
        precision().encode(env).as_c_arg(),
    ]
}

#[inline]
pub fn encode_duration(v: i64, time_unit: TimeUnit, env: Env) -> Result<Term, ExplorerError> {
    let duration_struct_keys = &duration_struct_keys(env);
    let duration_module = atoms::duration_module().encode(env).as_c_arg();

    Ok(unsafe_encode_duration!(
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
    let duration_module = atoms::duration_module().encode(env).as_c_arg();

    Ok(unsafe_iterator_series_to_list!(
        env,
        encode_chunked_array!(s.duration()?, env, |duration| unsafe_encode_duration!(
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
macro_rules! unsafe_encode_decimal {
    ($v: expr, $scale: expr, $decimal_struct_keys: ident, $decimal_module: ident, $env: ident) => {{
        let coef = $v.abs();
        let scale = -($scale as isize);
        let sign = $v.signum();
        // Elixir's Decimal has only 1 or -1. We need to treat positive zero as positive - 1.
        let sign = if sign == 0 { 1 } else { sign };

        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $decimal_struct_keys,
                    &[
                        $decimal_module,
                        coef.encode($env).as_c_arg(),
                        scale.encode($env).as_c_arg(),
                        sign.encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    }};
}

// Here we build the Decimal struct manually, as it's much faster than using NifStruct
fn decimal_struct_keys(env: Env) -> [NIF_TERM; 4] {
    [
        atom::__struct__().encode(env).as_c_arg(),
        atoms::coef().encode(env).as_c_arg(),
        atoms::exp().encode(env).as_c_arg(),
        atoms::sign().encode(env).as_c_arg(),
    ]
}

#[inline]
pub fn encode_decimal(v: i128, scale: usize, env: Env) -> Result<Term, ExplorerError> {
    let struct_keys = &decimal_struct_keys(env);
    let module_atom = atoms::decimal_module().encode(env).as_c_arg();

    Ok(unsafe_encode_decimal!(
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
    let module_atom = atoms::decimal_module().encode(env).as_c_arg();
    let decimal_chunked = s.decimal()?;
    let scale = decimal_chunked.scale();

    Ok(unsafe_iterator_series_to_list!(
        env,
        encode_chunked_array!(decimal_chunked, env, |decimal| unsafe_encode_decimal!(
            decimal,
            scale,
            struct_keys,
            module_atom,
            env
        ))
    ))
}

// ######### End of Decimal ##########

macro_rules! unsafe_encode_time {
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

        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $naive_time_struct_keys,
                    &[
                        $time_module,
                        $calendar_iso_module,
                        t.hour().encode($env).as_c_arg(),
                        t.minute().encode($env).as_c_arg(),
                        t.second().encode($env).as_c_arg(),
                        (limited_ms, 6).encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    }};
}

// Here we build the NaiveTime struct manually, as it's much faster than using NifStruct
// This is because we already have the keys (we know this at compile time), and the types,
// so we can build the struct directly.
fn naive_time_struct_keys(env: Env) -> [NIF_TERM; 6] {
    [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        hour().encode(env).as_c_arg(),
        minute().encode(env).as_c_arg(),
        second().encode(env).as_c_arg(),
        microsecond().encode(env).as_c_arg(),
    ]
}

#[inline]
fn encode_time(v: i64, env: Env) -> Result<Term, ExplorerError> {
    let naive_time_struct_keys = &naive_time_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let time_module = atoms::time_module().encode(env).as_c_arg();

    Ok(unsafe_encode_time!(
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
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let time_module = atoms::time_module().encode(env).as_c_arg();

    Ok(unsafe_iterator_series_to_list!(
        env,
        encode_chunked_array!(s.time()?, env, |time| unsafe_encode_time!(
            time,
            naive_time_struct_keys,
            calendar_iso_module,
            time_module,
            env
        ))
    ))
}

fn generic_string_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    Ok(unsafe_iterator_series_to_list!(
        env,
        s.str()?.into_iter().map(|option| option.encode(env))
    ))
}

fn generic_binary_series_to_list<'b>(
    resource: &ResourceArc<ExSeriesRef>,
    s: &Series,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError> {
    let env_as_c_arg = env.as_c_arg();
    let nil_as_c_arg = atom::nil().to_term(env).as_c_arg();
    let acc = unsafe { list::make_list(env_as_c_arg, &[]) };
    let list = s.binary()?.downcast_iter().rfold(acc, |acc, array| {
        array.iter().rfold(acc, |acc, v| {
            let term_as_c_arg = match v {
                Some(values) => unsafe { resource.make_binary_unsafe(env, |_| values) }
                    .to_term(env)
                    .as_c_arg(),
                None => nil_as_c_arg,
            };
            unsafe { list::make_list_cell(env_as_c_arg, term_as_c_arg, acc) }
        })
    });
    Ok(unsafe { Term::new(env, list) })
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

            Ok(unsafe_iterator_series_to_list!(
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
        Ok(unsafe_iterator_series_to_list!(
            $env,
            $s.$convert_function()?
                .into_iter()
                .map(|option| option.encode($env))
        ))
    };
}

#[inline]
fn null_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let nil_as_c_arg = atom::nil().to_term(env).as_c_arg();
    let env_as_c_arg = env.as_c_arg();
    let mut list = unsafe { list::make_list(env_as_c_arg, &[]) };
    for _n in 0..s.len() {
        list = unsafe { list::make_list_cell(env_as_c_arg, nil_as_c_arg, list) }
    }
    Ok(unsafe { Term::new(env, list) })
}

macro_rules! series_to_iovec {
    ($resource:ident, $v:expr, $env:ident, $in_type:ty) => {{
        Ok(unsafe_iterator_series_to_list!(
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
