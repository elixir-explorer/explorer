use chrono::prelude::*;
use polars::prelude::*;
use rustler::{Encoder, Env, NewBinary, OwnedBinary, ResourceArc, Term};
use std::collections::HashMap;
use std::{mem, slice};

use crate::atoms::{
    self, calendar, day, hour, infinity, microsecond, millisecond, minute, month, nan, nanosecond,
    neg_infinity, precision, second, value, year,
};
use crate::datatypes::{
    days_to_date, time64ns_to_time, timestamp_to_datetime, ExSeries, ExSeriesRef,
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
    return [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
    ];
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
        s.date()?.into_iter().map(|option| option
            .map(|v| unsafe_encode_date!(
                v,
                date_struct_keys,
                calendar_iso_module,
                date_module,
                env
            ))
            .encode(env))
    ))
}

macro_rules! unsafe_encode_datetime {
    ($v: expr, $naive_datetime_struct_keys: ident, $calendar_iso_module: ident, $naive_datetime_module: ident, $env: ident) => {{
        let dt = timestamp_to_datetime($v);
        let microseconds = dt.timestamp_subsec_micros();

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
                    $naive_datetime_struct_keys,
                    &[
                        $naive_datetime_module,
                        $calendar_iso_module,
                        dt.day().encode($env).as_c_arg(),
                        dt.month().encode($env).as_c_arg(),
                        dt.year().encode($env).as_c_arg(),
                        dt.hour().encode($env).as_c_arg(),
                        dt.minute().encode($env).as_c_arg(),
                        dt.second().encode($env).as_c_arg(),
                        (limited_ms, 6).encode($env).as_c_arg(),
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
    return [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        day().encode(env).as_c_arg(),
        month().encode(env).as_c_arg(),
        year().encode(env).as_c_arg(),
        hour().encode(env).as_c_arg(),
        minute().encode(env).as_c_arg(),
        second().encode(env).as_c_arg(),
        microsecond().encode(env).as_c_arg(),
    ];
}

#[inline]
fn datetime_to_microseconds(v: i64, time_unit: TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Milliseconds => v * 1000,
        TimeUnit::Microseconds => v,
        TimeUnit::Nanoseconds => (v as f64 * 0.001) as i64,
    }
}

#[inline]
pub fn encode_datetime(v: i64, time_unit: TimeUnit, env: Env) -> Result<Term, ExplorerError> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();
    let microseconds_time = datetime_to_microseconds(v, time_unit);

    Ok(unsafe_encode_datetime!(
        microseconds_time,
        naive_datetime_struct_keys,
        calendar_iso_module,
        naive_datetime_module,
        env
    ))
}

#[inline]
fn datetime_series_to_list<'b>(
    s: &Series,
    time_unit: TimeUnit,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();

    Ok(unsafe_iterator_series_to_list!(
        env,
        s.datetime()?.into_iter().map(|option| option
            .map(|v| {
                let microseconds_time = datetime_to_microseconds(v, time_unit);

                unsafe_encode_datetime!(
                    microseconds_time,
                    naive_datetime_struct_keys,
                    calendar_iso_module,
                    naive_datetime_module,
                    env
                )
            })
            .encode(env))
    ))
}

fn time_unit_to_atom(time_unit: TimeUnit) -> atom::Atom {
    match time_unit {
        TimeUnit::Milliseconds => millisecond(),
        TimeUnit::Microseconds => microsecond(),
        TimeUnit::Nanoseconds => nanosecond(),
    }
}

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
    return [
        atom::__struct__().encode(env).as_c_arg(),
        value().encode(env).as_c_arg(),
        precision().encode(env).as_c_arg(),
    ];
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
        s.duration()?.into_iter().map(|option| option
            .map(|v| {
                unsafe_encode_duration!(v, time_unit, duration_struct_keys, duration_module, env)
            })
            .encode(env))
    ))
}

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
    return [
        atom::__struct__().encode(env).as_c_arg(),
        calendar().encode(env).as_c_arg(),
        hour().encode(env).as_c_arg(),
        minute().encode(env).as_c_arg(),
        second().encode(env).as_c_arg(),
        microsecond().encode(env).as_c_arg(),
    ];
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
        s.time()?.into_iter().map(|option| option
            .map(|v| {
                unsafe_encode_time!(
                    v,
                    naive_time_struct_keys,
                    calendar_iso_module,
                    time_module,
                    env
                )
            })
            .encode(env))
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

fn categorical_series_to_list<'b>(
    s: &Series,
    env: Env<'b>,
    mapping: &Arc<RevMapping>,
) -> Result<Term<'b>, ExplorerError> {
    let env_as_c_arg = env.as_c_arg();
    let nil_as_c_arg = atom::nil().to_term(env).as_c_arg();
    let mut list = unsafe { list::make_list(env_as_c_arg, &[]) };

    let logical = s.categorical()?.physical();
    let cat_size = mapping.len();
    let mut terms: Vec<NIF_TERM> = vec![nil_as_c_arg; cat_size];

    for (index, term) in terms.iter_mut().enumerate() {
        if let Some(existing_str) = mapping.get_optional(index as u32) {
            let mut binary = NewBinary::new(env, existing_str.len());
            binary.copy_from_slice(existing_str.as_bytes());

            let binary_term: Term = binary.into();

            *term = binary_term.as_c_arg();
        }
    }

    for maybe_index in &logical.reverse() {
        let term_ref = match maybe_index {
            Some(index) if index < (cat_size as u32) => terms[index as usize],
            _ => nil_as_c_arg,
        };

        list = unsafe { list::make_list_cell(env_as_c_arg, term_ref, list) }
    }

    Ok(unsafe { Term::new(env, list) })
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
    ($resource:ident, $s:ident, $env:ident, $convert_function:ident, $in_type:ty) => {{
        Ok(unsafe_iterator_series_to_list!(
            $env,
            $s.$convert_function()?.downcast_iter().map(|array| {
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
        _ => term_from_value(v, env),
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

pub fn term_from_value<'b>(v: AnyValue, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    match v {
        AnyValue::Null => Ok(None::<bool>.encode(env)),
        AnyValue::Boolean(v) => Ok(Some(v).encode(env)),
        AnyValue::String(v) => Ok(Some(v).encode(env)),
        AnyValue::Int8(v) => Ok(Some(v).encode(env)),
        AnyValue::Int16(v) => Ok(Some(v).encode(env)),
        AnyValue::Int32(v) => Ok(Some(v).encode(env)),
        AnyValue::Int64(v) => Ok(Some(v).encode(env)),
        AnyValue::UInt8(v) => Ok(Some(v).encode(env)),
        AnyValue::UInt16(v) => Ok(Some(v).encode(env)),
        AnyValue::UInt32(v) => Ok(Some(v).encode(env)),
        AnyValue::UInt64(v) => Ok(Some(v).encode(env)),
        AnyValue::Float32(v) => Ok(Some(term_from_float32(v, env)).encode(env)),
        AnyValue::Float64(v) => Ok(Some(term_from_float64(v, env)).encode(env)),
        AnyValue::Date(v) => encode_date(v, env),
        AnyValue::Time(v) => encode_time(v, env),
        AnyValue::Datetime(v, time_unit, None) => encode_datetime(v, time_unit, env),
        AnyValue::Duration(v, time_unit) => encode_duration(v, time_unit, env),
        AnyValue::Categorical(idx, mapping, _) => Ok(mapping.get(idx).encode(env)),
        AnyValue::List(series) => list_from_series(ExSeries::new(series), env),
        AnyValue::Struct(_, _, fields) => v
            ._iter_struct_av()
            .zip(fields)
            .map(|(value, field)| Ok((field.name.as_str(), term_from_value(value, env)?)))
            .collect::<Result<HashMap<_, _>, ExplorerError>>()
            .map(|map| map.encode(env)),
        dt => panic!("cannot encode value {dt:?} to term"),
    }
}

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
        DataType::Datetime(time_unit, None) => datetime_series_to_list(&s, *time_unit, env),
        DataType::Duration(time_unit) => duration_series_to_list(&s, *time_unit, env),
        DataType::Binary => generic_binary_series_to_list(&s.resource, &s, env),
        DataType::String => generic_string_series_to_list(&s, env),
        DataType::Categorical(Some(mapping), _) => categorical_series_to_list(&s, env, mapping),
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
            .map(|value| term_from_value(value, env))
            .collect::<Result<Vec<_>, ExplorerError>>()
            .map(|values| values.encode(env)),
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
        DataType::Int8 => series_to_iovec!(resource, s, env, i8, i8),
        DataType::Int16 => series_to_iovec!(resource, s, env, i16, i16),
        DataType::Int32 => series_to_iovec!(resource, s, env, i32, i32),
        DataType::Int64 => series_to_iovec!(resource, s, env, i64, i64),
        DataType::UInt8 => series_to_iovec!(resource, s, env, u8, u8),
        DataType::UInt16 => series_to_iovec!(resource, s, env, u16, u16),
        DataType::UInt32 => series_to_iovec!(resource, s, env, u32, u32),
        DataType::UInt64 => series_to_iovec!(resource, s, env, u64, u64),
        DataType::Float32 => series_to_iovec!(resource, s, env, f32, f32),
        DataType::Float64 => series_to_iovec!(resource, s, env, f64, f64),
        DataType::Date => series_to_iovec!(resource, s, env, date, i32),
        DataType::Time => series_to_iovec!(resource, s, env, time, i64),
        DataType::Datetime(_, None) => {
            series_to_iovec!(resource, s, env, datetime, i64)
        }
        DataType::Duration(_) => {
            series_to_iovec!(resource, s, env, duration, i64)
        }
        DataType::Categorical(Some(_), _) => {
            let cat_series = s.cast(&DataType::UInt32)?;

            series_to_iovec!(resource, cat_series, env, u32, u32)
        }
        dt => panic!("to_iovec/1 not implemented for {dt:?}"),
    }
}
