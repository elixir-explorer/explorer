use chrono::prelude::*;
use polars::export::arrow::array::GenericBinaryArray;
use polars::prelude::*;
use rustler::{Encoder, Env, NewBinary, OwnedBinary, ResourceArc, Term};
use std::{mem, slice};

use crate::atoms::{
    self, calendar, day, hour, infinity, microsecond, minute, month, nan, neg_infinity, second,
    year,
};
use crate::datatypes::{
    days_to_date, time64ns_to_time, timestamp_to_datetime, ExSeries, ExSeriesRef,
};
use crate::ExplorerError;

use rustler::types::atom;
use rustler::wrapper::{binary, list, map, NIF_TERM};

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

fn generic_binary_series_to_list<'a, 'b, T, G>(
    resource: &ResourceArc<ExSeriesRef>,
    iter: T,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError>
where
    T: Iterator<Item = &'a G> + DoubleEndedIterator,
    G: GenericBinaryArray<i64>,
{
    let env_as_c_arg = env.as_c_arg();
    let nil_as_c_arg = atom::nil().to_term(env).as_c_arg();
    let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

    let list = iter.rfold(acc, |acc, array| {
        // Create a binary per array buffer
        let values = array.values();

        let binary = unsafe { resource.make_binary_unsafe(env, |_| values) }
            .to_term(env)
            .as_c_arg();

        // Offsets have one more element than values and validity,
        // so we read the last one as the initial accumulator and skip it.
        let len = array.offsets().len();
        let iter = array.offsets()[0..len - 1].iter();
        let mut last_offset = array.offsets()[len - 1] as NIF_TERM;

        let mut validity_iter = match array.validity() {
            Some(validity) => validity.iter(),
            None => polars::export::arrow::bitmap::utils::BitmapIter::new(&[], 0, 0),
        };

        iter.rfold(acc, |acc, uncast_offset| {
            let offset = *uncast_offset as NIF_TERM;

            let term_as_c_arg = if validity_iter.next_back().unwrap_or(true) {
                unsafe {
                    binary::make_subbinary(env_as_c_arg, binary, offset, last_offset - offset)
                }
            } else {
                nil_as_c_arg
            };

            last_offset = offset;
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

    let logical = s.categorical()?.logical();
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

// Convert f64 series taking into account NaN and Infinity floats (they are encoded as atoms).
#[inline]
fn float64_series_to_list<'b>(s: &Series, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    let nan_atom = nan().encode(env);
    let neg_infinity_atom = neg_infinity().encode(env);
    let infinity_atom = infinity().encode(env);
    let nil_atom = atom::nil().encode(env);

    Ok(unsafe_iterator_series_to_list!(
        env,
        s.f64()?.into_iter().map(|option| {
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

pub fn term_from_value<'b>(v: AnyValue, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    match v {
        AnyValue::Null => Ok(None::<bool>.encode(env)),
        AnyValue::Boolean(v) => Ok(Some(v).encode(env)),
        AnyValue::Utf8(v) => Ok(Some(v).encode(env)),
        AnyValue::Int64(v) => Ok(Some(v).encode(env)),
        AnyValue::Float64(v) => Ok(Some(term_from_float(v, env)).encode(env)),
        AnyValue::Date(v) => encode_date(v, env),
        AnyValue::Time(v) => encode_time(v, env),
        AnyValue::Datetime(v, time_unit, None) => encode_datetime(v, time_unit, env),
        AnyValue::Categorical(idx, mapping, _) => Ok(mapping.get(idx).encode(env)),
        dt => panic!("cannot encode value {dt:?} to term"),
    }
}

// Useful for series functions that can return float.
pub fn term_from_float(float: f64, env: Env<'_>) -> Term<'_> {
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

pub fn list_from_series(s: ExSeries, env: Env) -> Result<Term, ExplorerError> {
    match s.dtype() {
        DataType::Boolean => series_to_list!(s, env, bool),
        DataType::Int64 => series_to_list!(s, env, i64),
        DataType::Float64 => float64_series_to_list(&s, env),
        DataType::Date => date_series_to_list(&s, env),
        DataType::Time => time_series_to_list(&s, env),
        DataType::Datetime(time_unit, None) => datetime_series_to_list(&s, *time_unit, env),
        DataType::Utf8 => {
            generic_binary_series_to_list(&s.resource, s.utf8()?.downcast_iter(), env)
        }
        DataType::Binary => {
            generic_binary_series_to_list(&s.resource, s.binary()?.downcast_iter(), env)
        }
        DataType::Categorical(Some(mapping)) => categorical_series_to_list(&s, env, mapping),
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
        DataType::Int64 => series_to_iovec!(resource, s, env, i64, i64),
        DataType::Float64 => series_to_iovec!(resource, s, env, f64, f64),
        DataType::Date => series_to_iovec!(resource, s, env, date, i32),
        DataType::Time => series_to_iovec!(resource, s, env, time, i64),
        DataType::Datetime(TimeUnit::Microseconds, None) => {
            series_to_iovec!(resource, s, env, datetime, i64)
        }
        DataType::Categorical(Some(_)) => {
            let cat_series = s.cast(&DataType::UInt32)?;

            series_to_iovec!(resource, cat_series, env, u32, u32)
        }
        dt => panic!("to_iovec/1 not implemented for {dt:?}"),
    }
}
