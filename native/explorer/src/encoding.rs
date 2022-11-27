use chrono::prelude::*;
use polars::prelude::*;
use rustler::{Encoder, Env, OwnedBinary, ResourceArc, Term};
use std::{mem, slice};

use crate::atoms::{
    self, calendar, day, hour, infinity, microsecond, minute, month, nan, neg_infinity, second,
    year,
};
use crate::datatypes::{days_to_date, timestamp_to_datetime, ExSeries, ExSeriesRef};
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
                        (dt.timestamp_subsec_micros(), 6).encode($env).as_c_arg(),
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
fn time_to_microseconds(v: i64, time_unit: TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Milliseconds => v * 1000,
        TimeUnit::Microseconds => v,
        TimeUnit::Nanoseconds => (v as f64 * 0.001) as i64,
    }
}

#[inline]
fn encode_datetime(v: i64, time_unit: TimeUnit, env: Env) -> Result<Term, ExplorerError> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();
    let microseconds_time = time_to_microseconds(v, time_unit);

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
                let microseconds_time = time_to_microseconds(v, time_unit);

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

#[inline]
fn utf8_series_to_list<'b>(
    resource: &ResourceArc<ExSeriesRef>,
    s: &Series,
    env: Env<'b>,
) -> Result<Term<'b>, ExplorerError> {
    let utf8 = s.utf8()?;
    let env_as_c_arg = env.as_c_arg();
    let nil_as_c_arg = atom::nil().to_term(env).as_c_arg();
    let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

    let list = utf8.downcast_iter().rfold(acc, |acc, array| {
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

pub fn term_from_value<'b>(v: AnyValue, env: Env<'b>) -> Result<Term<'b>, ExplorerError> {
    match v {
        AnyValue::Null => Ok(None::<bool>.encode(env)),
        AnyValue::Boolean(v) => Ok(Some(v).encode(env)),
        AnyValue::Utf8(v) => Ok(Some(v).encode(env)),
        AnyValue::Int64(v) => Ok(Some(v).encode(env)),
        AnyValue::UInt32(v) => Ok(Some(v).encode(env)),
        AnyValue::Float64(v) => Ok(Some(v).encode(env)),
        AnyValue::Date(v) => encode_date(v, env),
        AnyValue::Datetime(v, time_unit, None) => encode_datetime(v, time_unit, env),
        dt => panic!("get/2 not implemented for {:?}", dt),
    }
}

pub fn list_from_series(data: ExSeries, env: Env) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;

    match s.dtype() {
        DataType::Boolean => series_to_list!(s, env, bool),
        DataType::Int64 => series_to_list!(s, env, i64),
        DataType::UInt32 => series_to_list!(s, env, u32),
        DataType::Utf8 => utf8_series_to_list(&data.resource, s, env),
        DataType::Float64 => float64_series_to_list(s, env),
        DataType::Date => date_series_to_list(s, env),
        DataType::Datetime(time_unit, None) => datetime_series_to_list(s, *time_unit, env),
        dt => panic!("to_list/1 not implemented for {:?}", dt),
    }
}

#[allow(clippy::size_of_in_element_count)]
pub fn iovec_from_series(data: ExSeries, env: Env) -> Result<Term, ExplorerError> {
    let s = &data.resource.0;
    let resource = &data.resource;

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
        DataType::UInt32 => series_to_iovec!(resource, s, env, u32, u32),
        DataType::Float64 => series_to_iovec!(resource, s, env, f64, f64),
        DataType::Utf8 => series_to_iovec!(resource, s, env, utf8, u8),
        DataType::Date => series_to_iovec!(resource, s, env, date, i32),
        DataType::Datetime(TimeUnit::Microseconds, None) => {
            series_to_iovec!(resource, s, env, datetime, i64)
        }
        dt => panic!("to_iovec/1 not implemented for {:?}", dt),
    }
}
