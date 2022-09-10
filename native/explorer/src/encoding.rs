use chrono::prelude::*;
use polars::export::arrow::bitmap::utils::zip_validity;
use polars::prelude::*;
use rustler::{Binary, Encoder, Env, NewBinary, Term};

use crate::atoms::{
    self, calendar, day, hour, infinity, microsecond, minute, month, nan, neg_infinity, second,
    year,
};
use crate::datatypes::{days_to_date, timestamp_to_datetime, ExSeriesRef};

use rustler::types::atom;
use rustler::wrapper::list;
use rustler::wrapper::{map, NIF_TERM};

pub fn term_from_value<'b>(v: AnyValue, env: Env<'b>) -> Term<'b> {
    match v {
        AnyValue::Null => None::<bool>.encode(env),
        AnyValue::Boolean(v) => Some(v).encode(env),
        AnyValue::Utf8(v) => Some(v).encode(env),
        AnyValue::Int8(v) => Some(v).encode(env),
        AnyValue::Int16(v) => Some(v).encode(env),
        AnyValue::Int32(v) => Some(v).encode(env),
        AnyValue::Int64(v) => Some(v).encode(env),
        AnyValue::UInt8(v) => Some(v).encode(env),
        AnyValue::UInt16(v) => Some(v).encode(env),
        AnyValue::UInt32(v) => Some(v).encode(env),
        AnyValue::UInt64(v) => Some(v).encode(env),
        AnyValue::Float64(v) => Some(v).encode(env),
        AnyValue::Float32(v) => Some(v).encode(env),
        AnyValue::Date(v) => encode_date(v, env),
        AnyValue::Datetime(v, time_unit, None) => encode_datetime(v, time_unit, env),
        dt => panic!("get/2 not implemented for {:?}", dt),
    }
}

// ExSeriesRef encoding

macro_rules! unsafe_encode_date {
    ($dt: ident, $date_struct_keys: ident, $calendar_iso_module: ident, $date_module: ident, $env: ident) => {
        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $date_struct_keys,
                    &[
                        $date_module,
                        $calendar_iso_module,
                        $dt.day().encode($env).as_c_arg(),
                        $dt.month().encode($env).as_c_arg(),
                        $dt.year().encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    };
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
fn encode_date(v: i32, env: Env) -> Term {
    let naive_date = days_to_date(v);
    let date_struct_keys = &date_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let date_module = atoms::date_module().encode(env).as_c_arg();
    unsafe_encode_date!(
        naive_date,
        date_struct_keys,
        calendar_iso_module,
        date_module,
        env
    )
}

#[inline]
fn encode_date_series<'b>(s: &Series, env: Env<'b>) -> Term<'b> {
    let date_struct_keys = &date_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let date_module = atoms::date_module().encode(env).as_c_arg();

    let env_as_c_arg = env.as_c_arg();
    let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

    let list = s.date().unwrap().into_iter().rfold(acc, |acc, option| {
        let item = option
            .map(|v| {
                let date = days_to_date(v);
                unsafe_encode_date!(
                    date,
                    date_struct_keys,
                    calendar_iso_module,
                    date_module,
                    env
                )
            })
            .encode(env)
            .as_c_arg();

        unsafe { list::make_list_cell(env_as_c_arg, item, acc) }
    });

    unsafe { Term::new(env, list) }
}

macro_rules! unsafe_encode_datetime {
    ($dt: ident, $naive_datetime_struct_keys: ident, $calendar_iso_module: ident, $naive_datetime_module: ident, $env: ident) => {
        unsafe {
            Term::new(
                $env,
                map::make_map_from_arrays(
                    $env.as_c_arg(),
                    $naive_datetime_struct_keys,
                    &[
                        $naive_datetime_module,
                        $calendar_iso_module,
                        $dt.day().encode($env).as_c_arg(),
                        $dt.month().encode($env).as_c_arg(),
                        $dt.year().encode($env).as_c_arg(),
                        $dt.hour().encode($env).as_c_arg(),
                        $dt.minute().encode($env).as_c_arg(),
                        $dt.second().encode($env).as_c_arg(),
                        ($dt.timestamp_subsec_micros(), 6).encode($env).as_c_arg(),
                    ],
                )
                .unwrap(),
            )
        }
    };
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
fn time_unit_to_factor(time_unit: TimeUnit) -> i64 {
    match time_unit {
        TimeUnit::Milliseconds => 1000,
        TimeUnit::Microseconds => 1,
        _ => unreachable!(),
    }
}

#[inline]
fn encode_datetime(v: i64, time_unit: TimeUnit, env: Env) -> Term {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();
    let factor = time_unit_to_factor(time_unit);
    let naive_datetime = timestamp_to_datetime(v * factor);
    unsafe_encode_datetime!(
        naive_datetime,
        naive_datetime_struct_keys,
        calendar_iso_module,
        naive_datetime_module,
        env
    )
}

#[inline]
fn encode_datetime_series<'b>(s: &Series, time_unit: TimeUnit, env: Env<'b>) -> Term<'b> {
    let naive_datetime_struct_keys = &naive_datetime_struct_keys(env);
    let calendar_iso_module = atoms::calendar_iso_module().encode(env).as_c_arg();
    let naive_datetime_module = atoms::naive_datetime_module().encode(env).as_c_arg();
    let factor = time_unit_to_factor(time_unit);

    let env_as_c_arg = env.as_c_arg();
    let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

    let list = s.datetime().unwrap().into_iter().rfold(acc, |acc, option| {
        let item = option
            .map(|x| {
                let naive_datetime = timestamp_to_datetime(x * factor);
                unsafe_encode_datetime!(
                    naive_datetime,
                    naive_datetime_struct_keys,
                    calendar_iso_module,
                    naive_datetime_module,
                    env
                )
            })
            .encode(env)
            .as_c_arg();

        unsafe { list::make_list_cell(env_as_c_arg, item, acc) }
    });

    unsafe { Term::new(env, list) }
}

#[inline]
fn encode_utf8_series<'b>(s: &Series, env: Env<'b>) -> Term<'b> {
    let utf8 = s.utf8().unwrap();
    let nil_atom = atom::nil().to_term(env);
    let mut items: Vec<NIF_TERM> = Vec::with_capacity(utf8.len());

    for array in utf8.downcast_iter() {
        // Create a binary per array buffer
        let values = array.values();
        let mut new_binary = NewBinary::new(env, values.len());
        new_binary.copy_from_slice(values.as_slice());
        let binary: Binary = new_binary.into();

        // Now allocate each string as a pointer to said binary
        let mut iter = array.offsets().iter();
        let mut last_offset = *iter.next().unwrap() as NIF_TERM;

        for wrapped_offset in zip_validity(iter, array.validity().as_ref().map(|x| x.iter())) {
            match wrapped_offset {
                Some(offset) => {
                    let uoffset = *offset as NIF_TERM;

                    items.push(
                        binary
                            .make_subbinary(last_offset, uoffset - last_offset)
                            .unwrap()
                            .to_term(env)
                            .as_c_arg(),
                    );

                    last_offset = uoffset
                }
                None => items.push(nil_atom.as_c_arg()),
            }
        }
    }

    unsafe { Term::new(env, list::make_list(env.as_c_arg(), &items)) }
}

// Convert f64 series taking into account NaN and Infinity floats (they are encoded as atoms).
#[inline]
fn encode_float64_series<'b>(s: &Series, env: Env<'b>) -> Term<'b> {
    let nan_atom = nan().encode(env);
    let neg_infinity_atom = neg_infinity().encode(env);
    let infinity_atom = infinity().encode(env);
    let nil_atom = atom::nil().encode(env);

    let env_as_c_arg = env.as_c_arg();
    let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

    let list = s.f64().unwrap().into_iter().rfold(acc, |acc, option| {
        let item = match option {
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
        .as_c_arg();

        unsafe { list::make_list_cell(env_as_c_arg, item, acc) }
    });

    unsafe { Term::new(env, list) }
}

macro_rules! encode {
    ($s:ident, $env:ident, $convert_function:ident) => {{
        let env_as_c_arg = $env.as_c_arg();
        let acc = unsafe { list::make_list(env_as_c_arg, &[]) };

        let list = $s
            .$convert_function()
            .unwrap()
            .into_iter()
            .rfold(acc, |acc, option| unsafe {
                list::make_list_cell(env_as_c_arg, option.encode($env).as_c_arg(), acc)
            });

        unsafe { Term::new($env, list) }
    }};
}

macro_rules! encode_list {
    ($s:ident, $env:ident, $convert_function:ident, $out_type:ty) => {
        $s.list()
            .unwrap()
            .into_iter()
            .map(|item| item)
            .collect::<Vec<Option<Series>>>()
            .iter()
            .map(|item| {
                item.clone()
                    .unwrap()
                    .$convert_function()
                    .unwrap()
                    .into_iter()
                    .map(|item| item)
                    .collect::<Vec<Option<$out_type>>>()
            })
            .collect::<Vec<Vec<Option<$out_type>>>>()
            .encode($env)
    };
}

impl Encoder for ExSeriesRef {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let s = &self.0;
        match s.dtype() {
            DataType::Boolean => encode!(s, env, bool),
            DataType::Int32 => encode!(s, env, i32),
            DataType::Int64 => encode!(s, env, i64),
            DataType::UInt8 => encode!(s, env, u8),
            DataType::UInt32 => encode!(s, env, u32),
            DataType::Utf8 => encode_utf8_series(s, env),
            DataType::Float64 => encode_float64_series(s, env),
            DataType::Date => encode_date_series(s, env),
            DataType::Datetime(time_unit, None) => encode_datetime_series(s, *time_unit, env),
            DataType::List(t) if t as &DataType == &DataType::UInt32 => {
                encode_list!(s, env, u32, u32)
            }
            dt => panic!("to_list/1 not implemented for {:?}", dt),
        }
    }
}
