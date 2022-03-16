use chrono::prelude::*;
use polars::prelude::*;
use rustler::resource::ResourceArc;
use rustler::{Atom, Encoder, Env, NifMap, NifStruct, Term};
use std::convert::TryInto;
use std::sync::RwLock;

use std::result::Result;

use crate::atoms;

pub struct ExDataFrameRef(pub RwLock<DataFrame>);
pub struct ExSeriesRef(pub Series);

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.DataFrame"]
pub struct ExDataFrame {
    pub resource: ResourceArc<ExDataFrameRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.Series"]
pub struct ExSeries {
    pub resource: ResourceArc<ExSeriesRef>,
}

impl ExDataFrameRef {
    pub fn new(df: DataFrame) -> Self {
        Self(RwLock::new(df))
    }
}

impl ExSeriesRef {
    pub fn new(s: Series) -> Self {
        Self(s)
    }
}

impl ExDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self {
            resource: ResourceArc::new(ExDataFrameRef::new(df)),
        }
    }
}

impl ExSeries {
    pub fn new(s: Series) -> Self {
        Self {
            resource: ResourceArc::new(ExSeriesRef::new(s)),
        }
    }
}

#[derive(NifMap)]
pub struct ExDate {
    pub __struct__: Atom,
    pub calendar: Atom,
    pub day: u32,
    pub month: u32,
    pub year: i32,
}

impl ExDate {
    fn new_from_chrono(d: NaiveDate) -> Self {
        ExDate {
            __struct__: atoms::date(),
            calendar: atoms::calendar(),
            day: d.day(),
            month: d.month(),
            year: d.year(),
        }
    }

    pub fn new_from_days(ts: i32) -> Self {
        let seconds = ts * 86_400;
        let dt = NaiveDateTime::from_timestamp(seconds.into(), 0);
        ExDate::new_from_chrono(NaiveDate::from_yo(dt.year(), dt.ordinal()))
    }

    pub fn to_days(&self) -> i32 {
        NaiveDate::from_ymd(self.year, self.month, self.day)
            .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1))
            .num_days()
            .try_into()
            .unwrap()
    }
}

fn encode_date<'b>(s: &Series, env: Env<'b>) -> Term<'b> {
    s.date()
        .unwrap()
        .as_date_iter()
        .map(|d| d.map(ExDate::new_from_chrono))
        .collect::<Vec<Option<ExDate>>>()
        .encode(env)
}

#[derive(NifMap)]
pub struct ExDateTime {
    pub __struct__: Atom,
    pub calendar: Atom,
    pub day: u32,
    pub month: u32,
    pub year: i32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: (u32, u32),
}

impl ExDateTime {
    fn new_from_chrono(dt: NaiveDateTime) -> Self {
        ExDateTime {
            __struct__: atoms::datetime(),
            calendar: atoms::calendar(),
            day: dt.day(),
            month: dt.month(),
            year: dt.year(),
            hour: dt.hour(),
            minute: dt.minute(),
            second: dt.second(),
            microsecond: (dt.timestamp_subsec_micros(), 3),
        }
    }

    pub fn new_from_milliseconds(ms: i64) -> Self {
        let sign = ms.signum();
        let seconds = match sign {
            -1 => ms / 1_000 - 1,
            _ => ms / 1_000,
        };
        let remainder = match sign {
            -1 => 1_000 + ms % 1_000,
            _ => ms % 1_000,
        };
        let nanoseconds = remainder.abs() * 1_000_000;
        ExDateTime::new_from_chrono(NaiveDateTime::from_timestamp(
            seconds,
            nanoseconds.try_into().unwrap(),
        ))
    }

    pub fn to_milliseconds(&self) -> i64 {
        NaiveDate::from_ymd(self.year, self.month, self.day)
            .and_hms_micro(self.hour, self.minute, self.second, self.microsecond.0)
            .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0))
            .num_milliseconds()
    }
}

fn encode_datetime<'b>(s: &Series, env: Env<'b>) -> Term<'b> {
    s.datetime()
        .unwrap()
        .into_iter()
        .map(|d| d.map(ExDateTime::new_from_milliseconds))
        .collect::<Vec<Option<ExDateTime>>>()
        .encode(env)
}

macro_rules! encode {
    ($s:ident, $env:ident, $convert_function:ident, $out_type:ty) => {
        $s.$convert_function()
            .unwrap()
            .into_iter()
            .map(|item| item)
            .collect::<Vec<Option<$out_type>>>()
            .encode($env)
    };
    ($s:ident, $env:ident, $convert_function:ident) => {
        $s.$convert_function()
            .unwrap()
            .into_iter()
            .map(|item| item)
            .collect::<Vec<Option<$convert_function>>>()
            .encode($env)
    };
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

impl<'a> Encoder for ExSeriesRef {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let s = &self.0;
        match s.dtype() {
            DataType::Boolean => encode!(s, env, bool),
            DataType::Utf8 => encode!(s, env, utf8, &str),
            DataType::Int32 => encode!(s, env, i32),
            DataType::Int64 => encode!(s, env, i64),
            DataType::UInt32 => encode!(s, env, u32),
            DataType::Float64 => encode!(s, env, f64),
            DataType::Date => encode_date(s, env),
            DataType::Datetime(TimeUnit::Milliseconds, None) => encode_datetime(s, env),
            DataType::List(t) if t as &DataType == &DataType::UInt32 => {
                encode_list!(s, env, u32, u32)
            }
            dt => panic!("to_list/1 not implemented for {:?}", dt),
        }
    }
}
