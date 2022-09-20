use crate::atoms;
use chrono::prelude::*;
use polars::prelude::*;
use rustler::{Atom, NifStruct, ResourceArc};
use std::convert::TryInto;

pub struct ExDataFrameRef(pub DataFrame);
pub struct ExExprRef(pub Expr);
pub struct ExLazyFrameRef(pub LazyFrame);
pub struct ExSeriesRef(pub Series);

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.DataFrame"]
pub struct ExDataFrame {
    pub resource: ResourceArc<ExDataFrameRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.Expression"]
pub struct ExExpr {
    pub resource: ResourceArc<ExExprRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.LazyDataFrame"]
pub struct ExLazyFrame {
    pub resource: ResourceArc<ExLazyFrameRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.Series"]
pub struct ExSeries {
    pub resource: ResourceArc<ExSeriesRef>,
}

impl ExDataFrameRef {
    pub fn new(df: DataFrame) -> Self {
        Self(df)
    }
}

impl ExExprRef {
    pub fn new(expr: Expr) -> Self {
        Self(expr)
    }
}

impl ExLazyFrameRef {
    pub fn new(df: LazyFrame) -> Self {
        Self(df)
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

impl ExExpr {
    pub fn new(expr: Expr) -> Self {
        Self {
            resource: ResourceArc::new(ExExprRef::new(expr)),
        }
    }
}

impl ExLazyFrame {
    pub fn new(df: LazyFrame) -> Self {
        Self {
            resource: ResourceArc::new(ExLazyFrameRef::new(df)),
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

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "Date"]
pub struct ExDate {
    pub calendar: Atom,
    pub day: u32,
    pub month: u32,
    pub year: i32,
}

impl From<i32> for ExDate {
    fn from(ts: i32) -> Self {
        days_to_date(ts).into()
    }
}

impl From<ExDate> for i32 {
    fn from(d: ExDate) -> i32 {
        NaiveDate::from_ymd(d.year, d.month, d.day)
            .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1))
            .num_days()
            .try_into()
            .unwrap()
    }
}

impl From<ExDate> for NaiveDate {
    fn from(d: ExDate) -> NaiveDate {
        NaiveDate::from_ymd(d.year, d.month, d.day)
    }
}

impl From<NaiveDate> for ExDate {
    fn from(d: NaiveDate) -> ExDate {
        ExDate {
            calendar: atoms::calendar_iso_module(),
            day: d.day(),
            month: d.month(),
            year: d.year(),
        }
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "NaiveDateTime"]
pub struct ExDateTime {
    pub calendar: Atom,
    pub day: u32,
    pub month: u32,
    pub year: i32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: (u32, u32),
}

pub use polars::export::arrow::temporal_conversions::date32_to_date as days_to_date;

/// Converts a microsecond i64 to a `NaiveDateTime`.
/// This is because when getting a timestamp, it might have negative values.
pub fn timestamp_to_datetime(microseconds: i64) -> NaiveDateTime {
    let sign = microseconds.signum();
    let seconds = match sign {
        -1 => microseconds / 1_000_000 - 1,
        _ => microseconds / 1_000_000,
    };
    let remainder = match sign {
        -1 => 1_000_000 + microseconds % 1_000_000,
        _ => microseconds % 1_000_000,
    };
    let nanoseconds = remainder.abs() * 1_000;
    NaiveDateTime::from_timestamp(seconds, nanoseconds.try_into().unwrap())
}

// Limit the number of digits in the microsecond part of a timestamp to 6.
// This is necessary because the microsecond part of Elixir is only 6 digits.
#[inline]
fn microseconds_six_digits(microseconds: u32) -> u32 {
    if microseconds > 999_999 {
        999_999
    } else {
        microseconds
    }
}

impl From<i64> for ExDateTime {
    fn from(microseconds: i64) -> Self {
        timestamp_to_datetime(microseconds).into()
    }
}

impl From<ExDateTime> for i64 {
    fn from(dt: ExDateTime) -> i64 {
        let duration = NaiveDate::from_ymd(dt.year, dt.month, dt.day)
            .and_hms_micro(dt.hour, dt.minute, dt.second, dt.microsecond.0)
            .signed_duration_since(NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0));

        match duration.num_microseconds() {
            Some(us) => us,
            None => duration.num_milliseconds() * 1_000,
        }
    }
}

impl From<ExDateTime> for NaiveDateTime {
    fn from(dt: ExDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd(dt.year, dt.month, dt.day).and_hms_micro(
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond.0,
        )
    }
}

impl From<NaiveDateTime> for ExDateTime {
    fn from(dt: NaiveDateTime) -> Self {
        ExDateTime {
            calendar: atoms::calendar_iso_module(),
            day: dt.day(),
            month: dt.month(),
            year: dt.year(),
            hour: dt.hour(),
            minute: dt.minute(),
            second: dt.second(),
            microsecond: (microseconds_six_digits(dt.timestamp_subsec_micros()), 6),
        }
    }
}
