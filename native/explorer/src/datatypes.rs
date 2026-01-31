mod ex_dtypes;

use crate::atoms;
use crate::ExplorerError;
use chrono::prelude::*;

#[cfg(feature = "cloud")]
use polars::prelude::cloud::CloudOptions;
use polars::prelude::*;
use rustler::{Atom, NifStruct, NifTaggedEnum, Resource, ResourceArc};
use std::fmt;
use std::ops::Deref;

#[cfg(feature = "aws")]
use std::str::FromStr;

#[cfg(feature = "aws")]
use polars::prelude::cloud::AmazonS3ConfigKey as S3Key;

use chrono_tz::{OffsetComponents, OffsetName, Tz};

pub use polars_arrow::datatypes::TimeUnit as ArrowTimeUnit;

pub use polars_arrow::temporal_conversions::{
    date32_to_date as days_to_date, timestamp_ms_to_datetime as timestamp_ms_to_naive_datetime,
    timestamp_ns_to_datetime as timestamp_ns_to_naive_datetime,
    timestamp_to_datetime as arrow_timestamp_to_datetime,
    timestamp_us_to_datetime as timestamp_us_to_naive_datetime,
};

pub use ex_dtypes::*;

pub struct ExDataFrameRef(pub DataFrame);

#[rustler::resource_impl]
impl Resource for ExDataFrameRef {}

pub struct ExExprRef(pub Expr);

#[rustler::resource_impl]
impl Resource for ExExprRef {}

pub struct ExLazyFrameRef(pub LazyFrame);

#[rustler::resource_impl]
impl Resource for ExLazyFrameRef {}

pub struct ExSeriesRef(pub Series);

#[rustler::resource_impl]
impl Resource for ExSeriesRef {}

// The structs that start with "Ex" are related to the modules in Elixir.
// Some of them are just wrappers around Polars data structs.
// For example, a "ExDataFrame" is a wrapper around Polars' "DataFrame".
//
// In order to facilitate the usage of these structs, we implement the
// "Deref" trait that points to the Polars' equivalent.
//
// See a detailed explanation in this chapter of the Rust Book:
// https://doc.rust-lang.org/nightly/book/ch15-02-deref.html
#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.DataFrame"]
pub struct ExDataFrame {
    pub resource: ResourceArc<ExDataFrameRef>,
}

impl std::panic::RefUnwindSafe for ExDataFrame {}

#[derive(NifStruct, Clone)]
#[module = "Explorer.PolarsBackend.Expression"]
pub struct ExExpr {
    pub resource: ResourceArc<ExExprRef>,
}

impl std::panic::RefUnwindSafe for ExExpr {}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.LazyFrame"]
pub struct ExLazyFrame {
    pub resource: ResourceArc<ExLazyFrameRef>,
}

impl std::panic::RefUnwindSafe for ExLazyFrame {}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.Series"]
pub struct ExSeries {
    pub resource: ResourceArc<ExSeriesRef>,
}

impl std::panic::RefUnwindSafe for ExSeries {}

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

    // Returns a clone of the DataFrame inside the ResourceArc container.
    pub fn clone_inner(&self) -> DataFrame {
        self.resource.0.clone()
    }
}

// Implement Deref so we can call `DataFrame` functions directly from a `ExDataFrame` struct.
impl Deref for ExDataFrame {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
    }
}

impl ExExpr {
    pub fn new(expr: Expr) -> Self {
        Self {
            resource: ResourceArc::new(ExExprRef::new(expr)),
        }
    }

    // Returns a clone of the Expr inside the ResourceArc container.
    pub fn clone_inner(&self) -> Expr {
        self.resource.0.clone()
    }
}

// Implement Deref so we can call `Expr` functions directly from a `ExExpr` struct.
impl Deref for ExExpr {
    type Target = Expr;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
    }
}

impl ExLazyFrame {
    pub fn new(df: LazyFrame) -> Self {
        Self {
            resource: ResourceArc::new(ExLazyFrameRef::new(df)),
        }
    }

    // Returns a clone of the LazyFrame inside the ResourceArc container.
    pub fn clone_inner(&self) -> LazyFrame {
        self.resource.0.clone()
    }
}

// Implement Deref so we can call `LazyFrame` functions directly from a `ExLazyFrame` struct.
impl Deref for ExLazyFrame {
    type Target = LazyFrame;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
    }
}

impl ExSeries {
    pub fn new(s: Series) -> Self {
        Self {
            resource: ResourceArc::new(ExSeriesRef::new(s)),
        }
    }

    // Returns a clone of the Series inside the ResourceArc container.
    pub fn clone_inner(&self) -> Series {
        self.resource.0.clone()
    }
}

// Implement Deref so we can call `Series` functions directly from a `ExSeries` struct.
impl Deref for ExSeries {
    type Target = Series;

    fn deref(&self) -> &Self::Target {
        &self.resource.0
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
        NaiveDate::from_ymd_opt(d.year, d.month, d.day)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days()
            .try_into()
            .unwrap()
    }
}

impl From<ExDate> for NaiveDate {
    fn from(d: ExDate) -> NaiveDate {
        NaiveDate::from_ymd_opt(d.year, d.month, d.day).unwrap()
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

impl Literal for ExDate {
    fn lit(self) -> Expr {
        NaiveDate::from(self).lit().dt().date()
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "Explorer.Duration"]
pub struct ExDuration {
    pub value: i64,
    pub precision: Atom,
}

impl From<ExDuration> for i64 {
    fn from(d: ExDuration) -> i64 {
        d.value
    }
}

impl Literal for ExDuration {
    fn lit(self) -> Expr {
        // Note: it's tempting to use `.lit()` on a `chrono::Duration` struct in this function, but
        // doing so will lose precision information as `chrono::Duration`s have no time units.
        Expr::Literal(LiteralValue::Scalar(Scalar::new_duration(
            self.value,
            time_unit_of_ex_duration(&self),
        )))
    }
}

fn time_unit_of_ex_duration(duration: &ExDuration) -> TimeUnit {
    let precision = duration.precision;
    if precision == atoms::millisecond() {
        TimeUnit::Milliseconds
    } else if precision == atoms::microsecond() {
        TimeUnit::Microseconds
    } else if precision == atoms::nanosecond() {
        TimeUnit::Nanoseconds
    } else {
        panic!("unrecognized precision: {precision:?}")
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "NaiveDateTime"]
pub struct ExNaiveDateTime {
    pub calendar: Atom,
    pub day: u32,
    pub hour: u32,
    pub microsecond: (u32, u32),
    pub minute: u32,
    pub month: u32,
    pub second: u32,
    pub year: i32,
}

pub fn timestamp_to_datetime(
    timestamp: i64,
    time_unit: TimeUnit,
    time_zone: Tz,
) -> chrono::DateTime<Tz> {
    let arrow_time_unit = convert_to_arrow_time_unit(time_unit);
    arrow_timestamp_to_datetime(timestamp, arrow_time_unit, &time_zone)
}

// Note: arrow has a native datetime analog to `timestamp_to_datetime` but it's
// private, so we're adapting its internals here.
pub fn timestamp_to_naive_datetime(timestamp: i64, time_unit: TimeUnit) -> NaiveDateTime {
    match time_unit {
        TimeUnit::Milliseconds => timestamp_ms_to_naive_datetime(timestamp),
        TimeUnit::Microseconds => timestamp_us_to_naive_datetime(timestamp),
        TimeUnit::Nanoseconds => timestamp_ns_to_naive_datetime(timestamp),
    }
}

fn convert_to_arrow_time_unit(time_unit: TimeUnit) -> ArrowTimeUnit {
    match time_unit {
        TimeUnit::Milliseconds => ArrowTimeUnit::Millisecond,
        TimeUnit::Microseconds => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanoseconds => ArrowTimeUnit::Nanosecond,
    }
}

/// Trait for the fractional part of seconds for Elixir's time-related structs.
pub trait ExMicrosecondTuple {
    /// Get the sub-second part.
    fn subsec_micros(&self) -> u32;

    /// Round the sub-second part to a max of 6 digits.
    fn subsec_micros_limited(&self) -> u32 {
        // In the event of a leap second, `subsec_micros()` may exceed 999,999.
        self.subsec_micros().min(999_999_u32)
    }

    /// Default representation.
    fn microsecond_tuple(&self) -> (u32, u32) {
        (self.subsec_micros_limited(), 6)
    }

    /// Representation rounded for specific time-units.
    fn microsecond_tuple_tu(&self, time_unit: TimeUnit) -> (u32, u32) {
        let us = self.subsec_micros_limited();

        match time_unit {
            // If Polars implements `TimeUnit::Seconds`, we'd return `(0, 0)`.
            TimeUnit::Microseconds | TimeUnit::Nanoseconds => (us, 6),
            TimeUnit::Milliseconds => (((us / 1_000) * 1_000), 3),
        }
    }
}

impl ExMicrosecondTuple for NaiveTime {
    fn subsec_micros(&self) -> u32 {
        self.nanosecond() / 1_000
    }
}

impl ExMicrosecondTuple for NaiveDateTime {
    fn subsec_micros(&self) -> u32 {
        self.and_utc().timestamp_subsec_micros()
    }
}

impl ExMicrosecondTuple for DateTime<Utc> {
    fn subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }
}

impl ExMicrosecondTuple for DateTime<Tz> {
    fn subsec_micros(&self) -> u32 {
        self.timestamp_subsec_micros()
    }
}

pub fn ex_naive_datetime_to_timestamp(
    ex_naive_datetime: ExNaiveDateTime,
    time_unit: TimeUnit,
) -> Result<i64, ExplorerError> {
    let naive_datetime = NaiveDateTime::from(ex_naive_datetime);

    match time_unit {
        TimeUnit::Milliseconds => Ok(naive_datetime.and_utc().timestamp_millis()),
        TimeUnit::Microseconds => Ok(naive_datetime.and_utc().timestamp_micros()),
        TimeUnit::Nanoseconds => naive_datetime.and_utc().timestamp_nanos_opt().ok_or(
            ExplorerError::TimestampConversion(
                "cannot represent naive datetime(ns) with `i64`".to_string(),
            ),
        ),
    }
}

impl From<i64> for ExNaiveDateTime {
    fn from(timestamp: i64) -> Self {
        timestamp_to_naive_datetime(timestamp, TimeUnit::Microseconds).into()
    }
}

impl From<ExNaiveDateTime> for NaiveDateTime {
    fn from(dt: ExNaiveDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day)
            .unwrap()
            .and_hms_micro_opt(dt.hour, dt.minute, dt.second, dt.microsecond.0)
            .unwrap()
    }
}

impl From<NaiveDateTime> for ExNaiveDateTime {
    fn from(dt: NaiveDateTime) -> Self {
        ExNaiveDateTime {
            calendar: atoms::calendar_iso_module(),
            day: dt.day(),
            month: dt.month(),
            year: dt.year(),
            hour: dt.hour(),
            minute: dt.minute(),
            second: dt.second(),
            microsecond: dt.microsecond_tuple(),
        }
    }
}

impl Literal for ExNaiveDateTime {
    fn lit(self) -> Expr {
        let ndt = NaiveDateTime::from(self);
        // We can't call `ndt.lit()` because Polars defaults to nanoseconds
        // for all years between 1386 and 2554, but Elixir only represents
        // microsecond precision natively. This code is copied from the
        // microsecond branch of their `.lit()` implementation.
        //
        // See here for details:
        // polars-time-0.38.3/src/date_range.rs::in_nanoseconds_window
        Expr::Literal(LiteralValue::Scalar(Scalar::new_datetime(
            ndt.and_utc().timestamp_micros(),
            TimeUnit::Microseconds,
            None,
        )))
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "DateTime"]
pub struct ExDateTime<'a> {
    pub calendar: Atom,
    pub day: u32,
    pub hour: u32,
    pub microsecond: (u32, u32),
    pub minute: u32,
    pub month: u32,
    pub second: u32,
    pub std_offset: i64,
    pub time_zone: &'a str,
    pub utc_offset: i64,
    pub year: i32,
    pub zone_abbr: &'a str,
}

pub fn ex_datetime_to_timestamp(
    ex_datetime: ExDateTime,
    time_unit: TimeUnit,
) -> Result<i64, ExplorerError> {
    let datetime = DateTime::from(ex_datetime);

    match time_unit {
        TimeUnit::Milliseconds => Ok(datetime.timestamp_millis()),
        TimeUnit::Microseconds => Ok(datetime.timestamp_micros()),
        TimeUnit::Nanoseconds => {
            datetime
                .timestamp_nanos_opt()
                .ok_or(ExplorerError::TimestampConversion(
                    "cannot represent datetime(ns) with `i64`".to_string(),
                ))
        }
    }
}

impl<'a> From<&'a DateTime<Tz>> for ExDateTime<'a> {
    fn from(dt_tz: &'a DateTime<Tz>) -> ExDateTime<'a> {
        let time_zone = dt_tz.offset().tz_id();
        let zone_abbr = dt_tz.offset().abbreviation();

        ExDateTime {
            calendar: atoms::calendar_iso_module(),
            day: dt_tz.day(),
            hour: dt_tz.hour(),
            microsecond: dt_tz.microsecond_tuple(),
            minute: dt_tz.minute(),
            month: dt_tz.month(),
            second: dt_tz.second(),
            std_offset: dt_tz.offset().dst_offset().num_seconds(),
            time_zone,
            utc_offset: dt_tz.offset().base_utc_offset().num_seconds(),
            year: dt_tz.year(),
            zone_abbr: zone_abbr.expect("expecting a valid zone abbr"),
        }
    }
}

impl From<ExDateTime<'_>> for DateTime<Tz> {
    fn from(ex_dt: ExDateTime<'_>) -> DateTime<Tz> {
        let time_zone = ex_dt.time_zone.parse::<Tz>().unwrap();

        // Best approach I could find to avoid warning:
        // https://github.com/chronotope/chrono/issues/873#issuecomment-1333716953
        let dt_tz_without_micro = time_zone
            .with_ymd_and_hms(
                ex_dt.year,
                ex_dt.month,
                ex_dt.day,
                ex_dt.hour,
                ex_dt.minute,
                ex_dt.second,
            )
            .unwrap();
        let micro = chrono::Duration::microseconds(ex_dt.microsecond.0.into());
        dt_tz_without_micro + micro
    }
}

impl From<ExDateTime<'_>> for NaiveDateTime {
    fn from(dt: ExDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day)
            .unwrap()
            .and_hms_micro_opt(dt.hour, dt.minute, dt.second, dt.microsecond.0)
            .unwrap()
    }
}

impl Literal for ExDateTime<'_> {
    fn lit(self) -> Expr {
        let ndt = NaiveDateTime::from(self);

        Expr::Literal(LiteralValue::Scalar(Scalar::new_datetime(
            ndt.and_utc().timestamp_micros(),
            TimeUnit::Microseconds,
            polars::prelude::TimeZone::opt_try_new(Some(self.time_zone)).unwrap(),
        )))
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "Time"]
pub struct ExTime {
    pub calendar: Atom,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: (u32, u32),
}

pub use polars_arrow::temporal_conversions::time64ns_to_time;

impl From<i64> for ExTime {
    fn from(nanoseconds: i64) -> Self {
        time64ns_to_time(nanoseconds).into()
    }
}

// In Polars, Time is represented as an i64 in nanoseconds.
// Since we don't have nanoseconds precision in Elixir,
// we just ignore the extra precision when is available.
impl From<ExTime> for i64 {
    fn from(t: ExTime) -> i64 {
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let duration = NaiveTime::from_hms_micro_opt(t.hour, t.minute, t.second, t.microsecond.0)
            .unwrap()
            .signed_duration_since(midnight);

        match duration.num_microseconds() {
            Some(us) => us * 1000,
            None => duration.num_milliseconds() * 1_000 * 1000,
        }
    }
}

impl From<ExTime> for NaiveTime {
    fn from(t: ExTime) -> NaiveTime {
        NaiveTime::from_hms_micro_opt(t.hour, t.minute, t.second, t.microsecond.0).unwrap()
    }
}

impl From<NaiveTime> for ExTime {
    fn from(t: NaiveTime) -> Self {
        ExTime {
            calendar: atoms::calendar_iso_module(),
            hour: t.hour(),
            minute: t.minute(),
            second: t.second(),
            microsecond: t.microsecond_tuple(),
        }
    }
}

impl Literal for ExTime {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Time,
            AnyValue::Time(self.into()),
        )))
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "Decimal"]
pub struct ExDecimal {
    pub sign: i8,
    // `coef` is a positive, arbitrary precision integer on the Elixir side.
    // It's convenient to represent it here as a signed `i128` because that's
    // what the Decimal dtype expects. While you could technically create an
    // `ExDecimal` struct with a negative `coef`, it's not a practical concern.
    pub coef: i128,
    pub exp: i64,
}

impl ExDecimal {
    pub fn new(signed_coef: i128, scale: usize) -> Self {
        Self {
            coef: signed_coef.abs(),
            sign: if signed_coef >= 0 { 1 } else { -1 },
            exp: -(scale as i64),
        }
    }

    pub fn signed_coef(self) -> Result<i128, ExplorerError> {
        let base = self.sign as i128 * self.coef;
        if self.exp > 0 {
            base.checked_mul(10_i128.pow(self.exp as u32))
                .ok_or_else(|| {
                    ExplorerError::Other(
                        "decimal coefficient overflow: value exceeds i128 limits".to_string(),
                    )
                })
        } else {
            Ok(base)
        }
    }

    pub fn scale(self) -> usize {
        // A positive `exp` represents an integer. Polars requires `scale == 0`
        // for integers. The case where `exp > 0` is handled by `.signed_coef`.
        if self.exp > 0 {
            0
        } else {
            self.exp
                .abs()
                .try_into()
                .expect("cannot convert exponent (Elixir) to scale (Rust)")
        }
    }

    pub fn default_precision() -> usize {
        // https://docs.pola.rs/api/python/stable/reference/api/polars.datatypes.Decimal.html
        // > If set to None (default), the precision is set to 38 (the maximum
        // > supported by Polars).
        38
    }
}

impl Literal for ExDecimal {
    fn lit(self) -> Expr {
        let precision = ExDecimal::default_precision();
        let scale = self.scale();
        Expr::Literal(LiteralValue::Scalar(Scalar::new(
            DataType::Decimal(precision, scale),
            AnyValue::Decimal(self.signed_coef(), precision, scale),
        )))
    }
}

/// Represents valid Elixir types that can be used as literals in Polars.
pub enum ExValidValue<'a> {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(&'a str),
    Date(ExDate),
    Time(ExTime),
    DateTime(ExNaiveDateTime),
    Duration(ExDuration),
    Decimal(ExDecimal),
}

impl ExValidValue<'_> {
    pub fn lit_with_matching_precision(self, data_type: &DataType) -> Expr {
        match data_type {
            DataType::Datetime(time_unit, _) => self.lit().dt().cast_time_unit(*time_unit),
            DataType::Duration(time_unit) => self.lit().dt().cast_time_unit(*time_unit),
            _ => self.lit(),
        }
    }
}

impl Literal for &ExValidValue<'_> {
    fn lit(self) -> Expr {
        match self {
            ExValidValue::I64(v) => v.lit(),
            ExValidValue::F64(v) => v.lit(),
            ExValidValue::Bool(v) => v.lit(),
            ExValidValue::Str(v) => v.lit(),
            ExValidValue::Date(v) => v.lit(),
            ExValidValue::Time(v) => v.lit(),
            ExValidValue::DateTime(v) => v.lit(),
            ExValidValue::Duration(v) => v.lit(),
            ExValidValue::Decimal(v) => v.lit(),
        }
    }
}

impl<'a> rustler::Decoder<'a> for ExValidValue<'a> {
    fn decode(term: rustler::Term<'a>) -> rustler::NifResult<Self> {
        use rustler::*;

        match term.get_type() {
            TermType::Atom => term.decode::<bool>().map(ExValidValue::Bool),
            TermType::Binary => term.decode::<&'a str>().map(ExValidValue::Str),
            TermType::Float => term.decode::<f64>().map(ExValidValue::F64),
            TermType::Integer => term.decode::<i64>().map(ExValidValue::I64),
            TermType::Map => {
                if let Ok(date) = term.decode::<ExDate>() {
                    Ok(ExValidValue::Date(date))
                } else if let Ok(time) = term.decode::<ExTime>() {
                    Ok(ExValidValue::Time(time))
                } else if let Ok(datetime) = term.decode::<ExNaiveDateTime>() {
                    Ok(ExValidValue::DateTime(datetime))
                } else if let Ok(duration) = term.decode::<ExDuration>() {
                    Ok(ExValidValue::Duration(duration))
                } else if let Ok(decimal) = term.decode::<ExDecimal>() {
                    Ok(ExValidValue::Decimal(decimal))
                } else {
                    Err(rustler::Error::BadArg)
                }
            }
            _ => Err(rustler::Error::BadArg),
        }
    }
}

// In Elixir this would be represented like this:
// * `:uncompressed` for `ExParquetCompression::Uncompressed`
// * `{:brotli, 7}` for `ExParquetCompression::Brotli(Some(7))`
#[derive(NifTaggedEnum)]
pub enum ExParquetCompression {
    Brotli(Option<u32>),
    Gzip(Option<u8>),
    Lz4raw,
    Snappy,
    Uncompressed,
    Zstd(Option<i32>),
}

#[derive(NifTaggedEnum)]
pub enum ExCorrelationMethod {
    Pearson,
    Spearman,
}

#[derive(NifTaggedEnum)]
pub enum ExRankMethod {
    Average,
    Min,
    Max,
    Dense,
    Ordinal,
    Random,
}

impl TryFrom<ExParquetCompression> for ParquetCompression {
    type Error = ExplorerError;

    fn try_from(value: ExParquetCompression) -> Result<Self, Self::Error> {
        let compression = match value {
            ExParquetCompression::Brotli(level) => {
                let brotli_level = match level.map(BrotliLevel::try_new) {
                    // Cant' use map because of ?
                    Some(result) => Some(result?),
                    None => None,
                };
                ParquetCompression::Brotli(brotli_level)
            }
            ExParquetCompression::Gzip(level) => {
                let gzip_level = match level.map(GzipLevel::try_new) {
                    Some(result) => Some(result?),
                    None => None,
                };
                ParquetCompression::Gzip(gzip_level)
            }
            ExParquetCompression::Lz4raw => ParquetCompression::Lz4Raw,
            ExParquetCompression::Snappy => ParquetCompression::Snappy,
            ExParquetCompression::Uncompressed => ParquetCompression::Uncompressed,
            ExParquetCompression::Zstd(level) => {
                let zstd_level = match level.map(ZstdLevel::try_new) {
                    Some(result) => Some(result?),
                    None => None,
                };
                ParquetCompression::Zstd(zstd_level)
            }
        };

        Ok(compression)
    }
}

// =========================
// ====== S3 Entry ======
// =========================

// Represents Explorer.FSS.S3Config struct from Elixir
#[derive(NifStruct, Clone, Debug)]
#[module = "Explorer.FSS.S3Config"]
pub struct ExS3Config {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: Option<String>,
    pub token: Option<String>,
}

// Represents {:s3, key, config} triplet from Elixir
#[derive(Clone, Debug)]
pub struct ExS3Entry {
    pub key: String,
    pub config: ExS3Config,
}

impl<'a> rustler::Decoder<'a> for ExS3Entry {
    fn decode(term: rustler::Term<'a>) -> rustler::NifResult<Self> {
        use rustler::*;

        // Expecting a tuple {:s3, key, %S3Config{}}
        let tuple: (Atom, String, ExS3Config) = term.decode()?;

        if tuple.0 != atoms::s3() {
            return Err(rustler::Error::BadArg);
        }

        Ok(ExS3Entry {
            key: tuple.1,
            config: tuple.2,
        })
    }
}

impl fmt::Display for ExS3Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(bucket_name) = &self.config.bucket {
            write!(f, "s3://{}/{}", bucket_name, self.key)
        } else {
            write!(f, "s3://default-explorer-bucket/{}", self.key)
        }
    }
}

#[cfg(feature = "aws")]
impl ExS3Config {
    pub fn to_cloud_options(&self) -> CloudOptions {
        let true_as_string = String::from("true");
        let mut aws_opts = vec![
            (S3Key::AccessKeyId, &self.access_key_id),
            (S3Key::SecretAccessKey, &self.secret_access_key),
            (S3Key::Region, &self.region),
            (S3Key::from_str("aws_allow_http").unwrap(), &true_as_string),
        ];

        aws_opts.push((S3Key::Endpoint, &self.endpoint));

        if let Some(token) = &self.token {
            aws_opts.push((S3Key::Token, token))
        }

        // When bucket is not present, we need to force the virtual host style
        // in order to ignore the bucket name.
        if self.bucket.is_none() {
            aws_opts.push((S3Key::VirtualHostedStyleRequest, &true_as_string));
        }

        CloudOptions::default().with_aws(aws_opts)
    }
}

impl From<ExExpr> for Expr {
    fn from(ex_expr: ExExpr) -> Self {
        ex_expr.clone_inner()
    }
}

use polars::prelude::QuoteStyle as PolarsQuoteStyle;

#[derive(NifTaggedEnum)]
pub enum ExQuoteStyle {
    Necessary,
    Always,
    NonNumeric,
    Never,
}

impl From<ExQuoteStyle> for PolarsQuoteStyle {
    fn from(style: ExQuoteStyle) -> Self {
        match style {
            ExQuoteStyle::Necessary => PolarsQuoteStyle::Necessary,
            ExQuoteStyle::Always => PolarsQuoteStyle::Always,
            ExQuoteStyle::NonNumeric => PolarsQuoteStyle::NonNumeric,
            ExQuoteStyle::Never => PolarsQuoteStyle::Never,
        }
    }
}

impl From<PolarsQuoteStyle> for ExQuoteStyle {
    fn from(style: PolarsQuoteStyle) -> Self {
        match style {
            PolarsQuoteStyle::Necessary => ExQuoteStyle::Necessary,
            PolarsQuoteStyle::Always => ExQuoteStyle::Always,
            PolarsQuoteStyle::NonNumeric => ExQuoteStyle::NonNumeric,
            PolarsQuoteStyle::Never => ExQuoteStyle::Never,
        }
    }
}
