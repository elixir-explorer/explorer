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

#[derive(NifStruct, Clone)]
#[module = "Explorer.PolarsBackend.Expression"]
pub struct ExExpr {
    pub resource: ResourceArc<ExExprRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.LazyFrame"]
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
        Expr::Literal(LiteralValue::Duration(
            self.value,
            time_unit_of_ex_duration(&self),
        ))
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

pub use polars::export::arrow::temporal_conversions::date32_to_date as days_to_date;

pub fn timestamp_to_datetime_utc(microseconds: i64) -> DateTime<Utc> {
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
    DateTime::<Utc>::from_timestamp(seconds, nanoseconds.try_into().unwrap())
        .expect("construct a UTC")
}

/// Converts a microsecond i64 to a `NaiveDateTime`.
/// This is because when getting a timestamp, it might have negative values.
pub fn timestamp_to_datetime(microseconds: i64) -> NaiveDateTime {
    timestamp_to_datetime_utc(microseconds).naive_utc()
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

impl From<i64> for ExNaiveDateTime {
    fn from(microseconds: i64) -> Self {
        timestamp_to_datetime(microseconds).into()
    }
}

impl From<ExNaiveDateTime> for i64 {
    fn from(dt: ExNaiveDateTime) -> i64 {
        let duration = NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day)
            .unwrap()
            .and_hms_micro_opt(dt.hour, dt.minute, dt.second, dt.microsecond.0)
            .unwrap()
            .signed_duration_since(
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            );

        match duration.num_microseconds() {
            Some(us) => us,
            None => duration.num_milliseconds() * 1_000,
        }
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
            microsecond: (
                microseconds_six_digits(dt.and_utc().timestamp_subsec_micros()),
                6,
            ),
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
        Expr::Literal(LiteralValue::DateTime(
            ndt.and_utc().timestamp_micros(),
            TimeUnit::Microseconds,
            None,
        ))
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

// impl From<i64> for ExDateTime<'_> {
//     fn from(microseconds: i64) -> Self {
//         timestamp_to_datetime_utc(microseconds).into()
//     }
// }

impl From<ExDateTime<'_>> for i64 {
    fn from(dt: ExDateTime<'_>) -> i64 {
        let duration = NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day)
            .unwrap()
            .and_hms_micro_opt(dt.hour, dt.minute, dt.second, dt.microsecond.0)
            .unwrap()
            .signed_duration_since(
                NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            );

        match duration.num_microseconds() {
            Some(us) => us,
            None => duration.num_milliseconds() * 1_000,
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
            microsecond: (microseconds_six_digits(dt_tz.timestamp_subsec_micros()), 6),
            minute: dt_tz.minute(),
            month: dt_tz.month(),
            second: dt_tz.second(),
            std_offset: dt_tz.offset().dst_offset().num_seconds(),
            time_zone,
            utc_offset: dt_tz.offset().base_utc_offset().num_seconds(),
            year: dt_tz.year(),
            zone_abbr,
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

impl<'tz> From<ExDateTime<'tz>> for NaiveDateTime {
    fn from(dt: ExDateTime) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(dt.year, dt.month, dt.day)
            .unwrap()
            .and_hms_micro_opt(dt.hour, dt.minute, dt.second, dt.microsecond.0)
            .unwrap()
    }
}

impl<'tz> Literal for ExDateTime<'tz> {
    fn lit(self) -> Expr {
        let ndt = NaiveDateTime::from(self);
        let time_zone = self.time_zone.to_string();

        Expr::Literal(LiteralValue::DateTime(
            ndt.and_utc().timestamp_micros(),
            TimeUnit::Microseconds,
            Some(time_zone.into()),
        ))
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

pub use polars::export::arrow::temporal_conversions::time64ns_to_time;

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
        let microseconds = t.nanosecond() / 1_000;

        let ex_microseconds = if microseconds > 0 {
            (microseconds_six_digits(microseconds), 6)
        } else {
            (0, 0)
        };

        ExTime {
            calendar: atoms::calendar_iso_module(),
            hour: t.hour(),
            minute: t.minute(),
            second: t.second(),
            microsecond: ex_microseconds,
        }
    }
}

impl Literal for ExTime {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Time(self.into()))
    }
}

#[derive(NifStruct, Copy, Clone, Debug)]
#[module = "Decimal"]
pub struct ExDecimal {
    pub sign: i8,
    // `coef` is a positive, arbitrary precision integer on the Elixir side.
    // It's convenient to represent it here as a signed `i128` because that's
    // what the Decimal dtype expects. While you could technically create an
    // `ExDecimal` struct with a negative `coef`, it's not a practical concern
    // because these structs are exclusively built from Elixir `Decimal`s which
    // are presumed valid; i.e. with positive `coef`s.
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

    pub fn signed_coef(self) -> i128 {
        self.sign as i128 * self.coef
    }

    pub fn scale(self) -> usize {
        self.exp
            .abs()
            .try_into()
            .expect("cannot convert exponent (Elixir) to scale (Rust)")
    }
}

impl Literal for ExDecimal {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Decimal(
            if self.sign.is_positive() {
                self.coef.into()
            } else {
                -self.coef
            },
            usize::try_from(-(self.exp)).expect("exponent should fit an usize"),
        ))
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

impl<'a> ExValidValue<'a> {
    pub fn lit_with_matching_precision(self, data_type: &DataType) -> Expr {
        match data_type {
            DataType::Datetime(time_unit, _) => self.lit().dt().cast_time_unit(*time_unit),
            DataType::Duration(time_unit) => self.lit().dt().cast_time_unit(*time_unit),
            _ => self.lit(),
        }
    }
}

impl<'a> Literal for &ExValidValue<'a> {
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
// ====== FSS Structs ======
// =========================

#[derive(NifStruct, Clone, Debug)]
#[module = "FSS.S3.Config"]
pub struct ExS3Config {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint: String,
    pub bucket: Option<String>,
    pub token: Option<String>,
}

#[derive(NifStruct, Clone, Debug)]
#[module = "FSS.S3.Entry"]
pub struct ExS3Entry {
    pub key: String,
    pub config: ExS3Config,
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
