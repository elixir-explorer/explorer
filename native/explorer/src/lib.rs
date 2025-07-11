// MiMalloc won´t compile on Windows with the GCC compiler.
// On Linux with Musl it won´t load correctly.
#[cfg(not(any(
    all(windows, target_env = "gnu"),
    all(target_os = "linux", target_env = "musl")
)))]
use mimalloc::MiMalloc;

#[cfg(not(any(
    all(windows, target_env = "gnu"),
    all(target_os = "linux", target_env = "musl")
)))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "cloud")]
mod cloud_writer;

mod dataframe;
mod datatypes;
mod encoding;
mod error;
mod expressions;
mod lazyframe;
mod local_message;
mod series;
mod sql_context;

pub use datatypes::{
    ExDataFrame, ExDataFrameRef, ExExpr, ExExprRef, ExLazyFrame, ExLazyFrameRef, ExSeries,
    ExSeriesRef,
};

pub use error::ExplorerError;
use expressions::*;
use series::*;
pub use sql_context::*;

mod atoms {
    rustler::atoms! {
        calendar_iso_module = "Elixir.Calendar.ISO",
        date_module = "Elixir.Date",
        datetime_module = "Elixir.DateTime",
        duration_module = "Elixir.Explorer.Duration",
        naive_datetime_module = "Elixir.NaiveDateTime",
        time_module = "Elixir.Time",
        decimal_module = "Elixir.Decimal",
        hour,
        minute,
        second,
        day,
        month,
        year,
        value,
        precision,
        millisecond,
        microsecond,
        nanosecond,
        calendar,
        nan,
        infinity,
        neg_infinity,
        std_offset,
        time_zone,
        utc_offset,
        zone_abbr,
        coef,
        exp,
        sign,
    }
}

rustler::init!("Elixir.Explorer.PolarsBackend.Native");
