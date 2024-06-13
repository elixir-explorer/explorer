use crate::datatypes::{ExDate, ExDateTime, ExDuration, ExNaiveDateTime, ExTime, ExTimeUnit};
use crate::{ExSeries, ExplorerError};

use polars::prelude::*;
use rustler::{ListIterator, Term, TermType};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_date(name: &str, val: Term) -> Result<ExSeries, ExplorerError> {
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<Option<i32>> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => item.decode::<i32>().map(Some).map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i32: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExDate>()
                .map(|ex_date| Some(i32::from(ex_date)))
                .map_err(|error| {
                    let message = format!("cannot decode a valid date from term. error: {error:?}");
                    ExplorerError::Other(message)
                }),
            TermType::Atom => Ok(None),
            term_type => {
                let message = format!("from_list/2 for dates not implemented for {term_type:?}");
                Err(ExplorerError::Other(message))
            }
        })
        .collect::<Result<Vec<Option<i32>>, ExplorerError>>()?;

    Series::new(name, values)
        .cast(&DataType::Date)
        .map(ExSeries::new)
        .map_err(|error| {
            ExplorerError::Other(format!(
                "from_list/2 cannot cast integer series to a valid date series: {error:?}"
            ))
        })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_naive_datetime(
    name: &str,
    val: Term,
    precision: ExTimeUnit,
) -> Result<ExSeries, ExplorerError> {
    let timeunit = TimeUnit::try_from(&precision)?;
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<Option<i64>> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => item.decode::<i64>().map(Some).map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExNaiveDateTime>()
                .map(|ex_naive_datetime| Some(i64::from(ex_naive_datetime)))
                .map_err(|error| {
                    let message =
                        format!("cannot decode a valid naive datetime from term. error: {error:?}");
                    ExplorerError::Other(message)
                }),
            TermType::Atom => Ok(None),
            term_type => {
                let message =
                    format!("from_list/2 for naive datetimess not implemented for {term_type:?}");
                Err(ExplorerError::Other(message))
            }
        })
        .collect::<Result<Vec<Option<i64>>, ExplorerError>>()?;

    Series::new(name, values)
        .cast(&DataType::Datetime(timeunit, None))
        .map(ExSeries::new)
        .map_err(|error| {
            ExplorerError::Other(format!(
                "from_list/2 cannot cast integer series to a valid naive datetime series: {error:?}"
            ))
        })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_datetime(
    name: &str,
    val: Term,
    precision: ExTimeUnit,
    time_zone_str: Option<&str>,
) -> Result<ExSeries, ExplorerError> {
    let timeunit = TimeUnit::try_from(&precision)?;
    let time_zone = time_zone_str.map(|s| s.to_string());
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<Option<i64>> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => item.decode::<i64>().map(Some).map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExDateTime>()
                .map(|ex_naive_datetime| Some(i64::from(ex_naive_datetime)))
                .map_err(|error| {
                    let message =
                        format!("cannot decode a valid datetime from term. error: {error:?}");
                    ExplorerError::Other(message)
                }),
            TermType::Atom => Ok(None),
            term_type => {
                let message =
                    format!("from_list/2 for datetimes not implemented for {term_type:?}");
                Err(ExplorerError::Other(message))
            }
        })
        .collect::<Result<Vec<Option<i64>>, ExplorerError>>()?;

    Series::new(name, values)
        .cast(&DataType::Datetime(timeunit, time_zone))
        .map(ExSeries::new)
        .map_err(|error| {
            ExplorerError::Other(format!(
                "from_list/2 cannot cast integer series to a valid datetime series: {error:?}"
            ))
        })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_duration(
    name: &str,
    val: Term,
    precision: ExTimeUnit,
) -> Result<ExSeries, ExplorerError> {
    let timeunit = TimeUnit::try_from(&precision)?;
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<Option<i64>> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => item.decode::<i64>().map(Some).map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExDuration>()
                .map(|ex_duration| Some(i64::from(ex_duration)))
                .map_err(|error| {
                    let message =
                        format!("cannot decode a valid duration from term. error: {error:?}");
                    ExplorerError::Other(message)
                }),
            TermType::Atom => Ok(None),
            term_type => {
                let message =
                    format!("from_list/2 for datetimes not implemented for {term_type:?}");
                Err(ExplorerError::Other(message))
            }
        })
        .collect::<Result<Vec<Option<i64>>, ExplorerError>>()?;

    Series::new(name, values)
        .cast(&DataType::Duration(timeunit))
        .map(ExSeries::new)
        .map_err(|error| {
            ExplorerError::Other(format!(
                "from_list/2 cannot cast integer series to a valid duration series: {error:?}"
            ))
        })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_time(name: &str, val: Term) -> Result<ExSeries, ExplorerError> {
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<Option<i64>> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => item.decode::<i64>().map(Some).map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExTime>()
                .map(|ex_duration| Some(i64::from(ex_duration)))
                .map_err(|error| {
                    let message =
                        format!("cannot decode a valid duration from term. error: {error:?}");
                    ExplorerError::Other(message)
                }),
            TermType::Atom => Ok(None),
            term_type => {
                let message = format!("from_list/2 for time not implemented for {term_type:?}");
                Err(ExplorerError::Other(message))
            }
        })
        .collect::<Result<Vec<Option<i64>>, ExplorerError>>()?;

    Series::new(name, values)
        .cast(&DataType::Time)
        .map(ExSeries::new)
        .map_err(|error| {
            ExplorerError::Other(format!(
                "from_list/2 cannot cast integer series to a valid time series: {error:?}"
            ))
        })
}
