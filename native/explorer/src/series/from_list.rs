use crate::atoms;
use crate::datatypes::{
    ExDate, ExDateTime, ExDuration, ExNaiveDateTime, ExSeriesDtype, ExTime, ExTimeUnit,
};
use crate::{ExSeries, ExplorerError};

use polars::datatypes::DataType;
use polars::prelude::*;
use rustler::{Binary, Error, ListIterator, NifResult, Term, TermType};
use std::slice;

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_date(name: &str, val: Term) -> Result<ExSeries, ExplorerError> {
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<Option<i32>> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => item.decode::<Option<i32>>().map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i32: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExDate>()
                .map(|ex_date| Some(i32::from(ex_date)))
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid date from term. error: {error:?}"
                    ))
                }),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for dates not implemented for {term_type:?}"
            ))),
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
            TermType::Integer => item.decode::<Option<i64>>().map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExNaiveDateTime>()
                .map(|ex_naive_datetime| Some(i64::from(ex_naive_datetime)))
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid naive datetime from term. error: {error:?}"
                    ))
                }),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for naive datetimes not implemented for {term_type:?}"
            ))),
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
            TermType::Integer => item.decode::<Option<i64>>().map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExDateTime>()
                .map(|ex_datetime| Some(i64::from(ex_datetime)))
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid datetime from term. error: {error:?}"
                    ))
                }),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for datetimes not implemented for {term_type:?}"
            ))),
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
            TermType::Integer => item.decode::<Option<i64>>().map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExDuration>()
                .map(|ex_duration| Some(i64::from(ex_duration)))
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid duration from term. error: {error:?}"
                    ))
                }),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for durations not implemented for {term_type:?}"
            ))),
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
            TermType::Integer => item.decode::<Option<i64>>().map_err(|err| {
                ExplorerError::Other(format!("int number is too big for an i64: {err:?}"))
            }),
            TermType::Map => item
                .decode::<ExTime>()
                .map(|ex_time| Some(i64::from(ex_time)))
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid time from term. error: {error:?}"
                    ))
                }),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for time not implemented for {term_type:?}"
            ))),
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

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_null(name: &str, length: usize) -> ExSeries {
    let s = Series::new_null(name, length);
    ExSeries::new(Series::new(name, s))
}

macro_rules! from_list {
    ($name:ident, $type:ty) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(name: &str, val: Term) -> NifResult<ExSeries> {
            val.decode::<Vec<Option<$type>>>()
                .map(|values| ExSeries::new(Series::new(name, values.as_slice())))
        }
    };
}

from_list!(s_from_list_s8, i8);
from_list!(s_from_list_s16, i16);
from_list!(s_from_list_s32, i32);
from_list!(s_from_list_s64, i64);

from_list!(s_from_list_u8, u8);
from_list!(s_from_list_u16, u16);
from_list!(s_from_list_u32, u32);
from_list!(s_from_list_u64, u64);

from_list!(s_from_list_bool, bool);
from_list!(s_from_list_str, String);

macro_rules! from_list_float {
    ($name:ident, $type:ty, $module:ident) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(name: &str, val: Term) -> NifResult<ExSeries> {
            let nan = atoms::nan();
            let infinity = atoms::infinity();
            let neg_infinity = atoms::neg_infinity();
            let nil = rustler::types::atom::nil();

            val.decode::<ListIterator>()?
                .map(|item| match item.get_type() {
                    TermType::Float => item.decode::<Option<$type>>(),
                    TermType::Integer => {
                        let int_value = item.decode::<i64>().unwrap();
                        Ok(Some(int_value as $type))
                    }
                    TermType::Atom => {
                        if nan.eq(&item) {
                            Ok(Some($module::NAN))
                        } else if infinity.eq(&item) {
                            Ok(Some($module::INFINITY))
                        } else if neg_infinity.eq(&item) {
                            Ok(Some($module::NEG_INFINITY))
                        } else if nil.eq(&item) {
                            Ok(None)
                        } else {
                            let message = format!(
                                "from_list/2 cannot read the atom `{item:?}` for a float series"
                            );
                            Err(Error::RaiseTerm(Box::new(message)))
                        }
                    }
                    term_type => {
                        let message = format!("from_list/2 not implemented for {term_type:?}");
                        Err(Error::RaiseTerm(Box::new(message)))
                    }
                })
                .collect::<NifResult<Vec<Option<$type>>>>()
                .map(|values| {
                    ExSeries::new(Series::new(name, values))
                })
        }
    };
}

from_list_float!(s_from_list_f32, f32, f32);
from_list_float!(s_from_list_f64, f64, f64);

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_binary(name: &str, val: Term) -> NifResult<ExSeries> {
    val.decode::<ListIterator>()?
        .map(|term| {
            term.decode::<Option<Binary>>()
                .map(|maybe_bin| maybe_bin.map(|bin| bin.as_slice()))
        })
        .collect::<NifResult<Vec<Option<&[u8]>>>>()
        .map(|values| ExSeries::new(Series::new(name, values)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_categories(name: &str, val: Term) -> NifResult<ExSeries> {
    let decoded = val.decode::<Vec<Option<String>>>()?;
    Ok(ExSeries::new(
        Series::new(name, decoded.as_slice())
            .cast(&DataType::Categorical(None, CategoricalOrdering::default()))
            .map_err(|err| {
                let message = format!(
                    "from_list/2 cannot cast a string series to categories series: {err:?}"
                );
                Error::RaiseTerm(Box::new(message))
            })?,
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_of_series(
    name: &str,
    series_term: Term,
    ex_dtype: ExSeriesDtype,
) -> NifResult<ExSeries> {
    let dtype = DataType::try_from(&ex_dtype).unwrap();

    series_term
        .decode::<Vec<Option<ExSeries>>>()
        .and_then(|series_vec| {
            let lists: Vec<Option<Series>> = series_vec
                .iter()
                .map(|maybe_series| {
                    maybe_series
                        .as_ref()
                        .map(|ex_series| ex_series.clone_inner())
                })
                .collect();

            Series::new(name, lists).cast(&dtype).map_err(|err| {
                let message = format!("from_list/2 cannot create series of lists: {err:?}");
                Error::RaiseTerm(Box::new(message))
            })
        })
        .map(ExSeries::new)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_of_series_as_structs(
    name: &str,
    series_term: Term,
    ex_dtype: ExSeriesDtype,
) -> NifResult<ExSeries> {
    let dtype = DataType::try_from(&ex_dtype).unwrap();
    let series_vec = series_term.decode::<Vec<ExSeries>>()?;

    StructChunked::from_series(
        name,
        series_vec
            .into_iter()
            .map(|s| s.clone_inner())
            .collect::<Vec<_>>()
            .as_slice(),
    )
    .map(|struct_chunked| struct_chunked.into_series())
    .and_then(|series| series.cast(&dtype))
    .map_err(|err| {
        let message = format!("from_list/2 cannot create series of structs: {err:?}");
        Error::RaiseTerm(Box::new(message))
    })
    .map(ExSeries::new)
}

macro_rules! from_binary {
    ($name:ident, $type:ty, $bytes:expr) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(name: &str, val: Binary) -> ExSeries {
            let slice = val.as_slice();
            let transmuted = unsafe {
                slice::from_raw_parts(slice.as_ptr() as *const $type, slice.len() / $bytes)
            };
            ExSeries::new(Series::new(name, transmuted))
        }
    };
}

from_binary!(s_from_binary_f32, f32, 4);
from_binary!(s_from_binary_f64, f64, 8);

from_binary!(s_from_binary_s8, i8, 1);
from_binary!(s_from_binary_s16, i16, 2);
from_binary!(s_from_binary_s32, i32, 4);
from_binary!(s_from_binary_s64, i64, 8);

from_binary!(s_from_binary_u8, u8, 1);
from_binary!(s_from_binary_u16, u16, 2);
from_binary!(s_from_binary_u32, u32, 4);
from_binary!(s_from_binary_u64, u64, 8);
