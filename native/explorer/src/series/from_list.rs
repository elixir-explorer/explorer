use crate::atoms;
use crate::datatypes::{
    ex_datetime_to_timestamp, ex_naive_datetime_to_timestamp, ExDate, ExDateTime, ExDecimal,
    ExDuration, ExNaiveDateTime, ExSeriesDtype, ExTime, ExTimeUnit,
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

    Series::new(name.into(), values)
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
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid naive datetime from term. error: {error:?}"
                    ))
                })
                .and_then(|ex_naive_datetime| {
                    ex_naive_datetime_to_timestamp(ex_naive_datetime, timeunit).map(Some)
                }),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for naive datetimes not implemented for {term_type:?}"
            ))),
        })
        .collect::<Result<Vec<Option<i64>>, ExplorerError>>()?;

    Series::new(name.into(), values)
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
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid datetime from term. error: {error:?}"
                    ))
                })
                .and_then(|ex_datetime| ex_datetime_to_timestamp(ex_datetime, timeunit).map(Some)),
            TermType::Atom => Ok(None),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for datetimes not implemented for {term_type:?}"
            ))),
        })
        .collect::<Result<Vec<Option<i64>>, ExplorerError>>()?;

    Series::new(name.into(), values)
        .cast(&DataType::Datetime(
            timeunit,
            time_zone.map(|value| value.into()),
        ))
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

    Series::new(name.into(), values)
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

    Series::new(name.into(), values)
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
    let s = Series::new_null(name.into(), length);
    ExSeries::new(Series::new(name.into(), s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_decimal(
    name: &str,
    val: Term,
    precision: Option<usize>,
    scale: Option<usize>,
) -> Result<ExSeries, ExplorerError> {
    let iterator = val
        .decode::<ListIterator>()
        .map_err(|err| ExplorerError::Other(format!("expecting list as term: {err:?}")))?;

    let values: Vec<AnyValue> = iterator
        .map(|item| match item.get_type() {
            TermType::Integer => {
                let s = scale.unwrap_or(0);
                item.decode::<i128>()
                    .map(|num| AnyValue::Decimal(num, s))
                    .map_err(|err| {
                        ExplorerError::Other(format!("int number is too big for an i128: {err:?}"))
                    })
            }

            TermType::Map => item
                .decode::<ExDecimal>()
                .map(|ex_decimal| AnyValue::Decimal(ex_decimal.signed_coef(), ex_decimal.scale()))
                .map_err(|error| {
                    ExplorerError::Other(format!(
                        "cannot decode a valid decimal from term; check that `coef` fits into an `i128`. error: {error:?}"
                    ))
                }),
            TermType::Atom => Ok(AnyValue::Null),

            TermType::Float => item
                .decode::<f64>()
                .map(|num| match native_float_to_decimal_parts(num) {
                    Some((integer, scale)) => AnyValue::Decimal(integer, scale),
                    None => AnyValue::Null,
                })
                .map_err(|err| {
                    ExplorerError::Other(format!("float number is too big f64: {err:?}"))
                }),
            term_type => Err(ExplorerError::Other(format!(
                "from_list/2 for decimals not implemented for {term_type:?}"
            ))),
        })
        .collect::<Result<Vec<AnyValue>, ExplorerError>>()?;

    let mut series = Series::from_any_values(name.into(), &values, true)?;

    match series.dtype() {
        DataType::Decimal(result_precision, result_scale) => {
            let p: Option<usize> = Some(precision.unwrap_or(result_precision.unwrap_or(38)));
            let s: Option<usize> = Some(scale.unwrap_or(result_scale.unwrap_or(0)));

            if *result_precision != p || *result_scale != s {
                series = series.cast(&DataType::Decimal(p, s))?;
            }
        }
        // An empty list will result in the `Null` dtype.
        DataType::Null => {
            let p = Some(precision.unwrap_or(38));
            let s = Some(scale.unwrap_or(0));
            series = series.cast(&DataType::Decimal(p, s))?;
        }
        other_dtype => panic!("expected dtype to be Decimal. found: {other_dtype:?}"),
    }

    Ok(ExSeries::new(series))
}

fn native_float_to_decimal_parts(float: f64) -> Option<(i128, usize)> {
    let float_str = float.to_string();

    match float_str.split_once(".") {
        Some((integer_part, fraction_part)) => {
            let new_str = integer_part.to_string() + fraction_part;
            let coef: i128 = new_str.parse().expect("expecting a valid i128 number");
            Some((coef, fraction_part.len()))
        }

        None => {
            let res_coef = float_str.parse::<i128>();
            match res_coef {
                Ok(coef) => Some((coef, 0)),
                Err(_) => None,
            }
        }
    }
}

macro_rules! from_list {
    ($name:ident, $type:ty) => {
        #[rustler::nif(schedule = "DirtyCpu")]
        pub fn $name(name: &str, val: Term) -> NifResult<ExSeries> {
            val.decode::<Vec<Option<$type>>>()
                .map(|values| ExSeries::new(Series::new(name.into(), values.as_slice())))
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
                    ExSeries::new(Series::new(name.into(), values))
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
        .map(|values| ExSeries::new(Series::new(name.into(), values)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_from_list_categories(name: &str, val: Term) -> NifResult<ExSeries> {
    let decoded = val.decode::<Vec<Option<String>>>()?;
    Ok(ExSeries::new(
        Series::new(name.into(), decoded.as_slice())
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

            Series::new(name.into(), lists).cast(&dtype).map_err(|err| {
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
    let columns = series_term
        .decode::<Vec<ExSeries>>()?
        .into_iter()
        .map(|c| Column::from(c.clone_inner()))
        .collect();

    let df = DataFrame::new(columns).unwrap();

    df.into_struct(name.into())
        .into_series()
        .cast(&dtype)
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
            ExSeries::new(Series::new(name.into(), transmuted))
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
