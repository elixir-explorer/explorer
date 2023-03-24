use crate::atoms;
use crate::{ExSeries, ExplorerError};
use polars::prelude::*;
use rustler::{Term, TermType};
use std::result::Result;

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_log_f_rhs(s: ExSeries, base: Term) -> Result<ExSeries, ExplorerError> {
    let nan = atoms::nan();
    let infinity = atoms::infinity();
    let neg_infinity = atoms::neg_infinity();

    let float = match base.get_type() {
        TermType::Number => base.decode::<f64>().unwrap(),
        TermType::Atom => {
            if nan.eq(&base) {
                f64::NAN
            } else if infinity.eq(&base) {
                f64::INFINITY
            } else if neg_infinity.eq(&base) {
                f64::NEG_INFINITY
            } else {
                panic!("log/2 invalid float")
            }
        }
        term_type => panic!("log/2 not implemented for {term_type:?}"),
    };

    let s = s.log(float);
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_log_i_rhs(s: ExSeries, base: u32) -> Result<ExSeries, ExplorerError> {
    let base = f64::try_from(base)
        .map_err(|_| ExplorerError::Other("base should be convertable to float".into()))?;

    let s = s.log(base).strict_cast(&DataType::Int64)?;
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_exponential(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s.exp()))
}
