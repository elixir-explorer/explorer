use crate::atoms;
use crate::{ExSeries, ExplorerError};
use polars::prelude::*;
use rustler::{Term, TermType};
use std::f64::consts::E;

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_log(s: ExSeries, base: Term) -> Result<ExSeries, ExplorerError> {
    let nan = atoms::nan();
    let infinity = atoms::infinity();
    let neg_infinity = atoms::neg_infinity();

    let float = match base.get_type() {
        TermType::Float => base.decode::<f64>().unwrap(),
        TermType::Atom => {
            if nan.eq(&base) {
                f64::NAN
            } else if infinity.eq(&base) {
                f64::INFINITY
            } else if neg_infinity.eq(&base) {
                f64::NEG_INFINITY
            } else {
                return Err(ExplorerError::Other("log/2 invalid float".into()));
            }
        }
        term_type => {
            return Err(ExplorerError::Other(format!(
                "log/2 not implemented for {term_type:?}"
            )))
        }
    };

    let s = s.log(float);
    Ok(ExSeries::new(s))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_log_natural(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s.log(E)))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn s_exp(s: ExSeries) -> Result<ExSeries, ExplorerError> {
    Ok(ExSeries::new(s.exp()))
}
