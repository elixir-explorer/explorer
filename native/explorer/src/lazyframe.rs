use std::result::Result;

use crate::{ExDataFrame, ExLazyFrame, ExplorerError};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_collect(data: ExLazyFrame) -> Result<ExDataFrame, ExplorerError> {
    Ok(ExDataFrame::new(data.resource.0.clone().collect()?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_fetch(data: ExLazyFrame, n_rows: usize) -> Result<ExDataFrame, ExplorerError> {
    Ok(ExDataFrame::new(data.resource.0.clone().fetch(n_rows)?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_describe_plan(data: ExLazyFrame, optimized: bool) -> Result<String, ExplorerError> {
    let lf = &data.resource.0;
    let plan = match optimized {
        true => lf.describe_optimized_plan()?,
        false => lf.describe_plan(),
    };
    Ok(plan)
}
