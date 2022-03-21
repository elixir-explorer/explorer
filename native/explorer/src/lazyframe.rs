use polars::prelude::*;
use std::result::Result;

use crate::{ExDataFrame, ExLazyFrame, ExplorerError};

/// Gets the first `length` rows using [`LazyFrame::fetch()`].
#[rustler::nif]
pub fn lf_fetch(data: ExLazyFrame, length: usize) -> Result<ExDataFrame, ExplorerError> {
    let new_df = data.resource.0.clone().fetch(length)?;
    Ok(ExDataFrame::new(new_df))
}

/// Describes the plan using [`LazyFrame::describe_plan()`].
#[rustler::nif]
pub fn lf_describe_plan(data: ExLazyFrame) -> Result<String, ExplorerError> {
    Ok(data.resource.0.describe_plan())
}

/// Execute the lazy operations and return a DataFrame. See [`LazyFrame::collect()`].
#[rustler::nif]
pub fn lf_collect(data: ExLazyFrame) -> Result<ExDataFrame, ExplorerError> {
    Ok(ExDataFrame::new(data.resource.0.clone().collect()?))
}

/// Select (and rename) columns from the query. See [`LazyFrame::select()`].
#[rustler::nif]
pub fn lf_select(data: ExLazyFrame, selection: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let col_exprs: Vec<Expr> = selection.iter().map(|el| col(*el)).collect();
    let new_lf = data.resource.0.clone().select(col_exprs);
    Ok(ExLazyFrame::new(new_lf))
}
/// Select (and rename) columns from the query. See [`LazyFrame::select()`].
#[rustler::nif]
pub fn lf_drop(data: ExLazyFrame, selection: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let col_exprs: Vec<Expr> = selection.iter().map(|el| col(*el)).collect();
    let new_lf = data.resource.0.clone().select(col_exprs);
    Ok(ExLazyFrame::new(new_lf))
}
