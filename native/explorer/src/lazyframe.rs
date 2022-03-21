use polars::prelude::*;
use std::result::Result;

use crate::{ExDataFrame, ExLazyFrame, ExplorerError};

macro_rules! lf_read {
    ($data: ident, $lf: ident, $body: block) => {
        match $data.resource.0.read() {
            Ok($lf) => $body,
            Err(_) => Err(ExplorerError::Internal(
                "Failed to take read lock for lf".into(),
            )),
        }
    };
}

macro_rules! lf_write {
    ($data: ident, $lf: ident, $body: block) => {
        match $data.resource.0.write() {
            Ok(mut $lf) => $body,
            Err(_) => Err(ExplorerError::Internal(
                "Failed to take read lock for lf".into(),
            )),
        }
    };
}

/// Gets the first `length` rows using [`LazyFrame::fetch()`].
#[rustler::nif]
pub fn lf_fetch(data: ExLazyFrame, length: usize) -> Result<ExDataFrame, ExplorerError> {
    lf_read!(data, lf, {
        let new_df = lf.clone().fetch(length)?;
        Ok(ExDataFrame::new(new_df))
    })
}

/// Gets the first `length` rows using [`LazyFrame::fetch()`].
#[rustler::nif]
pub fn lf_tail(data: ExLazyFrame, length: u32) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, { Ok(ExLazyFrame::new(lf.clone().tail(length))) })
}

/// Describes the plan using [`LazyFrame::describe_plan()`].
#[rustler::nif]
pub fn lf_describe_plan(data: ExLazyFrame) -> Result<String, ExplorerError> {
    lf_read!(data, lf, { Ok(lf.describe_plan()) })
}

/// Execute the lazy operations and return a DataFrame. See [`LazyFrame::collect()`].
/// This computes the lazyframe using [`LazyFrame::cache()`] first and replaces the lazyframe in
/// place. We then collect from the cached lazyframe.
#[rustler::nif]
pub fn lf_collect(data: ExLazyFrame) -> Result<ExDataFrame, ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        Ok(ExDataFrame::new(lf.clone().collect()?))
    })
}

/// Select (and rename) columns from the query. See [`LazyFrame::select()`].
#[rustler::nif]
pub fn lf_select(data: ExLazyFrame, selection: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let col_exprs: Vec<Expr> = selection.iter().map(|el| col(*el)).collect();
        let new_lf = lf.clone().select(col_exprs);
        Ok(ExLazyFrame::new(new_lf))
    })
}
/// Select (and rename) columns from the query. See [`LazyFrame::select()`].
#[rustler::nif]
pub fn lf_drop(data: ExLazyFrame, selection: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let new_lf = lf.clone().select(&[col("*").exclude(selection)]);
        Ok(ExLazyFrame::new(new_lf))
    })
}
