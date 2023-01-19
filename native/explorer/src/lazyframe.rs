use crate::{expressions::ex_expr_to_exprs, ExDataFrame, ExExpr, ExLazyFrame, ExplorerError};
use polars::prelude::*;
use std::result::Result;

// Loads the IO functions for read/writing CSV, NDJSON, Parquet, etc.
pub mod io;

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_collect(data: ExLazyFrame) -> Result<ExDataFrame, ExplorerError> {
    let df = data.clone_inner().collect()?;

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn lf_fetch(data: ExLazyFrame, n_rows: usize) -> Result<ExDataFrame, ExplorerError> {
    Ok(ExDataFrame::new(data.clone_inner().fetch(n_rows)?))
}

#[rustler::nif]
pub fn lf_describe_plan(data: ExLazyFrame, optimized: bool) -> Result<String, ExplorerError> {
    let lf = data.clone_inner();
    let plan = match optimized {
        true => lf.describe_optimized_plan()?,
        false => lf.describe_plan(),
    };
    Ok(plan)
}

#[rustler::nif]
pub fn lf_head(data: ExLazyFrame, length: u32) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    Ok(ExLazyFrame::new(lf.limit(length)))
}

#[rustler::nif]
pub fn lf_tail(data: ExLazyFrame, length: u32) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    Ok(ExLazyFrame::new(lf.tail(length)))
}

#[rustler::nif]
pub fn lf_names(data: ExLazyFrame) -> Result<Vec<String>, ExplorerError> {
    let lf = data.clone_inner();
    Ok(lf.schema()?.iter_names().cloned().collect())
}

#[rustler::nif]
pub fn lf_dtypes(data: ExLazyFrame) -> Result<Vec<String>, ExplorerError> {
    let lf = data.clone_inner();
    Ok(lf
        .schema()?
        .iter_dtypes()
        .map(|dtype| dtype.to_string())
        .collect())
}

#[rustler::nif]
pub fn lf_select(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().select(&[cols(columns)]);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_drop(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().select(&[col("*").exclude(columns)]);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_slice(data: ExLazyFrame, offset: i64, length: u32) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner();
    Ok(ExLazyFrame::new(lf.slice(offset, length)))
}

#[rustler::nif]
pub fn lf_filter_with(data: ExLazyFrame, ex_expr: ExExpr) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let expr = ex_expr.clone_inner();

    Ok(ExLazyFrame::new(ldf.filter(expr)))
}

#[rustler::nif]
pub fn lf_arrange_with(
    data: ExLazyFrame,
    expressions: Vec<ExExpr>,
    directions: Vec<bool>,
) -> Result<ExLazyFrame, ExplorerError> {
    let exprs = ex_expr_to_exprs(expressions);
    let ldf = data.clone_inner().sort_by_exprs(exprs, directions, false);

    Ok(ExLazyFrame::new(ldf))
}

#[rustler::nif]
pub fn lf_distinct(
    data: ExLazyFrame,
    subset: Vec<String>,
    columns_to_keep: Option<Vec<ExExpr>>,
) -> Result<ExLazyFrame, ExplorerError> {
    let df = data.clone_inner();
    let new_df = df.unique_stable(Some(subset), UniqueKeepStrategy::First);

    match columns_to_keep {
        Some(columns) => Ok(ExLazyFrame::new(new_df.select(ex_expr_to_exprs(columns)))),
        None => Ok(ExLazyFrame::new(new_df)),
    }
}

#[rustler::nif]
pub fn lf_mutate_with(
    data: ExLazyFrame,
    columns: Vec<ExExpr>,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let mutations = ex_expr_to_exprs(columns);

    Ok(ExLazyFrame::new(ldf.with_columns(mutations)))
}
