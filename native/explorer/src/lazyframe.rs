use crate::{
    dataframe::to_smart_strings, datatypes::ExSeriesDtype, expressions::ex_expr_to_exprs,
    ExDataFrame, ExExpr, ExLazyFrame, ExplorerError,
};
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
    let names = lf
        .schema()?
        .iter_names()
        .map(|smart_string| smart_string.to_string())
        .collect();

    Ok(names)
}

#[rustler::nif]
pub fn lf_dtypes(data: ExLazyFrame) -> Result<Vec<ExSeriesDtype>, ExplorerError> {
    let mut dtypes: Vec<ExSeriesDtype> = vec![];
    let schema = data.clone_inner().schema()?;

    for dtype in schema.iter_dtypes() {
        dtypes.push(ExSeriesDtype::try_from(dtype)?)
    }

    Ok(dtypes)
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
pub fn lf_explode(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().explode(columns);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_unnest(data: ExLazyFrame, columns: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    let lf = data.clone_inner().unnest(columns);
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_filter_with(data: ExLazyFrame, ex_expr: ExExpr) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let expr = ex_expr.clone_inner();

    Ok(ExLazyFrame::new(ldf.filter(expr)))
}

#[rustler::nif]
pub fn lf_sort_with(
    data: ExLazyFrame,
    expressions: Vec<ExExpr>,
    directions: Vec<bool>,
    maintain_order: bool,
    nulls_last: bool,
) -> Result<ExLazyFrame, ExplorerError> {
    let exprs = ex_expr_to_exprs(expressions);
    let ldf = data
        .clone_inner()
        .sort_by_exprs(exprs, directions, nulls_last, maintain_order);

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

#[rustler::nif]
pub fn lf_summarise_with(
    data: ExLazyFrame,
    groups: Vec<ExExpr>,
    aggs: Vec<ExExpr>,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let groups = ex_expr_to_exprs(groups);
    let aggs = ex_expr_to_exprs(aggs);

    let new_lf = if groups.is_empty() {
        // We do add a "shadow" column to be able to group by it.
        // This is going to force some aggregations like "mode" to be always inside
        // a "list".
        let s = Series::new_null("__explorer_null_for_group__", 1);
        ldf.with_column(s.lit())
            .group_by_stable(["__explorer_null_for_group__"])
            .agg(aggs)
            .select(&[col("*").exclude(["__explorer_null_for_group__"])])
    } else {
        ldf.group_by_stable(groups).agg(aggs)
    };

    Ok(ExLazyFrame::new(new_lf))
}

#[rustler::nif]
pub fn lf_rename_columns(
    data: ExLazyFrame,
    renames: Vec<(&str, &str)>,
) -> Result<ExLazyFrame, ExplorerError> {
    let df = data.clone_inner();
    let (existing, new): (Vec<_>, Vec<_>) = renames.iter().cloned().unzip();

    Ok(ExLazyFrame::new(df.rename(existing, new)))
}

#[rustler::nif]
pub fn lf_drop_nils(
    data: ExLazyFrame,
    subset: Option<Vec<ExExpr>>,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let columns = subset.map(ex_expr_to_exprs);

    Ok(ExLazyFrame::new(ldf.drop_nulls(columns)))
}

#[rustler::nif]
pub fn lf_pivot_longer(
    data: ExLazyFrame,
    id_vars: Vec<&str>,
    value_vars: Vec<&str>,
    names_to: String,
    values_to: String,
) -> Result<ExLazyFrame, ExplorerError> {
    let ldf = data.clone_inner();
    let melt_opts = MeltArgs {
        id_vars: to_smart_strings(id_vars),
        value_vars: to_smart_strings(value_vars),
        variable_name: Some(names_to.into()),
        value_name: Some(values_to.into()),
        streamable: true,
    };
    let new_df = ldf.melt(melt_opts);
    Ok(ExLazyFrame::new(new_df))
}

#[rustler::nif]
pub fn lf_join(
    data: ExLazyFrame,
    other: ExLazyFrame,
    left_on: Vec<ExExpr>,
    right_on: Vec<ExExpr>,
    how: &str,
    suffix: &str,
) -> Result<ExLazyFrame, ExplorerError> {
    let how = match how {
        "left" => JoinType::Left,
        "inner" => JoinType::Inner,
        "outer" => JoinType::Outer { coalesce: false },
        "cross" => JoinType::Cross,
        _ => {
            return Err(ExplorerError::Other(format!(
                "Join method {how} not supported"
            )))
        }
    };

    let ldf = data.clone_inner();
    let ldf1 = other.clone_inner();

    let new_ldf = ldf
        .join_builder()
        .with(ldf1)
        .how(how)
        .left_on(ex_expr_to_exprs(left_on))
        .right_on(ex_expr_to_exprs(right_on))
        .suffix(suffix)
        .finish();

    Ok(ExLazyFrame::new(new_ldf))
}

#[rustler::nif]
pub fn lf_concat_rows(lazy_frames: Vec<ExLazyFrame>) -> Result<ExLazyFrame, ExplorerError> {
    let inputs: Vec<LazyFrame> = lazy_frames.iter().map(|lf| lf.clone_inner()).collect();
    // TODO: Make sure union args options are configurable
    let union_args = UnionArgs::default();
    let out_df = concat(inputs, union_args)?;

    Ok(ExLazyFrame::new(out_df))
}

#[rustler::nif]
pub fn lf_concat_columns(
    data: ExLazyFrame,
    others: Vec<ExLazyFrame>,
) -> Result<ExLazyFrame, ExplorerError> {
    let id_column = "__row_count_id__";
    let first = data.clone_inner().with_row_index(id_column, None);

    // We need to be able to handle arbitrary column name overlap.
    // This builds up a join and suffixes conflicting names with _N where
    // N is the index of the df in the join array.
    let (out_df, _) = others
        .iter()
        .map(|data| data.clone_inner().with_row_index(id_column, None))
        .fold((first, 1), |(acc_df, count), df| {
            let suffix = format!("_{count}");
            let new_df = acc_df
                .join_builder()
                .with(df)
                .how(JoinType::Inner)
                .left_on([col(id_column)])
                .right_on([col(id_column)])
                .suffix(suffix)
                .finish();
            (new_df, count + 1)
        });

    Ok(ExLazyFrame::new(out_df.drop([id_column])))
}
