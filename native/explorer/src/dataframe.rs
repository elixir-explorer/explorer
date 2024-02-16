use polars::prelude::*;
use polars_ops::pivot::{pivot_stable, PivotAgg};

use polars::error::PolarsResult;
use polars::export::{arrow, arrow::ffi};
use std::collections::HashMap;
use std::result::Result;

use crate::datatypes::ExSeriesDtype;
use crate::ex_expr_to_exprs;
use crate::{ExDataFrame, ExExpr, ExLazyFrame, ExSeries, ExplorerError};
use either::Either;
use smartstring::alias::String as SmartString;

// Loads the IO functions for read/writing CSV, NDJSON, Parquet, etc.
pub mod io;

fn to_string_names(names: Vec<&str>) -> Vec<String> {
    names.into_iter().map(|s| s.to_string()).collect()
}

pub fn to_smart_strings(slices: Vec<&str>) -> Vec<SmartString> {
    slices.into_iter().map(|s| s.into()).collect()
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_transpose(
    df: ExDataFrame,
    keep_names_as: Option<&str>,
    new_col_names: Option<Vec<String>>,
) -> Result<ExDataFrame, ExplorerError> {
    let column_names = new_col_names.map(Either::Right);
    let new_df = df.clone_inner().transpose(keep_names_as, column_names)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_join(
    df: ExDataFrame,
    other: ExDataFrame,
    left_on: Vec<&str>,
    right_on: Vec<&str>,
    how: &str,
    suffix: Option<String>,
) -> Result<ExDataFrame, ExplorerError> {
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

    let mut join_args = JoinArgs::new(how);
    join_args.suffix = suffix;

    let new_df = df.join(&other, left_on, right_on, join_args)?;

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn df_names(df: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    let names = to_string_names(df.get_column_names());
    Ok(names)
}

#[rustler::nif]
pub fn df_dtypes(df: ExDataFrame) -> Result<Vec<ExSeriesDtype>, ExplorerError> {
    let mut dtypes: Vec<ExSeriesDtype> = vec![];

    for dtype in df.dtypes().iter() {
        dtypes.push(ExSeriesDtype::try_from(dtype)?)
    }

    Ok(dtypes)
}

#[rustler::nif]
pub fn df_shape(df: ExDataFrame) -> Result<(usize, usize), ExplorerError> {
    Ok(df.shape())
}

#[rustler::nif]
pub fn df_n_rows(df: ExDataFrame) -> Result<usize, ExplorerError> {
    Ok(df.height())
}

#[rustler::nif]
pub fn df_width(df: ExDataFrame) -> Result<usize, ExplorerError> {
    Ok(df.width())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_concat_rows(
    data: ExDataFrame,
    others: Vec<ExDataFrame>,
) -> Result<ExDataFrame, ExplorerError> {
    let mut out_df = data.clone();
    let names = out_df.get_column_names();
    let dfs = others
        .into_iter()
        .map(|ex_df| ex_df.select(&names))
        .collect::<Result<Vec<_>, _>>()?;

    for df in dfs {
        out_df.vstack_mut(&df)?;
    }
    // Follows recommendation from docs and rechunk after many vstacks.
    out_df.as_single_chunk_par();
    Ok(ExDataFrame::new(out_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_concat_columns(
    data: ExDataFrame,
    others: Vec<ExDataFrame>,
) -> Result<ExDataFrame, ExplorerError> {
    let id_column = "__row_count_id__";
    let first = data.clone_inner().lazy().with_row_index(id_column, None);

    // We need to be able to handle arbitrary column name overlap.
    // This builds up a join and suffixes conflicting names with _N where
    // N is the index of the df in the join array.
    let (out_df, _) = others
        .iter()
        .map(|data| data.clone_inner().lazy().with_row_index(id_column, None))
        .fold((first, 1), |(acc_df, count), lazy_df| {
            let suffix = format!("_{count}");
            let new_df = acc_df
                .join_builder()
                .with(lazy_df)
                .how(JoinType::Inner)
                .left_on([col(id_column)])
                .right_on([col(id_column)])
                .suffix(suffix)
                .finish();
            (new_df, count + 1)
        });

    Ok(ExDataFrame::new(out_df.drop([id_column]).collect()?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop_nils(
    df: ExDataFrame,
    subset: Option<Vec<String>>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.drop_nulls(subset.as_deref())?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop(df: ExDataFrame, name: &str) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.drop(name)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_select_at_idx(df: ExDataFrame, idx: usize) -> Result<Option<ExSeries>, ExplorerError> {
    let result = df.select_at_idx(idx).map(|s| ExSeries::new(s.clone()));
    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pull(df: ExDataFrame, name: &str) -> Result<ExSeries, ExplorerError> {
    let series = df.column(name).map(|s| ExSeries::new(s.clone()))?;
    Ok(series)
}

#[rustler::nif]
pub fn df_select(df: ExDataFrame, selection: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.select(selection)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_mask(df: ExDataFrame, mask: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    if let Ok(ca) = mask.bool() {
        let new_df = df.filter(ca)?;
        Ok(ExDataFrame::new(new_df))
    } else {
        Err(ExplorerError::Other("Expected a boolean mask".into()))
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_filter_with(
    df: ExDataFrame,
    ex_expr: ExExpr,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let exp = ex_expr.clone_inner();

    let new_df = if groups.is_empty() {
        df.clone_inner().lazy().filter(exp).collect()?
    } else {
        df.group_by_stable(groups)?
            .apply(|df| df.lazy().filter(exp.clone()).collect())?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_slice_by_indices(
    df: ExDataFrame,
    indices: Vec<u32>,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let idx = UInt32Chunked::from_vec("idx", indices);
    let new_df = if groups.is_empty() {
        df.take(&idx)?
    } else {
        df.group_by_stable(groups)?.apply(|df| df.take(&idx))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_slice_by_series(
    df: ExDataFrame,
    series: ExSeries,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    match series.strict_cast(&DataType::UInt32) {
        Ok(casted) => {
            let idx = casted.u32()?;

            let new_df = if groups.is_empty() {
                df.take(idx)?
            } else {
                df.group_by_stable(groups)?.apply(|df| df.take(idx))?
            };

            Ok(ExDataFrame::new(new_df))
        }
        Err(_) => Err(ExplorerError::Other(
            "slice/2 expects a series of positive integers".into(),
        )),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sample_n(
    df: ExDataFrame,
    n: u64,
    replace: bool,
    shuffle: bool,
    seed: Option<u64>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let n_s = Series::new("n", &[n]);
    let new_df = if groups.is_empty() {
        df.sample_n(&n_s, replace, shuffle, seed)?
    } else {
        df.group_by_stable(groups)?
            .apply(|df| df.sample_n(&n_s, replace, shuffle, seed))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sample_frac(
    df: ExDataFrame,
    frac: f64,
    replace: bool,
    shuffle: bool,
    seed: Option<u64>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let frac_s = Series::new("frac", &[frac]);
    let new_df = if groups.is_empty() {
        df.sample_frac(&frac_s, replace, shuffle, seed)?
    } else {
        df.group_by_stable(groups)?
            .apply(|df| df.sample_frac(&frac_s, replace, shuffle, seed))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
fn df_from_arrow_stream_pointer(stream_ptr: u64) -> Result<ExDataFrame, ExplorerError> {
    let stream_ptr = stream_ptr as *mut ffi::ArrowArrayStream;
    let stream_ref = unsafe { stream_ptr.as_mut() }
        .ok_or(ExplorerError::Other("Incorrect stream pointer".into()))?;

    let mut res = unsafe { ffi::ArrowArrayStreamReader::try_new(stream_ref) }
        .map_err(arrow_to_explorer_error)?;

    let df = match unsafe { res.next() } {
        None => DataFrame::empty(),
        Some(maybe) => {
            let mut acc = array_to_dataframe(maybe)?;

            while let Some(maybe) = unsafe { res.next() } {
                let df = array_to_dataframe(maybe)?;
                acc.vstack_mut(&df)?;
            }

            acc.align_chunks();
            acc
        }
    };

    Ok(ExDataFrame::new(df))
}

fn array_to_dataframe(
    stream_chunk: PolarsResult<Box<dyn arrow::array::Array>>,
) -> Result<DataFrame, ExplorerError> {
    let dyn_array = stream_chunk.map_err(arrow_to_explorer_error)?;

    let struct_array = dyn_array
        .as_any()
        .downcast_ref::<crate::dataframe::arrow::array::StructArray>()
        .ok_or(ExplorerError::Other(
            "Unable to downcast to StructArray in ArrowArrayStreamReader chunk".into(),
        ))?
        .clone();

    DataFrame::try_from(struct_array).map_err(ExplorerError::Polars)
}

fn arrow_to_explorer_error(error: impl std::fmt::Debug) -> ExplorerError {
    ExplorerError::Other(format!("Internal Arrow error: #{error:?}"))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sort_by(
    df: ExDataFrame,
    by_columns: Vec<String>,
    reverse: Vec<bool>,
    maintain_order: bool,
    multithreaded: bool,
    nulls_last: bool,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        // Note: we cannot use either df.sort or df.sort_with_options.
        // df.sort does not allow a nulls_last option.
        // df.sort_with_options only allows a single column.
        let by_columns = df.select_series(by_columns)?;
        df.sort_impl(
            by_columns,
            reverse,
            nulls_last,
            maintain_order,
            None,
            multithreaded,
        )?
    } else {
        df.group_by_stable(groups)?
            .apply(|df| df.sort(by_columns.clone(), reverse.clone(), maintain_order))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sort_with(
    data: ExDataFrame,
    expressions: Vec<ExExpr>,
    directions: Vec<bool>,
    maintain_order: bool,
    nulls_last: bool,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = data.clone_inner();
    let exprs = ex_expr_to_exprs(expressions);

    let new_df = if groups.is_empty() {
        df.lazy()
            .sort_by_exprs(exprs, directions, nulls_last, maintain_order)
            .collect()?
    } else {
        df.group_by_stable(groups)?.apply(|df| {
            df.lazy()
                .sort_by_exprs(&exprs, &directions, nulls_last, maintain_order)
                .collect()
        })?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_slice(
    df: ExDataFrame,
    offset: i64,
    length: usize,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        df.slice(offset, length)
    } else {
        df.group_by_stable(groups)?
            .apply(|df| Ok(df.slice(offset, length)))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_head(
    df: ExDataFrame,
    length: Option<usize>,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        df.head(length)
    } else {
        df.group_by_stable(groups)?
            .apply(|df| Ok(df.head(length)))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_tail(
    df: ExDataFrame,
    length: Option<usize>,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        df.tail(length)
    } else {
        df.group_by_stable(groups)?
            .apply(|df| Ok(df.tail(length)))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pivot_longer(
    df: ExDataFrame,
    id_vars: Vec<&str>,
    value_vars: Vec<&str>,
    names_to: String,
    values_to: String,
) -> Result<ExDataFrame, ExplorerError> {
    let melt_opts = MeltArgs {
        id_vars: to_smart_strings(id_vars),
        value_vars: to_smart_strings(value_vars),
        variable_name: Some(names_to.into()),
        value_name: Some(values_to.into()),
        streamable: true,
    };
    let new_df = df.melt2(melt_opts)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_distinct(
    df: ExDataFrame,
    subset: Vec<String>,
    columns_to_keep: Option<Vec<&str>>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.unique_stable(Some(&subset), UniqueKeepStrategy::First, None)?;

    match columns_to_keep {
        Some(columns) => Ok(ExDataFrame::new(new_df.select(columns)?)),
        None => Ok(ExDataFrame::new(new_df)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_to_dummies(df: ExDataFrame, selection: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let drop_first = false;
    let dummies = df
        .select(selection)
        .and_then(|df| df.to_dummies(None, drop_first))?;

    Ok(ExDataFrame::new(dummies))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_put_column(df: ExDataFrame, series: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let mut df = df.clone();
    let s = series.clone_inner();
    let new_df = df.with_column(s)?.clone();

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_nil_count(df: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.null_count();
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_mutate_with_exprs(
    df: ExDataFrame,
    columns: Vec<ExExpr>,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let mutations = ex_expr_to_exprs(columns);
    let new_df = if groups.is_empty() {
        df.clone_inner()
            .lazy()
            .with_comm_subexpr_elim(false)
            .with_columns(mutations)
            .collect()?
    } else {
        df.group_by_stable(groups)?
            .apply(|df| df.lazy().with_columns(&mutations).collect())?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn df_from_series(columns: Vec<ExSeries>) -> Result<ExDataFrame, ExplorerError> {
    let columns = columns.into_iter().map(|c| c.clone_inner()).collect();

    let df = DataFrame::new(columns)?;

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_rename_columns(
    df: ExDataFrame,
    renames: Vec<(&str, &str)>,
) -> Result<ExDataFrame, ExplorerError> {
    let mut df = df.clone();
    for (original, new_name) in renames {
        df.rename(original, new_name)?;
    }

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_groups(df: ExDataFrame, groups: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let groups = df.group_by(groups)?.groups()?;

    Ok(ExDataFrame::new(groups))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_group_indices(
    df: ExDataFrame,
    groups: Vec<&str>,
) -> Result<Vec<ExSeries>, ExplorerError> {
    let series = df
        .group_by_stable(groups)?
        .groups()?
        .column("groups")?
        .list()?
        .into_iter()
        .map(|series| ExSeries::new(series.unwrap()))
        .collect();
    Ok(series)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_summarise_with_exprs(
    df: ExDataFrame,
    groups: Vec<ExExpr>,
    aggs: Vec<ExExpr>,
) -> Result<ExDataFrame, ExplorerError> {
    let groups = ex_expr_to_exprs(groups);
    let aggs = ex_expr_to_exprs(aggs);

    let lf = df.clone_inner().lazy();

    let new_lf = if groups.is_empty() {
        // We do add a "shadow" column to be able to group by it.
        // This is going to force some aggregations like "mode" to be always inside
        // a "list".
        let s = Series::new_null("__explorer_null_for_group__", 1);
        lf.with_column(s.lit())
            .group_by_stable(["__explorer_null_for_group__"])
            .agg(aggs)
            .select(&[col("*").exclude(["__explorer_null_for_group__"])])
    } else {
        lf.group_by_stable(groups).agg(aggs)
    };

    Ok(ExDataFrame::new(new_lf.collect()?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pivot_wider(
    df: ExDataFrame,
    id_columns: Vec<&str>,
    pivot_column: &str,
    values_column: Vec<&str>,
    names_prefix: Option<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let mut counter: HashMap<String, u16> = HashMap::new();

    let mut new_df = pivot_stable(
        &df,
        values_column,
        id_columns.clone(),
        [pivot_column],
        false,
        Some(PivotAgg::First),
        None,
    )?;

    let mut new_names = to_string_names(new_df.get_column_names());

    for name in new_names.iter_mut() {
        let original_name = name.clone();

        if let Some(count) = counter.get(name) {
            if let Some(prefix) = names_prefix {
                *name = format!("{prefix}{name}");
            }

            if original_name == name.clone() {
                *name = format!("{name}_{count}");
            }

            counter
                .entry(name.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        } else {
            if !id_columns.contains(&original_name.as_str()) {
                if name == "null" {
                    *name = "nil".to_string();
                }

                if let Some(prefix) = names_prefix {
                    *name = format!("{prefix}{name}");
                }
            }

            counter.insert(name.to_string(), 1);
        }
    }

    new_df.set_column_names(&new_names)?;

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_explode(df: ExDataFrame, columns: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.explode(columns)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_unnest(df: ExDataFrame, columns: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.clone_inner().unnest(columns)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_lazy(df: ExDataFrame) -> Result<ExLazyFrame, ExplorerError> {
    let new_lf = df.clone_inner().lazy();
    Ok(ExLazyFrame::new(new_lf))
}
