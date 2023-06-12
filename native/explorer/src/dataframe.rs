use polars::prelude::*;
use polars_ops::pivot::{pivot_stable, PivotAgg};

use polars::export::arrow::ffi;
use std::collections::HashMap;
use std::result::Result;

use crate::ex_expr_to_exprs;
use crate::{ExDataFrame, ExExpr, ExLazyFrame, ExSeries, ExplorerError};
use smartstring::alias::String as SmartString;

// Loads the IO functions for read/writing CSV, NDJSON, Parquet, etc.
pub mod io;

// Helper to normalize integers and float column dtypes.
pub fn normalize_numeric_dtypes(df: &mut DataFrame) -> Result<DataFrame, crate::ExplorerError> {
    let dtypes = df.dtypes().into_iter().enumerate();

    for (idx, dtype) in dtypes {
        match dtype {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32 => df.apply_at_idx(idx, |s| s.cast(&DataType::Int64).unwrap())?,
            DataType::Float32 => df.apply_at_idx(idx, |s| s.cast(&DataType::Float64).unwrap())?,
            _ => df,
        };
    }

    Ok(df.clone())
}

fn to_string_names(names: Vec<&str>) -> Vec<String> {
    names.into_iter().map(|s| s.to_string()).collect()
}

pub fn to_smart_strings(slices: Vec<&str>) -> Vec<SmartString> {
    slices.into_iter().map(|s| s.into()).collect()
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
        "outer" => JoinType::Outer,
        "cross" => JoinType::Cross,
        _ => {
            return Err(ExplorerError::Other(format!(
                "Join method {how} not supported"
            )))
        }
    };

    let new_df = df.join(&other, left_on, right_on, how, suffix)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn df_names(df: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    let names = to_string_names(df.get_column_names());
    Ok(names)
}

#[rustler::nif]
pub fn df_dtypes(df: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    let result = df.dtypes().iter().map(|dtype| dtype.to_string()).collect();
    Ok(result)
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
    let first = data.clone_inner().lazy().with_row_count(id_column, None);

    // We need to be able to handle arbitrary column name overlap.
    // This builds up a join and suffixes conflicting names with _N where
    // N is the index of the df in the join array.
    let (out_df, _) = others
        .iter()
        .map(|data| data.clone_inner().lazy().with_row_count(id_column, None))
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

    Ok(ExDataFrame::new(
        out_df.drop_columns([id_column]).collect()?,
    ))
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
        df.groupby_stable(groups)?
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
        df.groupby_stable(groups)?.apply(|df| df.take(&idx))?
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
                df.groupby_stable(groups)?.apply(|df| df.take(idx))?
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
    n: usize,
    replace: bool,
    shuffle: bool,
    seed: Option<u64>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        df.sample_n(n, replace, shuffle, seed)?
    } else {
        df.groupby_stable(groups)?
            .apply(|df| df.sample_n(n, replace, shuffle, seed))?
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
    let new_df = if groups.is_empty() {
        df.sample_frac(frac, replace, shuffle, seed)?
    } else {
        df.groupby_stable(groups)?
            .apply(|df| df.sample_frac(frac, replace, shuffle, seed))?
    };

    Ok(ExDataFrame::new(new_df))
}

/// NOTE: The '_ref' parameter is needed to prevent the BEAM GC from collecting the stream too soon.
#[rustler::nif]
fn df_experiment(stream_ptr: u64, _ref: rustler::Term) -> Result<String, ExplorerError> {
    let stream_ptr = stream_ptr as *mut ffi::ArrowArrayStream;
    match unsafe { stream_ptr.as_mut() } {
        None => Err(ExplorerError::Other("Incorrect stream pointer".into())),
        Some(stream_ref) => {
            let mut res = unsafe { ffi::ArrowArrayStreamReader::try_new(stream_ref) }
                .map_err(arrow_to_explorer_error)?;

            while let Some(Ok(val)) = unsafe { res.next() } {
                println!("{:?}", val);
            }

            Ok("123".to_string())
        }
    }
}

fn arrow_to_explorer_error(error: impl std::fmt::Debug) -> ExplorerError {
    ExplorerError::Other(format!("Internal Arrow error: #{:?}", error))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_arrange(
    df: ExDataFrame,
    by_columns: Vec<String>,
    reverse: Vec<bool>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        df.sort(by_columns, reverse)?
    } else {
        df.groupby_stable(groups)?
            .apply(|df| df.sort(by_columns.clone(), reverse.clone()))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_arrange_with(
    data: ExDataFrame,
    expressions: Vec<ExExpr>,
    directions: Vec<bool>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = data.clone_inner();
    let exprs = ex_expr_to_exprs(expressions);

    let new_df = if groups.is_empty() {
        df.lazy()
            .sort_by_exprs(exprs, directions, false)
            .collect()?
    } else {
        df.groupby_stable(groups)?.apply(|df| {
            df.lazy()
                .sort_by_exprs(&exprs, &directions, false)
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
        df.groupby_stable(groups)?
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
        df.groupby_stable(groups)?.apply(|df| Ok(df.head(length)))?
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
        df.groupby_stable(groups)?.apply(|df| Ok(df.tail(length)))?
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
    let dummies = df.select(selection).and_then(|df| df.to_dummies(None))?;
    let series = dummies
        .iter()
        .map(|series| series.cast(&DataType::Int64).unwrap())
        .collect();
    Ok(ExDataFrame::new(DataFrame::new(series)?))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_put_column(df: ExDataFrame, series: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let mut df = df.clone();
    let s = series.clone_inner();
    let new_df = df.with_column(s)?.clone();

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_describe(
    df: ExDataFrame,
    percentiles: Option<Vec<f64>>,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.describe(percentiles.as_deref())?;

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_nil_count(df: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    let mut new_df = df.null_count();
    let new_df = normalize_numeric_dtypes(&mut new_df)?;
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
        df.clone_inner().lazy().with_columns(mutations).collect()?
    } else {
        df.groupby_stable(groups)?
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
        df.rename(original, new_name).expect("should rename");
    }

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_groups(df: ExDataFrame, groups: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let groups = df.groupby(groups)?.groups()?;

    Ok(ExDataFrame::new(groups))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_group_indices(
    df: ExDataFrame,
    groups: Vec<&str>,
) -> Result<Vec<ExSeries>, ExplorerError> {
    let series = df
        .groupby_stable(groups)?
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
        lf.with_columns(aggs).first()
    } else {
        lf.groupby_stable(groups).agg(aggs)
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
pub fn df_to_lazy(df: ExDataFrame) -> Result<ExLazyFrame, ExplorerError> {
    let new_lf = df.clone_inner().lazy();
    Ok(ExLazyFrame::new(new_lf))
}
