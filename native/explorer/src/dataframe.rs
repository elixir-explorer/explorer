use polars::frame::PivotColumnNaming;
use polars::prelude::*;

use polars_arrow::ffi;
use rustler::Env;
use std::collections::HashMap;

use crate::datatypes::ExSeriesDtype;
use crate::ex_expr_to_exprs;
use crate::series::shuffle_option;
use crate::{ExDataFrame, ExExpr, ExLazyFrame, ExSeries, ExplorerError};
use either::Either;

// Loads the IO functions for read/writing CSV, NDJSON, Parquet, etc.
pub mod io;

fn to_string_names(names: Vec<&str>) -> Vec<String> {
    names.into_iter().map(|s| s.to_string()).collect()
}

pub trait GroupByOptOrder {
    fn group_by_opt_order(
        &self,
        groups: Vec<&str>,
        stable_groups: bool,
    ) -> Result<GroupBy<'_>, PolarsError>;
}

impl GroupByOptOrder for ExDataFrame {
    fn group_by_opt_order(
        &self,
        groups: Vec<&str>,
        stable_groups: bool,
    ) -> Result<GroupBy<'_>, PolarsError> {
        if stable_groups {
            self.group_by_stable(groups)
        } else {
            self.group_by(groups)
        }
    }
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

#[rustler::nif]
pub fn df_names(df: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    let names = df
        .get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
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

#[rustler::nif]
pub fn df_estimated_size(df: ExDataFrame) -> Result<usize, ExplorerError> {
    Ok(df.estimated_size())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_concat_columns(dfs: Vec<ExDataFrame>) -> Result<ExDataFrame, ExplorerError> {
    let mut previous_names = PlHashSet::new();

    let cols = dfs
        .iter()
        .enumerate()
        .flat_map(|(idx, ex_df)| {
            let df = ex_df.clone_inner();

            df.columns()
                .iter()
                .map(|col| {
                    let name = col.name();
                    if previous_names.contains(&name.clone().to_string()) {
                        let new_name = format!("{name}_{idx}");
                        previous_names.insert(new_name.clone());
                        let mut new_col = col.clone();
                        new_col.rename(new_name.into());
                        new_col
                    } else {
                        previous_names.insert(name.to_string());
                        col.clone()
                    }
                })
                .collect::<Vec<Column>>()
        })
        .collect::<Vec<Column>>();

    let out_df = DataFrame::new_infer_height(cols)?;

    Ok(ExDataFrame::new(out_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop(df: ExDataFrame, name: &str) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.drop(name)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_select_at_idx(df: ExDataFrame, idx: usize) -> Result<Option<ExSeries>, ExplorerError> {
    let result = df
        .select_at_idx(idx)
        .map(|s| ExSeries::new(s.as_materialized_series().clone()));
    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pull(df: ExDataFrame, name: &str) -> Result<ExSeries, ExplorerError> {
    let series = df
        .column(name)
        .map(|s| ExSeries::new(s.as_materialized_series().clone()))?;
    Ok(series)
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
pub fn df_slice_by_indices(
    df: ExDataFrame,
    indices: Vec<u32>,
    groups: Vec<&str>,
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let idx = UInt32Chunked::from_vec("idx".into(), indices);
    let new_df = if groups.is_empty() {
        df.take(&idx)?
    } else {
        df.group_by_opt_order(groups, stable_groups)?
            .apply(|df| df.take(&idx))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_slice_by_series(
    df: ExDataFrame,
    series: ExSeries,
    groups: Vec<&str>,
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    match series.strict_cast(&DataType::UInt32) {
        Ok(casted) => {
            let idx = casted.u32()?;

            let new_df = if groups.is_empty() {
                df.take(idx)?
            } else {
                df.group_by_opt_order(groups, stable_groups)?
                    .apply(|df| df.take(idx))?
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
    groups: Vec<&str>,
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let n_s = Series::new("n".into(), &[n]);
    let new_df = if groups.is_empty() {
        df.sample_n(&n_s, replace, shuffle_option(shuffle), seed)?
    } else {
        df.group_by_opt_order(groups, stable_groups)?
            .apply(|df| df.sample_n(&n_s, replace, shuffle_option(shuffle), seed))?
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
    groups: Vec<&str>,
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let frac_s = Series::new("frac".into(), &[frac]);
    let new_df = if groups.is_empty() {
        df.sample_frac(&frac_s, replace, shuffle_option(shuffle), seed)?
    } else {
        df.group_by_opt_order(groups, stable_groups)?
            .apply(|df| df.sample_frac(&frac_s, replace, shuffle_option(shuffle), seed))?
    };

    Ok(ExDataFrame::new(new_df))
}

type ArrowStreamReader = ffi::ArrowArrayStreamReader<Box<ffi::ArrowArrayStream>>;

// This NIF runs on a dirty scheduler because reading the stream can take
// a non trivial amount of time (e.g. an ADBC driver fetching result chunks
// over the network for example).
//
// On a dirty scheduler the calling process can be killed while we are still
// reading: its links and monitors fire right away, even though this NIF keeps
// running until it returns. `Adbc.Connection` monitors the caller and, on
// `:DOWN`, releases the stream and hands the connection to the next queued
// query. Two things keep that safe:
//
//   * The Rust side takes ownership of the stream up front, so ADBC's release
//     is a no-op instead of freeing the stream while we use it.
//   * The Rust side stops pulling batches once the caller is gone, so it doesn't
//     keep using the connection after ADBC has handed it to another process.
#[rustler::nif(schedule = "DirtyIo")]
fn df_from_arrow_stream_pointer(env: Env, stream_ptr: u64) -> Result<ExDataFrame, ExplorerError> {
    let source = stream_ptr as *mut ffi::ArrowArrayStream;

    if source.is_null() {
        return Err(ExplorerError::Other("Incorrect stream pointer".into()));
    }

    // The Arrow C stream interface allows moving a stream by bitwise copy,
    // leaving a released (empty) stream behind in the producer's struct.
    // The Rust copy is released exactly once, when `res` is dropped.
    let owned = Box::new(unsafe { std::ptr::replace(source, ffi::ArrowArrayStream::empty()) });

    let mut res =
        unsafe { ffi::ArrowArrayStreamReader::try_new(owned) }.map_err(arrow_to_explorer_error)?;

    let df = match next_chunk(env, &mut res)? {
        None => DataFrame::empty(),
        Some(maybe) => {
            let mut acc = array_to_dataframe(maybe)?;

            while let Some(maybe) = next_chunk(env, &mut res)? {
                let df = array_to_dataframe(maybe)?;
                acc.vstack_mut(&df)?;
            }

            acc.align_chunks();
            acc
        }
    };

    Ok(ExDataFrame::new(df))
}

// Pull the next chunk from the stream while the calling process is still alive.
fn next_chunk(
    env: Env,
    res: &mut ArrowStreamReader,
) -> Result<Option<PolarsResult<Box<dyn polars_arrow::array::Array>>>, ExplorerError> {
    if unsafe { rustler::sys::enif_is_current_process_alive(env.as_c_arg()) } == 0 {
        return Err(ExplorerError::Other(
            "calling process exited while reading the Arrow stream".into(),
        ));
    }

    Ok(unsafe { res.next() })
}

fn array_to_dataframe(
    stream_chunk: PolarsResult<Box<dyn polars_arrow::array::Array>>,
) -> Result<DataFrame, ExplorerError> {
    let dyn_array = stream_chunk.map_err(arrow_to_explorer_error)?;

    let struct_array = dyn_array
        .as_any()
        .downcast_ref::<polars_arrow::array::StructArray>()
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
#[allow(clippy::too_many_arguments)]
pub fn df_sort_by(
    df: ExDataFrame,
    by_columns: Vec<String>,
    reverse: Vec<bool>,
    maintain_order: bool,
    multithreaded: bool,
    nulls_last: bool,
    groups: Vec<&str>,
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let sort_options = SortMultipleOptions::new()
        .with_maintain_order(maintain_order)
        .with_multithreaded(multithreaded)
        .with_nulls_last(nulls_last)
        .with_order_descending_multi(reverse);

    let new_df = if groups.is_empty() {
        df.sort(by_columns, sort_options)?
    } else {
        df.group_by_opt_order(groups, stable_groups)?
            .apply(|df| df.sort(by_columns.clone(), sort_options.clone()))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::too_many_arguments)]
pub fn df_sort_with(
    df: ExDataFrame,
    expressions: Vec<ExExpr>,
    directions: Vec<bool>,
    maintain_order: bool,
    multithreaded: bool,
    nulls_last: bool,
    groups: Vec<&str>,
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let exprs = ex_expr_to_exprs(expressions);

    let sort_options = SortMultipleOptions::new()
        .with_maintain_order(maintain_order)
        .with_multithreaded(multithreaded)
        .with_nulls_last(nulls_last)
        .with_order_descending_multi(directions);

    let new_df = if groups.is_empty() {
        df.clone_inner()
            .lazy()
            .sort_by_exprs(exprs, sort_options)
            .collect()?
    } else {
        df.group_by_opt_order(groups, stable_groups)?.apply(|df| {
            df.lazy()
                .sort_by_exprs(&exprs, sort_options.clone())
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
    stable_groups: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let new_df = if groups.is_empty() {
        df.slice(offset, length)
    } else {
        df.group_by_opt_order(groups, stable_groups)?
            .apply(|df| Ok(df.slice(offset, length)))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_to_dummies(df: ExDataFrame, selection: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let drop_first = false;
    let drop_nulls = false;
    let dummies = df
        .select(selection)
        .and_then(|df| df.to_dummies(None, drop_first, drop_nulls))?;

    Ok(ExDataFrame::new(dummies))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_put_column(df: ExDataFrame, series: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let mut df = df.clone();
    let s = series.clone_inner();
    let new_df = df.with_column(s.into_column())?.clone();

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_nil_count(df: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    let new_df = df.null_count();
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn df_from_series(columns: Vec<ExSeries>) -> Result<ExDataFrame, ExplorerError> {
    let columns = columns
        .into_iter()
        .map(|c| Column::from(c.clone_inner()))
        .collect();

    let df = DataFrame::new_infer_height(columns)?;

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
        .group_by_with_series(df.select_to_vec(groups)?, true, true)?
        .groups()?
        .column("groups")?
        .list()?
        .series_iter()
        .map(|series| ExSeries::new(series.unwrap()))
        .collect();
    Ok(series)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_partition_by(
    df: ExDataFrame,
    groups: Vec<&str>,
) -> Result<Vec<ExDataFrame>, ExplorerError> {
    let include_key = true;
    let dfs = df.partition_by_stable(groups, include_key)?;

    Ok(dfs.into_iter().map(ExDataFrame::new).collect())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pivot_wider(
    df: ExDataFrame,
    id_columns: Vec<&str>,
    pivot_column: &str,
    values_column: Vec<&str>,
    names_prefix: Option<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    // We need to preserve the original ID columns with a prefix,
    // so if there is any "new column name" coming from a "value column"
    // conflicting with some ID column, we can keep that ID column and
    // the new column names.
    let mut df = df.clone_inner();
    let explorer_prefix = "__explorer_column_id__";
    let temp_id_names: Vec<String> = id_columns
        .iter()
        .map(|id_name| format!("{explorer_prefix}{id_name}"))
        .collect();

    for (id_name, new_name) in id_columns.iter().zip(&temp_id_names) {
        df.rename(id_name, new_name.into())?;
    }

    // Polars removed the eager pivot, so we use the lazy one. It requires
    // the values of the "on" column upfront, which we compute here in order
    // of first appearance (the same order the eager pivot used to produce).
    let on_columns =
        df.select([pivot_column])?
            .unique_stable(None, UniqueKeepStrategy::First, None)?;

    let mut new_df = df
        .lazy()
        .pivot(
            by_name([pivot_column], true, false),
            Arc::new(on_columns),
            by_name(temp_id_names, true, false),
            by_name(values_column, true, false),
            element().first(),
            true,
            "_".into(),
            PivotColumnNaming::Auto,
        )
        .collect()?;

    // Instead of using the names from the pivoted DF, we go back
    // and restore the original ID column names, so we can use our
    // algo below to preserve all columns.
    let clean_names = new_df
        .get_column_names()
        .iter()
        .map(|name| name.trim_start_matches(explorer_prefix))
        .collect();

    let mut new_names = to_string_names(clean_names);
    let mut counter: HashMap<String, u16> = HashMap::new();

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
pub fn df_lazy(df: ExDataFrame) -> Result<ExLazyFrame, ExplorerError> {
    let new_lf = df.clone_inner().lazy();
    Ok(ExLazyFrame::new(new_lf))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_re_dtype(pattern: &str) -> Result<ExSeriesDtype, ExplorerError> {
    let s = Series::new("dummy".into(), [""])
        .into_frame()
        .lazy()
        .with_column(col("dummy").str().extract_groups(pattern)?.alias("dummy"))
        .collect()?
        .column("dummy")?
        .clone();
    let ex_dtype = ExSeriesDtype::try_from(s.dtype())?;
    Ok(ex_dtype)
}
