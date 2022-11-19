use polars::prelude::*;
use polars_ops::pivot::{pivot_stable, PivotAgg};

use rustler::{Binary, Env, NewBinary};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::result::Result;

use crate::series::{to_ex_series_collection, to_series_collection};

use crate::{ExDataFrame, ExExpr, ExLazyFrame, ExSeries, ExplorerError};

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
pub fn df_from_csv(
    filename: &str,
    infer_schema_length: Option<usize>,
    has_header: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    projection: Option<Vec<usize>>,
    sep: &str,
    do_rechunk: bool,
    column_names: Option<Vec<String>>,
    dtypes: Option<Vec<(&str, &str)>>,
    encoding: &str,
    null_char: String,
    parse_dates: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let encoding = match encoding {
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    };

    let schema: Option<Schema> = match dtypes {
        Some(dtypes) => {
            let mut schema = Schema::new();
            for (name, dtype) in dtypes {
                schema.with_column(name.to_string(), dtype_from_str(dtype)?)
            }
            Some(schema)
        }
        None => None,
    };

    let df = CsvReader::from_path(filename)?
        .infer_schema(infer_schema_length)
        .has_header(has_header)
        .with_parse_dates(parse_dates)
        .with_n_rows(stop_after_n_rows)
        .with_delimiter(sep.as_bytes()[0])
        .with_skip_rows(skip_rows)
        .with_projection(projection)
        .with_rechunk(do_rechunk)
        .with_encoding(encoding)
        .with_columns(column_names)
        .with_dtypes(schema.as_ref())
        .with_null_values(Some(NullValues::AllColumns(vec![null_char])))
        .finish()?;

    Ok(ExDataFrame::new(df))
}

fn dtype_from_str(dtype: &str) -> Result<DataType, ExplorerError> {
    match dtype {
        "str" => Ok(DataType::Utf8),
        "f64" => Ok(DataType::Float64),
        "i64" => Ok(DataType::Int64),
        "bool" => Ok(DataType::Boolean),
        "date" => Ok(DataType::Date),
        "datetime[ms]" => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        "datetime[μs]" => Ok(DataType::Datetime(TimeUnit::Microseconds, None)),
        "datetime[ns]" => Ok(DataType::Datetime(TimeUnit::Nanoseconds, None)),
        _ => Err(ExplorerError::Internal("Unrecognised datatype".into())),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_parquet(filename: &str) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);
    let df = ParquetReader::new(buf_reader).finish()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_parquet(
    data: ExDataFrame,
    filename: &str,
    compression: Option<&str>,
    compression_level: Option<i32>,
) -> Result<(), ExplorerError> {
    let df = &data.resource.0;
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    let compression = match (compression, compression_level) {
        (Some("snappy"), _) => ParquetCompression::Snappy,
        (Some("gzip"), level) => {
            let level = match level {
                Some(level) => Some(GzipLevel::try_new(u8::try_from(level)?)?),
                None => None,
            };
            ParquetCompression::Gzip(level)
        }
        (Some("brotli"), level) => {
            let level = match level {
                Some(level) => Some(BrotliLevel::try_new(u32::try_from(level)?)?),
                None => None,
            };
            ParquetCompression::Brotli(level)
        }
        (Some("zstd"), level) => {
            let level = match level {
                Some(level) => Some(ZstdLevel::try_new(level)?),
                None => None,
            };
            ParquetCompression::Zstd(level)
        }
        (Some("lz4raw"), _) => ParquetCompression::Lz4Raw,
        _ => ParquetCompression::Uncompressed,
    };
    ParquetWriter::new(&mut buf_writer)
        .with_compression(compression)
        .finish(&mut df.clone())?;
    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_csv(
    env: Env,
    data: ExDataFrame,
    has_headers: bool,
    delimiter: u8,
) -> Result<Binary, ExplorerError> {
    let df = &data.resource.0;
    let mut buf = vec![];

    CsvWriter::new(&mut buf)
        .has_header(has_headers)
        .with_delimiter(delimiter)
        .finish(&mut df.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_csv(
    data: ExDataFrame,
    filename: &str,
    has_headers: bool,
    delimiter: u8,
) -> Result<(), ExplorerError> {
    let df = &data.resource.0;
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    CsvWriter::new(&mut buf_writer)
        .has_header(has_headers)
        .with_delimiter(delimiter)
        .finish(&mut df.clone())?;
    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_ipc(
    filename: &str,
    columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);
    let df = IpcReader::new(buf_reader)
        .with_columns(columns)
        .with_projection(projection)
        .finish()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc(
    data: ExDataFrame,
    filename: &str,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let df = &data.resource.0;
    // Select the compression algorithm.
    let compression = match compression {
        Some("lz4") => Some(IpcCompression::LZ4),
        Some("zstd") => Some(IpcCompression::ZSTD),
        _ => None,
    };

    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    IpcWriter::new(&mut buf_writer)
        .with_compression(compression)
        .finish(&mut df.clone())?;
    Ok(())
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_ipc_stream(
    filename: &str,
    columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);
    let df = IpcStreamReader::new(buf_reader)
        .with_columns(columns)
        .with_projection(projection)
        .finish()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc_stream(
    data: ExDataFrame,
    filename: &str,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let df = &data.resource.0;
    // Select the compression algorithm.
    let compression = match compression {
        Some("LZ4") => Some(IpcCompression::LZ4),
        Some("ZSTD") => Some(IpcCompression::ZSTD),
        _ => None,
    };

    let mut file = File::create(filename).expect("could not create file");
    IpcStreamWriter::new(&mut file)
        .with_compression(compression)
        .finish(&mut df.clone())?;
    Ok(())
}

#[cfg(not(target_arch = "arm"))]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_ndjson(
    filename: &str,
    infer_schema_length: Option<usize>,
    batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);
    let df = JsonReader::new(buf_reader)
        .with_json_format(JsonFormat::JsonLines)
        .with_batch_size(batch_size)
        .infer_schema_len(infer_schema_length)
        .finish()?;

    Ok(ExDataFrame::new(df))
}

#[cfg(not(target_arch = "arm"))]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ndjson(data: ExDataFrame, filename: &str) -> Result<(), ExplorerError> {
    let df = &data.resource.0;
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    JsonWriter::new(&mut buf_writer).finish(&mut df.clone())?;
    Ok(())
}

#[cfg(target_arch = "arm")]
#[rustler::nif]
pub fn df_from_ndjson(
    _filename: &str,
    _infer_schema_length: Option<usize>,
    _batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    Err(ExplorerError::Other(format!(
        "NDJSON parsing is not enabled for this machine"
    )))
}

#[cfg(target_arch = "arm")]
#[rustler::nif]
pub fn df_to_ndjson(_data: ExDataFrame, _filename: &str) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other(format!(
        "NDJSON writing is not enabled for this machine"
    )))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_join(
    data: ExDataFrame,
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
                "Join method {} not supported",
                how
            )))
        }
    };

    let df = &data.resource.0;
    let df1 = &other.resource.0;
    let new_df = df.join(df1, left_on, right_on, how, suffix)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_get_columns(data: ExDataFrame) -> Result<Vec<ExSeries>, ExplorerError> {
    let df = &data.resource.0;
    Ok(to_ex_series_collection(df.get_columns().clone()))
}

#[rustler::nif]
pub fn df_names(data: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    let df = &data.resource.0;
    Ok(df.get_column_names_owned())
}

#[rustler::nif]
pub fn df_dtypes(data: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    let df = &data.resource.0;
    let result = df.dtypes().iter().map(|dtype| dtype.to_string()).collect();
    Ok(result)
}

#[rustler::nif]
pub fn df_shape(data: ExDataFrame) -> Result<(usize, usize), ExplorerError> {
    Ok(data.resource.0.shape())
}

#[rustler::nif]
pub fn df_n_rows(data: ExDataFrame) -> Result<usize, ExplorerError> {
    Ok(data.resource.0.height())
}

#[rustler::nif]
pub fn df_width(data: ExDataFrame) -> Result<usize, ExplorerError> {
    Ok(data.resource.0.width())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_concat_rows(
    data: ExDataFrame,
    others: Vec<ExDataFrame>,
) -> Result<ExDataFrame, ExplorerError> {
    let mut out_df = data.resource.0.clone();
    let names = out_df.get_column_names();
    let dfs = others
        .into_iter()
        .map(|ex_df| ex_df.resource.0.select(&names))
        .collect::<Result<Vec<_>, _>>()?;

    for df in dfs {
        out_df.vstack_mut(&df)?;
    }
    // Follows recommendation from docs and rechunk after many vstacks.
    out_df.rechunk();
    Ok(ExDataFrame::new(out_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_concat_columns(
    data: ExDataFrame,
    others: Vec<ExDataFrame>,
) -> Result<ExDataFrame, ExplorerError> {
    let id_column = "__row_count_id__";
    let first = data
        .resource
        .0
        .clone()
        .lazy()
        .with_row_count(id_column, None);

    // We need to be able to handle arbitrary column name overlap.
    // This builds up a join and suffixes conflicting names with _N where
    // N is the index of the df in the join array.
    let (out_df, _) = others
        .iter()
        .map(|data| {
            data.resource
                .0
                .clone()
                .lazy()
                .with_row_count(id_column, None)
        })
        .fold((first, 1), |(acc_df, count), df| {
            let suffix = format!("_{}", count);
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

    Ok(ExDataFrame::new(
        out_df.drop_columns([id_column]).collect()?,
    ))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop_nulls(
    data: ExDataFrame,
    subset: Option<Vec<String>>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: &DataFrame = &data.resource.0;
    let new_df = df.drop_nulls(subset.as_ref().map(|s| s.as_ref()))?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop(data: ExDataFrame, name: &str) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = df.drop(name)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_select_at_idx(data: ExDataFrame, idx: usize) -> Result<Option<ExSeries>, ExplorerError> {
    let df = &data.resource.0;
    let result = df.select_at_idx(idx).map(|s| ExSeries::new(s.clone()));
    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pull(data: ExDataFrame, name: &str) -> Result<ExSeries, ExplorerError> {
    let df = &data.resource.0;
    let series = df.column(name).map(|s| ExSeries::new(s.clone()))?;
    Ok(series)
}

#[rustler::nif]
pub fn df_select(data: ExDataFrame, selection: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = df.select(&selection)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_mask(data: ExDataFrame, mask: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let filter_series = &mask.resource.0;
    if let Ok(ca) = filter_series.bool() {
        let new_df = df.filter(ca)?;
        Ok(ExDataFrame::new(new_df))
    } else {
        Err(ExplorerError::Other("Expected a boolean mask".into()))
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_filter_with(
    data: ExDataFrame,
    ex_expr: ExExpr,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: DataFrame = data.resource.0.clone();
    let exp: Expr = ex_expr.resource.0.clone();

    let new_df = if groups.is_empty() {
        df.lazy().filter(exp).collect()?
    } else {
        df.groupby_stable(groups)?
            .apply(|df| df.lazy().filter(exp.clone()).collect())?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_slice_by_indices(
    data: ExDataFrame,
    indices: Vec<u32>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let idx = UInt32Chunked::from_vec("idx", indices);
    let new_df = df.take(&idx)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sample_n(
    data: ExDataFrame,
    n: usize,
    with_replacement: bool,
    seed: Option<u64>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = if groups.is_empty() {
        df.sample_n(n, with_replacement, false, seed)?
    } else {
        df.groupby_stable(groups)?
            .apply(|df| df.sample_n(n, with_replacement, false, seed))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sample_frac(
    data: ExDataFrame,
    frac: f64,
    with_replacement: bool,
    seed: Option<u64>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = if groups.is_empty() {
        df.sample_frac(frac, with_replacement, false, seed)?
    } else {
        df.groupby_stable(groups)?
            .apply(|df| df.sample_frac(frac, with_replacement, false, seed))?
    };

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_arrange(
    data: ExDataFrame,
    by_columns: Vec<String>,
    reverse: Vec<bool>,
    groups: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: DataFrame = data.resource.0.clone();

    let new_df = if groups.is_empty() {
        let new_df = &df.sort(by_columns, reverse)?;
        new_df.clone()
    } else {
        let new_df = &df
            .groupby_stable(groups)?
            .apply(|df| df.sort(by_columns.clone(), reverse.clone()))?;
        new_df.clone()
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
    let df: DataFrame = data.resource.0.clone();
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
    data: ExDataFrame,
    offset: i64,
    length: usize,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = df.slice(offset, length);
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_head(
    data: ExDataFrame,
    length: Option<usize>,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: DataFrame = data.resource.0.clone();

    let new_df = if groups.is_empty() {
        df.head(length)
    } else {
        df.groupby_stable(groups)?.apply(|df| Ok(df.head(length)))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_tail(
    data: ExDataFrame,
    length: Option<usize>,
    groups: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: DataFrame = data.resource.0.clone();

    let new_df = if groups.is_empty() {
        df.tail(length)
    } else {
        df.groupby_stable(groups)?.apply(|df| Ok(df.tail(length)))?
    };
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pivot_longer(
    data: ExDataFrame,
    id_vars: Vec<String>,
    value_vars: Vec<String>,
    names_to: String,
    values_to: String,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let melt_opts = MeltArgs {
        id_vars,
        value_vars,
        variable_name: Some(names_to),
        value_name: Some(values_to),
    };
    let new_df = df.melt2(melt_opts)?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_distinct(
    data: ExDataFrame,
    maintain_order: bool,
    subset: Vec<String>,
    columns_to_keep: Option<Vec<&str>>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: &DataFrame = &data.resource.0;
    let new_df = match maintain_order {
        false => df.unique(Some(&subset), UniqueKeepStrategy::First)?,
        true => df.unique_stable(Some(&subset), UniqueKeepStrategy::First)?,
    };

    match columns_to_keep {
        Some(columns) => Ok(ExDataFrame::new(new_df.select(&columns)?)),
        None => Ok(ExDataFrame::new(new_df)),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_to_dummies(
    data: ExDataFrame,
    selection: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = df.select(&selection).and_then(|df| df.to_dummies())?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_put_column(data: ExDataFrame, series: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    let mut df: DataFrame = data.resource.0.clone();
    let s: Series = series.resource.0.clone();
    let new_df = df.with_column(s)?;

    Ok(ExDataFrame::new(new_df.clone()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_mutate_with_exprs(
    data: ExDataFrame,
    columns: Vec<ExExpr>,
) -> Result<ExDataFrame, ExplorerError> {
    let df: DataFrame = data.resource.0.clone();
    let new_df = df
        .lazy()
        .with_columns(ex_expr_to_exprs(columns))
        .collect()?;

    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif]
pub fn df_from_series(columns: Vec<ExSeries>) -> Result<ExDataFrame, ExplorerError> {
    let columns = to_series_collection(columns);
    let df = DataFrame::new(columns)?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_rename_columns(
    data: ExDataFrame,
    renames: Vec<(&str, &str)>,
) -> Result<ExDataFrame, ExplorerError> {
    let mut df: DataFrame = data.resource.0.clone();
    for (original, new_name) in renames {
        df.rename(original, new_name).expect("should rename");
    }

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_groups(data: ExDataFrame, groups: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let groups = df.groupby(groups)?.groups()?;
    Ok(ExDataFrame::new(groups))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_group_indices(data: ExDataFrame, groups: Vec<&str>) -> Result<ExSeries, ExplorerError> {
    let df = &data.resource.0;
    let series = df
        .groupby_stable(groups)?
        .groups()?
        .column("groups")
        .map(|series| ExSeries::new(series.clone()))?;
    Ok(series)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_summarise_with_exprs(
    data: ExDataFrame,
    groups: Vec<ExExpr>,
    aggs: Vec<ExExpr>,
) -> Result<ExDataFrame, ExplorerError> {
    let df = data.resource.0.clone();
    let groups = ex_expr_to_exprs(groups);
    let aggs = ex_expr_to_exprs(aggs);

    let new_df = df.lazy().groupby_stable(groups).agg(aggs).collect()?;
    Ok(ExDataFrame::new(new_df))
}

fn ex_expr_to_exprs(ex_exprs: Vec<ExExpr>) -> Vec<Expr> {
    let exprs: Vec<Expr> = ex_exprs
        .iter()
        .map(|ex_expr| ex_expr.resource.0.clone())
        .collect();

    exprs
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pivot_wider(
    data: ExDataFrame,
    id_columns: Vec<&str>,
    pivot_column: &str,
    values_column: &str,
) -> Result<ExDataFrame, ExplorerError> {
    let df = &data.resource.0;
    let new_df = pivot_stable(
        df,
        [values_column],
        id_columns,
        [pivot_column],
        PivotAgg::First,
        false,
    )?;
    Ok(ExDataFrame::new(new_df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_to_lazy(data: ExDataFrame) -> Result<ExLazyFrame, ExplorerError> {
    let new_lf = data.resource.0.clone().lazy();
    Ok(ExLazyFrame::new(new_lf))
}
