use polars::prelude::*;
use rustler::{Term, TermType};

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::result::Result;

use crate::series::{to_ex_series_collection, to_series_collection};

use crate::{ExAnyValue, ExDataFrame, ExSeries, ExplorerError};

macro_rules! df_read {
    ($data: ident, $df: ident, $body: block) => {
        match $data.resource.0.read() {
            Ok($df) => $body,
            Err(_) => Err(ExplorerError::Internal(
                "Failed to take read lock for df".into(),
            )),
        }
    };
}

macro_rules! df_read_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.resource.0.read(), $other.resource.0.read()) {
            (Ok($df), Ok($df1)) => $body,
            _ => Err(ExplorerError::Internal(
                "Failed to take read locks for left and right".into(),
            )),
        }
    };
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
pub fn df_read_csv(
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
        .with_null_values(Some(NullValues::AllColumns(null_char)))
        .finish()?;

    Ok(ExDataFrame::new(df))
}

// TODO: consider adding "datetime[ns]"
fn dtype_from_str(dtype: &str) -> Result<DataType, ExplorerError> {
    match dtype {
        "str" => Ok(DataType::Utf8),
        "f64" => Ok(DataType::Float64),
        "i64" => Ok(DataType::Int64),
        "bool" => Ok(DataType::Boolean),
        "date" => Ok(DataType::Date),
        "datetime[ms]" => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        _ => Err(ExplorerError::Internal("Unrecognised datatype".into())),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_read_parquet(filename: &str) -> Result<ExDataFrame, ExplorerError> {
    let f = File::open(filename)?;
    let df = ParquetReader::new(f).finish()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_write_parquet(data: ExDataFrame, filename: &str) -> Result<(), ExplorerError> {
    df_read!(data, df, {
        let file = File::create(filename).expect("could not create file");
        ParquetWriter::new(file).finish(&mut df.clone())?;
        Ok(())
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_to_csv(
    data: ExDataFrame,
    has_headers: bool,
    delimiter: u8,
) -> Result<String, ExplorerError> {
    df_read!(data, df, {
        let mut buf: Vec<u8> = Vec::with_capacity(81920);
        CsvWriter::new(&mut buf)
            .has_header(has_headers)
            .with_delimiter(delimiter)
            .finish(&mut df.clone())?;

        let s = String::from_utf8(buf)?;
        Ok(s)
    })
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_csv_file(
    data: ExDataFrame,
    filename: &str,
    has_headers: bool,
    delimiter: u8,
) -> Result<(), ExplorerError> {
    df_read!(data, df, {
        let mut f = File::create(filename)?;
        CsvWriter::new(&mut f)
            .has_header(has_headers)
            .with_delimiter(delimiter)
            .finish(&mut df.clone())?;
        Ok(())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_read_ipc(
    filename: &str,
    columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> Result<ExDataFrame, ExplorerError> {
    let f = File::open(filename)?;
    let df = IpcReader::new(f)
        .with_columns(columns)
        .with_projection(projection)
        .finish()?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_write_ipc(
    data: ExDataFrame,
    filename: &str,
    with_compression: Option<&str>,
) -> Result<(), ExplorerError> {
    df_read!(data, df, {
        // Select the compression algorithm.
        let compression = match with_compression {
            Some("LZ4") => Some(IpcCompression::LZ4),
            Some("ZSTD") => Some(IpcCompression::ZSTD),
            _ => None,
        };

        let mut file = File::create(filename).expect("could not create file");
        IpcWriter::new(&mut file)
            .with_compression(compression)
            .finish(&mut df.clone())?;
        Ok(())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_read_ndjson(
    filename: &str,
    infer_schema_length: Option<usize>,
    with_batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);
    let df = JsonReader::new(buf_reader)
        .with_json_format(JsonFormat::JsonLines)
        .with_batch_size(with_batch_size)
        .infer_schema_len(infer_schema_length)
        .finish()?;

    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_write_ndjson(data: ExDataFrame, filename: &str) -> Result<(), ExplorerError> {
    df_read!(data, df, {
        let file = File::create(filename).expect("could not create file");
        JsonWriter::new(file).finish(&mut df.clone())?;
        Ok(())
    })
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_as_str(data: ExDataFrame) -> Result<String, ExplorerError> {
    df_read!(data, df, { Ok(format!("{:?}", df)) })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_from_map_rows(
    rows: Vec<HashMap<Term, Option<ExAnyValue>>>,
) -> Result<ExDataFrame, ExplorerError> {
    // Coerce the keys from `Term` (atom or binary) to `String`
    let rows: Vec<HashMap<String, &Option<ExAnyValue>>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(key, val)| (atom_or_binary_to_string(key), val))
                .collect()
        })
        .collect();

    // Get the map keys and sort them
    let mut keys = rows
        .first()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<String>>();

    case_insensitive_sort(&mut keys);

    // Coerce the vec of hashmaps to polars Rows
    let rows = rows
        .iter()
        .map(|row| row::Row::new(get_vals_in_order(row, &keys)))
        .collect::<Vec<row::Row>>();

    // Create the dataframe and assign the column names
    let mut df = DataFrame::from_rows(&rows)?;
    df.set_column_names(&keys)?;
    Ok(ExDataFrame::new(df))
}

fn atom_or_binary_to_string(val: &Term) -> String {
    match val.get_type() {
        TermType::Atom => val.atom_to_string().unwrap(),
        TermType::Binary => {
            String::from_utf8_lossy(val.into_binary().unwrap().as_slice()).to_string()
        }
        _ => panic!("Unsupported key value"),
    }
}

fn get_vals_in_order<'a>(
    map: &'a HashMap<String, &Option<ExAnyValue>>,
    keys: &'a [String],
) -> Vec<AnyValue<'a>> {
    keys.iter()
        .map(|key| match map.get(key) {
            None => AnyValue::Null,
            Some(None) => AnyValue::Null,
            // NOTE: This special case is necessary until/unless `DataFrame::from_rows` handles `AnyValue::Utf8Owned`. See: https://github.com/pola-rs/polars/issues/2941
            Some(Some(ExAnyValue::Utf8(val))) => AnyValue::Utf8(val),
            Some(Some(val)) => AnyValue::from(Some(val.clone())),
        })
        .collect()
}

fn case_insensitive_sort(strings: &mut [String]) {
    strings.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_from_keyword_rows(
    rows: Vec<Vec<(Term, Option<ExAnyValue>)>>,
) -> Result<ExDataFrame, ExplorerError> {
    // Get the keys and coerce them from `Term` (atom or binary) to `String`
    let keys = rows
        .first()
        .unwrap()
        .iter()
        .map(|(key, _)| atom_or_binary_to_string(key))
        .collect::<Vec<String>>();

    // Coerce the vec of tuples to polars Rows
    let rows = rows
        .iter()
        .map(|row| {
            let row = row
                .iter()
                .map(|(_, val)| match val {
                    // NOTE: This special case is necessary until/unless `DataFrame::from_rows` handles `AnyValue::Utf8Owned`. See: https://github.com/pola-rs/polars/issues/2941
                    Some(ExAnyValue::Utf8(val)) => AnyValue::Utf8(val),
                    Some(val) => AnyValue::from(Some(val.clone())),
                    None => AnyValue::Null,
                })
                .collect();
            row::Row::new(row)
        })
        .collect::<Vec<row::Row>>();

    // Create the dataframe and assign the column names
    let mut df = DataFrame::from_rows(&rows)?;
    df.set_column_names(&keys)?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif]
pub fn df_fill_none(data: ExDataFrame, strategy: &str) -> Result<ExDataFrame, ExplorerError> {
    let strat = match strategy {
        "backward" => FillNullStrategy::Backward,
        "forward" => FillNullStrategy::Forward,
        "min" => FillNullStrategy::Min,
        "max" => FillNullStrategy::Max,
        "mean" => FillNullStrategy::Mean,
        s => {
            return Err(ExplorerError::Other(format!(
                "Strategy {} not supported",
                s
            )))
        }
    };
    df_read!(data, df, {
        let new_df = df.fill_null(strat)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_join(
    data: ExDataFrame,
    other: ExDataFrame,
    left_on: Vec<&str>,
    right_on: Vec<&str>,
    how: &str,
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

    df_read_read!(data, other, df, df1, {
        let new_df = df.join(&*df1, left_on, right_on, how, None)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_get_columns(data: ExDataFrame) -> Result<Vec<ExSeries>, ExplorerError> {
    df_read!(data, df, {
        Ok(to_ex_series_collection(df.get_columns().clone()))
    })
}

#[rustler::nif]
pub fn df_columns(data: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    df_read!(data, df, {
        let cols = df.get_column_names();
        Ok(cols.into_iter().map(|s| s.to_string()).collect())
    })
}

#[rustler::nif]
pub fn df_dtypes(data: ExDataFrame) -> Result<Vec<String>, ExplorerError> {
    df_read!(data, df, {
        let result = df.dtypes().iter().map(|dtype| dtype.to_string()).collect();
        Ok(result)
    })
}

#[rustler::nif]
pub fn df_shape(data: ExDataFrame) -> Result<(usize, usize), ExplorerError> {
    df_read!(data, df, { Ok(df.shape()) })
}

#[rustler::nif]
pub fn df_height(data: ExDataFrame) -> Result<usize, ExplorerError> {
    df_read!(data, df, { Ok(df.height()) })
}

#[rustler::nif]
pub fn df_width(data: ExDataFrame) -> Result<usize, ExplorerError> {
    df_read!(data, df, { Ok(df.width()) })
}

#[rustler::nif]
pub fn df_hstack(data: ExDataFrame, cols: Vec<ExSeries>) -> Result<ExDataFrame, ExplorerError> {
    let cols = to_series_collection(cols);
    df_read!(data, df, {
        let new_df = df.hstack(&cols)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_vstack(data: ExDataFrame, other: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    df_read_read!(data, other, df, df1, {
        Ok(ExDataFrame::new(df.vstack(&df1.clone())?))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop_nulls(
    data: ExDataFrame,
    subset: Option<Vec<String>>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.drop_nulls(subset.as_ref().map(|s| s.as_ref()))?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop(data: ExDataFrame, name: &str) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = (&*df).drop(name)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_select_at_idx(data: ExDataFrame, idx: usize) -> Result<Option<ExSeries>, ExplorerError> {
    df_read!(data, df, {
        let result = df.select_at_idx(idx).map(|s| ExSeries::new(s.clone()));
        Ok(result)
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_column(data: ExDataFrame, name: &str) -> Result<ExSeries, ExplorerError> {
    df_read!(data, df, {
        let series = df.column(name).map(|s| ExSeries::new(s.clone()))?;
        Ok(series)
    })
}

#[rustler::nif]
pub fn df_select(data: ExDataFrame, selection: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.select(&selection)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_filter(data: ExDataFrame, mask: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let filter_series = &mask.resource.0;
        if let Ok(ca) = filter_series.bool() {
            let new_df = df.filter(ca)?;
            Ok(ExDataFrame::new(new_df))
        } else {
            Err(ExplorerError::Other("Expected a boolean mask".into()))
        }
    })
}

#[rustler::nif]
pub fn df_take(data: ExDataFrame, indices: Vec<u32>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let idx = UInt32Chunked::from_vec("idx", indices);
        let new_df = df.take(&idx)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_sort(
    data: ExDataFrame,
    by_column: &str,
    reverse: bool,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.sort([by_column], reverse)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_slice(
    data: ExDataFrame,
    offset: i64,
    length: usize,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.slice(offset, length);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_head(data: ExDataFrame, length: Option<usize>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.head(length);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_tail(data: ExDataFrame, length: Option<usize>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.tail(length);
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_clone(data: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, { Ok(ExDataFrame::new(df.clone())) })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_melt(
    data: ExDataFrame,
    id_vars: Vec<&str>,
    value_vars: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.melt(id_vars, value_vars)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_drop_duplicates(
    data: ExDataFrame,
    maintain_order: bool,
    subset: Vec<String>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = match maintain_order {
            false => df.distinct(Some(&subset), DistinctKeepStrategy::First)?,
            true => df.distinct_stable(Some(&subset), DistinctKeepStrategy::First)?,
        };
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_to_dummies(data: ExDataFrame) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.to_dummies()?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_with_column(data: ExDataFrame, col: ExSeries) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let mut new_df = df.clone();
        new_df.with_column(col.resource.0.clone())?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif]
pub fn df_new(cols: Vec<ExSeries>) -> Result<ExDataFrame, ExplorerError> {
    let cols = to_series_collection(cols);
    let df = DataFrame::new(cols)?;
    Ok(ExDataFrame::new(df))
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_set_column_names(
    data: ExDataFrame,
    names: Vec<&str>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let mut new_df = df.clone();
        new_df.set_column_names(&names)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_groups(data: ExDataFrame, groups: Vec<&str>) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let groups = df.groupby(groups)?.groups()?;
        Ok(ExDataFrame::new(groups))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_groupby_agg(
    data: ExDataFrame,
    groups: Vec<&str>,
    aggs: Vec<(&str, Vec<&str>)>,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df.groupby(groups)?.agg(&aggs)?;
        Ok(ExDataFrame::new(new_df))
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_pivot_wider(
    data: ExDataFrame,
    id_cols: Vec<&str>,
    pivot_column: &str,
    values_column: &str,
) -> Result<ExDataFrame, ExplorerError> {
    df_read!(data, df, {
        let new_df = df
            .groupby(id_cols)?
            .pivot([pivot_column], [values_column])
            .first()?;
        Ok(ExDataFrame::new(new_df))
    })
}
