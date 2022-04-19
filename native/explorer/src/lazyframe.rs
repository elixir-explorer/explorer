use crate::series::{to_ex_series_collection, to_series_collection};
use polars::prelude::*;
use rustler::{Term, TermType};
use std::collections::HashMap;
use std::fs::File;
use std::result::Result;

use crate::{ExAnyValue, ExDataFrame, ExLazyFrame, ExSeries, ExplorerError};

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

#[rustler::nif]
pub fn lf_read_csv(
    filename: String,
    infer_schema_length: Option<usize>,
    has_header: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    sep: &str,
    do_rechunk: bool,
    dtypes: Option<Vec<(&str, &str)>>,
    encoding: &str,
    null_char: String,
) -> Result<ExLazyFrame, ExplorerError> {
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

    let lf = LazyCsvReader::new(filename)
        .with_infer_schema_length(infer_schema_length)
        .has_header(has_header)
        .with_n_rows(stop_after_n_rows)
        .with_delimiter(sep.as_bytes()[0])
        .with_skip_rows(skip_rows)
        .with_rechunk(do_rechunk)
        .with_encoding(encoding)
        .with_dtype_overwrite(schema.as_ref())
        .with_null_values(Some(NullValues::AllColumns(null_char)))
        .finish()?;

    Ok(ExLazyFrame::new(lf))
}

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

#[rustler::nif]
pub fn lf_read_parquet(filename: String) -> Result<ExLazyFrame, ExplorerError> {
    let lf = LazyFrame::scan_parquet(filename, ScanArgsParquet::default())?;
    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif]
pub fn lf_write_parquet(data: ExLazyFrame, filename: &str) -> Result<(), ExplorerError> {
    lf_write!(data, lf, {
        let mut df = lf.clone().collect()?;
        *lf = df.clone().lazy();
        let file = File::create(filename).expect("could not create file");
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    })
}

#[rustler::nif]
pub fn lf_to_csv_string(
    data: ExLazyFrame,
    has_headers: bool,
    delimiter: u8,
) -> Result<String, ExplorerError> {
    lf_write!(data, lf, {
        let df = lf.clone().collect()?;
        *lf = df.clone().lazy();
        let mut buf: Vec<u8> = Vec::with_capacity(81920);
        CsvWriter::new(&mut buf)
            .has_header(has_headers)
            .with_delimiter(delimiter)
            .finish(&mut lf.clone().collect()?)?;

        let s = String::from_utf8(buf)?;
        Ok(s)
    })
}

#[rustler::nif]
pub fn lf_write_csv(
    data: ExLazyFrame,
    filename: &str,
    has_headers: bool,
    delimiter: u8,
) -> Result<(), ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        let mut f = File::create(filename)?;
        CsvWriter::new(&mut f)
            .has_header(has_headers)
            .with_delimiter(delimiter)
            .finish(&mut lf.clone().collect()?)?;
        Ok(())
    })
}

#[rustler::nif]
pub fn lf_as_str(data: ExLazyFrame) -> Result<String, ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        Ok(format!("{:?}", lf.clone().collect()?))
    })
}

#[rustler::nif]
pub fn lf_from_map_rows(
    rows: Vec<HashMap<Term, Option<ExAnyValue>>>,
) -> Result<ExLazyFrame, ExplorerError> {
    // Coerce the keys from `Term` (atom or binary) to `String`
    let rows: Vec<HashMap<String, &Option<ExAnyValue>>> = rows
        .into_iter()
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
        .into_iter()
        .map(|row| row::Row::new(get_vals_in_order(&row, &keys)))
        .collect::<Vec<row::Row>>();

    // Create the dataframe and assign the column names
    let mut df = DataFrame::from_rows(&rows)?;
    df.set_column_names(&keys)?;
    Ok(ExLazyFrame::new(df.lazy()))
}

#[rustler::nif]
pub fn lf_from_keyword_rows(
    rows: Vec<Vec<(Term, Option<ExAnyValue>)>>,
) -> Result<ExLazyFrame, ExplorerError> {
    // Get the keys and coerce them from `Term` (atom or binary) to `String`
    let keys = rows
        .first()
        .unwrap()
        .into_iter()
        .map(|(key, _)| atom_or_binary_to_string(key))
        .collect::<Vec<String>>();

    // Coerce the vec of tuples to polars Rows
    let rows = rows
        .into_iter()
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
    Ok(ExLazyFrame::new(df.lazy()))
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
    keys.into_iter()
        .map(|key| match map.get(key) {
            None => AnyValue::Null,
            Some(None) => AnyValue::Null,
            // NOTE: This special case is necessary until/unless `DataFrame::from_rows` handles `AnyValue::Utf8Owned`. See: https://github.com/pola-rs/polars/issues/2941
            Some(Some(ExAnyValue::Utf8(val))) => AnyValue::Utf8(val),
            Some(Some(val)) => AnyValue::from(Some(val.clone())),
        })
        .collect()
}

fn case_insensitive_sort(strings: &mut Vec<String>) {
    strings.sort_by(|a, b| a.to_lowercase().cmp(&b.to_lowercase()))
}

#[rustler::nif]
pub fn lf_get_columns(data: ExLazyFrame) -> Result<Vec<ExSeries>, ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        Ok(to_ex_series_collection(
            lf.clone().collect()?.get_columns().clone(),
        ))
    })
}

#[rustler::nif]
pub fn lf_get_column(data: ExLazyFrame, name: &str) -> Result<ExSeries, ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        Ok(ExSeries::new(
            lf.clone()
                .select(&[col(name)])
                .collect()?
                .column(name)?
                .clone(),
        ))
    })
}

#[rustler::nif]
pub fn lf_column_names(data: ExLazyFrame) -> Result<Vec<String>, ExplorerError> {
    lf_read!(data, lf, {
        Ok(lf.schema().iter_names().cloned().collect())
    })
}

#[rustler::nif]
pub fn lf_column_dtypes(data: ExLazyFrame) -> Result<Vec<String>, ExplorerError> {
    lf_read!(data, lf, {
        Ok(lf
            .schema()
            .iter_dtypes()
            .map(|dtype| dtype.to_string())
            .collect())
    })
}

#[rustler::nif]
pub fn lf_shape(data: ExLazyFrame) -> Result<(usize, usize), ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        Ok(lf.clone().collect()?.shape())
    })
}

#[rustler::nif]
pub fn lf_height(data: ExLazyFrame) -> Result<usize, ExplorerError> {
    lf_write!(data, lf, {
        *lf = lf.clone().cache();
        Ok(lf.clone().collect()?.height())
    })
}

#[rustler::nif]
pub fn lf_width(data: ExLazyFrame) -> Result<usize, ExplorerError> {
    lf_read!(data, lf, { Ok(lf.schema().iter().count()) })
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
        let col_exprs: Vec<Expr> = selection.into_iter().map(col).collect();
        let new_lf = lf.clone().select(col_exprs);
        Ok(ExLazyFrame::new(new_lf))
    })
}

#[rustler::nif]
pub fn lf_drop(data: ExLazyFrame, selection: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let new_lf = lf.clone().select(&[col("*").exclude(selection)]);
        Ok(ExLazyFrame::new(new_lf))
    })
}

#[rustler::nif]
pub fn lf_drop_nulls(
    data: ExLazyFrame,
    subset: Option<Vec<&str>>,
) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let exprs = match subset {
            None => None,
            Some(subset) => Some(subset.into_iter().map(col).collect()),
        };
        let new_lf = lf.clone().drop_nulls(exprs);
        Ok(ExLazyFrame::new(new_lf))
    })
}

#[rustler::nif]
pub fn lf_sort(
    data: ExLazyFrame,
    columns: Vec<(String, bool)>,
) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let (by_exprs, reverse): (Vec<Expr>, Vec<bool>) = columns
            .iter()
            .map(|(colname, reverse)| (col(colname), *reverse))
            .unzip();
        Ok(ExLazyFrame::new(
            lf.clone().sort_by_exprs(by_exprs, reverse),
        ))
    })
}

#[rustler::nif]
pub fn lf_slice(data: ExLazyFrame, offset: i64, length: u32) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        Ok(ExLazyFrame::new(lf.clone().slice(offset, length)))
    })
}

#[rustler::nif]
pub fn lf_melt(
    data: ExLazyFrame,
    id_vars: Vec<String>,
    value_vars: Vec<String>,
) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        Ok(ExLazyFrame::new(lf.clone().melt(id_vars, value_vars)))
    })
}

#[rustler::nif]
pub fn lf_distinct(
    data: ExLazyFrame,
    maintain_order: bool,
    subset: Vec<String>,
) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let new_lf = match maintain_order {
            false => lf
                .clone()
                .distinct(Some(subset), DistinctKeepStrategy::First),
            true => lf
                .clone()
                .distinct_stable(Some(subset), DistinctKeepStrategy::First),
        };
        Ok(ExLazyFrame::new(new_lf))
    })
}

#[rustler::nif]
pub fn lf_new(cols: Vec<ExSeries>) -> Result<ExLazyFrame, ExplorerError> {
    let cols = to_series_collection(cols);
    let lf = DataFrame::new(cols)?.lazy();
    Ok(ExLazyFrame::new(lf))
}
