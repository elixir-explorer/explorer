// This file contains the IO functions related to a dataframe.
// Each format has 8 functions related. They do the following:
//
// - dump: dump a dataframe to a string using the given format (like in a CSV string).
// - load: load a dataframe from a given string (let's say, from a CSV string).
// - from: reads a dataframe from a file that is encoded in a given format.
// - to: writes a dataframe to a file in a given format.
//
// Today we have the following formats: CSV, NDJSON, Parquet, Apache Arrow and Apache Arrow Stream.
//
use polars::prelude::*;

use rustler::{Binary, Env, NewBinary};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::result::Result;

use crate::{ExDataFrame, ExplorerError};

// ============ CSV ============ //

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
pub fn df_from_csv(
    filename: &str,
    infer_schema_length: Option<usize>,
    has_header: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    projection: Option<Vec<usize>>,
    delimiter_as_byte: u8,
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
        Some(dtypes) => Some(schema_from_dtypes_pairs(dtypes)?),

        None => None,
    };

    let df = CsvReader::from_path(filename)?
        .infer_schema(infer_schema_length)
        .has_header(has_header)
        .with_parse_dates(parse_dates)
        .with_n_rows(stop_after_n_rows)
        .with_delimiter(delimiter_as_byte)
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

fn schema_from_dtypes_pairs(dtypes: Vec<(&str, &str)>) -> Result<Schema, ExplorerError> {
    let mut schema = Schema::new();
    for (name, dtype_str) in dtypes {
        let dtype = dtype_from_str(dtype_str)?;
        schema.with_column(name.to_string(), dtype)
    }
    Ok(schema)
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

// ============ Parquet ============ //

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
    let compression = parquet_compression(compression, compression_level)?;

    ParquetWriter::new(&mut buf_writer)
        .with_compression(compression)
        .finish(&mut df.clone())?;
    Ok(())
}

fn parquet_compression(
    compression: Option<&str>,
    compression_level: Option<i32>,
) -> Result<ParquetCompression, ExplorerError> {
    let compression_type = match (compression, compression_level) {
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

    Ok(compression_type)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_parquet<'a>(
    env: Env<'a>,
    data: ExDataFrame,
    compression: Option<&str>,
    compression_level: Option<i32>,
) -> Result<Binary<'a>, ExplorerError> {
    let df = &data.resource.0;
    let mut buf = vec![];

    let compression = parquet_compression(compression, compression_level)?;

    ParquetWriter::new(&mut buf)
        .with_compression(compression)
        .finish(&mut df.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

// ============ IPC ============ //

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

// ============ IPC Streaming ============ //

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
        Some("lz4") => Some(IpcCompression::LZ4),
        Some("zstd") => Some(IpcCompression::ZSTD),
        _ => None,
    };

    let mut file = File::create(filename).expect("could not create file");
    IpcStreamWriter::new(&mut file)
        .with_compression(compression)
        .finish(&mut df.clone())?;
    Ok(())
}

// ============ NDJSON ============ //

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

    JsonWriter::new(&mut buf_writer)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut df.clone())?;
    Ok(())
}

#[cfg(not(target_arch = "arm"))]
#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_ndjson(env: Env, data: ExDataFrame) -> Result<Binary, ExplorerError> {
    let df = &data.resource.0;
    let mut buf = vec![];

    JsonWriter::new(&mut buf)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut df.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

// ============ ARM 32 specifics ============ //

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

#[cfg(target_arch = "arm")]
#[rustler::nif]
pub fn df_dump_ndjson(_env: Env, _data: ExDataFrame) -> Result<Binary, ExplorerError> {
    Err(ExplorerError::Other(format!(
        "NDJSON dumping is not enabled for this machine"
    )))
}
