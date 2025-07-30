// This file contains the IO functions related to a dataframe.
// Each format has 8 functions related. They do the following:
//
// - dump: dump a dataframe to a binary/string using the given format (like in a CSV string).
// - load: load a dataframe from a given binary/string (let's say, from a CSV string).
// - from: reads a dataframe from a file that is encoded in a given format.
// - to: writes a dataframe to a file in a given format.
//
// Today we have the following formats: CSV, NDJSON, Parquet, Apache Arrow and Apache Arrow Stream.
//
use polars::frame::chunk_df_for_writing;
use polars::prelude::*;
use polars::{io::schema_to_arrow_checked, prelude::CompatLevel};
use polars_arrow::io::ipc::write::{
    default_ipc_fields, encode_record_batch, schema_to_bytes, EncodedData, WriteOptions,
};
use std::num::NonZeroUsize;

use rustler::{Binary, Env, NewBinary};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Write};
use std::sync::Arc;

use crate::datatypes::{ExParquetCompression, ExQuoteStyle, ExS3Entry, ExSeriesDtype};
use crate::{ExDataFrame, ExplorerError};

#[cfg(feature = "cloud")]
use crate::cloud_writer::CloudWriter;

// ============ CSV ============ //

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::too_many_arguments)]
pub fn df_from_csv(
    filename: &str,
    infer_schema_length: Option<usize>,
    has_header: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    skip_rows_after_header: usize,
    projection: Option<Vec<usize>>,
    delimiter_as_byte: u8,
    do_rechunk: bool,
    column_names: Option<Vec<String>>,
    dtypes: Vec<(&str, ExSeriesDtype)>,
    encoding: &str,
    null_vals: Vec<String>,
    parse_dates: bool,
    eol_delimiter: Option<u8>,
) -> Result<ExDataFrame, ExplorerError> {
    let encoding = match encoding {
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    };

    let dataframe = CsvReadOptions::default()
        .with_schema_overwrite(schema_from_dtypes_pairs(dtypes)?)
        .with_infer_schema_length(infer_schema_length)
        .with_has_header(has_header)
        .with_n_rows(stop_after_n_rows)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_projection(projection.map(Arc::new))
        .with_rechunk(do_rechunk)
        .with_columns(column_names.map(|names| {
            names
                .iter()
                .map(|name| PlSmallStr::from_string(name.clone()))
                .collect()
        }))
        .with_parse_options(
            CsvParseOptions::default()
                .with_encoding(encoding)
                .with_truncate_ragged_lines(true)
                .with_try_parse_dates(parse_dates)
                .with_separator(delimiter_as_byte)
                .with_eol_char(eol_delimiter.unwrap_or(b'\n'))
                .with_null_values(Some(NullValues::AllColumns(
                    null_vals.iter().map(|val| val.into()).collect(),
                ))),
        )
        .try_into_reader_with_file_path(Some(filename.into()))?
        .finish();

    Ok(ExDataFrame::new(dataframe?))
}

pub fn schema_from_dtypes_pairs(
    dtypes: Vec<(&str, ExSeriesDtype)>,
) -> Result<Option<Arc<Schema>>, ExplorerError> {
    if dtypes.is_empty() {
        return Ok(None);
    }

    let mut schema = Schema::with_capacity(dtypes.len());
    for (name, ex_dtype) in dtypes {
        let dtype = DataType::try_from(&ex_dtype)?;
        schema.with_column(name.into(), dtype);
    }
    Ok(Some(Arc::new(schema)))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_csv(
    data: ExDataFrame,
    filename: &str,
    include_headers: bool,
    delimiter: u8,
    quote_style: ExQuoteStyle,
) -> Result<(), ExplorerError> {
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    CsvWriter::new(&mut buf_writer)
        .include_header(include_headers)
        .with_separator(delimiter)
        .with_quote_style(quote_style.into())
        .finish(&mut data.clone())?;
    Ok(())
}

#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_csv_cloud(
    data: ExDataFrame,
    ex_entry: ExS3Entry,
    include_headers: bool,
    delimiter: u8,
    quote_style: ExQuoteStyle,
) -> Result<(), ExplorerError> {
    let mut cloud_writer = build_aws_s3_cloud_writer(ex_entry)?;

    CsvWriter::new(&mut cloud_writer)
        .include_header(include_headers)
        .with_separator(delimiter)
        .with_quote_style(quote_style.into())
        .finish(&mut data.clone())?;

    let _ = cloud_writer.finish()?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_csv(
    env: Env,
    data: ExDataFrame,
    include_headers: bool,
    delimiter: u8,
    quote_style: ExQuoteStyle,
) -> Result<Binary, ExplorerError> {
    let mut buf = vec![];

    CsvWriter::new(&mut buf)
        .include_header(include_headers)
        .with_separator(delimiter)
        .with_quote_style(quote_style.into())
        .finish(&mut data.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::too_many_arguments)]
pub fn df_load_csv(
    binary: Binary,
    infer_schema_length: Option<usize>,
    has_header: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    skip_rows_after_header: usize,
    projection: Option<Vec<usize>>,
    delimiter_as_byte: u8,
    do_rechunk: bool,
    column_names: Option<Vec<String>>,
    dtypes: Vec<(&str, ExSeriesDtype)>,
    encoding: &str,
    null_vals: Vec<String>,
    parse_dates: bool,
    eol_delimiter: Option<u8>,
) -> Result<ExDataFrame, ExplorerError> {
    let encoding = match encoding {
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    };

    let cursor = Cursor::new(binary.as_slice());

    let dataframe = CsvReadOptions::default()
        .with_schema_overwrite(schema_from_dtypes_pairs(dtypes)?)
        .with_has_header(has_header)
        .with_infer_schema_length(infer_schema_length)
        .with_n_rows(stop_after_n_rows)
        .with_columns(column_names.map(|names| {
            names
                .iter()
                .map(|name| PlSmallStr::from_string(name.clone()))
                .collect()
        }))
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_projection(projection.map(Arc::new))
        .with_rechunk(do_rechunk)
        .with_parse_options(
            CsvParseOptions::default()
                .with_separator(delimiter_as_byte)
                .with_encoding(encoding)
                .with_null_values(Some(NullValues::AllColumns(
                    null_vals.iter().map(|x| x.into()).collect(),
                )))
                .with_try_parse_dates(parse_dates)
                .with_eol_char(eol_delimiter.unwrap_or(b'\n')),
        )
        .into_reader_with_file_handle(cursor)
        .finish();

    Ok(ExDataFrame::new(dataframe?))
}

// ============ Parquet ============ //

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_parquet(
    filename: &str,
    stop_after_n_rows: Option<usize>,
    column_names: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
    rechunk: bool,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);

    let reader = ParquetReader::new(buf_reader)
        .with_slice(stop_after_n_rows.map(|max| (0, max)))
        .with_columns(column_names)
        .with_projection(projection)
        .set_rechunk(rechunk);

    Ok(ExDataFrame::new(reader.finish()?))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_parquet(
    data: ExDataFrame,
    filename: &str,
    ex_compression: ExParquetCompression,
) -> Result<(), ExplorerError> {
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);

    let compression = ParquetCompression::try_from(ex_compression)?;

    ParquetWriter::new(&mut buf_writer)
        .with_compression(compression)
        .finish(&mut data.clone())?;
    Ok(())
}

#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_parquet_cloud(
    data: ExDataFrame,
    ex_entry: ExS3Entry,
    ex_compression: ExParquetCompression,
) -> Result<(), ExplorerError> {
    let mut cloud_writer = build_aws_s3_cloud_writer(ex_entry)?;

    let compression = ParquetCompression::try_from(ex_compression)?;

    ParquetWriter::new(&mut cloud_writer)
        .with_compression(compression)
        .finish(&mut data.clone())?;

    let _ = cloud_writer.finish()?;

    Ok(())
}

#[cfg(feature = "aws")]
fn object_store_to_explorer_error(error: impl std::fmt::Debug) -> ExplorerError {
    ExplorerError::Other(format!("Internal ObjectStore error: #{error:?}"))
}

#[cfg(feature = "aws")]
fn build_aws_s3_cloud_writer(ex_entry: ExS3Entry) -> Result<CloudWriter, ExplorerError> {
    let config = ex_entry.config;
    let mut aws_builder = object_store::aws::AmazonS3Builder::new()
        .with_region(&config.region)
        .with_access_key_id(&config.access_key_id)
        .with_secret_access_key(&config.secret_access_key)
        .with_allow_http(true)
        .with_endpoint(&config.endpoint);

    if let Some(bucket_name) = &config.bucket {
        aws_builder = aws_builder.with_bucket_name(bucket_name);
    } else {
        // We use the virtual host style, and the bucket name is going to be ignored
        // because it's assumed to be already in the endpoint URL.
        aws_builder = aws_builder
            .with_bucket_name("explorer-default-bucket-name")
            .with_virtual_hosted_style_request(true);
    }

    if let Some(token) = config.token {
        aws_builder = aws_builder.with_token(token);
    }

    let aws_s3 = aws_builder
        .build()
        .map_err(object_store_to_explorer_error)?;

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(aws_s3);
    CloudWriter::new(object_store, ex_entry.key.into())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_parquet(
    env: Env,
    data: ExDataFrame,
    ex_compression: ExParquetCompression,
) -> Result<Binary, ExplorerError> {
    let mut buf = vec![];

    let compression = ParquetCompression::try_from(ex_compression)?;

    ParquetWriter::new(&mut buf)
        .with_compression(compression)
        .finish(&mut data.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_load_parquet(binary: Binary) -> Result<ExDataFrame, ExplorerError> {
    let cursor = Cursor::new(binary.as_slice());
    let reader = ParquetReader::new(cursor);

    Ok(ExDataFrame::new(reader.finish()?))
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
    let reader = IpcReader::new(buf_reader)
        .with_columns(columns)
        .with_projection(projection);

    Ok(ExDataFrame::new(reader.finish()?))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc(
    data: ExDataFrame,
    filename: &str,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?),
        None => None,
    };

    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    IpcWriter::new(&mut buf_writer)
        .with_compression(compression)
        .finish(&mut data.clone())?;
    Ok(())
}

#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc_cloud(
    data: ExDataFrame,
    ex_entry: ExS3Entry,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?),
        None => None,
    };

    let mut cloud_writer = build_aws_s3_cloud_writer(ex_entry)?;

    IpcWriter::new(&mut cloud_writer)
        .with_compression(compression)
        .finish(&mut data.clone())?;

    let _ = cloud_writer.finish()?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_ipc<'a>(
    env: Env<'a>,
    data: ExDataFrame,
    compression: Option<&str>,
) -> Result<Binary<'a>, ExplorerError> {
    let mut buf = vec![];

    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?),
        None => None,
    };

    IpcWriter::new(&mut buf)
        .with_compression(compression)
        .finish(&mut data.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_load_ipc(
    binary: Binary,
    columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> Result<ExDataFrame, ExplorerError> {
    let cursor = Cursor::new(binary.as_slice());
    let reader = IpcReader::new(cursor)
        .with_columns(columns)
        .with_projection(projection);

    Ok(ExDataFrame::new(reader.finish()?))
}

fn decode_ipc_compression(compression: &str) -> Result<IpcCompression, ExplorerError> {
    match compression {
        "lz4" => Ok(IpcCompression::LZ4),
        "zstd" => Ok(IpcCompression::ZSTD),
        other => Err(ExplorerError::Other(format!(
            "the algorithm {other} is not supported for IPC compression"
        ))),
    }
}

fn decode_compact_level(compact_level: &str) -> Result<CompatLevel, ExplorerError> {
    match compact_level {
        "oldest" => Ok(CompatLevel::oldest()),
        "newest" => Ok(CompatLevel::newest()),
        other => Err(ExplorerError::Other(format!(
            "the compact level {other} is not supported"
        ))),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_ipc_schema<'a>(
    env: Env<'a>,
    df: ExDataFrame,
    compact_level: Option<&str>,
) -> Result<Binary<'a>, ExplorerError> {
    let compact_level = match compact_level {
        Some(level) => decode_compact_level(level)?,
        None => CompatLevel::oldest(),
    };
    let schema = schema_to_arrow_checked(df.schema(), compact_level, "ipc")?;
    let ipc_fields = default_ipc_fields(schema.iter_values());
    let schema_bytes = schema_to_bytes(&schema, &ipc_fields, None);
    let encoded_message = EncodedData {
        ipc_message: schema_bytes,
        arrow_data: Vec::new(),
    };

    let mut buf = vec![];
    write_message(&mut buf, &encoded_message)?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_ipc_record_batch<'a>(
    env: Env<'a>,
    df: ExDataFrame,
    max_chunk_size: Option<usize>,
    compression: Option<&str>,
    compact_level: Option<&str>,
) -> Result<Vec<Binary<'a>>, ExplorerError> {
    let data = &mut df.clone();

    let max_request_bytes = if let Some(max_chunk_size) = max_chunk_size {
        max_chunk_size
    } else {
        let base: usize = 2;
        10 * base.pow(20) // 10 MB
    };
    let chunk_num = data.estimated_size() / max_request_bytes + 1;
    let chunk_size = data.fields().len() / chunk_num;

    chunk_df_for_writing(data, chunk_size)?;

    let compact_level = match compact_level {
        Some(level) => decode_compact_level(level)?,
        None => CompatLevel::oldest(),
    };
    let iter = data.iter_chunks(compact_level, true);

    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?.into()),
        None => None,
    };
    let options = WriteOptions { compression };

    let mut result = Vec::new();

    for batch in iter {
        let mut encoded_message = Default::default();
        encode_record_batch(&batch, &options, &mut encoded_message);
        let encoded_message = std::mem::take(&mut encoded_message);

        let mut buf = vec![];
        write_message(&mut buf, &encoded_message)?;
        let mut values_binary = NewBinary::new(env, buf.len());
        values_binary.copy_from_slice(&buf);

        result.push(values_binary.into());
    }

    Ok(result)
}

/// Write a message's IPC data and buffers, returning metadata and buffer data lengths written
/// code from https://github.com/pola-rs/polars/blob/main/crates/polars-arrow/src/io/ipc/write/common_sync.rs
/// the original code is not public for external crates to use it
pub fn write_message<W: Write>(
    writer: &mut W,
    encoded: &EncodedData,
) -> PolarsResult<(usize, usize)> {
    let arrow_data_len = encoded.arrow_data.len();

    let a = 8 - 1;
    let buffer = &encoded.ipc_message;
    let flatbuf_size = buffer.len();
    let prefix_size = 8;
    let aligned_size = (flatbuf_size + prefix_size + a) & !a;
    let padding_bytes = aligned_size - flatbuf_size - prefix_size;

    write_continuation(writer, (aligned_size - prefix_size) as i32)?;

    // write the flatbuf
    if flatbuf_size > 0 {
        writer.write_all(buffer)?;
    }
    // write padding
    // aligned to a 8 byte boundary, so maximum is [u8;8]
    const PADDING_MAX: [u8; 8] = [0u8; 8];
    writer.write_all(&PADDING_MAX[..padding_bytes])?;

    // write arrow data
    let body_len = if arrow_data_len > 0 {
        write_body_buffers(writer, &encoded.arrow_data)?
    } else {
        0
    };

    Ok((aligned_size, body_len))
}

fn write_body_buffers<W: Write>(mut writer: W, data: &[u8]) -> PolarsResult<usize> {
    let len = data.len();
    let pad_len = pad_to_64(data.len());
    let total_len = len + pad_len;

    // write body buffer
    writer.write_all(data)?;
    if pad_len > 0 {
        writer.write_all(&vec![0u8; pad_len][..])?;
    }

    Ok(total_len)
}

/// Write a record batch to the writer, writing the message size before the message
/// if the record batch is being written to a stream
fn write_continuation<W: Write>(writer: &mut W, total_len: i32) -> PolarsResult<usize> {
    const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
    writer.write_all(&CONTINUATION_MARKER)?;
    writer.write_all(&total_len.to_le_bytes()[..])?;
    Ok(8)
}

/// Calculate an 8-byte boundary and return the number of bytes needed to pad to 8 bytes
#[inline]
fn pad_to_64(len: usize) -> usize {
    ((len + 63) & !63) - len
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
    let reader = IpcStreamReader::new(buf_reader)
        .with_columns(columns)
        .with_projection(projection);

    Ok(ExDataFrame::new(reader.finish()?))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc_stream(
    data: ExDataFrame,
    filename: &str,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?),
        None => None,
    };

    let mut file = File::create(filename)?;
    IpcStreamWriter::new(&mut file)
        .with_compression(compression)
        .finish(&mut data.clone())?;
    Ok(())
}

#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc_stream_cloud(
    data: ExDataFrame,
    ex_entry: ExS3Entry,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?),
        None => None,
    };

    let mut cloud_writer = build_aws_s3_cloud_writer(ex_entry)?;

    IpcStreamWriter::new(&mut cloud_writer)
        .with_compression(compression)
        .finish(&mut data.clone())?;

    let _ = cloud_writer.finish()?;

    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_ipc_stream<'a>(
    env: Env<'a>,
    data: ExDataFrame,
    compression: Option<&str>,
) -> Result<Binary<'a>, ExplorerError> {
    let mut buf = vec![];

    let compression = match compression {
        Some(algo) => Some(decode_ipc_compression(algo)?),
        None => None,
    };

    IpcStreamWriter::new(&mut buf)
        .with_compression(compression)
        .finish(&mut data.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_load_ipc_stream(
    binary: Binary,
    columns: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> Result<ExDataFrame, ExplorerError> {
    let cursor = Cursor::new(binary.as_slice());
    let reader = IpcStreamReader::new(cursor)
        .with_columns(columns)
        .with_projection(projection);

    Ok(ExDataFrame::new(reader.finish()?))
}

// ============ NDJSON ============ //

#[cfg(feature = "ndjson")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_ndjson(
    filename: &str,
    infer_schema_length: Option<usize>,
    batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);
    let batch_size = NonZeroUsize::new(batch_size).ok_or(ExplorerError::Other(
        "\"batch_size\" expected to be non zero.".to_string(),
    ))?;
    let reader = JsonReader::new(buf_reader)
        .with_json_format(JsonFormat::JsonLines)
        .with_batch_size(batch_size)
        .infer_schema_len(infer_schema_length.and_then(NonZeroUsize::new));

    Ok(ExDataFrame::new(reader.finish()?))
}

#[cfg(feature = "ndjson")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ndjson(data: ExDataFrame, filename: &str) -> Result<(), ExplorerError> {
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);

    JsonWriter::new(&mut buf_writer)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut data.clone())?;
    Ok(())
}

#[cfg(all(feature = "ndjson", feature = "aws"))]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ndjson_cloud(data: ExDataFrame, ex_entry: ExS3Entry) -> Result<(), ExplorerError> {
    let mut cloud_writer = build_aws_s3_cloud_writer(ex_entry)?;

    JsonWriter::new(&mut cloud_writer)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut data.clone())?;

    let _ = cloud_writer.finish()?;

    Ok(())
}

#[cfg(feature = "ndjson")]
#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_ndjson(env: Env, data: ExDataFrame) -> Result<Binary, ExplorerError> {
    let mut buf = vec![];

    JsonWriter::new(&mut buf)
        .with_json_format(JsonFormat::JsonLines)
        .finish(&mut data.clone())?;

    let mut values_binary = NewBinary::new(env, buf.len());
    values_binary.copy_from_slice(&buf);

    Ok(values_binary.into())
}

#[cfg(feature = "ndjson")]
#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_load_ndjson(
    binary: Binary,
    infer_schema_length: Option<usize>,
    batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    let cursor = Cursor::new(binary.as_slice());
    let batch_size = NonZeroUsize::new(batch_size).ok_or(ExplorerError::Other(
        "\"batch_size\" expected to be non zero.".to_string(),
    ))?;
    let reader = JsonReader::new(cursor)
        .with_json_format(JsonFormat::JsonLines)
        .with_batch_size(batch_size)
        .infer_schema_len(infer_schema_length.and_then(NonZeroUsize::new));

    Ok(ExDataFrame::new(reader.finish()?))
}

// ============ For when the feature is not enabled ============ //

#[cfg(not(feature = "ndjson"))]
#[rustler::nif]
pub fn df_from_ndjson(
    _filename: &str,
    _infer_schema_length: Option<usize>,
    _batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"ndjson\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "ndjson"))]
#[rustler::nif]
pub fn df_to_ndjson(_data: ExDataFrame, _filename: &str) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"ndjson\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "ndjson"))]
#[rustler::nif]
pub fn df_dump_ndjson(_data: ExDataFrame) -> Result<Binary<'static>, ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"ndjson\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "ndjson"))]
#[rustler::nif]
pub fn df_load_ndjson(
    _binary: Binary,
    _infer_schema_length: Option<usize>,
    _batch_size: usize,
) -> Result<ExDataFrame, ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"ndjson\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "aws"))]
#[rustler::nif]
pub fn df_to_parquet_cloud(
    _data: ExDataFrame,
    _ex_entry: ExS3Entry,
    _ex_compression: ExParquetCompression,
) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "aws"))]
#[rustler::nif]
pub fn df_to_csv_cloud(
    _data: ExDataFrame,
    _ex_entry: ExS3Entry,
    _has_headers: bool,
    _delimiter: u8,
    _quote_style: ExQuoteStyle,
) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "aws"))]
#[rustler::nif]
pub fn df_to_ipc_cloud(
    _data: ExDataFrame,
    _ex_entry: ExS3Entry,
    _compression: Option<&str>,
) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(feature = "aws"))]
#[rustler::nif]
pub fn df_to_ipc_stream_cloud(
    _data: ExDataFrame,
    _ex_entry: ExS3Entry,
    _compression: Option<&str>,
) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[cfg(not(any(feature = "ndjson", feature = "aws")))]
#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ndjson_cloud(_data: ExDataFrame, _ex_entry: ExS3Entry) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" and \"ndjson\" features enabled. \
        This is mostly due to these feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}
