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
use polars::prelude::*;

use rustler::{Binary, Env, NewBinary};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor};
use std::result::Result;
use std::sync::Arc;
use std::collections::BTreeMap;

use crate::datatypes::{ExParquetCompression, ExS3Entry, ExSeriesDtype};
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

    let reader = CsvReader::from_path(filename)?
        .infer_schema(infer_schema_length)
        .has_header(has_header)
        .truncate_ragged_lines(true)
        .with_try_parse_dates(parse_dates)
        .with_n_rows(stop_after_n_rows)
        .with_separator(delimiter_as_byte)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_projection(projection)
        .with_rechunk(do_rechunk)
        .with_encoding(encoding)
        .with_columns(column_names)
        .with_dtypes(Some(schema_from_dtypes_pairs(dtypes)?))
        .with_null_values(Some(NullValues::AllColumns(null_vals)))
        .with_end_of_line_char(eol_delimiter.unwrap_or(b'\n'));

    Ok(ExDataFrame::new(reader.finish()?))
}

pub fn schema_from_dtypes_pairs(
    dtypes: Vec<(&str, ExSeriesDtype)>,
) -> Result<Arc<Schema>, ExplorerError> {
    let mut schema = Schema::new();
    for (name, ex_dtype) in dtypes {
        let dtype = DataType::try_from(&ex_dtype)?;
        schema.with_column(name.into(), dtype);
    }
    Ok(Arc::new(schema))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_csv(
    data: ExDataFrame,
    filename: &str,
    include_headers: bool,
    delimiter: u8,
) -> Result<(), ExplorerError> {
    let file = File::create(filename)?;
    let mut buf_writer = BufWriter::new(file);
    CsvWriter::new(&mut buf_writer)
        .include_header(include_headers)
        .with_separator(delimiter)
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
) -> Result<(), ExplorerError> {
    let mut cloud_writer = build_aws_s3_cloud_writer(ex_entry)?;

    CsvWriter::new(&mut cloud_writer)
        .include_header(include_headers)
        .with_separator(delimiter)
        .finish(&mut data.clone())?;
    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn df_dump_csv(
    env: Env,
    data: ExDataFrame,
    include_headers: bool,
    delimiter: u8,
) -> Result<Binary, ExplorerError> {
    let mut buf = vec![];

    CsvWriter::new(&mut buf)
        .include_header(include_headers)
        .with_separator(delimiter)
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

    let reader = CsvReader::new(cursor)
        .infer_schema(infer_schema_length)
        .has_header(has_header)
        .with_try_parse_dates(parse_dates)
        .with_n_rows(stop_after_n_rows)
        .with_separator(delimiter_as_byte)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_projection(projection)
        .with_rechunk(do_rechunk)
        .with_encoding(encoding)
        .with_columns(column_names)
        .with_dtypes(Some(schema_from_dtypes_pairs(dtypes)?))
        .with_null_values(Some(NullValues::AllColumns(null_vals)))
        .with_end_of_line_char(eol_delimiter.unwrap_or(b'\n'));

    Ok(ExDataFrame::new(reader.finish()?))
}

// =========== deltalake =========== //

use deltalake::arrow::record_batch::RecordBatch;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::errors::DeltaTableError;
use deltalake::DeltaTable;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio::runtime;

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_delta(
    data: ExDataFrame,
    table_uri: &str
) -> Result<i64, ExplorerError> {
    Ok(runtime::Builder::new_current_thread().build().unwrap().block_on(do_df_to_delta(&data, table_uri))?)
}

fn to_delta_datatype(from: &crate::dataframe::arrow::datatypes::ArrowDataType) -> Result<deltalake::schema::SchemaDataType, ExplorerError> {
    match from {
        ArrowDataType::Utf8 => Ok(deltalake::schema::SchemaDataType::primitive("string".to_string())),
        ArrowDataType::LargeUtf8 => Ok(deltalake::schema::SchemaDataType::primitive("string".to_string())),
        ArrowDataType::Int64 => Ok(deltalake::schema::SchemaDataType::primitive("long".to_string())), // undocumented type
        ArrowDataType::Int32 => Ok(deltalake::schema::SchemaDataType::primitive("integer".to_string())),
        ArrowDataType::Int16 => Ok(deltalake::schema::SchemaDataType::primitive("short".to_string())),
        ArrowDataType::Int8 => Ok(deltalake::schema::SchemaDataType::primitive("byte".to_string())),
        ArrowDataType::UInt64 => Ok(deltalake::schema::SchemaDataType::primitive("long".to_string())), // undocumented type
        ArrowDataType::UInt32 => Ok(deltalake::schema::SchemaDataType::primitive("integer".to_string())),
        ArrowDataType::UInt16 => Ok(deltalake::schema::SchemaDataType::primitive("short".to_string())),
        ArrowDataType::UInt8 => Ok(deltalake::schema::SchemaDataType::primitive("byte".to_string())),
        ArrowDataType::Float32 => Ok(deltalake::schema::SchemaDataType::primitive("float".to_string())),
        ArrowDataType::Float64 => Ok(deltalake::schema::SchemaDataType::primitive("double".to_string())),
        ArrowDataType::Boolean => Ok(deltalake::schema::SchemaDataType::primitive("boolean".to_string())),
        ArrowDataType::Binary => Ok(deltalake::schema::SchemaDataType::primitive("binary".to_string())),
        ArrowDataType::FixedSizeBinary(_) => {
            Ok(deltalake::schema::SchemaDataType::primitive("binary".to_string()))
        }
        ArrowDataType::LargeBinary => {
            Ok(deltalake::schema::SchemaDataType::primitive("binary".to_string()))
        }
        ArrowDataType::Decimal(p, s) => Ok(deltalake::schema::SchemaDataType::primitive(format!(
            "decimal({p},{s})"
        ))),
        ArrowDataType::Decimal256(p, s) => Ok(deltalake::schema::SchemaDataType::primitive(format!(
            "decimal({p},{s})"
        ))),
        ArrowDataType::Date32 => Ok(deltalake::schema::SchemaDataType::primitive("date".to_string())),
        ArrowDataType::Date64 => Ok(deltalake::schema::SchemaDataType::primitive("date".to_string())),
        ArrowDataType::Timestamp(crate::dataframe::arrow::datatypes::TimeUnit::Microsecond, None) => {
            Ok(deltalake::schema::SchemaDataType::primitive("timestamp".to_string()))
        }
        ArrowDataType::Timestamp(crate::dataframe::arrow::datatypes::TimeUnit::Microsecond, Some(tz))
            if tz.eq_ignore_ascii_case("utc") =>
        {
            Ok(deltalake::schema::SchemaDataType::primitive("timestamp".to_string()))
        }
        ArrowDataType::Struct(fields) => {
            let converted_fields: Result<Vec<deltalake::schema::SchemaField>, _> = fields
                .iter()
                .map(|field| to_delta_schema_field(&field))
                .collect();
            Ok(deltalake::schema::SchemaDataType::r#struct(
                deltalake::schema::SchemaTypeStruct::new(converted_fields?),
            ))
        }
        ArrowDataType::List(field) => {
            Ok(deltalake::schema::SchemaDataType::array(deltalake::schema::SchemaTypeArray::new(
                Box::new(to_delta_datatype((*field).data_type())?),
                (*field).is_nullable,
            )))
        }
        ArrowDataType::LargeList(field) => {
            Ok(deltalake::schema::SchemaDataType::array(deltalake::schema::SchemaTypeArray::new(
                Box::new(to_delta_datatype((*field).data_type())?),
                (*field).is_nullable,
            )))
        }
        ArrowDataType::FixedSizeList(field, _) => {
            Ok(deltalake::schema::SchemaDataType::array(deltalake::schema::SchemaTypeArray::new(
                Box::new(to_delta_datatype((*field).data_type())?),
                (*field).is_nullable,
            )))
        }
        ArrowDataType::Map(field, _) => {
            if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                let key_type = to_delta_datatype(struct_fields[0].data_type())?;
                let value_type = to_delta_datatype(struct_fields[1].data_type())?;
                let value_type_nullable = struct_fields[1].is_nullable;
                Ok(deltalake::schema::SchemaDataType::map(deltalake::schema::SchemaTypeMap::new(
                    Box::new(key_type),
                    Box::new(value_type),
                    value_type_nullable,
                )))
            } else {
                panic!("DataType::Map should contain a struct field child");
            }
        }
        s => Err(ExplorerError::Other(format!(
            "Invalid data type for Delta Lake: {:?}", s
        ))),
    }
}

fn to_delta_schema_field(from: &crate::dataframe::arrow::datatypes::Field) -> Result<deltalake::schema::SchemaField, ExplorerError> {
    let metadata: std::collections::HashMap<_, serde_json::value::Value> = <BTreeMap<std::string::String, std::string::String> as Clone>::clone(&from.metadata).into_iter().map(|(key, value)| (key, serde_json::value::Value::String(value))).collect();
    let to_datatype = to_delta_datatype(&from.data_type)?;
    Ok(deltalake::schema::SchemaField::new(from.name.clone(), to_datatype, from.is_nullable, metadata))
}

fn to_delta_schema(from: &crate::dataframe::arrow::datatypes::ArrowSchema) -> Result<Vec<deltalake::schema::SchemaField>, ExplorerError> {
    let mut to_fields: Vec<deltalake::schema::SchemaField> = Vec::with_capacity(from.fields.capacity());
    for field in from.fields.iter() {
        let new_field = to_delta_schema_field(field)?;
        to_fields.push(new_field);
    }
    
    Ok(to_fields)
}

async fn do_df_to_delta(
    data: &ExDataFrame, 
    table_uri: &str
) -> Result<i64, ExplorerError> {
    match deltalake::Path::parse(&table_uri) {
        Ok(table_path) => {
            let maybe_table = deltalake::open_table(&table_path).await;
            let polars_schema = data.schema();
            let arrow_schema : crate::dataframe::arrow::datatypes::ArrowSchema = polars_schema.to_arrow();
            let deltalake_schema = to_delta_schema(&arrow_schema)?;
            let mut table = match maybe_table {
                Ok(table) => table,
                Err(DeltaTableError::NotATable(_)) => {
                    deltalake::DeltaOps::try_from_uri(table_path)
                        .await
                        .unwrap()
                        .create()
                        .with_columns(deltalake_schema)
                        .await?
                }
                Err(err) => return Err(err)?,
            };

            let writer_properties = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build();

            let mut writer = RecordBatchWriter::for_table(&table)?;
            writer = writer.with_writer_properties(writer_properties);

            let mut chunk_idx = 0usize;
            while let Some(batch) = convert_to_batch(&table, data, chunk_idx)? {
                writer.write(batch).await?;
                chunk_idx += 1;
            }

            let adds = writer
                .flush_and_commit(&mut table)
                .await?;
            Ok(adds)
        },
        Err(e) => {
            return Err(ExplorerError::Other(format!("Invalid table path: {:?}", e)));
        }
    }
}

fn convert_to_batch(table: &DeltaTable, data: &ExDataFrame, chunk_idx: usize) -> Result<Option<RecordBatch>, ExplorerError> {
    let metadata = table.get_metadata()?;
    if let Ok(arrow_schema) = <deltalake::arrow::datatypes::Schema as TryFrom<&deltalake::schema::Schema>>::try_from(
        &metadata.schema.clone(),
    ) {
        let arrow_schema_ref = Arc::new(arrow_schema);

        let arrow_array: Vec<deltalake::arrow::array::ArrayRef> = data.get_columns().iter().map(|series| {
            // todo: deltalake: cast array type without copying data, e.g,:
            //  from: &crate::dataframe::arrow::array::Int32Array, 
            //  to:   &deltalake::arrow::array::Int32Array

            // attemp 1: 
            //   failed: downcast_ref() returns None
            // let arrow_series = series.to_arrow(chunk_idx);
            // let arrow_series = arrow_series.as_any();
            // let arrow_series = arrow_series.downcast_ref::<deltalake::arrow::array::ArrayRef>().unwrap();
            
            // attemp 2:
            //   failed: segmentation fault
            let arrow_series = series.to_arrow(chunk_idx);
            let arrow_series = arrow_series.as_ref();
            let s: Arc<dyn deltalake::arrow::array::Array> = unsafe { std::mem::transmute(arrow_series) };
            s
        }).collect();

        Ok(Some(RecordBatch::try_new(arrow_schema_ref, arrow_array).expect("Failed to create RecordBatch")))
    } else {
        Err(ExplorerError::Other(format!("Failed to convert Delta Lake schema to Arrow schema")))
    }
}

// ============ Parquet ============ //

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_from_parquet(
    filename: &str,
    stop_after_n_rows: Option<usize>,
    column_names: Option<Vec<String>>,
    projection: Option<Vec<usize>>,
) -> Result<ExDataFrame, ExplorerError> {
    let file = File::open(filename)?;
    let buf_reader = BufReader::new(file);

    let reader = ParquetReader::new(buf_reader)
        .with_n_rows(stop_after_n_rows)
        .with_columns(column_names)
        .with_projection(projection);

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
    Ok(())
}

#[cfg(feature = "aws")]
fn object_store_to_explorer_error(error: impl std::fmt::Debug) -> ExplorerError {
    ExplorerError::Other(format!("Internal ObjectStore error: #{error:?}"))
}

#[cfg(feature = "aws")]
fn build_aws_s3_cloud_writer(
    ex_entry: ExS3Entry,
) -> Result<crate::cloud_writer::CloudWriter, ExplorerError> {
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

    let object_store: Box<dyn object_store::ObjectStore> = Box::new(aws_s3);

    crate::cloud_writer::CloudWriter::new(object_store, ex_entry.key.into())
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
    let reader = JsonReader::new(buf_reader)
        .with_json_format(JsonFormat::JsonLines)
        .with_batch_size(batch_size)
        .infer_schema_len(infer_schema_length);

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
    let reader = JsonReader::new(cursor)
        .with_json_format(JsonFormat::JsonLines)
        .with_batch_size(batch_size)
        .infer_schema_len(infer_schema_length);

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
