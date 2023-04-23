use polars::prelude::*;
use std::result::Result;

use crate::dataframe::io::schema_from_dtypes_pairs;
use crate::datatypes::ExParquetCompression;
use crate::{ExLazyFrame, ExplorerError};

#[rustler::nif]
pub fn lf_from_parquet(
    filename: &str,
    stop_after_n_rows: Option<usize>,
) -> Result<ExLazyFrame, ExplorerError> {
    let options = ScanArgsParquet {
        n_rows: stop_after_n_rows,
        ..Default::default()
    };
    let lf = LazyFrame::scan_parquet(filename, options)?;

    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_parquet(
    data: ExLazyFrame,
    filename: &str,
    ex_compression: ExParquetCompression,
) -> Result<(), ExplorerError> {
    let compression = ParquetCompression::try_from(ex_compression)?;
    let options = ParquetWriteOptions {
        compression,
        statistics: false,
        row_group_size: None,
        data_pagesize_limit: None,
        maintain_order: false,
    };
    let lf = data
        .clone_inner()
        .with_streaming(true)
        .with_common_subplan_elimination(false);

    lf.sink_parquet(filename.into(), options)?;

    Ok(())
}

#[rustler::nif]
pub fn lf_from_ipc(filename: &str) -> Result<ExLazyFrame, ExplorerError> {
    let lf = LazyFrame::scan_ipc(filename, Default::default())?;

    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_ipc(
    data: ExLazyFrame,
    filename: &str,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    // Select the compression algorithm.
    let compression = match compression {
        Some("lz4") => Some(IpcCompression::LZ4),
        Some("zstd") => Some(IpcCompression::ZSTD),
        _ => None,
    };
    let options = IpcWriterOptions {
        compression,
        maintain_order: false,
    };
    let lf = data
        .clone_inner()
        .with_streaming(true)
        .with_common_subplan_elimination(false);

    lf.sink_ipc(filename.into(), options)?;

    Ok(())
}

#[rustler::nif]
#[allow(clippy::too_many_arguments)]
pub fn lf_from_csv(
    filename: &str,
    infer_schema_length: Option<usize>,
    has_header: bool,
    stop_after_n_rows: Option<usize>,
    skip_rows: usize,
    delimiter_as_byte: u8,
    do_rechunk: bool,
    dtypes: Option<Vec<(&str, &str)>>,
    encoding: &str,
    null_char: String,
    parse_dates: bool,
) -> Result<ExLazyFrame, ExplorerError> {
    let encoding = match encoding {
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    };

    let schema = match dtypes {
        Some(dtypes) => Some(schema_from_dtypes_pairs(dtypes)?),

        None => None,
    };

    let df = LazyCsvReader::new(filename)
        .with_infer_schema_length(infer_schema_length)
        .has_header(has_header)
        .with_try_parse_dates(parse_dates)
        .with_n_rows(stop_after_n_rows)
        .with_delimiter(delimiter_as_byte)
        .with_skip_rows(skip_rows)
        .with_rechunk(do_rechunk)
        .with_encoding(encoding)
        .with_dtype_overwrite(schema.as_deref())
        .with_null_values(Some(NullValues::AllColumns(vec![null_char])))
        .finish()?;

    Ok(ExLazyFrame::new(df))
}

#[cfg(not(any(target_arch = "arm", target_arch = "riscv64")))]
#[rustler::nif]
pub fn lf_from_ndjson(
    filename: String,
    infer_schema_length: Option<usize>,
    batch_size: Option<usize>,
) -> Result<ExLazyFrame, ExplorerError> {
    let lf = LazyJsonLineReader::new(filename)
        .with_infer_schema_length(infer_schema_length)
        .with_batch_size(batch_size)
        .finish()?;

    Ok(ExLazyFrame::new(lf))
}

#[cfg(any(target_arch = "arm", target_arch = "riscv64"))]
#[rustler::nif]
pub fn lf_from_ndjson(
    _filename: &str,
    _infer_schema_length: Option<usize>,
    _batch_size: usize,
) -> Result<ExLazyFrame, ExplorerError> {
    Err(ExplorerError::Other(format!(
        "NDJSON parsing is not enabled for this machine"
    )))
}
