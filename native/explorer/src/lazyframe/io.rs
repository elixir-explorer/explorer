use polars::prelude::*;
use std::fs::File;
use std::io::BufWriter;
use std::num::NonZeroUsize;

use crate::dataframe::io::schema_from_dtypes_pairs;
use crate::datatypes::{ExParquetCompression, ExQuoteStyle, ExS3Entry, ExSeriesDtype};
use crate::{ExLazyFrame, ExplorerError};

#[rustler::nif]
pub fn lf_from_parquet(
    filename: &str,
    stop_after_n_rows: Option<usize>,
    columns: Option<Vec<String>>,
) -> Result<ExLazyFrame, ExplorerError> {
    let options = ScanArgsParquet {
        n_rows: stop_after_n_rows,
        ..Default::default()
    };

    let cols: Vec<Expr> = if let Some(cols) = columns {
        cols.iter().map(col).collect()
    } else {
        vec![all()]
    };

    let lf = LazyFrame::scan_parquet(filename, options)?.select(cols);

    Ok(ExLazyFrame::new(lf))
}

// When we have more cloud entries, we could accept an Enum.
#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_from_parquet_cloud(
    ex_entry: ExS3Entry,
    stop_after_n_rows: Option<usize>,
    columns: Option<Vec<String>>,
) -> Result<ExLazyFrame, ExplorerError> {
    let options = ScanArgsParquet {
        n_rows: stop_after_n_rows,
        cloud_options: Some(ex_entry.config.to_cloud_options()),
        ..Default::default()
    };
    let cols: Vec<Expr> = if let Some(cols) = columns {
        cols.iter().map(col).collect()
    } else {
        vec![all()]
    };
    let lf = LazyFrame::scan_parquet(ex_entry.to_string(), options)?
        .with_comm_subplan_elim(false)
        .with_new_streaming(true)
        .select(cols);

    Ok(ExLazyFrame::new(lf))
}

#[cfg(not(feature = "aws"))]
#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_from_parquet_cloud(
    _ex_entry: ExS3Entry,
    _stop_after_n_rows: Option<usize>,
    _columns: Option<Vec<String>>,
) -> Result<ExLazyFrame, ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_parquet(
    data: ExLazyFrame,
    filename: &str,
    ex_compression: ExParquetCompression,
    streaming: bool,
) -> Result<(), ExplorerError> {
    let compression = ParquetCompression::try_from(ex_compression)?;

    let lf = data.clone_inner();

    if streaming {
        let options = ParquetWriteOptions {
            compression,
            statistics: StatisticsOptions::empty(),
            row_group_size: None,
            data_page_size: None,
            ..Default::default()
        };
        let target = std::path::PathBuf::from(filename);
        let sink_options = SinkOptions {
            maintain_order: false,
            ..Default::default()
        };

        let _ = lf
            .with_comm_subplan_elim(false)
            .sink_parquet(SinkTarget::Path(target.into()), options, None, sink_options)?
            .collect();
        Ok(())
    } else {
        let mut df = lf.collect()?;

        let file = File::create(filename)?;
        let mut buf_writer = BufWriter::new(file);

        ParquetWriter::new(&mut buf_writer)
            .with_compression(compression)
            .finish(&mut df)?;

        Ok(())
    }
}

#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_parquet_cloud(
    data: ExLazyFrame,
    ex_entry: ExS3Entry,
    ex_compression: ExParquetCompression,
) -> Result<(), ExplorerError> {
    let lf = data.clone_inner();
    let cloud_options = Some(ex_entry.config.to_cloud_options());
    let compression = ParquetCompression::try_from(ex_compression)?;

    let options = ParquetWriteOptions {
        compression,
        statistics: StatisticsOptions::empty(),
        row_group_size: None,
        data_page_size: None,
        ..Default::default()
    };
    let target = std::path::PathBuf::from(ex_entry.to_string());
    let sink_options = SinkOptions {
        maintain_order: false,
        ..Default::default()
    };

    let _ = lf
        .with_comm_subplan_elim(false)
        .sink_parquet(
            SinkTarget::Path(target.into()),
            options,
            cloud_options,
            sink_options,
        )?
        .collect();
    Ok(())
}

#[cfg(not(feature = "aws"))]
#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_parquet_cloud(
    _data: ExLazyFrame,
    _ex_entry: ExS3Entry,
    _ex_compression: ExParquetCompression,
) -> Result<(), ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"aws\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_from_ipc(filename: &str) -> Result<ExLazyFrame, ExplorerError> {
    let lf = LazyFrame::scan_ipc(filename, Default::default())?;

    Ok(ExLazyFrame::new(lf))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_ipc(
    data: ExLazyFrame,
    filename: &str,
    compression: Option<&str>,
    streaming: bool,
) -> Result<(), ExplorerError> {
    // Select the compression algorithm.
    let compression = match compression {
        Some("lz4") => Some(IpcCompression::LZ4),
        Some("zstd") => Some(IpcCompression::ZSTD),
        _ => None,
    };

    let lf = data.clone_inner();

    if streaming {
        let options = IpcWriterOptions {
            compression,
            ..Default::default()
        };
        let target = std::path::PathBuf::from(filename);
        let sink_options = SinkOptions {
            maintain_order: false,
            ..Default::default()
        };
        let _ = lf
            .with_comm_subplan_elim(false)
            .sink_ipc(SinkTarget::Path(target.into()), options, None, sink_options)?
            .collect();
        Ok(())
    } else {
        let mut df = lf.collect()?;
        let file = File::create(filename)?;
        let mut buf_writer = BufWriter::new(file);
        IpcWriter::new(&mut buf_writer)
            .with_compression(compression)
            .finish(&mut df)?;
        Ok(())
    }
}

#[cfg(feature = "aws")]
#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_ipc_cloud(
    data: ExLazyFrame,
    ex_entry: ExS3Entry,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    let lf = data.clone_inner();
    let cloud_options = Some(ex_entry.config.to_cloud_options());
    // Select the compression algorithm.
    let compression = match compression {
        Some("lz4") => Some(IpcCompression::LZ4),
        Some("zstd") => Some(IpcCompression::ZSTD),
        _ => None,
    };

    let options = IpcWriterOptions {
        compression,
        ..Default::default()
    };
    let target = std::path::PathBuf::from(ex_entry.to_string());
    let sink_options = SinkOptions {
        maintain_order: false,
        ..Default::default()
    };
    let _ = lf
        .with_comm_subplan_elim(false)
        .sink_ipc(
            SinkTarget::Path(target.into()),
            options,
            cloud_options,
            sink_options,
        )?
        .collect();

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
    skip_rows_after_header: usize,
    delimiter_as_byte: u8,
    do_rechunk: bool,
    dtypes: Vec<(&str, ExSeriesDtype)>,
    encoding: &str,
    null_vals: Vec<String>,
    parse_dates: bool,
    eol_delimiter: Option<u8>,
) -> Result<ExLazyFrame, ExplorerError> {
    let encoding = match encoding {
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    };

    let df = LazyCsvReader::new(filename)
        .with_infer_schema_length(infer_schema_length)
        .with_has_header(has_header)
        .with_try_parse_dates(parse_dates)
        .with_n_rows(stop_after_n_rows)
        .with_separator(delimiter_as_byte)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_rechunk(do_rechunk)
        .with_encoding(encoding)
        .with_dtype_overwrite(schema_from_dtypes_pairs(dtypes)?)
        .with_null_values(Some(NullValues::AllColumns(
            null_vals.iter().map(|x| x.into()).collect(),
        )))
        .with_eol_char(eol_delimiter.unwrap_or(b'\n'))
        .finish()?;

    Ok(ExLazyFrame::new(df))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn lf_to_csv(
    data: ExLazyFrame,
    filename: &str,
    include_headers: bool,
    delimiter: u8,
    quote_style: ExQuoteStyle,
    streaming: bool,
) -> Result<(), ExplorerError> {
    let lf = data.clone_inner();
    if streaming {
        let serialize_options = SerializeOptions {
            separator: delimiter,
            ..Default::default()
        };

        let options = CsvWriterOptions {
            include_header: include_headers,
            serialize_options,
            ..Default::default()
        };
        let target = std::path::PathBuf::from(filename);
        let sink_options = SinkOptions {
            maintain_order: true,
            mkdir: true,
            sync_on_close: sync_on_close::SyncOnCloseType::None,
        };

        let _ = lf
            .with_comm_subplan_elim(false)
            .sink_csv(SinkTarget::Path(target.into()), options, None, sink_options)?
            .collect();

        Ok(())
    } else {
        let df = lf.collect()?;
        let file = File::create(filename)?;
        let mut buf_writer = BufWriter::new(file);

        CsvWriter::new(&mut buf_writer)
            .include_header(include_headers)
            .with_separator(delimiter)
            .with_quote_style(quote_style.into())
            .finish(&mut df.clone())?;
        Ok(())
    }
}

#[cfg(feature = "ndjson")]
#[rustler::nif]
pub fn lf_from_ndjson(
    filename: String,
    infer_schema_length: Option<usize>,
    batch_size: usize,
) -> Result<ExLazyFrame, ExplorerError> {
    let batch_size = NonZeroUsize::new(batch_size).ok_or(ExplorerError::Other(
        "\"batch_size\" expected to be non zero.".to_string(),
    ))?;
    let lf = LazyJsonLineReader::new(filename)
        .with_infer_schema_length(infer_schema_length.and_then(NonZeroUsize::new))
        .with_batch_size(Some(batch_size))
        .finish()?;

    Ok(ExLazyFrame::new(lf))
}

#[cfg(not(feature = "ndjson"))]
#[rustler::nif]
pub fn lf_from_ndjson(
    _filename: &str,
    _infer_schema_length: Option<usize>,
    _batch_size: usize,
) -> Result<ExLazyFrame, ExplorerError> {
    Err(ExplorerError::Other("Explorer was compiled without the \"ndjson\" feature enabled. \
        This is mostly due to this feature being incompatible with your computer's architecture. \
        Please read the section about precompilation in our README.md: https://github.com/elixir-explorer/explorer#precompilation".to_string()))
}
