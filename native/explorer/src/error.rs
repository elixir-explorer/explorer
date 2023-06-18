use rustler::{Encoder, Env, Term};
use std::io;
use thiserror::Error;

// Defines the atoms for each value of ExplorerError.
rustler::atoms! {
    io,
    utf8,
    polars,
    internal,
    other,
    try_from_int,
    parquet,
    unknown
}

#[derive(Error, Debug)]
pub enum ExplorerError {
    #[error("IO Error: {0}")]
    Io(#[from] io::Error),
    #[error("Utf8 Conversion Error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Polars Error: {0}")]
    Polars(#[from] polars::prelude::PolarsError),
    #[error("Internal Error: {0}")]
    Internal(String),
    #[error("Generic Error: {0}")]
    Other(String),
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Parquet(#[from] polars::export::arrow::io::parquet::read::ParquetError),
    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

impl Encoder for ExplorerError {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        format!("{self}").encode(env)
    }
}
