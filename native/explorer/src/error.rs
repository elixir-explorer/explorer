use rustler::{Encoder, Env, Term};
use std::io;
use thiserror::Error;

rustler::atoms! {
    ok,
    error
}

#[derive(Error, Debug)]
pub enum ExplorerError {
    #[error("IO Error")]
    Io(#[from] io::Error),
    #[error("Utf8 Conversion Error")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Polars Error")]
    Polars(#[from] polars::prelude::PolarsError),
    #[error("Internal Error: {0}")]
    Internal(String),
    #[error("Other error: {0}")]
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
        format!("{:?}", self).encode(env)
    }
}
