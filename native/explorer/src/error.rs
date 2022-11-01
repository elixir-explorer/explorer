use rustler::{Atom, Encoder, Env, Term};
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
        match self {
            ExplorerError::Io(ref value) => error_tuple(env, io(), value.to_string()),
            ExplorerError::Utf8(ref value) => error_tuple(env, utf8(), value.to_string()),
            ExplorerError::Polars(ref value) => error_tuple(env, polars(), value.to_string()),

            ExplorerError::Internal(ref value) => error_tuple(env, internal(), value.to_string()),

            ExplorerError::Other(ref value) => error_tuple(env, other(), value.to_string()),
            ExplorerError::TryFromInt(ref value) => {
                error_tuple(env, try_from_int(), value.to_string())
            }
            ExplorerError::Parquet(ref value) => error_tuple(env, parquet(), value.to_string()),
            ExplorerError::Unknown(ref value) => error_tuple(env, unknown(), value.to_string()),
        }
    }
}

// Encode an error tuple for better context at the Elixir side.
fn error_tuple(env: Env, atom: Atom, error_str: String) -> Term {
    (atom.encode(env), error_str.encode(env)).encode(env)
}
