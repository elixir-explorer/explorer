use polars::prelude::*;

use rustler::{Binary, Env, LocalPid, NewBinary};
use std::io::BufWriter;
use std::io::Write;
use std::result::Result;

use crate::{ExDataFrame, ExplorerError};

pub struct PidWriter<'a> {
    pub env: Env<'a>,
    pub pid: LocalPid,
}

impl Write for &PidWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        let mut values_binary = NewBinary::new(self.env, len);
        values_binary.copy_from_slice(buf);

        let message: Binary = values_binary.into();

        self.env.send(&self.pid, message.to_term(self.env));

        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn df_to_ipc_via_pid(
    env: Env,
    data: ExDataFrame,
    pid: rustler::LocalPid,
    compression: Option<&str>,
) -> Result<(), ExplorerError> {
    // Select the compression algorithm.
    let compression = match compression {
        Some("lz4") => Some(IpcCompression::LZ4),
        Some("zstd") => Some(IpcCompression::ZSTD),
        _ => None,
    };

    let pid_writer = PidWriter { env, pid };

    let mut buf_writer = BufWriter::new(&pid_writer);
    IpcWriter::new(&mut buf_writer)
        .with_compression(compression)
        .finish(&mut data.clone())?;
    Ok(())
}
