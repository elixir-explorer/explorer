use crate::ExplorerError;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

use object_store::buffered::BufWriter as OSBufWriter;
use tokio::io::AsyncWriteExt;
/// CloudWriter wraps the asynchronous interface of [ObjectStore's BufWriter](https://docs.rs/object_store/latest/object_store/buffered/struct.BufWriter.html)
/// in a synchronous interface which implements `std::io::Write`.
///
/// This allows it to be used in sync code which would otherwise write to a simple File or byte stream,
/// such as with `polars::prelude::CsvWriter`.
pub struct CloudWriter {
    // The Tokio runtime which the writer uses internally.
    runtime: tokio::runtime::Runtime,
    // Internal writer, constructed at creation
    writer: OSBufWriter,
}

impl CloudWriter {
    /// Construct a new CloudWriter
    ///
    /// Creates a new (current-thread) Tokio runtime
    /// which bridges the sync writing process with the async ObjectStore uploading.
    pub fn new(object_store: Arc<dyn ObjectStore>, path: Path) -> Result<Self, ExplorerError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()?;
        let writer = OSBufWriter::new(object_store, path);
        Ok(CloudWriter { writer, runtime })
    }
}

impl std::io::Write for CloudWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // SAFETY:
        // We extend the lifetime for the duration of this function. This is safe as well block the
        // async runtime here
        // This was copied from Polars' own CloudWriter.
        let buf = unsafe { std::mem::transmute::<&[u8], &'static [u8]>(buf) };

        self.runtime.block_on(async {
            let res = self.writer.write_all(buf).await;
            if res.is_err() {
                let _ = self.writer.abort().await;
            }
            Ok(buf.len())
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.runtime.block_on(async {
            let res = self.writer.flush().await;
            if res.is_err() {
                let _ = self.writer.abort().await;
            }
            Ok(())
        })
    }
}

impl Drop for CloudWriter {
    fn drop(&mut self) {
        let _ = self.runtime.block_on(self.writer.shutdown());
    }
}
