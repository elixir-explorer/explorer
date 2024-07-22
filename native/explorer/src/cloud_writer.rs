use crate::ExplorerError;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;

use object_store::buffered::BufWriter as OSBufWriter;
use tokio::io::AsyncWriteExt;

#[derive(Debug, PartialEq)]
enum CloudWriterStatus {
    Running,
    Stopped,
    Aborted,
}
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
    // The copy of the object_store
    object_store: Arc<dyn ObjectStore>,
    // Keep the path for the file, so we can use to read head.
    path: Path,
    // Private status of the current writer
    status: CloudWriterStatus,
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
        let writer = OSBufWriter::new(object_store.clone(), path.clone());

        Ok(CloudWriter {
            writer,
            runtime,
            object_store,
            path,
            status: CloudWriterStatus::Running,
        })
    }

    /// Make a head request to check if the upload has finished.
    pub fn finish(&mut self) -> Result<ObjectMeta, ExplorerError> {
        if self.status != CloudWriterStatus::Stopped {
            self.status = CloudWriterStatus::Stopped;
            let _ = self.runtime.block_on(self.writer.shutdown());
            self.runtime
                .block_on(self.object_store.head(&self.path))
                .map_err(|err| {
                    ExplorerError::Other(format!(
                        "cannot read information from file, which means the upload failed. {err}"
                    ))
                })
        } else {
            Err(ExplorerError::Other(
                "cannot finish cloud writer due to an error, or it was already finished.".into(),
            ))
        }
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
            // TODO: use writer.put to avoid copying data
            let res = self.writer.write(buf).await;
            if res.is_err() {
                let _ = self.writer.abort().await;
                self.status = CloudWriterStatus::Aborted;
            }
            res
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.runtime.block_on(async {
            let res = self.writer.flush().await;
            if res.is_err() {
                let _ = self.writer.abort().await;
                self.status = CloudWriterStatus::Aborted;
            }
            res
        })
    }
}

impl Drop for CloudWriter {
    fn drop(&mut self) {
        if self.status != CloudWriterStatus::Stopped {
            self.status = CloudWriterStatus::Stopped;
            let _ = self.runtime.block_on(self.writer.shutdown());
        }
    }
}
