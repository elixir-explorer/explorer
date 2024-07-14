use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::ExplorerError;
use object_store::path::Path;
use object_store::MultipartId;
use object_store::ObjectStore;

/// CloudWriter wraps the asynchronous interface of [ObjectStore::put_multipart](https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html#tymethod.put_multipart)
/// in a synchronous interface which implements `std::io::Write`.
///
/// This allows it to be used in sync code which would otherwise write to a simple File or byte stream,
/// such as with `polars::prelude::CsvWriter`.
pub struct CloudWriter {
    // Hold a reference to the store. The store itself is thread-safe.
    object_store: Box<dyn ObjectStore>,
    // The path in the object_store which we want to write to
    path: Path,
    // ID of a partially-done upload, used to abort the upload on error
    multipart_id: MultipartId,
    // The Tokio runtime which the writer uses internally.
    runtime: tokio::runtime::Runtime,
    // Internal writer, constructed at creation
    writer: Box<dyn AsyncWrite + Send + Unpin>,
}

impl CloudWriter {
    /// Construct a new CloudWriter
    ///
    /// Creates a new (current-thread) Tokio runtime
    /// which bridges the sync writing process with the async ObjectStore multipart uploading.
    pub fn new(object_store: Box<dyn ObjectStore>, path: Path) -> Result<Self, ExplorerError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .enable_io()
            .build()?;

        let (multipart_id, writer) =
            runtime.block_on(async { Self::build_writer(&object_store, &path).await })?;
        Ok(CloudWriter {
            object_store,
            path,
            multipart_id,
            runtime,
            writer,
        })
    }

    async fn build_writer(
        object_store: &dyn ObjectStore,
        path: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Send + Unpin>), ExplorerError> {
        let (multipart_id, async_s3_writer) = (object_store.put_multipart(path).await)
            .map_err(|_| ExplorerError::Other(format!("Could not put multipart to path {path}")))?;
        Ok((multipart_id, async_s3_writer))
    }

    fn abort(&self) {
        let _ = self.runtime.block_on(async {
            self.object_store
                .abort_multipart(&self.path, &self.multipart_id)
                .await
        });
    }
}

impl std::io::Write for CloudWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.runtime.block_on(self.writer.write(buf));
        if res.is_err() {
            self.abort();
        }
        res
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let res = self.runtime.block_on(self.writer.flush());
        if res.is_err() {
            self.abort();
        }
        res
    }
}

impl Drop for CloudWriter {
    fn drop(&mut self) {
        let _ = self.runtime.block_on(self.writer.shutdown());
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;

    use super::*;

    use polars::df;
    use polars::prelude::DataFrame;

    fn example_dataframe() -> DataFrame {
        df!(
            "foo" => &[1, 2, 3],
            "bar" => &[None, Some("bak"), Some("baz")],
        )
        .unwrap()
    }

    #[test]
    fn csv_to_local_objectstore_cloudwriter() {
        use polars::prelude::{CsvWriter, SerWriter};

        let mut df = example_dataframe();

        let object_store: Box<dyn ObjectStore> = Box::new(
            object_store::local::LocalFileSystem::new_with_prefix("/tmp/")
                .expect("Could not initialize connection"),
        );
        let object_store: Box<dyn ObjectStore> = object_store;

        let path: object_store::path::Path = "cloud_writer_example.csv".into();

        let mut cloud_writer = CloudWriter::new(object_store, path).unwrap();
        CsvWriter::new(&mut cloud_writer)
            .finish(&mut df)
            .expect("Could not write dataframe as CSV to remote location");
    }
}
