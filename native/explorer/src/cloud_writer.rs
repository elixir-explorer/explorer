use tokio::io::AsyncWrite;
use tokio_util::io::SyncIoBridge;

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
    writer: SyncIoBridge<Box<dyn AsyncWrite + Send + Unpin>>,
}

impl CloudWriter {
    /// Construct a new CloudWriter
    ///
    /// Creates a new (current-thread) Tokio runtime
    /// which bridges the sync writing process with the async ObjectStore multipart uploading.
    pub fn new(object_store: Box<dyn ObjectStore>, path: Path) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (multipart_id, writer) =
            runtime.block_on(async { Self::build_writer(&object_store, &path).await });
        CloudWriter {
            object_store,
            path,
            multipart_id,
            runtime,
            writer,
        }
    }

    async fn build_writer(
        object_store: &Box<dyn ObjectStore>,
        path: &Path,
    ) -> (
        MultipartId,
        SyncIoBridge<Box<dyn AsyncWrite + Send + Unpin>>,
    ) {
        let (multipart_id, async_s3_writer) = object_store
            .put_multipart(path)
            .await
            .expect("Could not create location to write to");
        let sync_s3_uploader = SyncIoBridge::new(async_s3_writer);
        (multipart_id, sync_s3_uploader)
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
        let res = self.writer.write(buf);
        if res.is_err() {
            self.abort();
        }
        res
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let res = self.writer.flush();
        if res.is_err() {
            self.abort();
        }
        res
    }
}

impl Drop for CloudWriter {
    fn drop(&mut self) {
        let _ = self.writer.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;

    use super::*;

    use polars::df;
    use polars::prelude::DataFrame;
    use polars::prelude::NamedFrom;

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

        let mut cloud_writer = CloudWriter::new(object_store, path);
        let csv_writer = CsvWriter::new(&mut cloud_writer)
            .finish(&mut df)
            .expect("Could not write dataframe as CSV to remote location");
    }
}
