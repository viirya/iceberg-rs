/*!
Iceberg table.
 */

use crate::model::manifest::{Content, FileFormat, Manifest, ManifestMetadata};
use crate::model::manifest_list::{ManifestFile, ManifestFileV2};
use crate::model::table::{FormatVersion, TableMetadata};
use crate::table::TableType::FileSystem;
use anyhow::Result;
use apache_avro::types::Value;
use arrow_array::RecordBatch;
use futures::executor::block_on;
use futures::future::{join_all, BoxFuture};
use futures::ready;
use futures::stream::BoxStream;
use futures::stream::Stream;
use futures::FutureExt;
use futures::StreamExt;
use object_store::{DynObjectStore, ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::async_reader::{AsyncReader, ParquetObjectReader};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use std::fmt::Formatter;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Tables can be either one of following types:
/// - FileSystem (https://iceberg.apache.org/spec/#file-system-tables)
/// - Metastore (https://iceberg.apache.org/spec/#metastore-tables) - Not supported yet.
pub enum TableType {
    /// Filesystem table
    FileSystem(Arc<dyn ObjectStore>),
}

/// Iceberg table
pub struct Table {
    #[allow(dead_code)]
    table_type: TableType,
    #[allow(dead_code)]
    metadata: TableMetadata,
    #[allow(dead_code)]
    metadata_location: String,
    #[allow(dead_code)]
    manifests: Vec<ManifestFile>,
}

/// The metadata folder name under table base location.
const METADATA_FOLDER: &str = "metadata";

impl Table {
    /// Returns filesystem table from given metadata location specified as the root of provided
    /// object store.
    pub async fn get_filesystem_table(version: u64, fs: Arc<DynObjectStore>) -> Result<Table> {
        let metadata_location = format!("{METADATA_FOLDER}/v{version}.metadata.json");
        let metadata = TableMetadata::get_metadata(&metadata_location, &fs).await?;
        let manifests = Table::get_manifests(&metadata, &fs).await?;

        Ok(Self {
            table_type: FileSystem(fs),
            metadata,
            metadata_location: metadata_location.to_string(),
            manifests,
        })
    }

    /// Returns the file system of this table.
    pub fn get_filesystem(&self) -> Result<Arc<DynObjectStore>> {
        match &self.table_type {
            FileSystem(fs) => Ok(fs.clone()),
        }
    }

    async fn read_manifest(&self, manifest_file: &ManifestFile) -> Result<Manifest> {
        let object_store = self.get_filesystem()?;
        let manifest_path = manifest_file.manifest_path();
        let table_location = self.metadata.location();
        let relative_path = manifest_path.trim_start_matches(table_location);

        let bytes: Cursor<Vec<u8>> = Cursor::new(
            object_store
                .get(&relative_path.into())
                .await
                .map_err(anyhow::Error::msg)?
                .bytes()
                .await?
                .into(),
        );

        Manifest::read_manifest(bytes)
    }

    /// Returns a list of parquet files from data files in this table.
    pub async fn get_parquet_files(&self) -> Result<Vec<String>> {
        let futures = self
            .manifests
            .iter()
            .map(|manifest_file| async {
                let manifest = self.read_manifest(manifest_file).await?;
                let ManifestMetadata::V2(metadata) = manifest.metadata;

                if metadata.content == Content::Data
                    && manifest.entry.file_format() == &FileFormat::Parquet
                {
                    Ok(manifest.entry.file_path().to_string())
                } else {
                    Err(anyhow::Error::msg("Can only get parquet files."))
                }
            })
            .into_iter()
            .collect::<Vec<_>>();

        let results = join_all(futures).await;
        let mut parquet_files: Vec<String> = vec![];

        for r in results {
            match r {
                Err(e) => return Err(e),
                Ok(file) => parquet_files.push(file),
            }
        }
        Ok(parquet_files)
    }

    /// Reads parquet files as a stream of arrow `RecordBatch`es.
    pub async fn read_batches(
        &self,
    ) -> Result<impl Stream<Item = core::result::Result<RecordBatch, ParquetError>>> {
        Ok(ParquetRecordBatchesStream {
            files: self.get_parquet_files().await?.into_iter(),
            object_store: self.get_filesystem()?.clone(),
            base_location: self.metadata.location().to_string(),
            state: StreamState::Init,
        })
    }

    /// Return all manifest files associated to the latest table snapshot.
    /// Reads the related manifest_list file and returns its entries.
    /// If the manifest list file is empty returns an empty vector.
    pub(crate) async fn get_manifests(
        metadata: &TableMetadata,
        object_store: &Arc<DynObjectStore>,
    ) -> Result<Vec<ManifestFile>> {
        match metadata.manifest_list() {
            Some(manifest_list) => {
                let relative_path = manifest_list.trim_start_matches(metadata.location());

                let bytes: Cursor<Vec<u8>> = Cursor::new(
                    object_store
                        .get(&relative_path.into())
                        .await
                        .map_err(anyhow::Error::msg)?
                        .bytes()
                        .await?
                        .into(),
                );
                // Read the file content only if the bytes are not empty otherwise return an empty vector
                if !bytes.get_ref().is_empty() {
                    let reader = apache_avro::Reader::new(bytes)?;
                    reader
                        .map(|record| {
                            read_manifest_file_from_avro(record, metadata.format_version())
                        })
                        .collect()
                } else {
                    Ok(Vec::new())
                }
            }
            None => Ok(Vec::new()),
        }
    }
}

/// Convert an avro value to a [`ManifestFile`] according to the provided format version
fn read_manifest_file_from_avro(
    entry: Result<Value, apache_avro::Error>,
    format_version: FormatVersion,
) -> Result<ManifestFile> {
    entry
        .and_then(|value| match format_version {
            FormatVersion::V2 => {
                apache_avro::from_value::<ManifestFileV2>(&value).map(ManifestFile::V2)
            }
        })
        .map_err(anyhow::Error::msg)
}

/// An asynchronous [`Stream`] of [`RecordBatch`] for parquet files.
pub struct ParquetRecordBatchesStream<I: Iterator<Item = String>> {
    files: I,
    object_store: Arc<DynObjectStore>,
    base_location: String,
    state: StreamState,
}

impl<I: Iterator<Item = String>> Unpin for ParquetRecordBatchesStream<I> {}

enum StreamState {
    Init,
    Streaming(BoxStream<'static, core::result::Result<RecordBatch, ParquetError>>),
    ReadingObject(ObjectMeta),
    Reading(
        BoxFuture<
            'static,
            core::result::Result<
                ArrowReaderBuilder<AsyncReader<ParquetObjectReader>>,
                ParquetError,
            >,
        >,
    ),
}

impl std::fmt::Debug for StreamState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamState::Init => write!(f, "StreamState::Init"),
            StreamState::Streaming(_) => write!(f, "StreamState::Streaming"),
            StreamState::ReadingObject(_) => write!(f, "StreamState::ReadingObject"),
            StreamState::Reading(_) => write!(f, "StreamState::Reading"),
        }
    }
}

impl<I> Stream for ParquetRecordBatchesStream<I>
where
    I: Iterator<Item = String>,
{
    type Item = core::result::Result<RecordBatch, ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let object_store = self.object_store.clone();
            match &mut self.state {
                StreamState::Init => {
                    // Read next file if present.
                    let next = self.files.next();
                    if let Some(file) = next {
                        let relative_path = file.trim_start_matches(&self.base_location).into();
                        let future = object_store.head(&relative_path);
                        let object_metadata = block_on(future);

                        match object_metadata {
                            Ok(object_metadata) => {
                                self.state = StreamState::ReadingObject(object_metadata);

                                let waker = cx.waker().clone();
                                waker.wake();

                                return Poll::Pending;
                            }
                            Err(err) => {
                                return Poll::Ready(Some(Err(ParquetError::General(
                                    err.to_string(),
                                ))));
                            }
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
                StreamState::Streaming(stream) => match ready!(stream.poll_next_unpin(cx)) {
                    None => {
                        self.state = StreamState::Init;
                        let waker = cx.waker().clone();
                        waker.wake();
                        return Poll::Pending;
                    }
                    Some(next_batch) => match next_batch {
                        Ok(batch) => return Poll::Ready(Some(Ok(batch))),
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    },
                },
                StreamState::ReadingObject(object_metadata) => {
                    let object_reader =
                        ParquetObjectReader::new(object_store, object_metadata.clone());
                    let builder = ParquetRecordBatchStreamBuilder::new(object_reader).boxed();

                    self.state = StreamState::Reading(builder);
                    let waker = cx.waker().clone();
                    waker.wake();
                    return Poll::Pending;
                }
                StreamState::Reading(builder) => match ready!(builder.poll_unpin(cx)) {
                    Ok(builder) => match builder.build() {
                        Ok(reader) => {
                            self.state = StreamState::Streaming(reader.boxed());
                            let waker = cx.waker().clone();
                            waker.wake();
                            return Poll::Pending;
                        }
                        Err(err) => {
                            return Poll::Ready(Some(Err(err)));
                        }
                    },
                    Err(err) => {
                        return Poll::Ready(Some(Err(ParquetError::General(err.to_string()))))
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::table::FormatVersion;
    use crate::table::Table;
    use arrow_array::cast::{as_primitive_array, as_string_array};
    use arrow_array::types::Int64Type;
    use arrow_array::Array;
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::DynObjectStore;
    use std::sync::Arc;

    #[tokio::test()]
    async fn test_read_table_from_location() {
        let object_store: Arc<DynObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("test/data/table1").unwrap());

        let table = Table::get_filesystem_table(2, object_store).await.unwrap();
        let metadata = table.metadata;

        assert_eq!(metadata.format_version(), FormatVersion::V2);

        let manifests = table.manifests;
        for manifest in manifests {
            assert_eq!(manifest.added_data_files_count(), Some(3));
        }
    }

    #[tokio::test()]
    async fn test_read_parquet_files_from_table() {
        let object_store: Arc<DynObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("test/data/table1").unwrap());

        let table = Table::get_filesystem_table(2, object_store).await.unwrap();
        let files = table.get_parquet_files().await.unwrap();
        assert_eq!(files.len(), 1);
    }

    #[tokio::test()]
    async fn test_read_parquet_files_as_batches() {
        let object_store: Arc<DynObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("test/data/table1").unwrap());

        let table = Table::get_filesystem_table(2, object_store).await.unwrap();
        let stream = table.read_batches().await.unwrap();
        let result = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(result.len(), 1);

        let batch = &result[0];
        let column1 = batch.column(0);
        let column2 = batch.column(1);

        let int_array = as_primitive_array::<Int64Type>(column1);
        let string_array = as_string_array(column2);

        assert_eq!(int_array.len(), 1);
        assert_eq!(int_array.value(0), 1);

        assert_eq!(string_array.len(), 1);
        assert_eq!(string_array.value(0), "a");
    }
}
