/*!
Iceberg table.
 */

use crate::model::manifest::{Content, FileFormat, Manifest, ManifestMetadata};
use crate::model::manifest_list::{ManifestFile, ManifestFileV2};
use crate::model::table::{FormatVersion, TableMetadata};
use crate::table::TableType::FileSystem;
use anyhow::Result;
use apache_avro::types::Value;
use futures::future::join_all;
use object_store::{DynObjectStore, ObjectStore};
use std::io::Cursor;
use std::sync::Arc;

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

#[cfg(test)]
mod tests {
    use crate::model::table::FormatVersion;
    use crate::table::Table;
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
}
