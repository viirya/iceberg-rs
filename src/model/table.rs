/*!
Defines the [table metadata](https://iceberg.apache.org/spec/#table-metadata).
The main struct here is [TableMetadataV2] which defines the data for a table.
*/
use anyhow::{anyhow, Result};
use object_store::DynObjectStore;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use crate::model::{
    partition::PartitionSpec,
    schema,
    snapshot::{Reference, SnapshotV2},
    sort,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadata of an iceberg table
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TableMetadata {
    /// Version 2 of the table metadata
    V2(TableMetadataV2),
}

impl TableMetadata {
    /// Reads metadata file from provided location and object store.
    pub async fn get_metadata(
        location: &str,
        object_store: &Arc<DynObjectStore>,
    ) -> Result<TableMetadata> {
        let bytes: Cursor<Vec<u8>> = Cursor::new(
            object_store
                .get(&location.into())
                .await
                .map_err(anyhow::Error::msg)?
                .bytes()
                .await?
                .into(),
        );
        let metadata: TableMetadataV2 = serde_json::from_reader(bytes)?;
        Ok(TableMetadata::V2(metadata))
    }

    /// Returns the optional current snapshot id.
    pub fn get_current_snapshot_id(&self) -> &Option<i64> {
        match self {
            TableMetadata::V2(metadata) => &metadata.current_snapshot_id,
        }
    }

    /// Get the manifest_list for the current snapshot of the table
    pub fn manifest_list(&self, snapshot_id: Option<i64>) -> Option<&str> {
        match self {
            TableMetadata::V2(metadata) => {
                metadata
                    .snapshots
                    .as_ref()
                    .zip(snapshot_id)
                    .and_then(|(snapshots, id)| {
                        snapshots
                            .iter()
                            .find(|snapshot| snapshot.snapshot_id == id)
                            .map(|snapshot| snapshot.manifest_list.as_str())
                    })
            }
        }
    }

    /// Returns the location of the table.
    pub fn location(&self) -> &str {
        match self {
            TableMetadata::V2(metadata) => metadata.location.as_str(),
        }
    }

    /// Get the format version of the table
    pub fn format_version(&self) -> FormatVersion {
        match self {
            TableMetadata::V2(_) => FormatVersion::V2,
        }
    }
}

/// Format version for iceberg table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FormatVersion {
    /// Iceberg v2 spec
    V2,
}

impl TryFrom<u8> for FormatVersion {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match char::from_u32(value as u32)
            .ok_or_else(|| anyhow!("Failed to convert u8 to char."))?
        {
            '2' => Ok(FormatVersion::V2),
            value => Err(anyhow!("Unsupported FormatVersion: {}.", value)),
        }
    }
}

impl From<FormatVersion> for u8 {
    fn from(value: FormatVersion) -> Self {
        match value {
            FormatVersion::V2 => '2' as u8,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "format-version")]
/// Fields for the version 2 of the table metadata.
pub struct TableMetadataV2 {
    /// Integer Version for the format.
    /// A UUID that identifies the table
    pub table_uuid: Uuid,
    /// Location tables base location
    pub location: String,
    /// The tables highest sequence number
    pub last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last updated.
    pub last_updated_ms: i64,
    /// An integer; the highest assigned column ID for the table.
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: Vec<schema::SchemaV2>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: Vec<PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    /// An integer; the highest assigned partition field ID across all partition specs for the table.
    pub last_partition_id: i32,
    /// A string to string map of table properties. This is used to control settings that
    /// affect reading and writing and is not intended to be used for arbitrary metadata.
    /// For example, commit.retry.num-retries is used to control the number of commit retries.
    pub properties: Option<HashMap<String, String>>,
    /// long ID of the current table snapshot; must be the same as the current
    /// ID of the main branch in refs.
    pub current_snapshot_id: Option<i64>,
    /// A list of valid snapshots. Valid snapshots are snapshots for which all
    /// data files exist in the file system. A data file must not be deleted
    /// from the file system until the last snapshot in which it was listed is
    /// garbage collected.
    pub snapshots: Option<Vec<SnapshotV2>>,
    /// A list (optional) of timestamp and snapshot ID pairs that encodes changes
    /// to the current snapshot for the table. Each time the current-snapshot-id
    /// is changed, a new entry should be added with the last-updated-ms
    /// and the new current-snapshot-id. When snapshots are expired from
    /// the list of valid snapshots, all entries before a snapshot that has
    /// expired should be removed.
    pub snapshot_log: Option<Vec<SnapshotLog>>,

    /// A list (optional) of timestamp and metadata file location pairs
    /// that encodes changes to the previous metadata files for the table.
    /// Each time a new metadata file is created, a new entry of the
    /// previous metadata file location should be added to the list.
    /// Tables can be configured to remove oldest metadata log entries and
    /// keep a fixed-size log of the most recent entries after a commit.
    pub metadata_log: Option<Vec<MetadataLog>>,

    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Vec<sort::SortOrder>,
    /// Default sort order id of the table. Note that this could be used by
    /// writers, but is not used when reading because reads use the specs
    /// stored in manifest files.
    pub default_sort_order_id: i64,
    /// A map of snapshot references. The map keys are the unique snapshot reference
    /// names in the table, and the map values are snapshot reference objects.
    /// There is always a main branch reference pointing to the current-snapshot-id
    /// even if the refs map is null.
    pub refs: Option<HashMap<String, Reference>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// Encodes changes to the previous metadata files for the table
pub struct MetadataLog {
    /// The file for the log.
    pub metadata_file: String,
    /// Time new metadata was created
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
/// A log of when each snapshot was made.
pub struct SnapshotLog {
    /// Id of the snapshot.
    pub snapshot_id: i64,
    /// Last updated timestamp
    pub timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use crate::model::table::TableMetadata;
    use anyhow::Result;
    use object_store::local::LocalFileSystem;
    use object_store::DynObjectStore;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use super::TableMetadataV2;

    #[test]
    fn test_deserialize_table_data_v2() -> Result<()> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
                "location": "s3://b/wh/data.db/table",
                "last-sequence-number" : 1,
                "last-updated-ms": 1515100955770,
                "last-column-id": 1,
                "schemas": [
                    {
                        "schema-id" : 1,
                        "type" : "struct",
                        "fields" :[
                            {
                                "id": 1,
                                "name": "struct_name",
                                "required": true,
                                "type": "fixed[1]"
                            }
                        ]
                    }
                ],
                "current-schema-id" : 1,
                "partition-specs": [
                    {
                        "spec-id": 1,
                        "fields": [
                            {  
                                "source-id": 4,  
                                "field-id": 1000,  
                                "name": "ts_day",  
                                "transform": "day"
                            } 
                        ]
                    }
                ],
                "default-spec-id": 1,
                "last-partition-id": 1,
                "properties": {
                    "commit.retry.num-retries": "1"
                },
                "metadata-log": [
                    {  
                        "metadata-file": "s3://bucket/.../v1.json",  
                        "timestamp-ms": 1515100
                    }
                ],
                "sort-orders": [],
                "default-sort-order-id": 0
            }
        "#;
        let metadata = serde_json::from_str::<TableMetadataV2>(&data)?;
        //test serialise deserialise works.
        let metadata_two: TableMetadataV2 =
            serde_json::from_str(&serde_json::to_string(&metadata)?)?;
        assert_eq!(metadata, metadata_two);

        Ok(())
    }

    #[test]
    fn test_invalid_table_uuid() -> Result<()> {
        let data = r#"
            {
                "format-version" : 2,
                "table-uuid": "xxxx"
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(&data).is_err());
        Ok(())
    }
    #[test]
    fn test_deserialize_table_data_v2_invalid_format_version() -> Result<()> {
        let data = r#"
            {
                "format-version" : 1
            }
        "#;
        assert!(serde_json::from_str::<TableMetadataV2>(&data).is_err());
        Ok(())
    }

    fn table_metadata_round_trip(metadata: &TableMetadataV2) {
        let metadata2: TableMetadataV2 =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(metadata, &metadata2);
    }

    #[test]
    fn test_table_metadata_v2() {
        let file = File::open("test/data/TableMetadataV2Valid.json").unwrap();
        let reader = BufReader::new(file);

        let metadata: TableMetadataV2 = serde_json::from_reader(reader).unwrap();
        table_metadata_round_trip(&metadata);
    }

    #[tokio::test()]
    async fn test_table_metadata_v2_from_object_store() {
        let object_store: Arc<DynObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix("test/data").unwrap());
        let TableMetadata::V2(metadata) =
            TableMetadata::get_metadata("TableMetadataV2Valid.json", &object_store)
                .await
                .unwrap();

        table_metadata_round_trip(&metadata);
    }
}
