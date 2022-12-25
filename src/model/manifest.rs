/*!
Manifest files
 */
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
};

use std::ops::Deref;

use crate::model::table::FormatVersion;
use crate::model::values::IcebergValue;
use anyhow::{anyhow, Context, Error, Result};
use serde::{
    de::{DeserializeOwned, MapAccess, Visitor},
    ser::{SerializeSeq, SerializeStruct},
    Deserialize, Deserializer, Serialize,
};
use serde_bytes::ByteBuf;
use serde_repr::{Deserialize_repr, Serialize_repr};

/// A manifest file is an immutable Avro file that lists data files or delete files, along with
/// each file's partition data tuple, metrics, and tracking information.
pub struct Manifest {
    /// The manifest metadata, stored in the Avro file's key-value metadata.
    pub metadata: ManifestMetadata,
    /// The manifest entry struct. The schema of a manifest file.
    pub entry: ManifestEntry,
}

impl Manifest {
    /// Read a manifest
    pub fn read_manifest<R: std::io::Read>(r: R) -> Result<Manifest> {
        let mut reader = apache_avro::Reader::new(r)?;

        let metadata = ManifestMetadata::read_from_avro(&reader)?;
        let entry = ManifestEntry::read_from_avro(&mut reader)?;
        Ok(Manifest { metadata, entry })
    }
}

/// Lists data files or delete files, along with each file’s
/// partition data tuple, metrics, and tracking information.
pub enum ManifestMetadata {
    /// V2 spec iceberg manifest metadata
    V2(ManifestMetadataV2),
}

impl ManifestMetadata {
    /// Read metadata from the avro reader
    pub fn read_from_avro<R: std::io::Read>(
        reader: &apache_avro::Reader<R>,
    ) -> Result<ManifestMetadata> {
        let read_string = |key: &str| {
            reader
                .user_metadata()
                .get(key)
                .map(|id| String::from_utf8(id.to_vec()).map_err(anyhow::Error::from))
                .transpose()
        };

        let format_version: Option<FormatVersion> = reader
            .user_metadata()
            .get("format-version")
            .and_then(|n| n[0].try_into().ok());
        match format_version {
            Some(FormatVersion::V2) => {
                let schema = read_string("schema")?.context("Metadata must have table schema")?;
                let schema_id = read_string("schema-id")?.unwrap();
                let partition_spec = read_string("partition-spec")?.unwrap();
                let partition_spec_id = read_string("partition-spec-id")?.unwrap();
                let content: Content = reader
                    .user_metadata()
                    .get("content")
                    .and_then(|n| n.clone().try_into().ok())
                    .unwrap();

                Ok(ManifestMetadata::V2(ManifestMetadataV2 {
                    schema,
                    schema_id,
                    partition_spec,
                    partition_spec_id,
                    format_version: format_version.unwrap(),
                    content,
                }))
            }
            None => Err(Error::msg("No format-version in the file. Invalid format.")),
        }
    }
}

/// Lists data files or delete files, along with each file’s
/// partition data tuple, metrics, and tracking information.
/// Should this be called metadata?
pub struct ManifestMetadataV2 {
    /// JSON representation of the table schema at the time the manifest was written
    /// Should this be Typed?
    pub schema: String,
    /// ID of the schema used to write the manifest as a string
    /// Should this be typed into a
    pub schema_id: String,
    /// JSON fields representation of the partition spec used to write the manifest
    pub partition_spec: String,
    /// ID of the partition spec used to write the manifest as a string
    pub partition_spec_id: String,
    /// Table format version number of the manifest as a string
    pub format_version: FormatVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    pub content: Content,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Used to track additions and deletions
pub enum Status {
    /// Existing files
    Existing = 0,
    /// Added files
    Added = 1,
    /// Deleted files. Deletions are informational only and not used in scans.
    Deleted = 2,
}

/// Entry in manifest
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum ManifestEntry {
    /// Manifest entry version 2
    V2(ManifestEntryV2),
}

impl ManifestEntry {
    /// Read manifest entry from the avro reader
    pub fn read_from_avro<R: std::io::Read>(
        reader: &mut apache_avro::Reader<R>,
    ) -> Result<ManifestEntry> {
        let record = reader
            .into_iter()
            .next()
            .context("Manifest Entry Expected")??;
        apache_avro::from_value::<ManifestEntryV2>(&record)
            .map(ManifestEntry::V2)
            .map_err(anyhow::Error::msg)
    }
}

/// Entry in manifest with the iceberg spec version 2.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ManifestEntryV2 {
    /// Used to track additions and deletions
    pub status: Status,
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited from manifest metadata when null.
    pub snapshot_id: Option<i64>,
    /// Sequence number when the file was added. Inherited when null.
    pub sequence_number: Option<i64>,
    /// File path, partition tuple, metrics, …
    pub data_file: DataFileV2,
}

impl ManifestEntry {
    /// Get schema of manifest entry.
    pub fn schema(partition_schema: &str, format_version: &FormatVersion) -> String {
        match format_version {
            FormatVersion::V2 => {
                let datafile_schema = DataFileV2::schema(partition_schema);
                r#"{
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                {
                    "name": "status",
                    "type": "int",
                    "field_id": 0
                },
                {
                    "name": "snapshot_id",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 1
                },
                {
                    "name": "sequence_number",
                    "type": [
                        "null",
                        "long"
                    ],
                    "default": null,
                    "field_id": 3
                },
                {
                    "name": "data_file",
                    "type": "#
                    .to_owned()
                    + &datafile_schema
                    + r#",
                    "field_id": 2
                }
            ]
        }"#
            }
        }
    }
    /// Partition data tuple, schema based on the partition spec output using partition field ids for the struct field ids
    pub fn partition_values(&self) -> &PartitionValues {
        match self {
            ManifestEntry::V2(entry) => &entry.data_file.partition,
        }
    }
    /// Full URI for the file with a FS scheme.
    pub fn file_path(&self) -> &str {
        match self {
            ManifestEntry::V2(entry) => &entry.data_file.file_path,
        }
    }
    /// Total file size in bytes
    pub fn file_size_in_bytes(&self) -> i64 {
        match self {
            ManifestEntry::V2(entry) => entry.data_file.file_size_in_bytes,
        }
    }
    /// Map from column id to lower bound in the column
    pub fn lower_bounds(&self) -> &Option<AvroMap<ByteBuf>> {
        match self {
            ManifestEntry::V2(entry) => &entry.data_file.lower_bounds,
        }
    }
    /// Map from column id to upper bound in the column
    pub fn upper_bounds(&self) -> &Option<AvroMap<ByteBuf>> {
        match self {
            ManifestEntry::V2(entry) => &entry.data_file.upper_bounds,
        }
    }
    /// Map from column id to number of null values
    pub fn null_value_counts(&self) -> &Option<AvroMap<i64>> {
        match self {
            ManifestEntry::V2(entry) => &entry.data_file.null_value_counts,
        }
    }
}

#[derive(Debug, Serialize_repr, Deserialize_repr, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Type of content stored by the data file.
pub enum Content {
    /// Data.
    Data = 0,
    /// Deletes at position.
    PositionDeletes = 1,
    /// Delete by equality.
    EqualityDeletes = 2,
}

impl TryFrom<Vec<u8>> for Content {
    type Error = anyhow::Error;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match String::from_utf8(value)?.to_uppercase().as_str() {
            "DATA" => Ok(Content::Data),
            "POSITION DELETES" => Ok(Content::PositionDeletes),
            "EQUALITY DELETES" => Ok(Content::EqualityDeletes),
            _ => Err(anyhow!("Failed to convert String to Content.")),
        }
    }
}

impl From<Content> for Vec<u8> {
    fn from(value: Content) -> Self {
        match value {
            Content::Data => "DATA".as_bytes().to_owned(),
            Content::PositionDeletes => "POSITION DELETES".as_bytes().to_owned(),
            Content::EqualityDeletes => "EQUALITY DELETES".as_bytes().to_owned(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(u8)]
/// Name of file format
pub enum FileFormat {
    /// Avro file
    Avro = 0,
    /// Orc file
    Orc = 1,
    /// Parquet file
    Parquet = 2,
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl Serialize for FileFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use FileFormat::*;
        match self {
            Avro => serializer.serialize_str("AVRO"),
            Orc => serializer.serialize_str("ORC"),
            Parquet => serializer.serialize_str("PARQUET"),
        }
    }
}

/// Serialize for PrimitiveType wit special handling for
/// Decimal and Fixed types.
impl<'de> Deserialize<'de> for FileFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "AVRO" {
            Ok(FileFormat::Avro)
        } else if s == "ORC" {
            Ok(FileFormat::Orc)
        } else if s == "PARQUET" {
            Ok(FileFormat::Parquet)
        } else {
            Err(serde::de::Error::custom("Invalid data file format."))
        }
    }
}

/// The partition struct stores the tuple of partition values for each file.
/// Its type is derived from the partition fields of the partition spec used to write the manifest file.
/// In v2, the partition struct’s field ids must match the ids from the partition spec.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionValues {
    fields: Vec<Option<IcebergValue>>,
    lookup: BTreeMap<String, usize>,
}

impl Deref for PartitionValues {
    type Target = [Option<IcebergValue>];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl FromIterator<(String, Option<IcebergValue>)> for PartitionValues {
    fn from_iter<I: IntoIterator<Item = (String, Option<IcebergValue>)>>(iter: I) -> Self {
        let mut fields = Vec::new();
        let mut lookup = BTreeMap::new();

        for (i, (key, value)) in iter.into_iter().enumerate() {
            fields.push(value);
            lookup.insert(key, i);
        }

        PartitionValues { fields, lookup }
    }
}

impl Serialize for PartitionValues {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut record = serializer.serialize_struct("r102", self.fields.len())?;
        for (i, value) in self.fields.iter().enumerate() {
            let (key, _) = self.lookup.iter().find(|(_, value)| **value == i).unwrap();
            record.serialize_field(Box::leak(key.clone().into_boxed_str()), value)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for PartitionValues {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PartitionStructVisitor;

        impl<'de> Visitor<'de> for PartitionStructVisitor {
            type Value = PartitionValues;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("map")
            }

            fn visit_map<V>(self, mut map: V) -> Result<PartitionValues, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut fields: Vec<Option<IcebergValue>> = Vec::new();
                let mut lookup: BTreeMap<String, usize> = BTreeMap::new();
                let mut index = 0;
                while let Some(key) = map.next_key()? {
                    fields.push(map.next_value()?);
                    lookup.insert(key, index);
                    index += 1;
                }
                Ok(PartitionValues { fields, lookup })
            }
        }
        deserializer.deserialize_struct(
            "r102",
            Box::leak(vec![].into_boxed_slice()),
            PartitionStructVisitor,
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct KeyValue<T: Serialize + Clone> {
    key: i32,
    value: T,
}

/// Utility struct to convert avro maps to rust hashmaps. Derefences to a Hashmap.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AvroMap<T: Serialize + Clone>(HashMap<i32, T>);

impl<T: Serialize + Clone> core::ops::Deref for AvroMap<T> {
    type Target = HashMap<i32, T>;

    fn deref(self: &'_ AvroMap<T>) -> &'_ Self::Target {
        &self.0
    }
}

impl<T: Serialize + Clone> Serialize for AvroMap<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let entries = self
            .0
            .iter()
            .map(|(key, value)| KeyValue {
                key: *key,
                value: (*value).clone(),
            })
            .collect::<Vec<KeyValue<T>>>();
        let mut seq = serializer.serialize_seq(Some(entries.len()))?;
        for element in entries {
            seq.serialize_element(&element)?;
        }
        seq.end()
    }
}

impl<'de, T: Serialize + DeserializeOwned + Clone> Deserialize<'de> for AvroMap<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec: Vec<KeyValue<T>> = Vec::deserialize(deserializer)?;
        Ok(AvroMap(HashMap::from_iter(
            vec.into_iter().map(|x| (x.key, x.value)),
        )))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
/// DataFile found in Manifest.
pub struct DataFileV2 {
    ///Type of content in data file.
    pub content: Content,
    /// Full URI for the file with a FS scheme.
    pub file_path: String,
    /// String file format name, avro, orc or parquet
    pub file_format: FileFormat,
    /// Partition data tuple, schema based on the partition spec output using partition field ids for the struct field ids
    pub partition: PartitionValues,
    /// Number of records in this file
    pub record_count: i64,
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// Map from column id to total size on disk
    pub column_sizes: Option<AvroMap<i64>>,
    /// Map from column id to number of values in the column (including null and NaN values)
    pub value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of null values
    pub null_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of NaN values
    pub nan_value_counts: Option<AvroMap<i64>>,
    /// Map from column id to number of distinct values in the column.
    pub distinct_counts: Option<AvroMap<i64>>,
    /// Map from column id to lower bound in the column
    pub lower_bounds: Option<AvroMap<ByteBuf>>,
    /// Map from column id to upper bound in the column
    pub upper_bounds: Option<AvroMap<ByteBuf>>,
    /// Implementation specific key metadata for encryption
    pub key_metadata: Option<ByteBuf>,
    /// Split offsets for the data file.
    pub split_offsets: Option<Vec<i64>>,
    /// Field ids used to determine row equality in equality delete files.
    pub equality_ids: Option<Vec<i32>>,
    /// ID representing sort order for this file
    pub sort_order_id: Option<i32>,
}

impl DataFileV2 {
    /// Get schema
    pub fn schema(partition_schema: &str) -> String {
        r#"{
            "type": "record",
            "name": "r2",
            "fields": [
                {
                    "name": "content",
                    "type": "int",
                    "field_id": 134
                },
                {
                    "name": "file_path",
                    "type": "string",
                    "field_id": 100
                },
                {
                    "name": "file_format",
                    "type": "string",
                    "field_id": 101
                },
                {
                    "name": "partition",
                    "type": "#
            .to_owned()
            + partition_schema
            + r#",
                    "field_id": 102
                },
                {
                    "name": "record_count",
                    "type": "long",
                    "field_id": 103
                },
                {
                    "name": "file_size_in_bytes",
                    "type": "long",
                    "field_id": 104
                },
                {
                    "name": "column_sizes",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k117_v118",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 117
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 118
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 108
                },
                {
                    "name": "value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k119_v120",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 119
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 120
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 109
                },
                {
                    "name": "null_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k121_v122",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 121
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 122
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 110
                },
                {
                    "name": "nan_value_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k138_v139",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 138
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 139
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 137
                },
                {
                    "name": "distinct_counts",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k123_v124",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 123
                                    },
                                    {
                                        "name": "value",
                                        "type": "long",
                                        "field-id": 124
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 111
                },
                {
                    "name": "lower_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k126_v127",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 126
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 127
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 125
                },
                {
                    "name": "upper_bounds",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "logicalType": "map",
                            "items": {
                                "type": "record",
                                "name": "k129_v130",
                                "fields": [
                                    {
                                        "name": "key",
                                        "type": "int",
                                        "field-id": 129
                                    },
                                    {
                                        "name": "value",
                                        "type": "bytes",
                                        "field-id": 130
                                    }
                                ]
                            }
                        }
                    ],
                    "default": null,
                    "field_id": 128
                },
                {
                    "name": "key_metadata",
                    "type": [
                        "null",
                        "bytes"
                    ],
                    "default": null,
                    "field_id": 131
                },
                {
                    "name": "split_offsets",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "long",
                            "element-id": 133
                        }
                    ],
                    "default": null,
                    "field_id": 132
                },
                {
                    "name": "equality_ids",
                    "type": [
                        "null",
                        {
                            "type": "array",
                            "items": "int",
                            "element-id": 136
                        }
                    ],
                    "default": null,
                    "field_id": 135
                },
                {
                    "name": "sort_order_id",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null,
                    "field_id": 140
                }
            ]
        }"#
    }
}

/// Read a manifest
pub fn read_manifest<R: std::io::Read>(r: R) -> Result<Manifest> {
    let mut reader = apache_avro::Reader::new(r)?;

    let metadata = ManifestMetadata::read_from_avro(&reader)?;
    let entry = ManifestEntry::read_from_avro(&mut reader)?;
    Ok(Manifest { metadata, entry })
}

#[cfg(test)]
mod tests {
    use crate::model::{
        partition::{PartitionField, PartitionSpec, Transform},
        schema::{AllType, PrimitiveType, SchemaV2, Struct, StructField},
    };

    use super::*;
    use apache_avro::{self, types::Value as AvroValue};
    use proptest::prelude::*;

    fn status_strategy() -> impl Strategy<Value = Status> {
        prop_oneof![
            Just(Status::Existing),
            Just(Status::Added),
            Just(Status::Deleted),
        ]
    }

    prop_compose! {
        fn arb_manifest_entry()(status in status_strategy(),
            snapshot_id in prop::option::of(any::<i64>()),
            sequence_number in prop::option::of(any::<i64>())
        )  -> ManifestEntry {
            ManifestEntry::V2(ManifestEntryV2{
                status,
                snapshot_id,
                sequence_number,
                data_file: DataFileV2 {
                    content: Content::Data,
                    file_path: "/".to_string(),
                    file_format: FileFormat::Parquet,
                    partition: PartitionValues::from_iter(vec![("ts_day".to_owned(), Some(IcebergValue::Int(1)))]),
                    record_count: 4,
                    file_size_in_bytes: 1200,
                    column_sizes: None,
                    value_counts: None,
                    null_value_counts: None,
                    nan_value_counts: None,
                    distinct_counts: None,
                    lower_bounds: Some(AvroMap(HashMap::from_iter(vec![(0,ByteBuf::from(vec![0,0,0,0]))]))),
                    upper_bounds: None,
                    key_metadata: None,
                    split_offsets: None,
                    equality_ids: None,
                    sort_order_id: None,
                }
            })
        }
    }

    proptest! {
        #[test]
        fn test_manifest_entry(a in arb_manifest_entry()) {

            let table_schema = SchemaV2 {
                schema_id: 0,
                identifier_field_ids: None,
                name_mapping: None,
                struct_fields: Struct {
                    fields: vec![StructField {
                        id: 4,
                        name: "day".to_owned(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::Int),
                        doc: None,
                    }],
                },
            };

            let spec = PartitionSpec {
                spec_id: 0,
                fields: vec![PartitionField {
                    source_id: 4,
                    field_id: 1000,
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                }],
            };

            let partition_schema = spec.schema(&table_schema.struct_fields).unwrap();

            let raw_schema = ManifestEntry::schema(&partition_schema, &FormatVersion::V2);

            let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

            let partition_spec = spec.to_json(&table_schema.struct_fields).unwrap();
            let partition_spec_id = "0";
            // TODO: make this a correct schema
            let table_schema = r#"{"schema": "0"}"#;
            let table_schema_id = "1";
            let format_version = FormatVersion::V2;
            let content = "DATA";

            let meta: std::collections::HashMap<String, apache_avro::types::Value> =
                std::collections::HashMap::from_iter(vec![
                    ("schema".to_string(), AvroValue::Bytes(table_schema.into())),
                    ("schema-id".to_string(), AvroValue::Bytes(table_schema_id.into())),
                    ("partition-spec".to_string(), AvroValue::Bytes(partition_spec.into())),
                    ("partition-spec-id".to_string(), AvroValue::Bytes(partition_spec_id.into())),
                    ("format-version".to_string(), AvroValue::Bytes(vec![u8::from(format_version)])),
                    ("content".to_string(), AvroValue::Bytes(content.into()))
                    ],
                );
            let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .user_metadata(meta)
            .build();
            writer.append_ser(a.clone()).unwrap();

            let encoded = writer.into_inner().unwrap();

            let reader = apache_avro::Reader::new( &encoded[..]).unwrap();

            for value in reader {
                let entry = apache_avro::from_value::<ManifestEntryV2>(&value.unwrap()).map(|entry| ManifestEntry::V2(entry)).unwrap();
                assert_eq!(a, entry)
            }

        }

        #[test]
        fn test_read_manifest(a in arb_manifest_entry()) {
            let table_schema = SchemaV2 {
                schema_id: 0,
                identifier_field_ids: None,
                name_mapping: None,
                struct_fields: Struct {
                    fields: vec![StructField {
                        id: 4,
                        name: "day".to_owned(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::Int),
                        doc: None,
                    }],
                },
            };

            let spec = PartitionSpec {
                spec_id: 0,
                fields: vec![PartitionField {
                    source_id: 4,
                    field_id: 1000,
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                }],
            };

            let partition_schema = spec.schema(&table_schema.struct_fields).unwrap();

            let raw_schema = ManifestEntry::schema(&partition_schema, &FormatVersion::V2);

            let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

            let partition_spec = spec.to_json(&table_schema.struct_fields).unwrap();
            let partition_spec_id = "0";
            // TODO: make this a correct schema
            let table_schema = r#"{"schema": "0"}"#;
            let table_schema_id = "1";
            let format_version = FormatVersion::V2;
            let content = "DATA";

            let meta: std::collections::HashMap<String, apache_avro::types::Value> =
                std::collections::HashMap::from_iter(vec![
                    ("schema".to_string(), AvroValue::Bytes(table_schema.into())),
                    ("schema-id".to_string(), AvroValue::Bytes(table_schema_id.into())),
                    ("partition-spec".to_string(), AvroValue::Bytes(partition_spec.clone().into())),
                    ("partition-spec-id".to_string(), AvroValue::Bytes(partition_spec_id.into())),
                    ("format-version".to_string(), AvroValue::Bytes(vec![u8::from(format_version.clone())])),
                    ("content".to_string(), AvroValue::Bytes(content.into()))
                    ],
                );
            let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .user_metadata(meta)
            .build();
            writer.append_ser(a.clone()).unwrap();

            let encoded = writer.into_inner().unwrap();

            let reader = apache_avro::Reader::new( &encoded[..]).unwrap();
            let ManifestMetadata::V2(metadata) = ManifestMetadata::read_from_avro(&reader).unwrap();
            assert_eq!(metadata.schema, table_schema.to_string());
            assert_eq!(metadata.schema_id, table_schema_id.to_string());
            assert_eq!(metadata.partition_spec, partition_spec.to_string());
            assert_eq!(metadata.partition_spec_id, partition_spec_id.to_string());
            assert_eq!(metadata.format_version, format_version);
        }

        #[test]
        fn test_read_manifest_entry(a in arb_manifest_entry()) {

            let table_schema = SchemaV2 {
                schema_id: 0,
                identifier_field_ids: None,
                name_mapping: None,
                struct_fields: Struct {
                    fields: vec![StructField {
                        id: 4,
                        name: "day".to_owned(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::Int),
                        doc: None,
                    }],
                },
            };

            let spec = PartitionSpec {
                spec_id: 0,
                fields: vec![PartitionField {
                    source_id: 4,
                    field_id: 1000,
                    name: "ts_day".to_string(),
                    transform: Transform::Day,
                }],
            };

            let partition_schema = spec.schema(&table_schema.struct_fields).unwrap();

            let raw_schema = ManifestEntry::schema(&partition_schema, &FormatVersion::V2);

            let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

            let partition_spec = spec.to_json(&table_schema.struct_fields).unwrap();
            let partition_spec_id = "0";
            // TODO: make this a correct schema
            let table_schema = r#"{"schema": "0"}"#;
            let table_schema_id = "1";
            let format_version = "1";
            let content = "DATA";

            let meta: std::collections::HashMap<String, apache_avro::types::Value> =
                std::collections::HashMap::from_iter(vec![
                    ("schema".to_string(), AvroValue::Bytes(table_schema.into())),
                    ("schema-id".to_string(), AvroValue::Bytes(table_schema_id.into())),
                    ("partition-spec".to_string(), AvroValue::Bytes(partition_spec.into())),
                    ("partition-spec-id".to_string(), AvroValue::Bytes(partition_spec_id.into())),
                    ("format-version".to_string(), AvroValue::Bytes(format_version.into())),
                    ("content".to_string(), AvroValue::Bytes(content.into()))
                    ],
                );
            let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(vec![])
            .user_metadata(meta)
            .build();
            writer.append_ser(a.clone()).unwrap();

            let encoded = writer.into_inner().unwrap();

            let mut reader = apache_avro::Reader::new( &encoded[..]).unwrap();
            let metadata_entry = ManifestEntry::read_from_avro(&mut reader).unwrap();
            assert_eq!(a, metadata_entry);
        }
    }

    #[test]
    pub fn test_partition_values() {
        let partition_values =
            PartitionValues::from_iter(vec![("ts_day".to_owned(), Some(IcebergValue::Int(1)))]);

        let table_schema = SchemaV2 {
            schema_id: 0,
            identifier_field_ids: None,
            name_mapping: None,
            struct_fields: Struct {
                fields: vec![StructField {
                    id: 4,
                    name: "day".to_owned(),
                    required: false,
                    field_type: AllType::Primitive(PrimitiveType::Int),
                    doc: None,
                }],
            },
        };

        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 4,
                field_id: 1000,
                name: "ts_day".to_string(),
                transform: Transform::Day,
            }],
        };

        let raw_schema = spec.schema(&table_schema.struct_fields).unwrap();

        dbg!(&raw_schema);

        let schema = apache_avro::Schema::parse_str(&raw_schema).unwrap();

        let mut writer = apache_avro::Writer::new(&schema, Vec::new());

        writer.append_ser(partition_values.clone()).unwrap();

        let encoded = writer.into_inner().unwrap();

        let reader = apache_avro::Reader::new(&*encoded).unwrap();

        for record in reader {
            let result = apache_avro::from_value::<PartitionValues>(&record.unwrap()).unwrap();
            assert_eq!(partition_values, result);
        }
    }
}
