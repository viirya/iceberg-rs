/*!
Iceberg table.
 */

use std::sync::Arc;
use object_store::ObjectStore;
use crate::model::table::TableMetadata;

/// Tables can be either one of following types:
/// - FileSystem (https://iceberg.apache.org/spec/#file-system-tables)
/// - Metastore (https://iceberg.apache.org/spec/#metastore-tables) - Not supported yet.
pub enum TableType {
    /// Filesystem table
    FileSystem(Arc<dyn ObjectStore>),
}

/// Iceberg table
pub struct Table {
    table_type: TableType,
    metadata: TableMetadata,
    metadata_location: String,
    // manifests: Vec<ManifestFile>,
}