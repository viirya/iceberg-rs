/*!
Iceberg table.
 */

use crate::model::table::TableMetadata;
use object_store::ObjectStore;
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
    table_type: TableType,
    metadata: TableMetadata,
    metadata_location: String,
    // manifests: Vec<ManifestFile>,
}
