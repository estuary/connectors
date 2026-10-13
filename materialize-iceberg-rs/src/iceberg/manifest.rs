use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use apache_avro::Writer;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::path::Path as ObjectPath;
use serde::Serialize;
use uuid::Uuid;

use super::schema::manifest_entry_schema;
use super::state::PendingManifest;

const STATUS_ADDED: i32 = 1;
const CONTENT_DATA: i32 = 0;

#[derive(Serialize)]
struct Partition {}

#[derive(Serialize)]
struct DataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: Partition,
    record_count: i64,
    file_size_in_bytes: i64,
    split_offsets: Option<Vec<i64>>,
}

#[derive(Serialize)]
struct ManifestEntry {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFile,
}

/// The subset of a written Parquet data file's stats needed for its
/// manifest-entry.
pub struct DataFileInfo {
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub split_offsets: Vec<i64>,
}

/// Writes an Avro `manifest` file for a single newly-added data file, and
/// uploads it to object storage. `table_schema_json` is the Iceberg schema
/// JSON recorded in the manifest's Avro file metadata, as required by the
/// spec.
pub async fn write_manifest(
    object_store: &Arc<dyn ObjectStore>,
    prefix: &str,
    table: &str,
    table_schema_json: &str,
    snapshot_id: i64,
    file: DataFileInfo,
) -> Result<PendingManifest> {
    let schema = manifest_entry_schema();

    let entry = ManifestEntry {
        status: STATUS_ADDED,
        snapshot_id: Some(snapshot_id),
        // Inherited from the manifest-list entry at read time; we don't yet
        // track a table-wide sequence number source independent of our own
        // checkpoint, so entries always inherit.
        sequence_number: None,
        file_sequence_number: None,
        data_file: DataFile {
            content: CONTENT_DATA,
            file_path: file.file_path.clone(),
            file_format: "PARQUET".to_string(),
            partition: Partition {},
            record_count: file.record_count,
            file_size_in_bytes: file.file_size_in_bytes,
            split_offsets: if file.split_offsets.is_empty() {
                None
            } else {
                Some(file.split_offsets)
            },
        },
    };

    let mut writer = Writer::new(schema, Vec::new()).context("creating Avro manifest writer")?;
    writer.add_user_metadata("schema".to_string(), table_schema_json)?;
    writer.add_user_metadata("partition-spec".to_string(), "[]")?;
    writer.add_user_metadata("partition-spec-id".to_string(), "0")?;
    writer.add_user_metadata("format-version".to_string(), "2")?;
    writer.add_user_metadata("content".to_string(), "data")?;
    writer.append_ser(entry)?;
    let bytes = writer.into_inner().context("encoding Avro manifest")?;

    let length = bytes.len() as i64;
    let path = ObjectPath::from(format!(
        "{prefix}/{table}/metadata/{}-m0.avro",
        Uuid::new_v4()
    ));

    object_store
        .put(&path, PutPayload::from(bytes))
        .await
        .context("uploading manifest file")?;

    Ok(PendingManifest {
        path: path.to_string(),
        length,
        added_files_count: 1,
        added_rows_count: file.record_count,
        added_files_size: file.file_size_in_bytes,
    })
}
