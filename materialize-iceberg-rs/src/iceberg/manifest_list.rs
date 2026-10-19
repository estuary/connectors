use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use apache_avro::Reader;
use apache_avro::Writer;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use super::schema::manifest_list_schema;
use super::state::PendingManifest;

/// A row of the manifest-list (the Iceberg `manifest_file` struct). Renamed
/// to match `manifest_list_schema()`'s Avro record name: `apache_avro`'s
/// generic struct deserializer rejects a schema whose record name doesn't
/// match the Rust type name, and the Avro/Iceberg spec names this record
/// `manifest_file`, not `ManifestFile`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename = "manifest_file")]
pub struct ManifestFile {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub content: i32,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files_count: i32,
    pub existing_files_count: i32,
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
}

impl ManifestFile {
    fn from_pending(
        pending: &PendingManifest,
        snapshot_id: i64,
        sequence_number: i64,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: pending.path.clone(),
            manifest_length: pending.length,
            partition_spec_id: 0,
            content: pending.content,
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id: snapshot_id,
            added_files_count: pending.added_files_count,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: pending.added_rows_count,
            existing_rows_count: 0,
            deleted_rows_count: 0,
        }
    }
}

/// Downloads and decodes a previously-written manifest-list, returning its
/// `manifest_file` rows so they can be carried forward into a new one.
pub async fn read_manifest_list(
    object_store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<Vec<ManifestFile>> {
    let bytes = object_store
        .get(&ObjectPath::from(path.to_string()))
        .await
        .context("fetching previous manifest-list")?
        .bytes()
        .await
        .context("reading previous manifest-list body")?;

    let reader = Reader::new(&bytes[..]).context("opening manifest-list Avro reader")?;
    reader
        .into_deser_iter::<ManifestFile>()
        .collect::<Result<Vec<_>, _>>()
        .context("decoding manifest-list entries")
}

/// Builds and uploads a new manifest-list carrying forward `prior_entries`
/// plus one entry per manifest in `pending`, and returns its object store
/// path.
pub async fn write_manifest_list(
    object_store: &Arc<dyn ObjectStore>,
    prefix: &str,
    table: &str,
    snapshot_id: i64,
    sequence_number: i64,
    mut prior_entries: Vec<ManifestFile>,
    pending: &[PendingManifest],
) -> Result<String> {
    for manifest in pending {
        prior_entries.push(ManifestFile::from_pending(
            manifest,
            snapshot_id,
            sequence_number,
        ));
    }

    let schema = manifest_list_schema();
    let mut writer =
        Writer::new(schema, Vec::new()).context("creating Avro manifest-list writer")?;
    writer.add_user_metadata("format-version".to_string(), "2")?;
    writer.add_user_metadata("snapshot-id".to_string(), snapshot_id.to_string())?;
    for entry in &prior_entries {
        writer.append_ser(entry)?;
    }
    let bytes = writer.into_inner().context("encoding Avro manifest-list")?;

    let path = ObjectPath::from(format!(
        "{prefix}/{table}/metadata/snap-{snapshot_id}-{}.avro",
        Uuid::new_v4()
    ));

    object_store
        .put(&path, PutPayload::from(bytes))
        .await
        .context("uploading manifest-list")?;

    Ok(path.to_string())
}
