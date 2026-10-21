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

use super::schema::manifest_entry_schema;
use super::state::PendingManifest;

const STATUS_ADDED: i32 = 1;
pub const CONTENT_DATA: i32 = 0;
pub const CONTENT_DELETES: i32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(rename = "r102")]
struct Partition {}

/// One entry of `lower_bounds`/`upper_bounds`: Avro's map type only supports
/// string keys, so the Iceberg spec encodes `map<int, bytes>` as an array of
/// these `{key, value}` records instead. `lower_bounds` and `upper_bounds`
/// each have their own distinctly-named record type per the spec, so they
/// can't share one Rust type (apache_avro's struct deserializer matches by
/// exact Avro record name).
#[derive(Serialize, Deserialize)]
#[serde(rename = "k126_v127")]
struct LowerBoundEntry {
    key: i32,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "k129_v130")]
struct UpperBoundEntry {
    key: i32,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "r2")]
struct DataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: Partition,
    record_count: i64,
    file_size_in_bytes: i64,
    lower_bounds: Option<Vec<LowerBoundEntry>>,
    upper_bounds: Option<Vec<UpperBoundEntry>>,
    split_offsets: Option<Vec<i64>>,
    referenced_data_file: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename = "manifest_entry")]
struct ManifestEntry {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFile,
}

/// The subset of a written Parquet data file's stats needed for its
/// manifest-entry. `lower_bounds`/`upper_bounds` are `(field-id, encoded
/// value)` pairs — see `src/iceberg/single_value.rs`.
pub struct DataFileInfo {
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub split_offsets: Vec<i64>,
    pub lower_bounds: Vec<(i32, Vec<u8>)>,
    pub upper_bounds: Vec<(i32, Vec<u8>)>,
}

/// The subset of a written positional-delete Parquet file's stats needed
/// for its manifest-entry.
pub struct DeleteFileInfo {
    pub file_path: String,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    /// The single data file this delete file's positions apply to.
    pub referenced_data_file: String,
}

/// A decoded manifest entry, as returned by [`read_manifest`].
#[derive(Debug)]
pub struct ManifestEntryInfo {
    pub content: i32,
    pub file_path: String,
    pub record_count: i64,
    pub lower_bounds: Vec<(i32, Vec<u8>)>,
    pub upper_bounds: Vec<(i32, Vec<u8>)>,
    pub referenced_data_file: Option<String>,
}

/// Writes an Avro `manifest` file for a single newly-added data file, and
/// uploads it to object storage. `table_schema_json` is the Iceberg schema
/// JSON recorded in the manifest's Avro file metadata, as required by the
/// spec.
pub async fn write_manifest(
    object_store: &Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    table: &str,
    table_schema_json: &str,
    snapshot_id: i64,
    file: DataFileInfo,
) -> Result<PendingManifest> {
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
            lower_bounds: to_bound_entries(file.lower_bounds, |key, value| LowerBoundEntry {
                key,
                value,
            }),
            upper_bounds: to_bound_entries(file.upper_bounds, |key, value| UpperBoundEntry {
                key,
                value,
            }),
            split_offsets: if file.split_offsets.is_empty() {
                None
            } else {
                Some(file.split_offsets)
            },
            referenced_data_file: None,
        },
    };

    let (path, length) = upload_manifest(
        object_store,
        prefix,
        bucket,
        table,
        table_schema_json,
        "data",
        entry,
    )
    .await?;

    Ok(PendingManifest {
        path,
        length,
        content: CONTENT_DATA,
        added_files_count: 1,
        added_rows_count: file.record_count,
        added_files_size: file.file_size_in_bytes,
    })
}

/// Writes an Avro `manifest` file for a single newly-written positional-
/// delete file, and uploads it to object storage.
pub async fn write_delete_manifest(
    object_store: &Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    table: &str,
    table_schema_json: &str,
    snapshot_id: i64,
    file: DeleteFileInfo,
) -> Result<PendingManifest> {
    let entry = ManifestEntry {
        status: STATUS_ADDED,
        snapshot_id: Some(snapshot_id),
        sequence_number: None,
        file_sequence_number: None,
        data_file: DataFile {
            content: CONTENT_DELETES,
            file_path: file.file_path.clone(),
            file_format: "PARQUET".to_string(),
            partition: Partition {},
            record_count: file.record_count,
            file_size_in_bytes: file.file_size_in_bytes,
            lower_bounds: None,
            upper_bounds: None,
            split_offsets: None,
            referenced_data_file: Some(file.referenced_data_file),
        },
    };

    let (path, length) = upload_manifest(
        object_store,
        prefix,
        bucket,
        table,
        table_schema_json,
        "deletes",
        entry,
    )
    .await?;

    Ok(PendingManifest {
        path,
        length,
        content: CONTENT_DELETES,
        added_files_count: 1,
        added_rows_count: file.record_count,
        added_files_size: file.file_size_in_bytes,
    })
}

async fn upload_manifest(
    object_store: &Arc<dyn ObjectStore>,
    prefix: &str,
    bucket: &str,
    table: &str,
    table_schema_json: &str,
    content: &str,
    entry: ManifestEntry,
) -> Result<(String, i64)> {
    let schema = manifest_entry_schema();

    let mut writer = Writer::new(schema, Vec::new()).context("creating Avro manifest writer")?;
    writer.add_user_metadata("schema".to_string(), table_schema_json)?;
    writer.add_user_metadata("partition-spec".to_string(), "[]")?;
    writer.add_user_metadata("partition-spec-id".to_string(), "0")?;
    writer.add_user_metadata("format-version".to_string(), "2")?;
    writer.add_user_metadata("content".to_string(), content)?;
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

    Ok((format!("s3://{bucket}/{path}"), length))
}

/// Downloads and decodes a manifest file, returning its entries.
pub async fn read_manifest(
    object_store: &Arc<dyn ObjectStore>,
    path: &str,
) -> Result<Vec<ManifestEntryInfo>> {
    let bytes = object_store
        .get(&ObjectPath::from(path.to_string()))
        .await
        .context("fetching manifest")?
        .bytes()
        .await
        .context("reading manifest body")?;

    let reader = Reader::new(&bytes[..]).context("opening manifest Avro reader")?;
    let entries: Vec<ManifestEntry> = reader
        .into_deser_iter::<ManifestEntry>()
        .collect::<Result<Vec<_>, _>>()
        .context("decoding manifest entries")?;

    Ok(entries
        .into_iter()
        .map(|entry| ManifestEntryInfo {
            content: entry.data_file.content,
            file_path: entry.data_file.file_path,
            record_count: entry.data_file.record_count,
            lower_bounds: from_bound_entries(entry.data_file.lower_bounds, |e: LowerBoundEntry| {
                (e.key, e.value)
            }),
            upper_bounds: from_bound_entries(entry.data_file.upper_bounds, |e: UpperBoundEntry| {
                (e.key, e.value)
            }),
            referenced_data_file: entry.data_file.referenced_data_file,
        })
        .collect())
}

fn to_bound_entries<T>(
    bounds: Vec<(i32, Vec<u8>)>,
    build: impl Fn(i32, Vec<u8>) -> T,
) -> Option<Vec<T>> {
    if bounds.is_empty() {
        None
    } else {
        Some(
            bounds
                .into_iter()
                .map(|(key, value)| build(key, value))
                .collect(),
        )
    }
}

fn from_bound_entries<T>(
    entries: Option<Vec<T>>,
    unwrap: impl Fn(T) -> (i32, Vec<u8>),
) -> Vec<(i32, Vec<u8>)> {
    entries
        .map(|entries| entries.into_iter().map(unwrap).collect())
        .unwrap_or_default()
}
