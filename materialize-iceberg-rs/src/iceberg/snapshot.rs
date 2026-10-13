use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// An Iceberg snapshot. Since table metadata.json is not written yet, this
/// isn't persisted anywhere durable in Iceberg terms — it's logged, and its
/// identifying fields are folded into the connector's own checkpoint so the
/// next commit can extend it.
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub schema_id: i32,
    pub summary: BTreeMap<String, String>,
}

pub fn build_snapshot(
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    manifest_list: String,
    added_files_count: i32,
    added_rows_count: i64,
    added_files_size: i64,
) -> Snapshot {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let mut summary = BTreeMap::new();
    summary.insert("operation".to_string(), "append".to_string());
    summary.insert(
        "added-data-files".to_string(),
        added_files_count.to_string(),
    );
    summary.insert("added-records".to_string(), added_rows_count.to_string());
    summary.insert(
        "added-files-size".to_string(),
        added_files_size.to_string(),
    );

    Snapshot {
        snapshot_id,
        parent_snapshot_id,
        sequence_number,
        timestamp_ms,
        manifest_list,
        schema_id: 0,
        summary,
    }
}
