use std::collections::BTreeMap;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use uuid::Uuid;

use super::snapshot::Snapshot;

/// Iceberg table metadata (`metadata.json`), format-version 2. Partition
/// specs and sort orders are always the fixed empty default — partitioning
/// and schema evolution aren't supported yet.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    pub last_column_id: i32,
    pub current_schema_id: i32,
    pub schemas: Vec<Value>,
    pub default_spec_id: i32,
    pub partition_specs: Vec<Value>,
    pub last_partition_id: i32,
    pub default_sort_order_id: i32,
    pub sort_orders: Vec<Value>,
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    #[serde(default)]
    pub metadata_log: Vec<MetadataLogEntry>,
    #[serde(default)]
    pub refs: BTreeMap<String, SnapshotRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLogEntry {
    pub timestamp_ms: i64,
    pub snapshot_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLogEntry {
    pub timestamp_ms: i64,
    pub metadata_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub ref_type: String,
}

// Partition fields are conventionally numbered starting at 1000, so "no
// partition fields assigned yet" is recorded as 999.
const UNASSIGNED_LAST_PARTITION_ID: i32 = 999;

impl TableMetadata {
    pub fn new(location: String, schema: Value, last_column_id: i32) -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: Uuid::new_v4().to_string(),
            location,
            last_sequence_number: 0,
            last_updated_ms: now_ms(),
            last_column_id,
            current_schema_id: 0,
            schemas: vec![schema],
            default_spec_id: 0,
            partition_specs: vec![json!({"spec-id": 0, "fields": []})],
            last_partition_id: UNASSIGNED_LAST_PARTITION_ID,
            default_sort_order_id: 0,
            sort_orders: vec![json!({"order-id": 0, "fields": []})],
            properties: BTreeMap::new(),
            current_snapshot_id: None,
            snapshots: Vec::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            refs: BTreeMap::new(),
        }
    }

    pub fn add_snapshot(&mut self, snapshot: Snapshot) {
        self.last_sequence_number = snapshot.sequence_number;
        self.last_updated_ms = snapshot.timestamp_ms;
        let snapshot_id = snapshot.snapshot_id;
        let timestamp_ms = snapshot.timestamp_ms;

        self.snapshots.push(snapshot);
        self.current_snapshot_id = Some(snapshot_id);
        self.snapshot_log.push(SnapshotLogEntry {
            timestamp_ms,
            snapshot_id,
        });
        self.refs.insert(
            "main".to_string(),
            SnapshotRef {
                snapshot_id,
                ref_type: "branch".to_string(),
            },
        );
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
