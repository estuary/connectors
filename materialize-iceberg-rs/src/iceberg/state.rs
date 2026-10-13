use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Connector checkpoint, persisted via the Flow runtime's `state_json` /
/// `ConnectorState` mechanism. It tracks, per binding, enough to extend the
/// current Iceberg snapshot across commits without a table metadata.json or
/// catalog: the current manifest-list location and the manifests written in
/// a commit that haven't been folded into a manifest-list yet.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ConnectorState {
    #[serde(default)]
    pub bindings: HashMap<String, BindingCheckpoint>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BindingCheckpoint {
    /// Current head snapshot, `None` before the first commit.
    pub snapshot_id: Option<i64>,
    #[serde(default)]
    pub sequence_number: i64,
    /// Object store path of the current manifest-list.
    pub manifest_list: Option<String>,
    /// Manifests written by StartCommit that Acknowledge hasn't yet folded
    /// into a new manifest-list.
    #[serde(default)]
    pub pending_manifests: Vec<PendingManifest>,
    /// The snapshot id assigned to the in-flight commit's manifest entries,
    /// chosen at StartCommit time so manifests can reference it before
    /// Acknowledge builds the manifest-list and snapshot.
    pub pending_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingManifest {
    pub path: String,
    pub length: i64,
    pub added_files_count: i32,
    pub added_rows_count: i64,
    pub added_files_size: i64,
}

impl ConnectorState {
    pub fn from_json(bytes: &[u8]) -> Result<ConnectorState> {
        if bytes.is_empty() {
            return Ok(ConnectorState::default());
        }
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn to_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }
}
