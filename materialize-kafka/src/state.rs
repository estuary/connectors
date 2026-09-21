use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Connector state persisted by the runtime across sessions.
#[derive(Serialize, Deserialize, Default, Debug, PartialEq)]
pub struct ConnectorState {
    /// Whether Avro schemas are registered with their logical types, so that
    /// a date-time projection is a timestamp rather than a string. Unset
    /// until the first Apply decides it, and fixed thereafter because
    /// changing it changes the wire type of fields in existing topics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avro_logical_types: Option<bool>,
}

impl ConnectorState {
    pub fn parse(state_json: &[u8]) -> Result<Self> {
        if state_json.is_empty() {
            return Ok(Self::default());
        }
        serde_json::from_slice(state_json).context("parsing connector state")
    }

    pub fn avro_logical_types(&self) -> bool {
        self.avro_logical_types.unwrap_or(false)
    }
}
