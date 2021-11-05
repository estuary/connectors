use super::elastic_search_data_types::ESTypeOverride;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Input {
    pub schema_uri: String,
    pub schema_json: String,
    pub overrides: Vec<ESTypeOverride>,
}
