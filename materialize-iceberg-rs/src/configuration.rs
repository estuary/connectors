use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct EndpointConfig {}

#[derive(Serialize, Deserialize)]
pub struct Resource {
    pub table: String,
}

impl JsonSchema for Resource {
    fn schema_name() -> Cow<'static, str> {
        "ResourceConfig".into()
    }

    fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
        serde_json::from_value(serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Iceberg Resource Configuration",
            "type": "object",
            "required": [
                "table"
            ],
            "properties": {
                "table": {
                    "title": "Table",
                    "description": "Name of the Iceberg table to materialize to.",
                    "type": "string",
                    "x-collection-name": true
                }
            }
        }))
        .unwrap()
    }
}

pub fn schema_for<T: JsonSchema>() -> schemars::Schema {
    schemars::generate::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}
