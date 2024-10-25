use anyhow::Result;
use proto_flow::capture::{request::Discover, response::discovered};
use rdkafka::consumer::Consumer;
use serde_json::json;

use crate::{
    configuration::{EndpointConfig, Resource},
    KAFKA_TIMEOUT,
};

static KAFKA_INTERNAL_TOPICS: [&str; 2] = ["__consumer_offsets", "__amazon_msk_canary"];

pub async fn do_discover(req: Discover) -> Result<Vec<discovered::Binding>> {
    let config: EndpointConfig = serde_json::from_str(&req.config_json)?;
    let consumer = config.to_consumer().await?;

    let meta = consumer.fetch_metadata(None, KAFKA_TIMEOUT)?;

    let mut all_topics: Vec<String> = meta
        .topics()
        .iter()
        .filter_map(|t| {
            let name = t.name();
            if KAFKA_INTERNAL_TOPICS.contains(&name) {
                None
            } else {
                Some(name.to_string())
            }
        })
        .collect();

    all_topics.sort();

    // TODO(whb): Consider information from schema registry for generating
    // schemas, and selecting topics to capture.

    Ok(all_topics
        .into_iter()
        .map(|s| discovered::Binding {
            recommended_name: s.clone(),
            resource_config_json: serde_json::to_string(&Resource { topic: s.clone() })
                .expect("resource config must serialize"),
            document_schema_json: serde_json::to_string(&json!({
                "x-infer-schema": true,
                "type": "object",
                "properties": {
                    "_meta": {
                        "type": "object",
                        "properties": {
                            "topic": {
                                "description": "The topic the message was read from",
                                "type": "string",
                            },
                            "partition": {
                                "description": "The partition the message was read from",
                                "type": "integer",
                            },
                            "offset": {
                                "description": "The offset of the message within the partition",
                                "type": "integer",
                            }
                        },
                        "required": ["partition", "offset"]
                    }
                },
                "required": ["_meta"]
            }))
            .expect("document schema must serialize"),
            key: vec!["/_meta/partition".to_string(), "/_meta/offset".to_string()],
            resource_path: vec![s.clone()],
            ..Default::default()
        })
        .collect())
}
