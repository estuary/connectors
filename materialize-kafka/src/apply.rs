use std::collections::HashSet;

use anyhow::{Context, Result};
use proto_flow::materialize::request::Apply;
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};

use crate::{
    configuration::{EndpointConfig, Resource},
    KAFKA_TIMEOUT,
};

pub async fn do_apply(req: Apply) -> Result<String> {
    let spec = req
        .materialization
        .expect("must have a materialization spec");

    let config: EndpointConfig = serde_json::from_str(&spec.config_json)?;
    let admin = config.to_admin()?;
    let admin_opts = AdminOptions::default().request_timeout(Some(KAFKA_TIMEOUT));

    let existing_topics = admin
        .inner()
        .fetch_metadata(None, None)
        .context("fetching metadata")?
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect::<HashSet<String>>();

    let configured_topics = spec
        .bindings
        .iter()
        .map(|binding| {
            let res: Resource = serde_json::from_str(&binding.resource_config_json)?;
            Ok(res.topic)
        })
        .collect::<Result<Vec<String>>>()?;

    let topics_to_create = configured_topics
        .iter()
        .filter_map(|topic| {
            if existing_topics.contains(topic) {
                None
            } else {
                Some(NewTopic::new(
                    topic,
                    config.topic_partitions,
                    TopicReplication::Fixed(config.topic_replication_factor),
                ))
            }
        })
        .collect::<Vec<NewTopic>>();

    if topics_to_create.is_empty() {
        return Ok("".to_string());
    }

    Ok(admin
        .create_topics(&topics_to_create, &admin_opts)
        .await
        .context("creating topics")?
        .into_iter()
        .map(|res| {
            res.map_err(|err| anyhow::anyhow!("failed to create topic: {} (code {})", err.0, err.1))
        })
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .map(|t| format!("create topic {}", t))
        .collect::<Vec<String>>()
        .join("\n"))
}
