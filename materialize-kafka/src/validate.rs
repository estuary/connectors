use anyhow::{Context, Result};
use proto_flow::{
    flow::{MaterializationSpec, Projection, SerPolicy},
    materialize::{
        request::Validate,
        response::validated::{constraint, Binding, Constraint},
    },
};
use rdkafka::producer::Producer;

use crate::{
    configuration::{EndpointConfig, Resource, SchemaRegistryConfig},
    KAFKA_TIMEOUT,
};

pub async fn do_validate(req: Validate) -> Result<Vec<Binding>> {
    let config: EndpointConfig = serde_json::from_str(&req.config_json)?;
    config.validate()?;
    let producer = config.to_producer()?;

    // Kafka connectivity check.
    producer.client()
        .fetch_metadata(None, KAFKA_TIMEOUT)
        .context("Could not connect to bootstrap server with the provided configuration. This may be due to an incorrect configuration for authentication or bootstrap servers. Double check your configuration and try again.")?;

    // Schema registry connectivity check.
    if let Some(SchemaRegistryConfig {
        endpoint,
        username,
        password,
    }) = config.schema_registry
    {
        reqwest::Client::new()
            .get(format!("{endpoint}/config"))
            .basic_auth(username, Some(password))
            .timeout(KAFKA_TIMEOUT)
            .send()
            .await?
            .error_for_status()
            .context("Could not connect to the configured schema registry. Double check your configuration and try again.")?;
    }

    req.bindings
        .iter()
        .map(|binding| {
            let res: Resource = serde_json::from_str(&binding.resource_config_json)?;

            Ok(Binding {
                constraints: binding
                    .collection
                    .as_ref()
                    .expect("binding must have collection spec")
                    .projections
                    .iter()
                    .map(|p| {
                        (
                            p.field.clone(),
                            constraint_for_projection(p, &res, req.last_materialization.as_ref()),
                        )
                    })
                    .collect(),
                resource_path: vec![res.topic],
                delta_updates: true,
                ser_policy: Some(SerPolicy {
                    str_truncate_after: 1 << 16,
                    nested_obj_truncate_after: 1000,
                    array_truncate_after: 1000,
                }),
            })
        })
        .collect::<Result<Vec<Binding>>>()
}

fn constraint_for_projection(
    p: &Projection,
    res: &Resource,
    last_spec: Option<&MaterializationSpec>,
) -> Constraint {
    let mut constraint = if p.is_primary_key {
        Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Primary key locations should usually be materialized".to_string(),
        }
    } else if p.ptr.is_empty() {
        Constraint {
            r#type: constraint::Type::FieldOptional.into(),
            reason: "The root document may be materialized".to_string(),
        }
    } else if p.field == "flow_published_at" || !p.ptr.strip_prefix("/").unwrap().contains("/") {
        Constraint {
            r#type: constraint::Type::LocationRecommended.into(),
            reason: "Top-level locations should usually be materialized".to_string(),
        }
    } else {
        Constraint {
            r#type: constraint::Type::FieldOptional.into(),
            reason: "This field may be materialized".to_string(),
        }
    };

    // Continue to recommend previously selected fields even if they would have
    // otherwise been optional.
    if let Some(last_spec) = last_spec {
        let last_binding = last_spec
            .bindings
            .iter()
            .find(|b| b.resource_path[0] == res.topic);

        if let Some(last_binding) = last_binding {
            let last_field_selection = last_binding
                .field_selection
                .as_ref()
                .expect("prior binding must have field selection");

            if p.ptr.is_empty() && !last_field_selection.document.is_empty() {
                constraint = Constraint {
                    r#type: constraint::Type::LocationRecommended.into(),
                    reason: "This location is the document of the current materialization"
                        .to_string(),
                }
            } else if last_field_selection.values.binary_search(&p.field).is_ok() {
                constraint = Constraint {
                    r#type: constraint::Type::LocationRecommended.into(),
                    reason: "This location is part of the current materialization".to_string(),
                }
            }
        };
    };

    constraint
}
