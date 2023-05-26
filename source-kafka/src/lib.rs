extern crate serde_with;

use proto_flow::capture::{Response, response, request};
use proto_flow::flow::{ConnectorState, CaptureSpec, RangeSpec};
use serde_json::json;
use schemars::schema_for;
use crate::connector::ConnectorConfig;

use std::fmt::Debug;
use std::io::Write;

pub mod catalog;
pub mod configuration;
pub mod connector;
pub mod kafka;
pub mod state;

pub struct KafkaConnector;

const PROTOCOL_VERSION: u32 = 3032023;

impl connector::Connector for KafkaConnector {
    type Config = configuration::Configuration;
    type State = state::CheckpointSet;

    fn spec(output: &mut dyn Write) -> eyre::Result<()> {
        let message  = Response {
            spec: Some(response::Spec {
                protocol: PROTOCOL_VERSION,
                config_schema_json: serde_json::to_string(&schema_for!(configuration::Configuration))?,
                resource_config_schema_json: serde_json::to_string(&json!({
                    "type": "object",
                    "properties": {
                        "stream": {
                            "type": "string",
                            "x-collection-name": true,
                        }
                    }
                }))?,
                documentation_url: "https://go.estuary.dev/source-kafka".to_string(),
                oauth2: None,
            }),
            ..Default::default()
        };

        connector::write_message(output, message)?;
        Ok(())
    }

    fn validate(output: &mut dyn Write, validate: request::Validate) -> eyre::Result<()> {
        let config = Self::Config::parse(&validate.config_json)?;
        let consumer = kafka::consumer_from_config(&config)?;
        let message = kafka::test_connection(&consumer, validate.bindings)?;

        connector::write_message(output, message)?;
        Ok(())
    }

    fn discover(output: &mut dyn Write, discover: request::Discover) -> eyre::Result<()> {
        let config = Self::Config::parse(&discover.config_json)?;
        let consumer = kafka::consumer_from_config(&config)?;
        let metadata = kafka::fetch_metadata(&consumer)?;
        let bindings = kafka::available_streams(&metadata);
        let message = Response {
            discovered: Some(response::Discovered {
                bindings
            }),
            ..Default::default()
        };

        connector::write_message(output, message)?;
        Ok(())
    }

    fn apply(output: &mut dyn Write, _config: Self::Config) -> eyre::Result<()> {
        connector::write_message(output, Response {
            applied: Some(response::Applied {
                action_description: "".to_string(),
            }),
            ..Default::default()
        })?;

        Ok(())
    }

    fn read(
        output: &mut dyn Write,
        config: Self::Config,
        capture: CaptureSpec,
        range: Option<RangeSpec>,
        persisted_state: Option<Self::State>,
    ) -> eyre::Result<()> {
        let consumer = kafka::consumer_from_config(&config)?;
        let metadata = kafka::fetch_metadata(&consumer)?;

        let mut checkpoints = state::CheckpointSet::reconcile_catalog_state(
            &metadata,
            &capture,
            range.as_ref(),
            &persisted_state.unwrap_or_default(),
        )?;
        kafka::subscribe(&consumer, &checkpoints)?;

        connector::write_message(output, Response {
            opened: Some(response::Opened {
                explicit_acknowledgements: false,
            }),
            ..Default::default()
        })?;

        loop {
            let msg = consumer
                .poll(None)
                .expect("Polling without a timeout should always produce a message")
                .map_err(kafka::Error::Read)?;

            let (record, checkpoint) = kafka::process_message(&msg, &capture.bindings)?;

            let delta_state = response::Checkpoint {
                state: Some(ConnectorState {
                    updated_json: serde_json::to_string(&json!({
                        checkpoint.topic.clone(): {
                            checkpoint.partition.to_string(): checkpoint.offset
                        }
                    }))?,
                    merge_patch: false
                })
            };
            checkpoints.add(checkpoint);

            connector::write_message(output, Response { captured: Some(record), ..Default::default() })?;
            connector::write_message(output, Response { checkpoint: Some(delta_state), ..Default::default() })?;
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed when interacting with kafka")]
    Kafka(#[from] kafka::Error),

    #[error("failed to process message")]
    Message(#[from] kafka::ProcessingError),

    #[error("failed to execute catalog")]
    Catalog(#[from] catalog::Error),

    #[error("failed to track state")]
    State(#[from] state::Error),
}
