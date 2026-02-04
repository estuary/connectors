use std::io::Write;

use anyhow::{Context, Result};
use configuration::{schema_for, EndpointConfig, Resource, SchemaRegistryConfig};
use discover::do_discover;
use proto_flow::capture::{
    request::Validate,
    response::{
        validated::Binding as ValidatedBinding, Applied, Discovered, Opened, Spec, Validated,
    },
    Request, Response,
};
use pull::do_pull;
use rdkafka::consumer::Consumer;
use schema_registry::SchemaRegistryClient;
use tokio::io::{self, AsyncBufReadExt};

pub mod configuration;
pub mod discover;
pub mod msk_oauthbearer;
pub mod protobuf;
pub mod pull;
pub mod schema_registry;

const KAFKA_METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub async fn run_connector(
    mut stdin: io::BufReader<io::Stdin>,
    mut stdout: std::io::Stdout,
) -> Result<(), anyhow::Error> {
    tracing::info!("running connector");

    let mut line = String::new();

    while stdin.read_line(&mut line).await? != 0 {
        let request: Request = serde_json::from_str(&line)?;
        line.clear();

        if request.spec.is_some() {
            let res = Response {
                spec: Some(Spec {
                    protocol: 3032023,
                    config_schema_json: serde_json::to_string(&schema_for::<EndpointConfig>())?.into(),
                    resource_config_schema_json: serde_json::to_string(&schema_for::<Resource>())?.into(),
                    documentation_url: "https://go.estuary.dev/source-kafka".to_string(),
                    oauth2: None,
                    resource_path_pointers: vec!["/topic".to_string()],
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if let Some(req) = request.discover {
            let res = Response {
                discovered: Some(Discovered {
                    bindings: do_discover(req).await?,
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if let Some(req) = request.validate {
            let res = Response {
                validated: Some(Validated {
                    bindings: do_validate(req).await?,
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if request.apply.is_some() {
            let res = Response {
                applied: Some(Applied {
                    action_description: String::new(),
                }),
                ..Default::default()
            };

            write_capture_response(res, &mut stdout)?;
        } else if let Some(req) = request.open {
            write_capture_response(
                Response {
                    opened: Some(Opened {
                        explicit_acknowledgements: false,
                    }),
                    ..Default::default()
                },
                &mut stdout,
            )?;

            let eof = tokio::spawn(async move {
                let mut line_string = String::new();
                match stdin.read_line(&mut line_string).await? {
                    0 => Ok(()),
                    n => anyhow::bail!(
                        "read {} bytes from stdin when explicit acknowledgements were not requested",
                        n
                    ),
                }
            });

            let pull = tokio::spawn(do_pull(req, stdout));

            tokio::select! {
                pull_res = pull => pull_res??,
                eof_res = eof => eof_res??,
            }

            return Ok(());
        } else {
            anyhow::bail!("invalid request, expected spec|discover|validate|apply|open");
        }
    }

    Ok(())
}

pub fn write_capture_response(
    response: Response,
    stdout: &mut std::io::Stdout,
) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *stdout, &response).context("serializing response")?;
    writeln!(stdout).context("writing response newline")?;

    if response.captured.is_none() {
        stdout.flush().context("flushing stdout")?;
    }
    Ok(())
}

async fn do_validate(req: Validate) -> Result<Vec<ValidatedBinding>> {
    let config: EndpointConfig = serde_json::from_slice(&req.config_json)?;
    let consumer = config.to_consumer().await?;

    consumer
        .fetch_metadata(None, KAFKA_METADATA_TIMEOUT)
        .context("Could not connect to bootstrap server with the provided configuration. This may be due to an incorrect configuration for authentication or bootstrap servers. Double check your configuration and try again.")?;

    match config.schema_registry {
        SchemaRegistryConfig::ConfluentSchemaRegistry {
            endpoint,
            username,
            password,
        } => {
            let client = SchemaRegistryClient::new(endpoint, username, password);
            client
                .schemas_for_topics(&[])
                .await
                .context("Could not connect to the configured schema registry. Double check your configuration and try again.")?;
        }
        SchemaRegistryConfig::NoSchemaRegistry { .. } => (),
    };

    req.bindings
        .iter()
        .map(|binding| {
            let res: Resource = serde_json::from_slice(&binding.resource_config_json)?;
            Ok(ValidatedBinding {
                resource_path: vec![res.topic],
            })
        })
        .collect()
}
