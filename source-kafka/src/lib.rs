use anyhow::{Context, Result};
use configuration::{schema_for, EndpointConfig, Resource};
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
use tokio::io::AsyncWriteExt;
use tokio::io::{self, AsyncBufReadExt};

pub mod configuration;
pub mod discover;
pub mod msk_oauthbearer;
pub mod pull;
pub mod schema_registry;

const KAFKA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub async fn run_connector(
    mut stdin: io::BufReader<io::Stdin>,
    mut stdout: io::Stdout,
) -> Result<(), anyhow::Error> {
    tracing::info!("running connector");

    let mut line = String::new();

    if stdin.read_line(&mut line).await? == 0 {
        return Ok(()); // Clean EOF.
    };
    let request: Request = serde_json::from_str(&line)?;

    if request.spec.is_some() {
        let res = Response {
            spec: Some(Spec {
                protocol: 3032023,
                config_schema_json: serde_json::to_string(&schema_for::<EndpointConfig>())?,
                resource_config_schema_json: serde_json::to_string(&schema_for::<Resource>())?,
                documentation_url: "https://go.estuary.dev/source-kafka".to_string(),
                oauth2: None,
                resource_path_pointers: vec!["/topic".to_string()],
            }),
            ..Default::default()
        };

        write_capture_response(res, &mut stdout).await?;
    } else if let Some(req) = request.discover {
        let res = Response {
            discovered: Some(Discovered {
                bindings: do_discover(req).await?,
            }),
            ..Default::default()
        };

        write_capture_response(res, &mut stdout).await?;
    } else if let Some(req) = request.validate {
        let res = Response {
            validated: Some(Validated {
                bindings: do_validate(req).await?,
            }),
            ..Default::default()
        };

        write_capture_response(res, &mut stdout).await?;
    } else if request.apply.is_some() {
        let res = Response {
            applied: Some(Applied {
                action_description: String::new(),
            }),
            ..Default::default()
        };

        write_capture_response(res, &mut stdout).await?;
    } else if let Some(req) = request.open {
        write_capture_response(
            Response {
                opened: Some(Opened {
                    explicit_acknowledgements: false,
                }),
                ..Default::default()
            },
            &mut stdout,
        )
        .await?;

        let eof = tokio::spawn(async move {
            match stdin.read_line(&mut line).await? {
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
    } else {
        anyhow::bail!("invalid request, expected spec|discover|validate|apply|open");
    }

    Ok(())
}

pub async fn write_capture_response(
    response: Response,
    stdout: &mut io::Stdout,
) -> anyhow::Result<()> {
    let resp = serde_json::to_vec(&response).context("serializing response")?;
    stdout.write_all(&resp).await.context("writing response")?;
    stdout
        .write_u8(b'\n')
        .await
        .context("writing response newline")?;
    if response.captured.is_none() {
        stdout.flush().await?;
    }
    Ok(())
}

async fn do_validate(req: Validate) -> Result<Vec<ValidatedBinding>> {
    let config: EndpointConfig = serde_json::from_str(&req.config_json)?;
    let consumer = config.to_consumer().await?;

    consumer.fetch_metadata(None, KAFKA_TIMEOUT)?;

    let mut out = vec![];

    for b in &req.bindings {
        let res: Resource = serde_json::from_str(&b.resource_config_json)?;
        let validated = ValidatedBinding {
            resource_path: vec![res.topic],
        };
        out.push(validated);
    }

    Ok(out)
}
