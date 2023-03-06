/// The connector_protocol module is just a copypasta of the `connector-protocol` crate in the flow repo.
/// There's a number of breaking changes planned for that, and this copy/paste just insulates the connector
/// from those anticipated changes a bit. The intent is to rip this out in favor of using a json-encoding of
/// the protobuf message types from `capture.proto` and `flow.proto`.
pub mod connector_protocol;
pub mod server;
pub mod transactor;

use anyhow::Context;

use connector_protocol::{
    capture::{
        ApplyBinding, DiscoveredBinding, Request, Response, ValidateBinding, ValidatedBinding,
    },
    CollectionSpec, RawValue,
};
use schemars::{schema::RootSchema, JsonSchema};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt};

#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct EndpointConfig {
    /// Optional bearer token to authenticate webhook requests.
    ///
    /// WARNING: If this is empty or unset, then anyone who knows the URL of the connector
    /// will be able to write data to your collections.
    #[serde(default)]
    require_auth_token: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConfig {
    /// The URL path to use for adding documents to this binding. Defaults to the name of the collection.
    #[serde(default)]
    pub path: Option<String>,
    /// Set the /_meta/webhookId from the given HTTP header in each request.
    ///
    /// If not set, then a random id will be generated automatically. If set, then each request will be required
    /// to have the header, and the header value will be used as the value of `/_meta/webhookId`.
    #[serde(default)]
    pub id_from_header: Option<String>,
}

pub struct Binding {
    pub collection: CollectionSpec,
    pub resource_path: Vec<String>,
    pub resource_config: ResourceConfig,
}

pub struct HttpIngestConnector {}

fn schema_for<T: JsonSchema>() -> RootSchema {
    schemars::gen::SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<T>()
}

pub async fn run_connector(
    mut stdin: io::BufReader<io::Stdin>,
    stdout: io::Stdout,
) -> Result<(), anyhow::Error> {
    let req = read_capture_request(&mut stdin)
        .await
        .context("reading request")?;
    let result = match req {
        Request::Spec {} => do_spec(stdout).await,
        Request::Discover { config } => do_discover(config, stdout).await,
        Request::Validate {
            config, bindings, ..
        } => do_validate(config, bindings, stdout).await,
        Request::Apply { .. } => do_apply(stdout).await,
        Request::Open {
            config, bindings, ..
        } => do_pull(config, bindings, stdin, stdout).await,
        Request::Acknowledge {} => Err(anyhow::anyhow!("unexpected 'Acknowledge' request")),
    };

    if let Err(err) = result.as_ref() {
        tracing::error!(error = %err, "operation failed");
    } else {
        tracing::debug!("connector run successful");
    }
    result
}

async fn do_pull(
    endpoint_config: RawValue,
    bindings: Vec<ApplyBinding>,
    stdin: io::BufReader<io::Stdin>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    let config = serde_json::from_str::<EndpointConfig>(endpoint_config.get())
        .context("deserializing endpoint config")?;
    let mut typed_bindings = Vec::with_capacity(bindings.len());
    for ApplyBinding {
        collection,
        resource_config,
        resource_path,
    } in bindings
    {
        let resource_config = serde_json::from_str::<ResourceConfig>(resource_config.get())
            .context("deserializing resource config")?;
        typed_bindings.push(Binding {
            collection,
            resource_path,
            resource_config,
        });
    }
    write_capture_response(
        Response::Opened {
            explicit_acknowledgements: true,
        },
        &mut stdout,
    )
    .await?;

    server::run_server(config, typed_bindings, stdin, stdout).await
}

async fn do_apply(mut stdout: io::Stdout) -> anyhow::Result<()> {
    // There's nothing to apply
    write_capture_response(
        Response::Applied {
            action_description: String::new(),
        },
        &mut stdout,
    )
    .await
}

async fn do_spec(mut stdout: io::Stdout) -> anyhow::Result<()> {
    let config_schema = to_raw_value(&schema_for::<EndpointConfig>())?;
    let resource_config_schema = to_raw_value(&schema_for::<ResourceConfig>())?;
    let response = Response::Spec {
        documentation_url: "https://go.estuary.dev/http-ingest".to_string(),
        config_schema,
        resource_config_schema,
        oauth2: None,
    };
    write_capture_response(response, &mut stdout).await
}

async fn do_discover(config: RawValue, mut stdout: io::Stdout) -> anyhow::Result<()> {
    // make sure we can parse the config, just as an extra sanity check
    let _ =
        serde_json::from_str::<EndpointConfig>(config.get()).context("parsing endpoint config")?;
    let bindings = vec![discovered_webhook_collection()];
    let response = Response::Discovered { bindings };
    write_capture_response(response, &mut stdout).await
}

async fn do_validate(
    config: RawValue,
    bindings: Vec<ValidateBinding>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    let config = serde_json::from_str::<EndpointConfig>(config.get())
        .context("deserializing endpoint config")?;
    let mut output = Vec::with_capacity(bindings.len());
    let mut typed_bindings = Vec::with_capacity(bindings.len());
    for ValidateBinding {
        collection,
        resource_config,
    } in bindings
    {
        let resource_config = serde_json::from_str::<ResourceConfig>(resource_config.get())
            .context("deserializing resource config")?;

        let resource_path = if let Some(path) = resource_config.path.as_ref() {
            vec![server::ensure_slash_prefix(path.as_str().trim())]
        } else {
            vec![collection.name.clone()]
        };

        output.push(ValidatedBinding {
            resource_path: resource_path.clone(),
        });
        typed_bindings.push(Binding {
            collection,
            resource_path,
            resource_config,
        });
    }

    // Check to make sure we can successfully create an openapi spec
    server::openapi_spec(&config, &typed_bindings)
        .context("cannot create openapi spec from bindings")?;
    let response = Response::Validated { bindings: output };
    write_capture_response(response, &mut stdout).await
}

pub async fn read_capture_request(stdin: &mut io::BufReader<io::Stdin>) -> anyhow::Result<Request> {
    let mut buf = String::with_capacity(4096);
    stdin
        .read_line(&mut buf)
        .await
        .context("reading next request line")?;
    if buf.trim().is_empty() {
        anyhow::bail!("unexpected EOF reading request from stdin");
    }
    let deser = serde_json::from_str(&buf).context("deserializing request")?;
    Ok(deser)
}

/// Writes the response to stdout, and waits to a flush to complete. The flush ensures that the complete
/// response will be written, even if the runtime is immediately shutdown after this call returns.
pub async fn write_capture_response(
    response: Response,
    stdout: &mut io::Stdout,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let resp = serde_json::to_vec(&response).context("serializing response")?;
    stdout.write_all(&resp).await.context("writing response")?;
    stdout
        .write_u8(b'\n')
        .await
        .context("writing response newline")?;
    stdout.flush().await?;
    Ok(())
}

pub fn to_raw_value(val: &impl serde::Serialize) -> anyhow::Result<RawValue> {
    let ser = serde_json::to_string(val)?;
    Ok(RawValue(serde_json::value::RawValue::from_string(ser)?))
}

fn discovered_webhook_collection() -> DiscoveredBinding {
    DiscoveredBinding {
        recommended_name: "webhook-data".to_string(),
        resource_config: to_raw_value(&ResourceConfig::default()).unwrap(),
        document_schema: to_raw_value(&serde_json::json!({
            "type": "object",
            "properties": {
                "_meta": {
                    "type": "object",
                    "description": "These fields are automatically added by the connector, and do not need to be specified in the request body",
                    "properties": {
                        "webhookId": {
                            "type": "string",
                            "description": "The id of the webhook request, which is automatically added by the connector"
                        },
                        "headers": {
                            "type": "object",
                            "description": "HTTP headers that were sent with the request will get added here. Headers that are known to be sensitive or not useful will not be included",
                            "additionalProperties": { "type": "string" }
                        },
                        "receivedAt": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Timestamp of when the request was received by the connector"
                        }
                    },
                    "required": ["webhookId", "receivedAt"]
                }
            },
            "required": ["_meta"]
        }))
        .unwrap(),
        key: vec!["/_meta/webhookId".to_string()],
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn endpoint_config_schema() {
        let schema = schema_for::<EndpointConfig>();
        insta::assert_json_snapshot!(schema);
    }

    #[test]
    fn resource_config_schema() {
        let schema = schema_for::<ResourceConfig>();
        insta::assert_json_snapshot!(schema);
    }
}
