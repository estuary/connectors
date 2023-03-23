pub mod server;
pub mod transactor;

use anyhow::Context;

use proto_flow::{
    capture::{
        request::validate::Binding as ValidateBinding,
        request::Open,
        response::validated::Binding as ValidatedBinding,
        response::{discovered::Binding as DiscoveredBinding, Applied},
        response::{Discovered, Opened, Spec, Validated},
        Request, Response,
    },
    flow::{capture_spec::Binding as ApplyBinding, CaptureSpec, CollectionSpec},
};
use schemars::{
    gen,
    schema::{self, RootSchema},
    JsonSchema,
};
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
    #[schemars(schema_with = "require_auth_token_schema")]
    require_auth_token: Option<String>,
}

fn require_auth_token_schema(_gen: &mut gen::SchemaGenerator) -> schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "secret": true,
    }))
    .unwrap()
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
    let Request {
        spec,
        discover,
        validate,
        apply,
        open,
        ..
    } = req;
    if let Some(_) = spec {
        return do_spec(stdout).await;
    }
    if let Some(discover_req) = discover {
        return do_discover(discover_req.config_json, stdout).await;
    }
    if let Some(validate_req) = validate {
        return do_validate(validate_req.config_json, validate_req.bindings, stdout).await;
    }
    if let Some(_) = apply {
        return do_apply(stdout).await;
    }
    if let Some(open_req) = open {
        return do_pull(open_req, stdin, stdout).await;
    }
    Err(anyhow::anyhow!(
        "invalid request, expected spec|discover|validate|apply|open"
    ))
}

async fn do_pull(
    Open { capture, .. }: Open,
    stdin: io::BufReader<io::Stdin>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    let Some(CaptureSpec { config_json, bindings, .. }) = capture else {
        anyhow::bail!("open request is missing capture spec");
    };

    let config = serde_json::from_str::<EndpointConfig>(&config_json)
        .context("deserializing endpoint config")?;
    let mut typed_bindings = Vec::with_capacity(bindings.len());
    for ApplyBinding {
        collection,
        resource_config_json,
        resource_path,
    } in bindings
    {
        let Some(collection) = collection else {
            anyhow::bail!("binding is missing collection spec");
        };
        let resource_config = serde_json::from_str::<ResourceConfig>(&resource_config_json)
            .context("deserializing resource config")?;
        typed_bindings.push(Binding {
            collection,
            resource_path,
            resource_config,
        });
    }

    let resp = Response {
        opened: Some(Opened {
            explicit_acknowledgements: true,
        }),
        ..Default::default()
    };
    write_capture_response(resp, &mut stdout).await?;

    server::run_server(config, typed_bindings, stdin, stdout).await
}

async fn do_apply(mut stdout: io::Stdout) -> anyhow::Result<()> {
    // There's nothing to apply
    write_capture_response(
        Response {
            applied: Some(Applied {
                action_description: String::new(),
            }),
            ..Default::default()
        },
        &mut stdout,
    )
    .await
}

async fn do_spec(mut stdout: io::Stdout) -> anyhow::Result<()> {
    let config_schema_json = serde_json::to_string(&schema_for::<EndpointConfig>())?;
    let resource_config_schema_json = serde_json::to_string(&schema_for::<ResourceConfig>())?;
    let response = Response {
        spec: Some(Spec {
            protocol: 3032023,
            config_schema_json,
            resource_config_schema_json,
            documentation_url: "https://go.estuary.dev/http-ingest".to_string(),
            oauth2: None,
        }),
        ..Default::default()
    };
    write_capture_response(response, &mut stdout).await
}

async fn do_discover(config: String, mut stdout: io::Stdout) -> anyhow::Result<()> {
    // make sure we can parse the config, just as an extra sanity check
    let _ = serde_json::from_str::<EndpointConfig>(&config).context("parsing endpoint config")?;
    let bindings = vec![discovered_webhook_collection()];
    let response = Response {
        discovered: Some(Discovered { bindings }),
        ..Default::default()
    };
    write_capture_response(response, &mut stdout).await
}

async fn do_validate(
    config: String,
    bindings: Vec<ValidateBinding>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    let config =
        serde_json::from_str::<EndpointConfig>(&config).context("deserializing endpoint config")?;
    let mut output = Vec::with_capacity(bindings.len());
    let mut typed_bindings = Vec::with_capacity(bindings.len());
    for ValidateBinding {
        collection,
        resource_config_json,
    } in bindings
    {
        let Some(collection) = collection else {
            anyhow::bail!("missing collection in binding");
        };
        let resource_config = serde_json::from_str::<ResourceConfig>(&resource_config_json)
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
    let response = Response {
        validated: Some(Validated { bindings: output }),
        ..Default::default()
    };
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

fn discovered_webhook_collection() -> DiscoveredBinding {
    DiscoveredBinding {
        recommended_name: "webhook-data".to_string(),
        resource_config_json: serde_json::to_string(&ResourceConfig::default()).unwrap(),
        document_schema_json: serde_json::to_string(&serde_json::json!({
            "type": "object",
            "x-infer-schema": true,
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
