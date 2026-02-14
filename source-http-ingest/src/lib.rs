pub mod server;
pub mod signature;
pub mod transactor;

use std::collections::{HashMap, HashSet};

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
use schemars::{generate, JsonSchema, Schema};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt};

pub use signature::{SignatureAlgorithm, WebhookSignatureConfig, WebhookSignatureVerifier};

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

    /// List of URL paths to accept requests at.
    ///
    /// Discovery will return a separate collection for each given path. Paths
    /// must be provided without any percent encoding, and should not include
    /// any query parameters or fragment.
    ///
    /// Paths can include path parameters, following the syntax of OpenAPI path
    /// templating. For example, `/vendors/{vendorId}/products/{productId}`,
    /// which would accept a request to `/vendors/abc/products/123`. This would
    /// result in captured data with
    /// `"_meta": { "pathParams": { "vendorId": "abc", "productId": "123"}, ...}`
    #[serde(default)]
    #[schemars(default = "paths_schema_default", schema_with = "paths_schema")]
    paths: Vec<String>,

    /// List of allowed CORS origins. If empty, then CORS will be disabled. Otherwise, each item
    /// in the list will be interpreted as a specific request origin that will be permitted by the
    /// `Access-Control-Allow-Origin` header for preflight requests coming from that origin. As a special
    /// case, the value `*` is permitted in order to allow all origins. The `*` should be used with extreme
    /// caution, however. See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin
    #[serde(default)]
    #[schemars(default, schema_with = "cors_schema")]
    allowed_cors_origins: Vec<String>,

    /// Configuration for verifying webhook signatures.
    /// If set, incoming requests must include valid signature headers.
    #[serde(default)]
    #[schemars(
        default = "signature_config_schema_default",
        schema_with = "webhook_signature_schema"
    )]
    signature_config: WebhookSignatureConfig,
}

fn signature_config_schema_default() -> serde_json::Value {
    serde_json::json!({ "provider": "none" })
}

/// Sets the default value that's used only in the JSON schema. This is _not_ the default that's used
/// when deserializing the endpoint config, because we need to preserve backward compatibility with
/// existing tasks that don't have `paths` in their endpoint configs.
fn paths_schema_default() -> Vec<String> {
    vec!["/webhook-data".to_string()]
}

fn paths_schema(_gen: &mut generate::SchemaGenerator) -> Schema {
    serde_json::from_value(serde_json::json!({
        "title": "URL paths",
        "type": "array",
        "items": {
            "type": "string",
            "pattern": "/.+",
        },
        "order": 1
    }))
    .unwrap()
}

fn cors_schema(_gen: &mut generate::SchemaGenerator) -> Schema {
    // This schema is a little more permissive than would otherwise be ideal.
    // We'd like to use something like `oneOf: [{format: hostname}, {const: '*'}]`,
    // but the UI does not handle that construct well.
    serde_json::from_value(serde_json::json!({
        "title": "CORS Allowed Origins",
        "type": "array",
        "items": {
            "type": "string"
        },
        "order": 3
    }))
    .unwrap()
}

fn require_auth_token_schema(_gen: &mut generate::SchemaGenerator) -> Schema {
    serde_json::from_value(serde_json::json!({
        "title": "Authentication token",
        "type": ["string", "null"],
        "secret": true,
        "order": 2
    }))
    .unwrap()
}

fn webhook_signature_schema(_gen: &mut generate::SchemaGenerator) -> Schema {
    serde_json::from_value(serde_json::json!({
        "title": "Signature Verification",
        "description": "Configuration for verifying webhook signatures.",
        "type": "object",
        "order": 4,
        "default": { "provider": "none" },
        "discriminator": {
            "propertyName": "provider"
        },
        "oneOf": [
            {
                "title": "None",
                "properties": {
                    "provider": {
                        "type": "string",
                        "const": "none",
                        "default": "none",
                        "order": 0
                    }
                },
                "required": ["provider"]
            },
            {
                "title": "Twilio SendGrid",
                "properties": {
                    "provider": {
                        "type": "string",
                        "const": "twilio",
                        "default": "twilio",
                        "order": 0
                    },
                    "publicKey": {
                        "type": "string",
                        "title": "Public Key",
                        "description": "PEM-encoded ECDSA public key from Twilio SendGrid Event Webhook settings.",
                        "secret": true,
                        "multiline": true,
                        "order": 1
                    },
                    "maxSignatureAge": {
                        "type": "integer",
                        "title": "Max Signature Age",
                        "description": "Maximum age of a signed request in seconds before it is rejected. Defaults to 300 (5 minutes).",
                        "default": 300,
                        "minimum": 1,
                        "maximum": 259200,
                        "order": 2
                    }
                },
                "required": ["provider", "publicKey"]
            },
            {
                "title": "Custom",
                "properties": {
                    "provider": {
                        "type": "string",
                        "const": "custom",
                        "default": "custom",
                        "order": 0
                    },
                    "algorithm": {
                        "type": "string",
                        "title": "Algorithm",
                        "enum": ["ecdsa"],
                        "default": "ecdsa",
                        "order": 1
                    },
                    "publicKey": {
                        "type": "string",
                        "title": "Public Key",
                        "description": "PEM-encoded public key.",
                        "secret": true,
                        "multiline": true,
                        "order": 2
                    },
                    "signatureHeader": {
                        "type": "string",
                        "title": "Signature Header",
                        "description": "HTTP header containing the base64-encoded signature.",
                        "order": 3
                    },
                    "timestampHeader": {
                        "type": "string",
                        "title": "Timestamp Header",
                        "description": "Optional HTTP header containing the timestamp.",
                        "order": 4
                    },
                    "maxSignatureAge": {
                        "type": "integer",
                        "title": "Max Signature Age",
                        "description": "Maximum age of a signed request in seconds before it is rejected. Only applies when a Timestamp Header is configured. Defaults to 300 (5 minutes).",
                        "default": 300,
                        "minimum": 1,
                        "maximum": 259200,
                        "order": 5
                    }
                },
                "required": ["provider", "algorithm", "publicKey", "signatureHeader"]
            }
        ]
    }))
    .unwrap()
}

#[derive(Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConfig {
    #[serde(default)]
    pub stream: Option<String>,
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

impl ResourceConfig {
    /// The resource path is just an arrbitrary string that uniquely names the binding.
    /// The `stream` is allowed to be missing in order to retain compatibility with existing
    /// captures. Discovery will always output a non-empty `stream`, though.
    pub fn resource_path(&self) -> Vec<String> {
        let p = self
            .stream
            .clone()
            .unwrap_or_else(|| "webhook-data".to_string());
        vec![p]
    }
}

pub struct Binding {
    pub collection: CollectionSpec,
    pub resource_config: ResourceConfig,
}

impl Binding {
    pub fn url_path(&self) -> String {
        let path = self
            .resource_config
            .path
            .as_deref()
            .unwrap_or_else(|| self.collection.name.as_str());
        if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        }
    }
}

pub struct HttpIngestConnector {}

fn schema_for<T: JsonSchema>() -> Schema {
    schemars::generate::SchemaSettings::draft2019_09()
        .with_transform(remove_default_from_objects)
        .into_generator()
        .into_root_schema_for::<T>()
}

/// Removes "default": null from schemas that have "type": "object".
/// This is needed because schemars adds "default": null for Option<T> fields
/// with #[serde(default)], which causes UI issues when combined with "type": "object".
fn remove_default_from_objects(schema: &mut Schema) {
    if let Some(serde_json::Value::String(t)) = schema.get("type") {
        if t == "object" {
            if let Some(serde_json::Value::Null) = schema.get("default") {
                schema.remove("default");
            }
        }
    }
    schemars::transform::transform_subschemas(&mut remove_default_from_objects, schema);
}

pub async fn run_connector(
    mut stdin: io::BufReader<io::Stdin>,
    mut stdout: io::Stdout,
) -> Result<(), anyhow::Error> {
    while let Some(req) = read_capture_request(&mut stdin).await? {
        let Request {
            spec,
            discover,
            validate,
            apply,
            open,
            ..
        } = &req;

        if let Some(_) = spec {
            () = do_spec(&mut stdout).await?;
        } else if let Some(discover_req) = discover {
            () = do_discover(&discover_req.config_json, &mut stdout).await?;
        } else if let Some(validate_req) = validate {
            () = do_validate(
                &validate_req.config_json,
                &validate_req.bindings,
                &mut stdout,
            )
            .await?;
        } else if let Some(_) = apply {
            () = do_apply(&mut stdout).await?;
        } else if let Some(open_req) = open {
            return do_pull(open_req, stdin, stdout).await;
        } else {
            return Err(anyhow::anyhow!(
                "expected spec|discover|validate|apply|open, but received {req:?}"
            ));
        }
    }
    Ok(())
}

async fn do_pull(
    Open { capture, .. }: &Open,
    stdin: io::BufReader<io::Stdin>,
    mut stdout: io::Stdout,
) -> anyhow::Result<()> {
    let Some(CaptureSpec {
        config_json,
        bindings,
        ..
    }) = &capture
    else {
        anyhow::bail!("open request is missing capture spec");
    };

    let config = serde_json::from_slice::<EndpointConfig>(&config_json)
        .context("deserializing endpoint config")?;
    let mut typed_bindings = Vec::with_capacity(bindings.len());
    for ApplyBinding {
        collection,
        resource_config_json,
        ..
    } in bindings
    {
        let Some(collection) = collection else {
            anyhow::bail!("binding is missing collection spec");
        };
        let resource_config = serde_json::from_slice::<ResourceConfig>(&resource_config_json)
            .context("deserializing resource config")?;
        typed_bindings.push(Binding {
            collection: collection.clone(),
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

async fn do_apply(stdout: &mut io::Stdout) -> anyhow::Result<()> {
    // There's nothing to apply
    write_capture_response(
        Response {
            applied: Some(Applied {
                action_description: String::new(),
            }),
            ..Default::default()
        },
        stdout,
    )
    .await
}

async fn do_spec(stdout: &mut io::Stdout) -> anyhow::Result<()> {
    let config_schema_json = serde_json::to_vec(&schema_for::<EndpointConfig>())?;
    let resource_config_schema_json = serde_json::to_vec(&schema_for::<ResourceConfig>())?;
    let response = Response {
        spec: Some(Spec {
            protocol: 3032023,
            config_schema_json: config_schema_json.into(),
            resource_config_schema_json: resource_config_schema_json.into(),
            documentation_url: "https://go.estuary.dev/http-ingest".to_string(),
            oauth2: None,
            resource_path_pointers: vec!["/stream".to_string()],
        }),
        ..Default::default()
    };
    write_capture_response(response, stdout).await
}

async fn do_discover(config: &[u8], stdout: &mut io::Stdout) -> anyhow::Result<()> {
    let config =
        serde_json::from_slice::<EndpointConfig>(config).context("parsing endpoint config")?;
    let discovered = generate_discover_response(config)?;
    let response = Response {
        discovered: Some(discovered),
        ..Default::default()
    };
    write_capture_response(response, stdout).await
}

fn generate_discover_response(endpoint_config: EndpointConfig) -> anyhow::Result<Discovered> {
    let mut bindings = Vec::new();
    for path in endpoint_config.paths.iter() {
        bindings.push(discovered_webhook_collection(Some(&path)));
    }
    if bindings.is_empty() {
        bindings.push(discovered_webhook_collection(None));
    }
    Ok(Discovered { bindings })
}

async fn do_validate(
    config: &[u8],
    bindings: &[ValidateBinding],
    stdout: &mut io::Stdout,
) -> anyhow::Result<()> {
    let config = serde_json::from_slice::<EndpointConfig>(config)
        .context("deserializing endpoint config")?;
    // So that we can validate that they are all present in the bindings.
    let mut endpoint_paths = config
        .paths
        .iter()
        .map(|s| s.as_str())
        .collect::<HashSet<_>>();
    let mut output = Vec::with_capacity(bindings.len());
    let mut typed_bindings = Vec::with_capacity(bindings.len());
    let mut existing_paths = HashMap::new();
    for ValidateBinding {
        collection,
        resource_config_json,
        backfill: _,
    } in bindings
    {
        let Some(collection) = collection else {
            anyhow::bail!("missing collection in binding");
        };
        let resource_config = serde_json::from_slice::<ResourceConfig>(&resource_config_json)
            .context("deserializing resource config")?;

        if let Some(rp) = &resource_config.path {
            endpoint_paths.remove(rp.as_str());

            // Normalize all parameters to a constant name, so that we can check for conflicts.
            // This is meant to catch cases where a user enters `/a/{foo}` and `/a/{bar}`, which
            // would otherwise fail when we try to start the server.
            let path_ident = server::transform_path_params(rp, |_| String::from("{parameter}"));
            if let Some(prev) = existing_paths.insert(path_ident, rp.to_owned()) {
                anyhow::bail!(
                    "path parameter conflict: {} and {} both match requests for the same paths. Remove or change one of the paths",
                    prev,
                    rp,
                );
            }
        }
        output.push(ValidatedBinding {
            resource_path: resource_config.resource_path(),
        });
        typed_bindings.push(Binding {
            collection: collection.clone(),
            resource_config,
        });
    }
    if !endpoint_paths.is_empty() {
        let missing = endpoint_paths.into_iter().collect::<Vec<_>>();
        anyhow::bail!("endpoint config contains paths [{}], which are not represented in the list of bindings", missing.join(", "));
    }

    // Check to make sure we can successfully create an openapi spec
    server::openapi_spec(&config, &typed_bindings)
        .context("cannot create openapi spec from bindings")?;

    // Ensure that cors origins are valid
    let _ = server::parse_cors_allowed_origins(&config.allowed_cors_origins)
        .context("invalid allowedCorsOrigins value")?;

    // Ensure that signature verification keys are valid
    let _: Box<dyn WebhookSignatureVerifier> = config.signature_config.try_into()?;

    let response = Response {
        validated: Some(Validated { bindings: output }),
        ..Default::default()
    };
    write_capture_response(response, stdout).await
}

pub async fn read_capture_request(
    stdin: &mut io::BufReader<io::Stdin>,
) -> anyhow::Result<Option<Request>> {
    let mut buf = String::with_capacity(4096);
    stdin
        .read_line(&mut buf)
        .await
        .context("reading next request line")?;

    match buf.is_empty() {
        true => Ok(None),
        false => Ok(serde_json::from_str(&buf).context("deserializing request")?),
    }
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

fn discovered_webhook_collection(path: Option<&str>) -> DiscoveredBinding {
    let mut resource_config = ResourceConfig::default();
    resource_config.stream = path.map(|s| s.to_owned());
    resource_config.path = path.map(|s| s.to_owned());

    let path_params = path
        .map(|p| server::openapi_path_parameters(p).collect::<Vec<_>>())
        .unwrap_or_default();

    // reqPath is _not_ required so that we don't break existing tasks where
    // some of the data won't have it.
    let mut meta_required = vec![server::properties::ID, server::properties::TS];
    // But pathParams _can_ be required if they're configured, because no
    // pre-existing task configuration can include them.
    if !path_params.is_empty() {
        meta_required.push(server::properties::PATH_PARAMS);
    }
    let path_params_properties = path_params
        .iter()
        .map(|n| {
            (
                n.to_string(),
                serde_json::json!({
                    "type": "string",
                    "description": "The value of the path parameter",
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    DiscoveredBinding {
        disable: false,
        recommended_name: path.map(|p| p.trim_matches('/')).unwrap_or("webhook-data").to_string(),
        resource_config_json: serde_json::to_vec(&resource_config).unwrap().into(),
        document_schema_json: serde_json::to_vec(&serde_json::json!({
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
                        },
                        "reqPath": {
                            "type": "string",
                            "description": "The configured path at which the request was received. Will include parameter placeholders if the path has them"
                        },
                        "pathParams": {
                            "type": "object",
                            "description": "Parameters extracted from the path of the request, if configured",
                            "properties": path_params_properties,
                            "required": path_params,
                        }
                    },
                    "required": meta_required,
                }
            },
            "required": ["_meta"]
        }))
        .unwrap().into(),
        key: vec!["/_meta/webhookId".to_string()],
        is_fallback_key: true,
        resource_path: resource_config.resource_path(),
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

    #[test]
    fn test_discover_backwards_compatibility() {
        // Previous versions of the connector did not have `paths`, and the goal is to ensure
        // that tasks created with those configurations will still have the same url paths.
        // Look in the snapshot to ensure that the `path` of the discovered resource is empty.
        let config: EndpointConfig = serde_json::from_str("{}").unwrap();
        assert!(config.paths.is_empty());
        let result = generate_discover_response(config).unwrap();
        insta::assert_json_snapshot!(result);
    }

    #[test]
    fn test_discover_with_paths() {
        // Ensure that the resources have `path` and `stream` set based on the paths passed here.
        let config = EndpointConfig {
            require_auth_token: None,
            paths: vec!["/foo".to_string(), "/bar/baz".to_string()],
            allowed_cors_origins: Vec::new(),
            signature_config: WebhookSignatureConfig::default(),
        };
        let result = generate_discover_response(config).unwrap();
        insta::assert_json_snapshot!(result);
    }
}
