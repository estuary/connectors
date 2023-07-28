use crate::{transactor::Transactor, Binding, EndpointConfig};

use anyhow::Context;
use axum::{
    body::Bytes,
    error_handling::HandleErrorLayer,
    extract::{DefaultBodyLimit, Json, Path, State},
    routing, BoxError, Router,
};
use http::status::StatusCode;
use serde_json::Value;
use tower::ServiceBuilder;
use tower_http::decompression::RequestDecompressionLayer;
use utoipa::openapi::{self, schema, security, OpenApi, OpenApiBuilder};
use utoipa_swagger_ui::SwaggerUi;

use std::{collections::HashMap, sync::Arc};
use tokio::io;
use tokio::sync::Mutex;

pub async fn run_server(
    endpoint_config: EndpointConfig,
    bindings: Vec<Binding>,
    stdin: io::BufReader<io::Stdin>,
    stdout: io::Stdout,
) -> anyhow::Result<()> {
    let openapi_spec =
        openapi_spec(&endpoint_config, &bindings).context("creating openapi spec")?;

    let handler = Handler::try_new(stdin, stdout, endpoint_config, bindings)?;

    let router = Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", openapi_spec))
        // The root path redirects to the swagger ui, so that a user who clicks a link to just
        // the hostname will be redirected to a more useful page.
        .route(
            "/",
            routing::get(|| async { axum::response::Redirect::permanent("/swagger-ui/") }),
        )
        // There's just one route that handles all bindings. The path will be provided as an argument.
        .route("/*path", routing::post(handle_webhook).put(handle_webhook))
        // Set the body limit to be the same as the max document size allowed by Flow (64MiB)
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(
            // Handle decompression of request bodies. The order of these is important!
            // This layer applies to all routes defined _before_ it, so the max body size is
            // the size after decompression.
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    tracing::info!(%error, "request body decompression error");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "error decompressing request body",
                    )
                }))
                .layer(RequestDecompressionLayer::new()),
        )
        .with_state(Arc::new(handler));

    let address = std::net::SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, listen_on_port()));
    tracing::info!(%address, "listening for connections");
    axum::Server::bind(&address)
        .serve(router.into_make_service())
        .await
        .map_err(anyhow::Error::from)
}

// If you turn on debug logging, you get a pretty decent request handling log from just this attribute :)
#[tracing::instrument(
    level = "debug",
    skip(handler, request_headers, body),
    fields(response_status)
)]
async fn handle_webhook(
    State(handler): State<Arc<Handler>>,
    request_headers: axum::http::header::HeaderMap,
    Path(path): Path<String>,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let resp = match handler.handle_webhook(path, request_headers, body).await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!(error = %err, "failed to handle request");
            let body = serde_json::json!({ "error": err.to_string() });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
        }
    };
    tracing::Span::current().record("response_status", resp.0.as_str());
    resp
}

struct CollectionHandler {
    binding_index: u32,
    validator: doc::Validator,
    id_header: Option<String>,
}

fn required_string_header<'a>(
    headers: &'a axum::http::HeaderMap,
    key: &str,
) -> anyhow::Result<&'a str> {
    headers
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("missing required header: {}", key))
        .and_then(|value| {
            value.to_str().map_err(|_| {
                anyhow::anyhow!(
                    "invalid '{}' header value contains non-ascii characters",
                    key
                )
            })
        })
}

/// Known locations where we populate metadata in the documents we capture.
mod pointers {
    use doc::Pointer;

    lazy_static::lazy_static! {
        pub static ref ID: Pointer = Pointer::from_str("/_meta/webhookId");
        pub static ref TS: Pointer = Pointer::from_str("/_meta/receivedAt");
        pub static ref HEADERS: Pointer = Pointer::from_str("/_meta/headers");
    }
}

impl CollectionHandler {
    fn prepare_doc(
        &self,
        mut object: Value,
        id: String,
        headers: serde_json::Map<String, Value>,
        ts: String,
    ) -> anyhow::Result<(u32, Value)> {
        if !object.is_object() {
            anyhow::bail!("expected a JSON object, got {}", type_name(&object));
        }

        if let Some(loc) = pointers::ID.create_value(&mut object) {
            *loc = Value::String(id);
        } else {
            anyhow::bail!("invalid document, cannot create _meta object");
        }

        if let Some(loc) = pointers::TS.create_value(&mut object) {
            *loc = Value::String(ts);
        }

        if let Some(loc) = pointers::HEADERS.create_value(&mut object) {
            *loc = Value::Object(headers);
        }
        Ok((self.binding_index, object))
    }

    /// Turns a JSON request body into an array of one or more documents, along with
    /// the binding index of the collection.
    fn prepare_documents(
        &self,
        body: Value,
        request_headers: axum::http::header::HeaderMap,
    ) -> anyhow::Result<Vec<(u32, Value)>> {
        let header_id = if let Some(header_key) = self.id_header.as_ref() {
            // If the config specified a header to use as the id, then require it to be present.
            Some(required_string_header(&request_headers, header_key.as_str())?.to_owned())
        } else {
            None
        };

        // Build the JSON object containing headers to add to the document(s)
        let header_json = request_headers
            .into_iter()
            .filter_map(|(maybe_key, value)| {
                maybe_key // Ignore empty header names
                    // Filter out any sensitive headers or useless headers.
                    .filter(|k| !REDACT_HEADERS.contains(k))
                    .and_then(|key| {
                        value // Ignores values that are not valid utf8
                            .to_str()
                            .ok()
                            .map(|value| (key.to_string(), Value::String(value.to_string())))
                    })
            })
            .collect::<serde_json::Map<_, _>>();

        let ts = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        match body {
            obj @ Value::Object(_) => {
                let id = header_id.unwrap_or_else(new_uuid);
                let prepped = self.prepare_doc(obj, id, header_json, ts)?;
                Ok(vec![prepped])
            }
            Value::Array(arr) => {
                let mut prepped = Vec::with_capacity(arr.len());
                for (i, inner) in arr.into_iter().enumerate() {
                    // suffix the header id, if provided, with the index of each
                    // document in the array so that each has a unique id.
                    let id = header_id
                        .as_ref()
                        .map(|header| format!("{header}/{i}"))
                        .unwrap_or_else(new_uuid);
                    prepped.push(self.prepare_doc(inner, id, header_json.clone(), ts.clone())?);
                }
                Ok(prepped)
            }
            other => Err(anyhow::anyhow!(
                "expected an object or array, got {}",
                type_name(&other)
            )),
        }
    }
}

struct Handler {
    /// If an Authorization header is required, this will be the full expected value of that header,
    /// including the "Bearer " prefix.
    require_auth_header: Option<String>,
    /// Map of http url path (without the leading slash) to the handler for that collection.
    handlers_by_path: Mutex<HashMap<String, CollectionHandler>>,
    io: Transactor,
}

impl Handler {
    fn try_new(
        stdin: io::BufReader<io::Stdin>,
        stdout: io::Stdout,
        endpoint_config: EndpointConfig,
        bindings: Vec<Binding>,
    ) -> anyhow::Result<Handler> {
        let mut collections_by_path = HashMap::with_capacity(bindings.len());

        for (i, binding) in bindings.into_iter().enumerate() {
            let binding_index = i as u32;

            let Binding {
                resource_path,
                collection,
                resource_config,
            } = binding;
            let url_path = resource_path
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing resource path for binding: {i}"))?;
            tracing::info!(%url_path, collection = %collection.name, "binding http url path to collection");

            // In documentation and configuration, we always represent the path with the leading /.
            // But the axum `Path` extractor always strips out the leading /, so we strip it here
            // to make matching easier.
            let path = if let Some(without_slash) = url_path.strip_prefix('/') {
                without_slash.to_owned()
            } else {
                url_path
            };

            let schema_value = serde_json::from_str::<Value>(&collection.write_schema_json)
                .context("parsing write_schema_json")?;
            let uri = url::Url::parse("http://not.areal.host/").unwrap();
            let schema = json::schema::build::build_schema(uri, &schema_value)?;
            let validator = doc::Validator::new(schema)?;

            collections_by_path.insert(
                path,
                CollectionHandler {
                    validator,
                    binding_index,
                    id_header: resource_config.id_from_header,
                },
            );
        }

        // Pre-format the expected auth header, to make checking requests easier.
        let require_auth_header = endpoint_config
            .require_auth_token
            .map(|token| format!("Bearer {token}"));
        if require_auth_header.is_none() {
            tracing::warn!("authentication is disabled, allowing writes from anyone");
        }

        let io = Transactor::start(stdin, stdout);

        Ok(Handler {
            handlers_by_path: Mutex::new(collections_by_path),
            io,
            require_auth_header,
        })
    }

    async fn handle_webhook(
        &self,
        collection_path: String,
        request_headers: axum::http::header::HeaderMap,
        body: Bytes,
    ) -> anyhow::Result<(StatusCode, Json<Value>)> {
        if let Some(expected_header) = self.require_auth_header.as_ref() {
            match required_string_header(&request_headers, "Authorization") {
                Ok(auth) if auth == expected_header => { /* request is authorized */ }
                Ok(_) => {
                    return Ok(err_response(
                        StatusCode::FORBIDDEN,
                        anyhow::anyhow!("invalid authorization token"),
                        &collection_path,
                    ));
                }
                Err(err) => {
                    return Ok(err_response(
                        StatusCode::UNAUTHORIZED,
                        err,
                        &collection_path,
                    ));
                }
            }
        }

        // Try to deserialize the body as json _before_ acquiring the handlers lock,
        // to prevent slow connection from impacting others.
        let json: Value = match serde_json::from_slice(&body) {
            Ok(j) => j,
            Err(err) => {
                let content_type = request_headers.get("content-type");
                let content_encoding = request_headers.get("content-encoding");

                // Limit the length of the body so we don't overwhelm the logs.
                // This is just so we can get an idea of whether the data came
                // across in some other format.
                let body_prefix = String::from_utf8_lossy(&body[..(body.len().min(256))]);
                tracing::warn!(%collection_path, ?content_type, ?content_encoding, %body_prefix, error = ?err, "failed parsing request body as json");
                return Ok(err_response(
                    StatusCode::BAD_REQUEST,
                    anyhow::anyhow!("invalid json: {err}"),
                    &collection_path,
                ));
            }
        };

        let start = std::time::Instant::now();
        let mut handlers_guard = self.handlers_by_path.lock().await;

        let Some(collection) = handlers_guard.get_mut(collection_path.as_str()) else {
            tracing::info!(uri_path = %collection_path, "unknown uri path");
            return Ok((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))));
        };

        tracing::debug!(elapsed_ms = %start.elapsed().as_millis(), "acquired lock on handler");
        let enhanced_docs = match collection.prepare_documents(json, request_headers) {
            Ok(docs) => docs,
            Err(err) => return Ok(err_response(StatusCode::BAD_REQUEST, err, &collection_path)),
        };

        // It's important that we validate all the documents before publishing
        // any of them. We don't have the ability to "roll back" a partial
        // publish apart from exiting with an error, which would potentially
        // impact other requests. So this ensures that each request is all-or-
        // nothing, which is probably simpler for users to reason about anyway.
        for (_, doc) in enhanced_docs.iter() {
            let validation_result = collection.validator.validate(None, doc)?;
            if let Err(validation_err) = validation_result.ok() {
                tracing::info!(error = %validation_err, uri_path = %collection_path, "request document failed validation");
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(
                        serde_json::json!({"error": "request body failed validation", "validationError": validation_err }),
                    ),
                ));
            }
        }
        std::mem::drop(handlers_guard);

        let n_docs = enhanced_docs.len();
        tracing::debug!(%n_docs, elapsed_ms = %start.elapsed().as_millis(), "documents are valid and ready to publish");
        self.io.publish(enhanced_docs).await?;
        tracing::debug!(%n_docs, elapsed_ms = %start.elapsed().as_millis(), "documents published successfully");
        Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "published": n_docs })),
        ))
    }
}

const JSON_CONTENT_TYPE: &str = "application/json";

pub fn openapi_spec<'a>(
    endpoint_config: &EndpointConfig,
    bindings: &[Binding],
) -> anyhow::Result<OpenApi> {
    let failure_schema = schema::ObjectBuilder::new()
        .property(
            "error",
            schema::Object::with_type(schema::SchemaType::String),
        )
        .build();
    let success_schema = schema::ObjectBuilder::new()
        .property(
            "published",
            schema::Object::with_type(schema::SchemaType::Integer),
        )
        .build();
    let mut components = schema::ComponentsBuilder::new()
        .response(
            "successResponse",
            openapi::ResponseBuilder::new().content(
                JSON_CONTENT_TYPE,
                openapi::content::ContentBuilder::new()
                    .example(Some(serde_json::json!({"published": 1})))
                    .schema(success_schema)
                    .build(),
            ),
        )
        .response(
            "failureResponse",
            openapi::ResponseBuilder::new().content(
                JSON_CONTENT_TYPE,
                openapi::content::ContentBuilder::new()
                    .example(Some(
                        serde_json::json!({"error": "missing required header 'X-Webhook-Id'"}),
                    ))
                    .schema(failure_schema)
                    .build(),
            ),
        );
    if endpoint_config.require_auth_token.is_some() {
        let sec = security::SecurityScheme::Http(
            security::HttpBuilder::new()
                .scheme(security::HttpAuthScheme::Bearer)
                .build(),
        );
        components = components.security_scheme("bearerAuth", sec);
    }

    let mut paths = openapi::PathsBuilder::new();

    for binding in bindings.iter() {
        let url_path = ensure_slash_prefix(binding.resource_path[0].as_str());

        let openapi_schema =
            serde_json::from_str::<openapi::Schema>(&binding.collection.write_schema_json)
                .context("deserializing collection schema")?;

        let mut content_builder = openapi::content::ContentBuilder::new().schema(openapi_schema);

        // As a special case, if the key of the target collection is `[/_meta/webhookId]`, then
        // we use an empty document as the example document instead of allowing the example to
        // be generated automatically from the JSON schema. The `/_meta/webhook`, and other values
        // under `/_meta/` are added automatically by the connector. So it's confusing to see them
        // in the example documents, and doubly confusing when the connector _overwrites_ the values
        // you sent in the payload. This hack makes for a much less confusing first-time experience
        // when using the default discovered collection. Checking the collection key is just a
        // convenient, though imperfect, means of checking whether this binding is for the discovered
        // "webhook-data" collection, which allows any valid JSON object by default.
        if let Some(key_ptr) = binding.collection.key.first() {
            if key_ptr.as_str() == "/_meta/webhookId" {
                content_builder =
                    content_builder.example(Some(serde_json::json!({"hello": "world!"})));
            }
        }
        let request_body = openapi::request_body::RequestBodyBuilder::new()
            .content(JSON_CONTENT_TYPE, content_builder.build())
            .description(Some(format!(
                "a JSON object conforming to the schema of the collection '{}'. \
                Note that '_meta' properties will be added automatically by the connector",
                binding.collection.name
            )))
            .required(Some(openapi::Required::True)) // lol IDK why
            .build();

        let responses = openapi::ResponsesBuilder::new()
            .response("200", openapi::Ref::from_response_name("successResponse"))
            .response("400", openapi::Ref::from_response_name("failureResponse"))
            .build();
        let mut op_builder = openapi::path::OperationBuilder::new()
            .request_body(Some(request_body))
            .responses(responses)
            .description(Some("append a document to the flow collection"));
        if endpoint_config.require_auth_token.is_some() {
            op_builder = op_builder.security(security::SecurityRequirement::new(
                "bearerAuth",
                Option::<String>::None,
            ));
        }
        if let Some(header_name) = binding.resource_config.id_from_header.as_ref() {
            op_builder = op_builder.parameter(
                openapi::path::ParameterBuilder::new()
                    .name(header_name.to_owned())
                    .parameter_in(openapi::path::ParameterIn::Header)
                    .required(openapi::Required::True)
                    .description(Some(
                        "Required header that will be bound to the /_meta/webhookId property",
                    ))
                    .example(Some(serde_json::json!("abcd1234"))),
            )
        }
        let operation = op_builder.build();
        let path_item = openapi::path::PathItemBuilder::new()
            .operation(openapi::PathItemType::Post, operation.clone())
            .operation(openapi::PathItemType::Put, operation.clone())
            .build();
        paths = paths.path(url_path, path_item);
    }

    let spec = OpenApiBuilder::new()
        .info(
            openapi::InfoBuilder::new()
                .title("Flow HTTP ingest")
                .version(env!("CARGO_PKG_VERSION"))
                .build(),
        )
        .components(Some(components.build()))
        .paths(paths.build())
        .build();
    Ok(spec)
}

pub fn ensure_slash_prefix(path: &str) -> String {
    if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    }
}

fn listen_on_port() -> u16 {
    if let Ok(port_str) = std::env::var("SOURCE_HTTP_INGEST_PORT") {
        port_str
            .parse()
            .expect("invalid SOURCE_HTTP_INGEST_PORT value")
    } else {
        8080
    }
}

lazy_static::lazy_static! {
    /// These are headers that should be excluded from the document that's output
    /// into the Flow collection.
    static ref REDACT_HEADERS: std::collections::HashSet<axum::http::HeaderName> = {
        let headers = [
            "authorization",
            "cookie",
            "content-type",
            "accept",
        ];
        headers.iter().map(|s| axum::http::HeaderName::from_static(*s)).collect()
    };
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn openapi_spec_generation() {
        let endpoint_config = EndpointConfig {
            require_auth_token: Some("testToken".to_string()),
        };
        let binding0 = Binding {
            collection: serde_json::from_value(serde_json::json!({
                "name": "aliceCo/test/webhook-data",
                "write_schema_json": {
                    "type": "object",
                    "properties": {
                        "_meta": {
                            "type": "object",
                            "properties": { "webhookId": {"type": "string"}},
                            "required": ["webhookId"]
                        }
                    },
                    "required": ["_meta"]
                },
                "key": ["/_meta/webhookId"],
                "partitionFields": [],
                "projections": []
            }))
            .unwrap(),
            resource_path: vec!["/aliceCo/test/webhook-data".to_string()],
            resource_config: crate::ResourceConfig {
                path: None,
                id_from_header: Some("X-Webhook-Id".to_string()),
            },
        };

        let binding1 = Binding {
            collection: serde_json::from_value(serde_json::json!({
                "name": "aliceCo/another/collection",
                "write_schema_json": {
                    "type": "object",
                    "properties": {
                        "foo": {
                            "type": "object",
                            "properties": { "bar": {"type": "string"}},
                            "required": ["bar"]
                        }
                    },
                    "required": ["foo"]
                },
                "key": ["/foo/bar"],
                "partitionFields": [],
                "projections": []
            }))
            .unwrap(),
            resource_path: vec!["/another.json".to_string()],
            resource_config: crate::ResourceConfig {
                path: Some("/another.json".to_string()),
                id_from_header: None,
            },
        };

        let spec =
            openapi_spec(&endpoint_config, &[binding0, binding1]).expect("failed to generate spec");
        let json = spec.to_pretty_json().expect("failed to serialize json");
        insta::assert_snapshot!(json);
    }
}

fn err_response(
    status: StatusCode,
    err: anyhow::Error,
    request_path: &str,
) -> (StatusCode, Json<Value>) {
    tracing::info!(%request_path, error = ?err, "responding with error");
    let json = serde_json::json!({ "error": err.to_string() });
    (status, Json(json))
}

fn type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}
