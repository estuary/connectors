use crate::{transactor::Transactor, Binding, EndpointConfig};

use anyhow::Context;
use axum::{
    extract::{Json, Path, State},
    routing, Router,
};
use http::status::StatusCode;
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
    Json(body): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
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

fn err_response(status: StatusCode, err: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    let json = serde_json::json!({ "error": err.to_string() });
    (status, Json(json))
}

impl CollectionHandler {
    fn prepare_document(
        &self,
        mut body: serde_json::Value,
        request_headers: axum::http::header::HeaderMap,
    ) -> anyhow::Result<serde_json::Value> {
        let id = if let Some(header_key) = self.id_header.as_ref() {
            // If the config specified a header to use as the id, then require it to be present.
            required_string_header(&request_headers, header_key.as_str())?.to_owned()
        } else {
            // we're supposed to generate a random uuid.
            uuid::Uuid::new_v4().to_string()
        };

        let id_pointer = doc::Pointer::from_str("/_meta/webhookId");
        if let Some(loc) = id_pointer.create_value(&mut body) {
            *loc = serde_json::Value::String(id);
        } else {
            anyhow::bail!("invalid document, cannot create _meta object");
        }

        for (maybe_key, value) in request_headers {
            // Are both the key and value ascii strings? If not, then skip this header.
            let Some(key) = maybe_key else {
                continue;
            };
            let Ok(value_str) = value.to_str() else {
                continue;
            };

            // Filter out any sensitive headers or useless headers. Note that we don't do
            // json pointer escaping on the header names. We _could_, but it hardly seems
            // worth the extra code complexity, given that these character rarely appear in
            // header keys, and the consequence if they do are that we'll either ignore the
            // header (due to `ptr.create_value` returning `None`), or else the document will
            // fail validation (due to a header value being an object or array).
            if !REDACT_HEADERS.contains(&key) {
                let ptr = doc::Pointer::from_str(&format!("/_meta/headers/{}", key.as_str()));
                // If we're unable to create the /_meta/headers object, then don't consider it an error.
                // These headers are nice to have, but are not necessarily requried.
                if let Some(loc) = ptr.create_value(&mut body) {
                    *loc = serde_json::Value::String(value_str.to_owned());
                }
            }
        }

        let ts_pointer = doc::Pointer::from_str("/_meta/receivedAt");
        if let Some(loc) = ts_pointer.create_value(&mut body) {
            let ts = time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap();
            *loc = serde_json::Value::String(ts);
        }

        Ok(body)
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
                mut resource_path,
                mut collection,
                resource_config,
            } = binding;
            let url_path = resource_path.pop().unwrap();
            tracing::info!(%url_path, collection = %collection.name, "binding http url path to collection");

            // In documentation and configuration, we always represent the path with the leading /.
            // But the axum `Path` extractor always strips out the leading /, so we strip it here
            // to make matching easier.
            let path = if let Some(without_slash) = url_path.strip_prefix('/') {
                without_slash.to_owned()
            } else {
                url_path
            };

            let schema_json = collection.write_schema.take().unwrap_or_else(|| {
                collection
                    .schema
                    .take()
                    .expect("collection must have schema if write_schema is missing")
            });
            let schema_value =
                serde_json::from_str::<serde_json::Value>(schema_json.get()).unwrap();
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
        body: serde_json::Value,
    ) -> anyhow::Result<(StatusCode, Json<serde_json::Value>)> {
        if let Some(expected_header) = self.require_auth_header.as_ref() {
            match required_string_header(&request_headers, "Authorization") {
                Ok(auth) if auth == expected_header => { /* request is authorized */ }
                Ok(_) => {
                    return Ok(err_response(
                        StatusCode::FORBIDDEN,
                        anyhow::anyhow!("invalid authorization token"),
                    ));
                }
                Err(err) => {
                    return Ok(err_response(StatusCode::UNAUTHORIZED, err));
                }
            }
        }

        let mut handlers_guard = self.handlers_by_path.lock().await;

        let Some(collection) = handlers_guard.get_mut(collection_path.as_str()) else {
            return Ok((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": "not found"}))));
        };

        let enhanced_doc = match collection.prepare_document(body, request_headers) {
            Ok(doc) => doc,
            Err(err) => return Ok(err_response(StatusCode::BAD_REQUEST, err)),
        };

        let validation_result = collection.validator.validate(None, &enhanced_doc)?;
        if let Err(validation_err) = validation_result.ok() {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(
                    serde_json::json!({"error": "request body failed validation", "validationError": validation_err }),
                ),
            ));
        }
        let txn = vec![(collection.binding_index, enhanced_doc)];
        std::mem::drop(handlers_guard);

        self.io.publish(txn).await?;
        Ok((StatusCode::OK, Json(serde_json::json!({"published": 1 }))))
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

        let raw_schema = binding.collection.schema.as_ref().unwrap();
        let openapi_schema = serde_json::from_str::<openapi::Schema>(raw_schema.get())
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
                "schema": {
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
                "schema": {
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
