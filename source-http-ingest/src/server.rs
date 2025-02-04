use crate::{transactor::Transactor, Binding, EndpointConfig};

use anyhow::Context;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Json, Path, Query, State},
    routing, Router,
};
use doc::Annotation;
use http::{header::InvalidHeaderValue, status::StatusCode, HeaderValue};
use json::validator::Validator;
use models::RawValue;
use serde_json::Value;
use tower_http::{cors, decompression::RequestDecompressionLayer};
use utoipa::openapi::{self, schema, security, OpenApi, OpenApiBuilder};
use utoipa_swagger_ui::SwaggerUi;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::io;
use tokio::sync::Mutex;

pub fn parse_cors_allowed_origins(
    cors_allow_origins: &[String],
) -> anyhow::Result<Option<cors::AllowOrigin>> {
    if cors_allow_origins.is_empty() {
        Ok(None)
    } else if cors_allow_origins.iter().any(|o| o.trim() == "*") {
        anyhow::ensure!(
            cors_allow_origins.len() == 1,
            "cannot specify multiple allowed cors origins if using '*' to allow all"
        );
        Ok(Some(cors::AllowOrigin::any()))
    } else {
        let list = cors_allow_origins
            .iter()
            .map(|origin| HeaderValue::from_str(origin.trim()))
            .collect::<Result<Vec<HeaderValue>, InvalidHeaderValue>>()
            .context("invalid cors allowed origin value")?;
        Ok(Some(cors::AllowOrigin::list(list)))
    }
}

pub async fn run_server(
    endpoint_config: EndpointConfig,
    bindings: Vec<Binding>,
    stdin: io::BufReader<io::Stdin>,
    stdout: io::Stdout,
) -> anyhow::Result<()> {
    let openapi_spec =
        openapi_spec(&endpoint_config, &bindings).context("creating openapi spec")?;
    let cors_allow_origin = parse_cors_allowed_origins(&endpoint_config.allowed_cors_origins)
        .expect("allowedCorsOrigins must be valid");

    let mut router = Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-doc/openapi.json", openapi_spec))
        // The root path redirects to the swagger ui, so that a user who clicks a link to just
        // the hostname will be redirected to a more useful page.
        .route(
            "/",
            routing::get(|| async { axum::response::Redirect::permanent("/swagger-ui/") }),
        );

    for binding in bindings.iter() {
        // The paths in the endpoint and resource config may include parameters, which
        // are specified using the openapi path syntax (e.g. `/a/{aId}`). We translate
        // these into axum's path syntax (e.g. `/a/:aId`) so that we can use axum to
        // extract the path parameters for us.
        let openapi_path = binding.url_path();
        let axum_path = openapi_to_axum_path(&openapi_path);
        tracing::info!(path = %openapi_path, %axum_path, "configuring handler for route");
        router = router.route(&axum_path, routing::post(handle_webhook));
    }

    // Set the body limit to be the same as the max document size allowed by Flow (64MiB)
    router = router
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        // Handle decompression of request bodies. The order of these is important!
        // This layer applies to all routes defined _before_ it, so the max body size is
        // the size after decompression.
        .layer(RequestDecompressionLayer::new())
        .route_layer(
            tower_http::trace::TraceLayer::new_for_http()
                .make_span_with(|req: &http::Request<_>| {
                    tracing::debug_span!(
                        "http-request",
                        path = req.uri().path(),
                        status_code = tracing::field::Empty,
                        published = tracing::field::Empty,
                        handler_time_ms = tracing::field::Empty,
                    )
                })
                .on_response(
                    |response: &http::Response<_>,
                     latency: std::time::Duration,
                     span: &tracing::Span| {
                        span.record("status_code", &tracing::field::display(response.status()));
                        span.record("handler_time_ms", latency.as_millis());
                    },
                ),
        );

    if let Some(allowed_origins) = cors_allow_origin {
        let cors = tower_http::cors::CorsLayer::new()
            .allow_origin(allowed_origins)
            .allow_methods(tower_http::cors::AllowMethods::list([
                http::Method::POST,
                http::Method::PUT,
            ]))
            .allow_headers(tower_http::cors::AllowHeaders::mirror_request());
        router = router.layer(cors);
    }

    let handler = Handler::try_new(stdin, stdout, endpoint_config, bindings)?;
    let router = router.with_state(Arc::new(handler));

    let address = std::net::SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, listen_on_port()));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .context("listening on port")?;
    tracing::info!(%address, "listening for connections");
    axum::serve(listener, router.into_make_service())
        .await
        .context("running server")?;
    Ok(())
}

async fn handle_webhook(
    State(handler): State<Arc<Handler>>,
    request_headers: axum::http::header::HeaderMap,
    // This is the configured path from axum, which will be used lookup the handler.
    matched_path: axum::extract::MatchedPath,
    path_params: Option<Path<serde_json::Map<String, Value>>>,
    Query(query_params): Query<serde_json::Map<String, serde_json::Value>>,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    match handler
        .handle_webhook(
            matched_path.as_str(),
            path_params.map(|p| p.0).unwrap_or_default(),
            request_headers,
            query_params,
            body,
        )
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            // Format the error to include causes when rendering to the logs.
            let error_str = format!("{err:?}");
            tracing::error!(error = %error_str, "failed to handle request");
            let body = serde_json::json!({ "error": err.to_string() });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
        }
    }
}

struct CollectionHandler {
    schema_url: url::Url,
    schema_index: json::schema::index::Index<'static, doc::Annotation>,
    binding_index: u32,
    id_header: Option<String>,
    /// The path from the resource configuration for this binding, which will be
    /// added into each captured document.
    configured_path: String,
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
        pub static ref QUERY_PARAMS: Pointer = Pointer::from_str("/_meta/query");
    }
}

pub mod properties {
    pub const META: &str = "_meta";
    pub const ID: &str = "webhookId";
    pub const TS: &str = "receivedAt";
    pub const QUERY: &str = "query";
    pub const HEADERS: &str = "headers";
    pub const REQ_PATH: &str = "reqPath";
    pub const PATH_PARAMS: &str = "pathParams";
}

impl CollectionHandler {
    fn prepare_doc(
        &self,
        mut object: JsonObj,
        id: String,
        req_path: String,
        path_params: serde_json::Map<String, Value>,
        headers: serde_json::Map<String, Value>,
        query_params: serde_json::Map<String, Value>,
        ts: String,
    ) -> anyhow::Result<(u32, String)> {
        // If the object already contains a _meta property, parse it so that we can add to it.
        let mut meta: serde_json::Map<String, Value> = if object.contains_key(properties::META) {
            let val = object.remove(properties::META).unwrap();
            serde_json::from_str(val.get()).context("parsing existing _meta property")?
        } else {
            serde_json::Map::with_capacity(4)
        };

        meta.insert(properties::ID.to_string(), Value::String(id));
        meta.insert(properties::TS.to_string(), Value::String(ts));
        meta.insert(properties::HEADERS.to_string(), Value::Object(headers));
        meta.insert(properties::REQ_PATH.to_string(), Value::String(req_path));
        if !path_params.is_empty() {
            meta.insert(
                properties::PATH_PARAMS.to_string(),
                Value::Object(path_params),
            );
        }

        // It seems like most people probably won't use query parameters at all, so only include
        // them if non-empty. That way, users who don't use them won't have columns added for them
        // in their materializations.
        if !query_params.is_empty() {
            meta.insert(properties::QUERY.to_string(), Value::Object(query_params));
        }

        let meta_value = models::RawValue::from_value(&Value::Object(meta));
        object.insert(properties::META.to_string(), meta_value);

        let serialized = serde_json::to_string(&object).context("serializing prepared document")?;

        Ok((self.binding_index, serialized))
    }

    /// Turns a JSON request body into an array of one or more documents, along with
    /// the binding index of the collection.
    fn prepare_documents(
        &self,
        body: JsonBody,
        path_params: serde_json::Map<String, serde_json::Value>,
        request_headers: axum::http::header::HeaderMap,
        query_params: serde_json::Map<String, serde_json::Value>,
    ) -> anyhow::Result<Vec<(u32, String)>> {
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
            JsonBody::Object(obj) => {
                let id = header_id.unwrap_or_else(new_uuid);
                let prepped = self.prepare_doc(
                    obj,
                    id,
                    self.configured_path.clone(),
                    path_params,
                    header_json,
                    query_params,
                    ts,
                )?;
                Ok(vec![prepped])
            }
            JsonBody::Array(arr) => {
                let mut prepped = Vec::with_capacity(arr.len());
                for (i, inner) in arr.into_iter().enumerate() {
                    // suffix the header id, if provided, with the index of each
                    // document in the array so that each has a unique id.
                    let id = header_id
                        .as_ref()
                        .map(|header| format!("{header}/{i}"))
                        .unwrap_or_else(new_uuid);
                    prepped.push(self.prepare_doc(
                        inner,
                        id,
                        self.configured_path.clone(),
                        path_params.clone(),
                        header_json.clone(),
                        query_params.clone(),
                        ts.clone(),
                    )?);
                }
                Ok(prepped)
            }
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
            let configured_path = binding.url_path();

            tracing::info!(%configured_path, collection = %binding.collection.name, "binding http url path to collection");

            let schema_value = serde_json::from_str::<Value>(&binding.collection.write_schema_json)
                .context("parsing write_schema_json")?;
            let schema = json::schema::build::build_schema(
                url::Url::parse("http://not.areal.host/").unwrap(),
                &schema_value,
            )?;
            // We must get the resolved uri after building the schema, since the one we pass in is
            // only used for schemas that don't already have an absolute url as their `$id`.
            let schema_url = schema.curi.clone();

            // We intentionally leak the memory here in order to get a `&'static Schema`, because
            // the schema index only works with references. The workaround would be to add a
            // function to `doc::Validator` to allow it to validate a `&str` instead of requiring
            // a parsed document.
            let schema_box = Box::new(schema);
            let schema_ref: &'static json::schema::Schema<Annotation> = Box::leak(schema_box);
            let mut index_builder = json::schema::index::IndexBuilder::new();
            index_builder
                .add(schema_ref)
                .context("adding schema to index")?;
            let index = index_builder.into_index();

            let axum_path = openapi_to_axum_path(&configured_path);
            collections_by_path.insert(
                axum_path,
                CollectionHandler {
                    schema_url,
                    schema_index: index,
                    binding_index,
                    id_header: binding.resource_config.id_from_header,
                    configured_path,
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
        matched_path: &str,
        path_params: serde_json::Map<String, Value>,
        request_headers: axum::http::header::HeaderMap,
        query_params: serde_json::Map<String, serde_json::Value>,
        body: Bytes,
    ) -> anyhow::Result<(StatusCode, Json<Value>)> {
        if let Some(expected_header) = self.require_auth_header.as_ref() {
            match required_string_header(&request_headers, "Authorization") {
                Ok(auth) if auth == expected_header => { /* request is authorized */ }
                Ok(_) => {
                    return Ok(err_response(
                        StatusCode::FORBIDDEN,
                        anyhow::anyhow!("invalid authorization token"),
                        matched_path,
                    ));
                }
                Err(err) => {
                    return Ok(err_response(StatusCode::UNAUTHORIZED, err, matched_path));
                }
            }
        }

        let parse_result = if body.trim_ascii_start().starts_with(b"[") {
            serde_json::from_slice::<Vec<JsonObj>>(&body).map(JsonBody::Array)
        } else {
            serde_json::from_slice::<JsonObj>(&body).map(JsonBody::Object)
        };
        let json: JsonBody = match parse_result {
            Ok(j) => j,
            Err(err) => {
                let content_type = request_headers.get("content-type");
                let content_encoding = request_headers.get("content-encoding");

                // Limit the length of the body so we don't overwhelm the logs.
                // This is just so we can get an idea of whether the data came
                // across in some other format.
                let body_prefix = String::from_utf8_lossy(&body[..(body.len().min(256))]);
                tracing::warn!(%matched_path, ?content_type, ?content_encoding, %body_prefix, error = ?err, "failed parsing request body as json");
                return Ok(err_response(
                    StatusCode::BAD_REQUEST,
                    anyhow::anyhow!("invalid json: {err}"),
                    &matched_path,
                ));
            }
        };

        let start = std::time::Instant::now();
        let mut handlers_guard = self.handlers_by_path.lock().await;

        // Lookup the handler by the configured path, which may be different
        // from the actual request path if it contains path parameters.
        let Some(collection) = handlers_guard.get_mut(matched_path) else {
            tracing::info!(uri_path = %matched_path, "unknown uri path");
            return Ok((
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "not found"})),
            ));
        };

        tracing::debug!(elapsed_ms = %start.elapsed().as_millis(), "acquired lock on handler");
        let enhanced_docs =
            match collection.prepare_documents(json, path_params, request_headers, query_params) {
                Ok(docs) => docs,
                Err(err) => return Ok(err_response(StatusCode::BAD_REQUEST, err, &matched_path)),
            };

        // It's important that we validate all the documents before publishing
        // any of them. We don't have the ability to "roll back" a partial
        // publish apart from exiting with an error, which would potentially
        // impact other requests. So this ensures that each request is all-or-
        // nothing, which is probably simpler for users to reason about anyway.
        for (_, doc) in enhanced_docs.iter() {
            let CollectionHandler {
                schema_index,
                schema_url,
                ..
            } = &collection;
            let mut validator: Validator<doc::Annotation, json::validator::SpanContext> =
                Validator::new(schema_index);
            validator
                .prepare(schema_url)
                .context("preparing validator")?;

            let mut deserializer = serde_json::Deserializer::from_str(&doc);
            json::de::walk(&mut deserializer, &mut validator).context("validating document")?;

            if validator.invalid() {
                let basic_output = json::validator::build_basic_output(validator.outcomes());
                tracing::info!(%basic_output, uri_path = %matched_path, "request document failed validation");
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(
                        serde_json::json!({"error": "request body failed validation", "basicOutput": basic_output }),
                    ),
                ));
            }
        }
        std::mem::drop(handlers_guard);

        let n_docs = enhanced_docs.len();
        tracing::debug!(%n_docs, elapsed_ms = %start.elapsed().as_millis(), "documents are valid and ready to publish");
        self.io.publish(enhanced_docs).await?;
        tracing::Span::current().record("published", n_docs);
        Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "published": n_docs })),
        ))
    }
}

const JSON_CONTENT_TYPE: &str = "application/json";

fn openapi_path_param(path_component: &str) -> Result<&str, &str> {
    if path_component.starts_with('{') && path_component.ends_with('}') && path_component.len() > 2
    {
        Ok(&path_component[1..path_component.len() - 1])
    } else {
        Err(path_component)
    }
}

// Converts the openapi path syntax to the axum path syntax. This only affects
// paths that contain path parameters, since openapi uses curly braces for those,
// while axum uses a leading colon. Paths that don't include parameters will be
// returned
fn openapi_to_axum_path(path: &str) -> String {
    transform_path_params(path, |param| format!(":{}", param))
}

/// Transforms openapi path parameters using the given `transform`, which will be
/// passed each parameter name.
pub fn transform_path_params<F>(path: &str, transform: F) -> String
where
    F: Fn(&str) -> String,
{
    use itertools::Itertools;

    let trailing_slash = if path.ends_with('/') && path.len() > 1 {
        "/"
    } else {
        ""
    };
    let rel_path = path
        .trim_start_matches('/')
        .trim_end_matches('/')
        .split('/')
        .map(|part| match openapi_path_param(part) {
            Ok(param_name) => transform(param_name),
            Err(constant) => constant.to_owned(),
        })
        .format("/");
    format!("/{rel_path}{trailing_slash}")
}

/// Returns a list of path parameters in the given path specification. Path
/// parameters are specified in the OpenAPI path format, e.g.
/// `/vendors/{vendorId}/products/{productId}`, which would result in
/// `["vendorId", "productId"]`.
pub fn openapi_path_parameters(endpoint_config_path: &str) -> impl Iterator<Item = &str> {
    endpoint_config_path
        .split('/')
        .filter_map(|component| openapi_path_param(component).ok())
}

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
        let url_path = ensure_slash_prefix(&binding.url_path());

        let openapi_schema =
            serde_json::from_str::<openapi::Schema>(&binding.collection.write_schema_json)
                .context("The collection JSON schema (or writeSchema) could not be parsed as an \
                    OpenAPI schema. Please ensure that no fields have multiple types or conditional \
                    schemas, as those are unsupported by the connector at this time")?;

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

        for path_param in openapi_path_parameters(&url_path) {
            op_builder = op_builder.parameter(
                openapi::path::ParameterBuilder::new()
                    .name(path_param.to_owned())
                    .parameter_in(openapi::path::ParameterIn::Path)
                    .required(openapi::Required::True)
                    .description(Some("a url path parameter"))
                    .example(Some(serde_json::json!("abcd1234"))),
            );
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
            paths: Vec::new(),
            allowed_cors_origins: Vec::new(),
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
            resource_config: crate::ResourceConfig {
                stream: None,
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
            resource_config: crate::ResourceConfig {
                stream: Some("my-binding".to_string()),
                path: Some("/another.json".to_string()),
                id_from_header: None,
            },
        };

        let spec =
            openapi_spec(&endpoint_config, &[binding0, binding1]).expect("failed to generate spec");
        let json = spec.to_pretty_json().expect("failed to serialize json");
        insta::assert_snapshot!(json);
    }

    #[test]
    fn test_path_params() {
        for (path, expected) in &[
            (
                "/vendors/{vendorId}/products/{productId}",
                vec!["vendorId", "productId"],
            ),
            ("/vendors/{vendorId}/products", vec!["vendorId"]),
            ("/vendors/{vendorId}", vec!["vendorId"]),
            ("/vendors", vec![]),
            ("/", vec![]),
        ] {
            let actual: Vec<_> = super::openapi_path_parameters(path).collect();
            assert_eq!(actual, *expected);
        }
    }

    #[test]
    fn test_path_to_axum() {
        for (openapi_path, expected) in &[
            (
                "/vendors/{vendorId}/products/{productId}",
                "/vendors/:vendorId/products/:productId",
            ),
            (
                "/vendors/{vendorId}/products/",
                "/vendors/:vendorId/products/",
            ),
            ("/vendors/{vendorId}", "/vendors/:vendorId"),
            ("/vendors", "/vendors"),
            ("/", "/"),
        ] {
            let actual = super::openapi_to_axum_path(openapi_path);
            assert_eq!(actual, *expected);
        }
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

fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

type JsonObj = BTreeMap<String, RawValue>;

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum JsonBody {
    Object(JsonObj),
    Array(Vec<JsonObj>),
}
