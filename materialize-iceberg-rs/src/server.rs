use std::sync::Arc;

use anyhow::Context;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use serde_json::Value;
use serde_json::json;
use tokio::sync::RwLock;

use crate::iceberg::catalog::CATALOG_PREFIX;
use crate::iceberg::catalog::CatalogState;
use crate::iceberg::catalog::CommitTransactionRequest;

/// Holds the embedded REST catalog's object store + prefix once the
/// connector's `Open` request is processed. `None` until then, since the
/// HTTP server starts before the materialization spec (and thus the
/// endpoint config) is known.
pub type SharedCatalog = Arc<RwLock<Option<Arc<CatalogState>>>>;

pub fn new_shared_catalog() -> SharedCatalog {
    Arc::new(RwLock::new(None))
}

pub async fn set_catalog(shared: &SharedCatalog, state: CatalogState) {
    *shared.write().await = Some(Arc::new(state));
}

pub async fn run_server(catalog: SharedCatalog) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/v1/config", get(config))
        .route(
            &format!("/v1/{CATALOG_PREFIX}/transactions/commit"),
            post(commit_transaction),
        )
        .with_state(catalog);

    let port = listen_port();
    let address = std::net::SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, port));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .context("listening on port")?;

    // The externally-reachable URL for this port is assigned by the
    // data-plane gateway and isn't knowable from inside the connector.
    tracing::info!(port, "listening for connector-networking connections");

    axum::serve(listener, router)
        .await
        .context("serving connector-networking requests")
}

async fn config() -> Json<Value> {
    Json(json!({}))
}

async fn commit_transaction(
    State(catalog): State<SharedCatalog>,
    Json(request): Json<CommitTransactionRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let catalog = catalog.read().await.clone().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "catalog is not yet initialized".to_string(),
    ))?;

    for change in request.table_changes {
        catalog
            .commit(&change.identifier.name, &change.updates)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    }

    Ok(StatusCode::NO_CONTENT)
}

pub fn listen_port() -> u16 {
    if let Ok(port_str) = std::env::var("MATERIALIZE_ICEBERG_RS_PORT") {
        port_str
            .parse()
            .expect("invalid MATERIALIZE_ICEBERG_RS_PORT value")
    } else {
        8080
    }
}
