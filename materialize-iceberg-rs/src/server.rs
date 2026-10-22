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

use crate::iceberg::catalog::CATALOG_PREFIX;
use crate::iceberg::catalog::Catalog;
use crate::iceberg::catalog::CommitTransactionRequest;

/// Aborts the embedded catalog server's task when dropped, so it's torn down
/// as soon as `run_transactions` (which owns this guard) exits.
pub struct ServerGuard(tokio::task::JoinHandle<()>);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Spawns the embedded REST catalog server, bound to `catalog`'s table data
/// for the lifetime of the returned guard.
pub fn spawn(catalog: Arc<Catalog>) -> ServerGuard {
    ServerGuard(tokio::spawn(async move {
        if let Err(err) = run_server(catalog).await {
            tracing::error!(error = %err, "connector-networking server exited");
        }
    }))
}

async fn run_server(catalog: Arc<Catalog>) -> anyhow::Result<()> {
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
    State(catalog): State<Arc<Catalog>>,
    Json(request): Json<CommitTransactionRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
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
