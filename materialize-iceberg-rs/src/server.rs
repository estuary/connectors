use anyhow::Context;
use axum::{Json, Router, routing::get};
use serde_json::{Value, json};

pub async fn run_server() -> anyhow::Result<()> {
    let router = Router::new().route("/v1/config", get(config));

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

fn listen_port() -> u16 {
    if let Ok(port_str) = std::env::var("MATERIALIZE_ICEBERG_RS_PORT") {
        port_str
            .parse()
            .expect("invalid MATERIALIZE_ICEBERG_RS_PORT value")
    } else {
        8080
    }
}
