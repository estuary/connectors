pub mod interface;
pub mod sshforwarding;
pub mod errors;
pub mod logging;
pub mod networkproxy;

use std::io::{self, Write};

use interface::NetworkProxyConfig;
use logging::{init_tracing, Must};

#[tokio::main]
async fn main() -> io::Result<()> {
    init_tracing();
    let proxy_config: NetworkProxyConfig = serde_json::from_reader(io::stdin()).or_bail("valid input.");
    let mut proxy = proxy_config.new_proxy();

    proxy.prepare().await.or_bail("Failed to prepare network proxy.");

    let service_future = tokio::task::spawn(async move {
        proxy.start_serve().await.or_bail("Failed to start proxy service.");
    });

    // Write "READY" to stdio to unblock Go logic.
    io::stdout()
        .write_all(b"READY\n")
        .expect("Failed to write to stdout");
    io::stdout().flush().or_bail("Failed flushing output.");

    service_future.await.or_bail("Network proxy serving stopped.");

    Ok(())
}