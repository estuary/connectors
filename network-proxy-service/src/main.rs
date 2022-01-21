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
    let proxy_config: NetworkProxyConfig = serde_json::from_reader(io::stdin()).or_bail("Failed to deserialize network proxy config.");
    let mut proxy = proxy_config.new_proxy();

    proxy.prepare().await.or_bail("Failed to prepare network proxy.");

    // Write "READY" to stdio to unblock Go logic.
    println!("READY");
    io::stdout().flush().or_bail("Failed flushing output.");

    proxy.start_serve().await.or_bail("Failed to start proxy service.");

    Ok(())
}