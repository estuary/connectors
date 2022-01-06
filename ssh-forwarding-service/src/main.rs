pub mod interface;
pub mod tunnel;
pub mod errors;
pub mod logging;

use std::io::{self, Write};

use interface::{Input, Output};
use tunnel::SshTunnel;
use logging::{init_tracing, Must};

#[tokio::main]
async fn main() -> io::Result<()> {
    init_tracing();
    let input: Input = serde_json::from_reader(io::stdin()).or_bail("valid input.");
    let mut ssh_tunnel =
        SshTunnel::create(input.ssh_forwarding_config.ssh_endpoint, input.local_port).await
        .or_bail("Failed to create to SSH tunnel.");

    ssh_tunnel.authenticate(
        &input.ssh_forwarding_config.ssh_user,
        &input.ssh_forwarding_config.ssh_private_key_base64).await
    .or_bail("SSH service failed to authenticate");

    let serve_future = ssh_tunnel.start_serve(
        &input.ssh_forwarding_config.remote_host, input.ssh_forwarding_config.remote_port);

    // Write output to stdio.
    serde_json::to_writer(
        io::stdout(),
        &Output {
            deployed_local_port: input.local_port,
        },
    ).unwrap();

    io::stdout()
        .write_all(&[0])
        .expect("Failed to write to stdout");
    io::stdout().flush().expect("Failed flush output.");

    serve_future.await.or_bail("ssh tunnel failed during serving.");

    Ok(())
}