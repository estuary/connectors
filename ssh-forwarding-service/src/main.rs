pub mod errors;
pub mod interface;
pub mod ssh_agent_service;
pub mod ssh_forwarding_service;
pub mod util;

use std::io::{self, Write};
use std::process;

use signal_hook::{consts::SIGTERM, iterator::Signals};

use interface::{Input, Output};
use ssh_agent_service::SshAgentService;
use ssh_forwarding_service::SshForwardingService;

fn main() {
    init_tracing();
    let input: Input = serde_json::from_reader(io::stdin())
        .or_bail("Failed parsing json data streamed in from stdin.");

    let ssh_agent = SshAgentService::create().or_bail("SSH agent failed to create.");
    ssh_agent
        .add_ssh_key(&input.ssh_forwarding_config.ssh_private_key_base64)
        .or_bail("Failed to add base64-encoded private key to SSH agent.");

    let mut local_forwarding =
        SshForwardingService::create(&input.ssh_forwarding_config, input.local_port, &ssh_agent)
            .or_bail("Failed to create ssh forwarding service.");
    local_forwarding
        .poll_until_service_up(input.max_polling_retry_times)
        .or_bail("Local forwarding service is unavailable.");

    // Write output to stdio.
    serde_json::to_writer(
        io::stdout(),
        &Output {
            deployed_local_port: local_forwarding.deployed_local_port(),
        },
    )
    .or_bail("Failed write output.");
    io::stdout()
        .write_all(&[0])
        .expect("Failed to write to stdout");
    io::stdout().flush().expect("Failed flush output.");

    // Wait for sigterm to terminate the service.
    let mut signals = Signals::new(&[SIGTERM]).expect("Failed creating Signals.");
    for sig in signals.forever() {
        local_forwarding.stop();
        unsafe {
            libc::kill(ssh_agent.pid() as i32, sig);
        }
        std::process::exit(0);
    }
}

// TODO: Extract the common logic to a separate crate shared by connectors?
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        // TODO(jixiang): make this controlled by the input from GO side.
        .with_env_filter("info")
        .json()
        .flatten_event(true)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_current_span(true)
        .with_span_list(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_target(false)
        .init();
}

trait Must<T> {
    fn or_bail(self, message: &str) -> T;
}

impl<T, E> Must<T> for Result<T, E>
where
    E: std::fmt::Display + std::fmt::Debug,
{
    fn or_bail(self, message: &str) -> T {
        match self {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!(error_details = ?e, message);
                tracing::error!(error = %e, message);
                process::exit(1);
            }
        }
    }
}
