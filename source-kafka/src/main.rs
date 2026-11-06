// Links in the allocator crate, which sets the global allocator to jemalloc.
extern crate allocator;

use anyhow::Context;
use source_kafka::run_connector;
use tokio::io;

// The Flow runtime reads the connector's output from a pipe that holds 64 KiB
// by default. A bigger pipe lets the connector keep writing while the runtime
// pauses, and gives the runtime output to read while the connector pauses.
const STDOUT_PIPE_SIZE: libc::c_int = 1024 * 1024;

fn main() -> anyhow::Result<()> {
    let runtime = start_runtime()?;
    enlarge_stdout_pipe(STDOUT_PIPE_SIZE);

    let stdin = io::BufReader::new(io::stdin());
    // Wrap stdout in a BufWriter so captured documents are batched into a few
    // large writes. Rust's `Stdout` is itself a LineWriter that would otherwise
    // flush a syscall on every newline.
    let stdout = std::io::BufWriter::with_capacity(256 * 1024, std::io::stdout());

    let result = runtime.block_on(run_connector(stdin, stdout));

    if let Err(err) = result.as_ref() {
        tracing::error!(error = ?err, "operation failed");
    } else {
        tracing::debug!("connector run successful");
    }

    runtime.shutdown_background();

    result
}

/// Does nothing if stdout isn't a pipe, such as when the connector runs in a
/// terminal.
#[cfg(target_os = "linux")]
fn enlarge_stdout_pipe(size: libc::c_int) {
    use std::os::fd::AsRawFd;

    let fd = std::io::stdout().as_raw_fd();
    // SAFETY: these fcntl commands only read or change the capacity of the pipe
    // behind `fd`, and fail if `fd` isn't a pipe.
    let current = unsafe { libc::fcntl(fd, libc::F_GETPIPE_SZ) };
    if current < 0 || current >= size {
        return;
    }
    if unsafe { libc::fcntl(fd, libc::F_SETPIPE_SZ, size) } < 0 {
        tracing::warn!(
            error = %std::io::Error::last_os_error(),
            current,
            requested = size,
            "couldn't enlarge the stdout pipe",
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn enlarge_stdout_pipe(_size: libc::c_int) {}

fn start_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    // The level string "debug" results in enabling debug logging for all the crates
    // in the dependency tree, which produces a ridiculous amount of output.
    // So map the debug level to a filter that will still use info level for other crates.
    let level_str = match std::env::var("LOG_LEVEL").ok() {
        Some(lvl) if lvl.as_str() == "debug" => "source_kafka=debug,info".to_string(),
        Some(other) => other,
        None => "info".to_string(),
    };

    let log_level = tracing_subscriber::EnvFilter::builder().parse_lossy(level_str);
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(log_level)
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

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    Ok(runtime)
}
