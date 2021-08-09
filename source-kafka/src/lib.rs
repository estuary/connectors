extern crate serde_with;

use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

mod airbyte;
mod catalog;
mod configuration;

pub mod connector;

pub fn run(cmd: connector::Command) -> color_eyre::Result<()> {
    setup_tracing();
    color_eyre::install()?;

    info!("You have selected {:?}", cmd);

    Ok(())
}

fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
