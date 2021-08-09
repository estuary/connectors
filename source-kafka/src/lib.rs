extern crate serde_with;

use std::io::stdout;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

mod airbyte;
mod catalog;
mod configuration;

pub mod connector;

pub fn run(cmd: connector::Command) -> color_eyre::Result<()> {
    setup_tracing();
    color_eyre::install()?;

    match cmd {
        connector::Command::Spec => run_spec(),
        _other => todo!("Command not yet supported"),
    }

    Ok(())
}

fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

#[tracing::instrument(level = "debug")]
fn run_spec() {
    let message: airbyte::Spec<configuration::Configuration> = airbyte::Spec::new(true, vec![]);

    write_message(message);
}

fn write_message<M: airbyte::Message>(message: M) {
    serde_json::to_writer(&mut stdout(), &airbyte::Envelope::from(message))
        .expect("to serialize and write the message");

    // Include a newline to break up the document stream.
    println!();
}
