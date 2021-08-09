extern crate serde_with;

use std::fmt::Debug;
use std::fs::File;
use std::io::{stdout, BufReader};
use std::path::Path;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

mod airbyte;
mod catalog;
mod configuration;
mod kafka;

pub mod connector;

pub fn run(cmd: connector::Command) -> color_eyre::Result<()> {
    setup_tracing();
    color_eyre::install()?;

    match cmd {
        connector::Command::Spec => run_spec()?,
        connector::Command::Check { config } => run_check(&config)?,
        connector::Command::Discover { config } => run_discover(&config)?,
        _other => todo!("Command not yet supported"),
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("failed to validate connector configuration")]
    Configuration(#[from] configuration::Error),

    #[error("failed to validate connector catalog")]
    Catalog(#[from] catalog::Error),

    #[error("failed when interacting with kafka")]
    Kafka(#[from] kafka::Error),
}

fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::ENTER | FmtSpan::EXIT)
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

#[tracing::instrument(level = "debug")]
fn run_spec() -> Result<(), Error> {
    let message: airbyte::Spec<configuration::Configuration> = airbyte::Spec::new(true, vec![]);

    write_message(message);
    Ok(())
}

#[tracing::instrument(level = "debug")]
fn run_check<P: AsRef<Path> + Debug>(config_path: P) -> Result<(), Error> {
    let configuration = read_config_file(config_path)?;
    let consumer = kafka::consumer_from_config(&configuration)?;
    let message = kafka::test_connection(&consumer);

    write_message(message);
    Ok(())
}

#[tracing::instrument(level = "debug")]
fn run_discover<P: AsRef<Path> + Debug>(config_path: P) -> Result<(), Error> {
    let configuration = read_config_file(config_path)?;
    let consumer = kafka::consumer_from_config(&configuration)?;
    let metadata = kafka::fetch_metadata(&consumer)?;
    let streams = kafka::available_streams(&metadata);
    let message = airbyte::Catalog::new(streams);

    write_message(message);
    Ok(())
}

fn write_message<M: airbyte::Message>(message: M) {
    serde_json::to_writer(&mut stdout(), &airbyte::Envelope::from(message))
        .expect("to serialize and write the message");

    // Include a newline to break up the document stream.
    println!();
}

fn read_config_file<P: AsRef<Path>>(
    config_path: P,
) -> Result<configuration::Configuration, configuration::Error> {
    let file = File::open(config_path)?;
    let reader = BufReader::new(file);
    let configuration = configuration::Configuration::parse(reader)?;

    Ok(configuration)
}
