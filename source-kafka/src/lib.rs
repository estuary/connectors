extern crate serde_with;

use std::convert::TryFrom;
use std::fmt::Debug;
use std::fs::File;
use std::io::stdout;
use std::io::BufReader;
use std::path::Path;

use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

mod airbyte;
mod catalog;
mod configuration;
mod kafka;
mod state;

pub mod connector;

pub fn run(cmd: connector::Command) -> color_eyre::Result<()> {
    setup_tracing();
    color_eyre::install()?;

    match cmd {
        connector::Command::Spec => run_spec()?,
        connector::Command::Check { config } => run_check(&config)?,
        connector::Command::Discover { config } => run_discover(&config)?,
        connector::Command::Read {
            config,
            catalog,
            state,
        } => run_read(&config, &catalog, state.as_ref())?,
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

    #[error("failed to process message")]
    Message(#[from] kafka::ProcessingError),

    #[error("failed to track state")]
    State(#[from] state::Error),
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

#[tracing::instrument(level = "debug")]
fn run_read<P: AsRef<Path> + Debug>(
    config_path: P,
    catalog_path: P,
    state_path: Option<P>,
) -> Result<(), Error> {
    let configuration = read_config_file(config_path)?;
    let catalog = read_catalog_file(catalog_path)?;
    let persisted_state = read_state_file(state_path)?;

    let consumer = kafka::consumer_from_config(&configuration)?;
    let metadata = kafka::fetch_metadata(&consumer)?;

    let mut topic_states =
        state::TopicSet::reconcile_catalog_state(&metadata, &catalog, &persisted_state)?;
    kafka::subscribe(&consumer, &topic_states)?;

    for (msg, i) in consumer.iter().zip(0..) {
        let msg = msg.map_err(kafka::Error::Read)?;
        let (message, topic) = kafka::process_message(&msg)?;

        write_message(message);

        topic_states.checkpoint(&topic);
        write_message(airbyte::State::try_from(&topic_states)?);

        if i % 5 == 0 {
            info!(?topic, "Processed {} messages!", i + 1);
        }
    }

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

fn read_catalog_file<P: AsRef<Path>>(
    catalog_path: P,
) -> Result<catalog::ConfiguredCatalog, catalog::Error> {
    let file = File::open(catalog_path)?;
    let reader = BufReader::new(file);
    let catalog = serde_json::from_reader(reader)?;

    Ok(catalog)
}

fn read_state_file<P: AsRef<Path>>(state_path: Option<P>) -> Result<state::TopicSet, state::Error> {
    if let Some(state_path) = state_path {
        let file = File::open(state_path)?;
        let reader = BufReader::new(file);
        let topics = serde_json::from_reader(reader)?;

        Ok(topics)
    } else {
        Ok(state::TopicSet::default())
    }
}
