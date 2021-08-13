extern crate serde_with;

use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::Write;

mod airbyte;
mod catalog;
mod configuration;
mod halting;
mod kafka;
mod state;

pub mod connector;

pub struct KafkaConnector;

impl connector::Connector for KafkaConnector {
    type Config = configuration::Configuration;
    type ConfiguredCatalog = catalog::ConfiguredCatalog;
    type State = state::TopicSet;

    fn spec(output: &mut dyn Write) -> color_eyre::Result<()> {
        let message: airbyte::Spec<configuration::Configuration> = airbyte::Spec::new(true, vec![]);

        connector::write_message(output, message)?;
        Ok(())
    }

    fn check(output: &mut dyn Write, config: Self::Config) -> color_eyre::Result<()> {
        let consumer = kafka::consumer_from_config(&config)?;
        let message = kafka::test_connection(&consumer);

        connector::write_message(output, message)?;
        Ok(())
    }

    fn discover(output: &mut dyn Write, config: Self::Config) -> color_eyre::Result<()> {
        let consumer = kafka::consumer_from_config(&config)?;
        let metadata = kafka::fetch_metadata(&consumer)?;
        let streams = kafka::available_streams(&metadata);
        let message = airbyte::Catalog::new(streams);

        connector::write_message(output, message)?;
        Ok(())
    }

    fn read(
        output: &mut dyn Write,
        config: Self::Config,
        catalog: Self::ConfiguredCatalog,
        persisted_state: Option<Self::State>,
    ) -> color_eyre::Result<()> {
        let consumer = kafka::consumer_from_config(&config)?;
        let metadata = kafka::fetch_metadata(&consumer)?;

        let mut topic_states = state::TopicSet::reconcile_catalog_state(
            &metadata,
            &catalog,
            &persisted_state.unwrap_or_default(),
        )?;
        kafka::subscribe(&consumer, &topic_states)?;
        let watermarks = kafka::high_watermarks(&consumer, &topic_states)?;
        let halt_check = halting::HaltCheck::new(&catalog, watermarks);

        while !halt_check.should_halt(&topic_states) {
            let msg = consumer
                .poll(None)
                .expect("Polling without a timeout should always produce a message")
                .map_err(kafka::Error::Read)?;

            let (record, topic) = kafka::process_message(&msg)?;

            topic_states.checkpoint(&topic);

            connector::write_message(output, record)?;
            connector::write_message(output, airbyte::State::try_from(&topic_states)?)?;
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed when interacting with kafka")]
    Kafka(#[from] kafka::Error),

    #[error("failed to process message")]
    Message(#[from] kafka::ProcessingError),

    #[error("failed to execute catalog")]
    Catalog(#[from] catalog::Error),

    #[error("failed to track state")]
    State(#[from] state::Error),
}
