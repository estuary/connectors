use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use rdkafka::ClientConfig;

use crate::airbyte;
use crate::configuration::Configuration;

const KAFKA_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error creating consumer from config")]
    Config(#[source] KafkaError),

    #[error("failed to fetch cluster metadata")]
    Metadata(#[source] KafkaError),
}

pub fn consumer_from_config(configuration: &Configuration) -> Result<BaseConsumer, Error> {
    ClientConfig::new()
        .set("bootstrap.servers", configuration.brokers())
        .set("enable.auto.commit", "false")
        .set("group.id", "source-kafka")
        .create()
        .map_err(Error::Config)
}

pub fn test_connection<C: Consumer>(consumer: &C) -> airbyte::ConnectionStatus {
    match fetch_metadata(consumer) {
        Ok(metadata) => airbyte::ConnectionStatus::new(
            "SUCCEEDED".to_owned(),
            format!("{} topics found", metadata.topics().len()),
        ),
        Err(e) => {
            airbyte::ConnectionStatus::new("FAILED".to_owned(), format!("{:#} -- {:?}", &e, &e))
        }
    }
}

pub fn fetch_metadata<C: Consumer>(consumer: &C) -> Result<Metadata, Error> {
    consumer
        .fetch_metadata(None, Some(KAFKA_TIMEOUT))
        .map_err(Error::Metadata)
}
