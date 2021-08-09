use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::{Metadata, MetadataTopic};
use rdkafka::{ClientConfig, Message, Offset, Timestamp, TopicPartitionList};

use crate::airbyte;
use crate::configuration::Configuration;

const KAFKA_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error creating consumer from config")]
    Config(#[source] KafkaError),

    #[error("failed to fetch cluster metadata")]
    Metadata(#[source] KafkaError),

    #[error("failed to subscribe to topic")]
    Subscription(#[source] KafkaError),

    #[error("failed to read message")]
    Read(#[source] KafkaError),
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

pub fn available_streams(metadata: &Metadata) -> Vec<airbyte::Stream> {
    metadata
        .topics()
        .iter()
        .filter(reject_internal_topics)
        .map(MetadataTopic::name)
        .map(|s| airbyte::Stream::new(s.to_owned()))
        .collect()
}

/// Subscribes to the given topic partitions and begins reading from the specified offsets.
///
/// **Warning**: This will _unsubscribe_ the consumer from any previous topic partitions.
pub fn subscribe(
    consumer: &BaseConsumer,
    topic: &str,
    partition_offsets: &[(i32, Offset)],
) -> Result<(), Error> {
    let mut topic_partition_list = TopicPartitionList::new();
    for (p, o) in partition_offsets {
        topic_partition_list
            .add_partition_offset(topic, *p, *o)
            .map_err(Error::Subscription)?;
    }
    consumer
        .assign(&topic_partition_list)
        .map_err(Error::Subscription)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("failed when interacting with kafka")]
    Kafka(#[from] KafkaError),

    #[error("failed to parse message `{0}`")]
    Parsing(String, #[source] serde_json::Error),

    #[error("message contained no payload")]
    EmptyMessage,
}

pub fn process_message<'m>(
    msg: &'m BorrowedMessage<'m>,
) -> Result<airbyte::Record, ProcessingError> {
    let payload = parse_message(msg)?;
    let emitted_at = timestamp_to_datetime(msg.timestamp());
    let namespace = format!("Partition {}", msg.partition());

    let message = airbyte::Record::new(msg.topic().to_owned(), payload, emitted_at, namespace);
    Ok(message)
}

// TODO: replace with CLI parser?
fn parse_message<'m>(msg: &'m BorrowedMessage<'m>) -> Result<serde_json::Value, ProcessingError> {
    let bytes = msg.payload().ok_or(ProcessingError::EmptyMessage)?;

    serde_json::from_slice(bytes).map_err(|serde_error| {
        // TODO: Capturing the raw_payload as a string is handy for
        // debugging, but may not be what we want long term.
        ProcessingError::Parsing(String::from_utf8_lossy(bytes).to_string(), serde_error)
    })
}

static KAFKA_INTERNAL_TOPICS: [&str; 1] = ["__consumer_offsets"];

fn reject_internal_topics(topic: &&MetadataTopic) -> bool {
    !KAFKA_INTERNAL_TOPICS.contains(&topic.name())
}

fn timestamp_to_datetime(timestamp: Timestamp) -> DateTime<Utc> {
    // Kafka Timestamps are rather complicated. They can be broker-centric,
    // producer-centric, or missing entirely based upon Topic configuration.
    // This should be uniform for all the Partitions in a Topic though. We
    // ultimately don't care much which variety they're using, as we're letting
    // Kafka enforce the ordering of the Messages.
    timestamp
        .to_millis()
        .map(|ms| DateTime::from_utc(NaiveDateTime::from_timestamp(ms / 1000, 0), Utc))
        .unwrap_or_else(Utc::now)
}
