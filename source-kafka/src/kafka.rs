use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::{Metadata, MetadataPartition, MetadataTopic};
use rdkafka::{ClientConfig, Message, Timestamp, TopicPartitionList};

use crate::configuration::Configuration;
use crate::{airbyte, catalog, state};

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
    let mut config = ClientConfig::new();

    config.set("bootstrap.servers", configuration.brokers());

    // We want to avoid writing ConsumerGroup commits back to Kafka. We manage
    // our own transactional semantics within Flow, so we don't need to rely on
    // Kafka to help with that.
    config.set("enable.auto.commit", "false");

    // Despite wanting to avoid using ConsumerGroups, we *must* set this
    // `group.id` in order to subscribe to topics. librdkafka will throw an
    // error if this is left blank.
    config.set("group.id", "source-kafka");

    config.set("security.protocol", configuration.security_protocol());

    if let Some(ref auth) = configuration.authentication {
        config.set("sasl.mechanism", auth.mechanism.to_string());
        config.set("sasl.username", &auth.username);
        config.set("sasl.password", &auth.password);
    }

    config.create().map_err(Error::Config)
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

pub fn find_topic<'m>(metadata: &'m Metadata, needle: &str) -> Option<&'m MetadataTopic> {
    metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == needle)
}

/// Subscribes to the given topic/partitions and begins reading from the specified offsets.
///
/// **Warning**: This will _unsubscribe_ the consumer from any previous topic partitions.
pub fn subscribe(
    consumer: &BaseConsumer,
    topics: &state::TopicSet,
) -> Result<TopicPartitionList, Error> {
    let mut topic_partition_list = TopicPartitionList::new();
    for topic in &topics.0 {
        topic_partition_list
            .add_partition_offset(&topic.name, topic.partition, topic.offset.next().into())
            .map_err(Error::Subscription)?;
    }

    consumer
        .assign(&topic_partition_list)
        .map_err(Error::Subscription)?;

    Ok(topic_partition_list)
}

pub fn high_watermarks(
    consumer: &BaseConsumer,
    topics: &state::TopicSet,
) -> Result<state::TopicSet, Error> {
    let mut watermarks = state::TopicSet::default();

    for topic in topics.0.iter() {
        // The low watermark represents the first message that can be read.
        // The high watermark is the "latest head" offset of the partition. This
        // is effectively the index of the next message to be read.
        let (low, high) = consumer
            .fetch_watermarks(&topic.name, topic.partition, KAFKA_TIMEOUT)
            .map_err(Error::Metadata)?;

        let offset = if high == 0 || high == low {
            // We can consider a partition to be at the beginning if:
            // - If the next message we receive will be Offset=0, then we haven't
            // read *any* messages yet.
            // - The low watermark can change over time as Kafka performs
            // compaction on the partition. If we've compacted away _all_ the
            // previous messages, the next message we can read will be the very
            // first message which currently exists in this partition.
            state::Offset::Start
        } else {
            // We subtract 1 from the high watermark since it is zero-indexed.
            // If we try to read up *through* `high`, we'll potentially sit here
            // forever waiting on the next message to come through.
            state::Offset::UpThrough(high - 1)
        };

        watermarks.add_new(state::Topic::new(&topic.name, topic.partition, offset));
    }

    Ok(watermarks)
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
) -> Result<(airbyte::Record, state::Topic), ProcessingError> {
    let payload = parse_message(msg)?;
    let emitted_at = timestamp_to_datetime(msg.timestamp());
    let namespace = format!("Partition {}", msg.partition());

    let message = airbyte::Record::new(msg.topic().to_owned(), payload, emitted_at, namespace);
    let state = state::Topic::new(msg.topic(), msg.partition(), state::Offset::from(msg));
    Ok((message, state))
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

pub fn build_shard_key(topic: &MetadataTopic, partition: &MetadataPartition) -> catalog::ShardKey {
    catalog::ShardKey::default()
        .add_str(topic.name())
        .add_int(partition.id())
}
