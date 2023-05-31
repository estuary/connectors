use std::time::Duration;

use proto_flow::capture::{Response, response, request};
use proto_flow::flow::capture_spec::Binding;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::{Metadata, MetadataPartition, MetadataTopic};
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use serde_json::json;

use crate::catalog::Resource;
use crate::configuration::Configuration;
use crate::{catalog, state};

const KAFKA_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error creating consumer from config")]
    Config(#[source] KafkaError),

    #[error("failed to fetch cluster metadata ({0})")]
    Metadata(String, #[source] KafkaError),

    #[error("failed to fetch watermarks")]
    Watermarks(#[source] KafkaError),

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

pub fn test_connection<C: Consumer>(configuration: &Configuration, consumer: &C, bindings: Vec<request::validate::Binding>) -> Result<Response, Error> {
    let metadata = fetch_metadata(configuration, consumer)?;
    Ok(Response {
        validated: Some(response::Validated {
            bindings: metadata.topics().iter().filter(|topic| {
                bindings.iter().any(|binding| {
                    let res: Resource = serde_json::from_str(&binding.resource_config_json).expect("parse resource config");
                    res.stream == topic.name()
                })
            }).map(|topic| {
                response::validated::Binding {
                    resource_path: vec![topic.name().to_string()],
                }
            }).collect()
        }),
        ..Default::default()
    })
}

pub fn fetch_metadata<C: Consumer>(configuration: &Configuration, consumer: &C) -> Result<Metadata, Error> {
    consumer
        .fetch_metadata(None, Some(KAFKA_TIMEOUT))
        .map_err(|err| Error::Metadata(configuration.brokers(), err))
}

pub fn available_streams(metadata: &Metadata) -> Vec<response::discovered::Binding> {
    metadata
        .topics()
        .iter()
        .filter(reject_internal_topics)
        .map(MetadataTopic::name)
        .map(|s| response::discovered::Binding {
            recommended_name: s.to_owned(),
            resource_config_json: serde_json::to_string(&json!({
                "stream": s.to_owned(),
            })).expect("resource config"),
            document_schema_json: serde_json::to_string(&json!({
                "x-infer-schema": true,
                "type": "object",
                "properties": {
                    "_meta": {
                        "type": "object",
                        "properties": {
                            "partition": {
                                "description": "The partition the message was read from",
                                "type": "integer",
                            },
                            "offset": {
                                "description": "The offset of the message within the partition",
                                "type": "integer",
                            }
                        },
                        "required": ["partition", "offset"]
                    }
                },
                "required": ["_meta"]
            })).expect("document schema"),
            key: vec!["/_meta/partition".to_string(), "/_meta/offset".to_string()],
        })
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
    checkpoints: &state::CheckpointSet,
) -> Result<TopicPartitionList, Error> {
    let mut topic_partition_list = TopicPartitionList::new();
    for checkpoint in checkpoints.iter() {
        topic_partition_list
            .add_partition_offset(
                &checkpoint.topic,
                checkpoint.partition,
                checkpoint.offset.next().into(),
            )
            .map_err(Error::Subscription)?;
    }

    consumer
        .assign(&topic_partition_list)
        .map_err(Error::Subscription)?;

    Ok(topic_partition_list)
}

pub fn high_watermarks(
    consumer: &BaseConsumer,
    checkpoints: &state::CheckpointSet,
) -> Result<state::CheckpointSet, Error> {
    let mut watermarks = state::CheckpointSet::default();

    for checkpoint in checkpoints.iter() {
        // The low watermark represents the first message that can be read.
        // The high watermark is the "latest head" offset of the partition. This
        // is effectively the index of the next message to be read.
        let (low, high) = consumer
            .fetch_watermarks(&checkpoint.topic, checkpoint.partition, KAFKA_TIMEOUT)
            .map_err(Error::Watermarks)?;

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

        watermarks.add(state::Checkpoint::new(
            &checkpoint.topic,
            checkpoint.partition,
            offset,
        ));
    }

    Ok(watermarks)
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("failed when interacting with kafka")]
    Kafka(#[from] KafkaError),

    #[error("failed to parse message `{0}`")]
    Parsing(String, #[source] serde_json::Error),

    #[error("failed to encode message: `{0}`")]
    Encoding(#[source] serde_json::Error),

    #[error("message contained no payload")]
    EmptyMessage,
}

pub fn process_message<'m>(
    msg: &'m BorrowedMessage<'m>,
    bindings: &Vec<Binding>,
) -> Result<(response::Captured, state::Checkpoint), ProcessingError> {
    let mut payload = parse_message(msg)?;

    let binding_index = bindings.iter().position(|s| {
        let res = serde_json::from_str::<Resource>(&s.resource_config_json).expect("to parse resource config");
        res.stream == msg.topic()
    }).expect("got message for unknown binding");

    let meta = json!({
        "partition": msg.partition(),
        "offset": msg.offset()
    });

    payload.as_object_mut().unwrap()
        .insert("_meta".to_string(), meta);

    let message = response::Captured {
        binding: binding_index as u32,
        doc_json: serde_json::to_string(&payload).map_err(ProcessingError::Encoding)?,
    };
    let checkpoint = state::Checkpoint::new(msg.topic(), msg.partition(), state::Offset::from(msg));
    Ok((message, checkpoint))
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

pub fn build_shard_key(topic: &MetadataTopic, partition: &MetadataPartition) -> catalog::ShardKey {
    catalog::ShardKey::default()
        .add_str(topic.name())
        .add_int(partition.id())
}
