use std::convert::TryFrom;

use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::Metadata;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{airbyte, catalog, kafka};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read the state file")]
    File(#[from] std::io::Error),

    #[error("failed to validate connector state")]
    Format(#[from] serde_json::Error),

    #[error("failed to serialize state: {0:?}")]
    Serialization(TopicSet, serde_json::Error),
}

/// Represents how far into a partition we've already consumed. The `Offset` value
/// stored in a state file is a record of successfully processing a message.
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Offset {
    /// The very beginning of the partition. **Not the same as `UpThrough(0)`**.
    Start,
    /// The very end of the partition. We won't necessarily know the offset number,
    /// but we can use it to avoid reading from the beginning.
    End,
    /// The specific offset we've read. `UpThrough(0)` would mean we've already
    /// consumed the message with offset=0.
    UpThrough(i64),
}

impl Offset {
    /// Returns a new `Offset` representing the next offset to start consuming.
    ///
    /// When we're re-subscribing to a partition, we want to avoid re-reading the
    /// message we last consumed.
    pub fn next(&self) -> Self {
        match self {
            Self::Start => Self::Start,
            Self::End => Self::End,
            Self::UpThrough(n) => Self::UpThrough(*n + 1),
        }
    }
}

impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        match (self, other) {
            (Offset::Start, Offset::Start) => Some(Ordering::Equal),
            (Offset::Start, Offset::End) => Some(Ordering::Less),
            (Offset::Start, Offset::UpThrough(-1)) => Some(Ordering::Equal),
            (Offset::Start, Offset::UpThrough(_)) => Some(Ordering::Less),
            (Offset::End, Offset::Start) => Some(Ordering::Greater),
            (Offset::End, Offset::End) => Some(Ordering::Equal),
            (Offset::End, Offset::UpThrough(_)) => Some(Ordering::Greater),
            (Offset::UpThrough(-1), Offset::Start) => Some(Ordering::Equal),
            (Offset::UpThrough(_), Offset::Start) => Some(Ordering::Greater),
            (Offset::UpThrough(_), Offset::End) => Some(Ordering::Less),
            (Offset::UpThrough(l), Offset::UpThrough(r)) => Some(l.cmp(r)),
        }
    }
}

impl From<Offset> for rdkafka::Offset {
    fn from(orig: Offset) -> Self {
        match orig {
            Offset::Start => rdkafka::Offset::Beginning,
            Offset::UpThrough(n) => rdkafka::Offset::Offset(n),
            Offset::End => rdkafka::Offset::End,
        }
    }
}

impl<'m> From<&BorrowedMessage<'m>> for Offset {
    /// When constructing an `Offset` from a Kafka Message directly, we know it's
    /// referencing a offset-value from the start of the partition and that it's
    /// been consumed.
    fn from(msg: &BorrowedMessage) -> Self {
        use rdkafka::Message;

        Offset::UpThrough(msg.offset())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Topic {
    pub name: String,
    pub partition: i32,
    pub offset: Offset,
}

impl Topic {
    pub fn new<O: Into<Offset>>(topic: &str, partition: i32, offset: O) -> Self {
        Self {
            name: topic.to_owned(),
            partition,
            offset: offset.into(),
        }
    }
}
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TopicSet(pub Vec<Topic>);

impl TopicSet {
    pub fn add_new(&mut self, new_topic: Topic) {
        match self.find_entry_mut(&new_topic.name, new_topic.partition) {
            Some(_existing_topic) => (), // Do nothing
            None => self.0.push(new_topic),
        }
    }

    pub fn checkpoint(&mut self, updated_topic: &Topic) {
        let mut topic = self
            .find_entry_mut(&updated_topic.name, updated_topic.partition)
            .expect("the topic must be configured before checkpointing");

        topic.offset = updated_topic.offset;
    }

    /// Finds the union of the topics in:
    ///   1. The connected Kafka Cluster
    ///   2. The ConfiguredCatalogSpec
    ///   3. The StateFile
    pub fn reconcile_catalog_state(
        metadata: &Metadata,
        catalog: &catalog::ConfiguredCatalog,
        loaded_state: &TopicSet,
    ) -> Result<TopicSet, catalog::Error> {
        let mut reconciled = TopicSet::default();

        for configured_stream in catalog.streams.iter() {
            if let Some(topic) = kafka::find_topic(metadata, &configured_stream.stream.name) {
                for partition in topic.partitions() {
                    if catalog.responsible_for_shard(kafka::build_shard_key(topic, partition)) {
                        info!("Responsible for {}/{}: YES", topic.name(), partition.id());

                        let offset = loaded_state
                            .offset_for(topic.name(), partition.id())
                            .unwrap_or(Offset::Start);

                        reconciled.add_new(Topic::new(topic.name(), partition.id(), offset));
                    } else {
                        info!("Responsible for {}/{}: NO", topic.name(), partition.id());
                    }
                }
            } else {
                return Err(catalog::Error::MissingStream(
                    configured_stream.stream.name.clone(),
                ));
            }
        }

        Ok(reconciled)
    }

    pub fn offset_for(&self, topic: &str, partition_id: i32) -> Option<Offset> {
        self.find_entry(topic, partition_id).map(|t| t.offset)
    }

    fn find_entry(&self, topic: &str, partition: i32) -> Option<&Topic> {
        self.0
            .iter()
            .find(|t| t.name == topic && t.partition == partition)
    }

    fn find_entry_mut(&mut self, topic: &str, partition: i32) -> Option<&mut Topic> {
        self.0
            .iter_mut()
            .find(|t| t.name == topic && t.partition == partition)
    }
}

impl TryFrom<&TopicSet> for airbyte::State {
    type Error = Error;

    fn try_from(topics: &TopicSet) -> Result<Self, Self::Error> {
        let value = serde_json::to_value(topics).map_err(|e| {
            // Include the Debug-able State along with the serde error
            Error::Serialization(topics.clone(), e)
        })?;
        let state = airbyte::State::new(value);

        Ok(state)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn merge_test() {
        let check_offsets = |topics: &TopicSet, foo0, foo1, bar0| {
            assert_eq!(topics.0[0].offset, foo0);
            assert_eq!(topics.0[1].offset, foo1);
            assert_eq!(topics.0[2].offset, bar0);
        };

        let update_offset = |topic, new_offset| {
            return Topic {
                offset: new_offset,
                ..topic
            };
        };

        let mut topics = TopicSet::default();

        let foo0 = Topic::new("foo", 0, Offset::Start);
        let foo1 = Topic::new("foo", 1, Offset::UpThrough(10));
        let bar0 = Topic::new("bar", 0, Offset::Start);

        topics.0.push(foo0.clone());
        topics.0.push(foo1.clone());
        topics.0.push(bar0.clone());

        check_offsets(&topics, Offset::Start, Offset::UpThrough(10), Offset::Start);

        let foo0 = update_offset(foo0, Offset::UpThrough(0));
        topics.checkpoint(&foo0);
        check_offsets(
            &topics,
            Offset::UpThrough(0),
            Offset::UpThrough(10),
            Offset::Start,
        );

        let foo0 = update_offset(foo0, Offset::UpThrough(1));
        topics.checkpoint(&foo0);
        check_offsets(
            &topics,
            Offset::UpThrough(1),
            Offset::UpThrough(10),
            Offset::Start,
        );

        let foo1 = update_offset(foo1, Offset::UpThrough(11));
        topics.checkpoint(&foo1);
        check_offsets(
            &topics,
            Offset::UpThrough(1),
            Offset::UpThrough(11),
            Offset::Start,
        );
    }
}
