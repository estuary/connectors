use std::collections::HashSet;
use std::convert::TryFrom;

use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::Metadata;
use serde::{Deserialize, Serialize};

use crate::airbyte;

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
    pub fn discover_existing_partitions(
        &mut self,
        catalog: &airbyte::Catalog,
        metadata: &Metadata,
    ) {
        let catalog_streams = catalog
            .streams
            .iter()
            .map(|s| &s.name[..])
            .collect::<HashSet<&str>>();
        let mut in_kafka = HashSet::new();

        for topic in metadata.topics() {
            if catalog_streams.contains(topic.name()) {
                for partition in topic.partitions() {
                    in_kafka.insert((topic.name(), partition.id()));
                    self.add_new(Topic::new(topic.name(), partition.id(), Offset::Start));
                }
            }
        }

        let filtered = self
            .0
            .drain(0..)
            .filter(|topic| in_kafka.contains(&(&topic.name, topic.partition)))
            .collect();
        self.0 = filtered;
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
