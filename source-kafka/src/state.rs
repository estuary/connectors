use std::collections::BTreeMap;

use proto_flow::flow::{CaptureSpec, RangeSpec};
use rdkafka::message::BorrowedMessage;
use rdkafka::metadata::Metadata;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{catalog::{self, Resource, responsible_for_shard}, connector, kafka};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read the state file")]
    File(#[from] std::io::Error),

    #[error("failed to validate connector state")]
    Format(#[from] serde_json::Error),

    #[error("failed to serialize state: {0:?}")]
    Serialization(Checkpoint, serde_json::Error),
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
pub struct Checkpoint {
    pub topic: String,
    pub partition: i32,
    pub offset: Offset,
}

impl Checkpoint {
    pub fn new<O: Into<Offset>>(topic: &str, partition: i32, offset: O) -> Self {
        Self {
            topic: topic.to_owned(),
            partition,
            offset: offset.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct CheckpointSet(pub BTreeMap<String, BTreeMap<i32, Offset>>);

impl CheckpointSet {
    pub fn add(&mut self, new_checkpoint: Checkpoint) {
        self.0
            .entry(new_checkpoint.topic)
            .or_insert_with(Default::default)
            .insert(new_checkpoint.partition, new_checkpoint.offset);
    }

    /// Finds the union of the topics in:
    ///   1. The connected Kafka Cluster
    ///   2. The ConfiguredCatalogSpec
    ///   3. The StateFile
    pub fn reconcile_catalog_state(
        metadata: &Metadata,
        capture: &CaptureSpec,
        range: Option<&RangeSpec>,
        loaded_state: &CheckpointSet,
    ) -> Result<CheckpointSet, catalog::Error> {
        let mut reconciled = CheckpointSet::default();

        for binding in capture.bindings.iter() {
            let res: Resource = serde_json::from_str(&binding.resource_config_json)?;
            if let Some(topic) = kafka::find_topic(metadata, &res.stream) {
                for partition in topic.partitions() {
                    if let Some(range) = range {
                        if responsible_for_shard(range, kafka::build_shard_key(topic, partition)) {
                            info!("Responsible for {}/{}: YES", topic.name(), partition.id());

                            let offset = loaded_state
                                .offset_for(topic.name(), partition.id())
                                .unwrap_or(Offset::Start);

                            reconciled.add(Checkpoint::new(topic.name(), partition.id(), offset));
                        } else {
                            info!("Responsible for {}/{}: NO", topic.name(), partition.id());
                        }
                    } else {
                        let offset = loaded_state
                            .offset_for(topic.name(), partition.id())
                            .unwrap_or(Offset::Start);

                        reconciled.add(Checkpoint::new(topic.name(), partition.id(), offset));
                    }
                }
            } else {
                return Err(catalog::Error::MissingStream(
                    res.stream.clone(),
                ));
            }
        }

        Ok(reconciled)
    }

    pub fn offset_for(&self, topic: &str, partition_id: i32) -> Option<Offset> {
        self.0
            .get(topic)
            .and_then(|partitions| partitions.get(&partition_id).map(Clone::clone))
    }

    pub fn iter(&self) -> impl Iterator<Item = Checkpoint> + '_ {
        self.0.iter().flat_map(|(topic, partitions)| {
            partitions
                .iter()
                .map(move |(partition, offset)| Checkpoint::new(topic, *partition, *offset))
        })
    }
}

impl connector::ConnectorConfig for CheckpointSet {
    type Error = Error;

    fn parse(reader: &str) -> Result<Self, Self::Error> {
        let checkpoints = serde_json::from_str(reader)?;

        Ok(checkpoints)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use connector::ConnectorConfig;

    #[test]
    fn merge_test() {
        let check_offsets = |checkpoints: &CheckpointSet, foo0, foo1, bar0| {
            assert_eq!(checkpoints.0["foo"][&0], foo0);
            assert_eq!(checkpoints.0["foo"][&1], foo1);
            assert_eq!(checkpoints.0["bar"][&0], bar0);
        };

        let update_offset = |checkpoint, new_offset| {
            Checkpoint {
                offset: new_offset,
                ..checkpoint
            }
        };

        let mut checkpoints = CheckpointSet::default();

        let foo0 = Checkpoint::new("foo", 0, Offset::Start);
        let foo1 = Checkpoint::new("foo", 1, Offset::UpThrough(10));
        let bar0 = Checkpoint::new("bar", 0, Offset::Start);

        checkpoints.add(foo0.clone());
        checkpoints.add(foo1.clone());
        checkpoints.add(bar0);

        check_offsets(
            &checkpoints,
            Offset::Start,
            Offset::UpThrough(10),
            Offset::Start,
        );

        let foo0 = update_offset(foo0, Offset::UpThrough(0));
        checkpoints.add(foo0.clone());
        check_offsets(
            &checkpoints,
            Offset::UpThrough(0),
            Offset::UpThrough(10),
            Offset::Start,
        );

        let foo0 = update_offset(foo0, Offset::UpThrough(1));
        checkpoints.add(foo0);
        check_offsets(
            &checkpoints,
            Offset::UpThrough(1),
            Offset::UpThrough(10),
            Offset::Start,
        );

        let foo1 = update_offset(foo1, Offset::UpThrough(11));
        checkpoints.add(foo1);
        check_offsets(
            &checkpoints,
            Offset::UpThrough(1),
            Offset::UpThrough(11),
            Offset::Start,
        );
    }

    #[test]
    fn parse_state_file_test() {
        let input = r#"
                {
                    "test": {
                        "0": { "UpThrough": 100 }
                    }
                }
        "#;

        CheckpointSet::parse(input).expect("to parse");
    }

    #[test]
    fn parse_empty_state_file_test() {
        let input = "{}\n";

        CheckpointSet::parse(input).expect("to parse");
    }
}
