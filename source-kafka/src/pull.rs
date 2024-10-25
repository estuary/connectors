use crate::{
    configuration::{EndpointConfig, FlowConsumerContext, Resource},
    write_capture_response,
};
use anyhow::Result;
use hex::decode;
use highway::{HighwayHash, HighwayHasher, Key};
use lazy_static::lazy_static;
use proto_flow::{
    capture::{
        request::Open,
        response::{self, Checkpoint},
        Response,
    },
    flow::{capture_spec::Binding, ConnectorState, RangeSpec},
};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    metadata::MetadataPartition,
    Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tokio::io::{self};

#[derive(Debug, Deserialize, Serialize, Default)]
struct CaptureState {
    #[serde(rename = "bindingStateV1")]
    resources: HashMap<String, ResourceState>,
}

impl CaptureState {
    fn state_slice(state_key: &str, partition: i32, offset: i64) -> Self {
        let mut partitions = HashMap::new();
        partitions.insert(partition, offset);
        let mut resources = HashMap::new();
        resources.insert(state_key.to_string(), ResourceState { partitions });
        Self { resources }
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct ResourceState {
    partitions: HashMap<i32, i64>,
}

struct BindingInfo {
    binding_index: u32,
    state_key: String,
}

pub async fn do_pull(req: Open, mut stdout: io::Stdout) -> Result<()> {
    let spec = req.capture.expect("open must contain a capture spec");

    let state = if req.state_json == "{}" {
        CaptureState::default()
    } else {
        serde_json::from_str(&req.state_json)?
    };

    let config: EndpointConfig = serde_json::from_str(&spec.config_json)?;
    let mut consumer = config.to_consumer().await?;

    let topics_to_bindings =
        setup_consumer(&mut consumer, state, &spec.bindings, &req.range).await?;

    loop {
        let msg = consumer.recv().await?;

        let binding_info = topics_to_bindings
            .get(msg.topic())
            .expect("got a message for an unknown binding");

        let mut bytes = msg.payload().expect("message must contain a payload");
        // Strip a Confluent Schema Registry magic byte and schema ID.
        if bytes.starts_with(&[0]) && bytes.len() >= 5 {
            bytes = &bytes[5..];
        }

        let meta = json!({
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset()
        });

        let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
        value
            .as_object_mut()
            .unwrap()
            .insert("_meta".to_string(), meta);

        let message = response::Captured {
            binding: binding_info.binding_index,
            doc_json: serde_json::to_string(&value)?,
        };

        let checkpoint =
            CaptureState::state_slice(&binding_info.state_key, msg.partition(), msg.offset());

        write_capture_response(
            Response {
                captured: Some(message),
                ..Default::default()
            },
            &mut stdout,
        )
        .await?;

        write_capture_response(
            Response {
                checkpoint: Some(Checkpoint {
                    state: Some(ConnectorState {
                        updated_json: serde_json::to_string(&checkpoint)?,
                        merge_patch: true,
                    }),
                }),
                ..Default::default()
            },
            &mut stdout,
        )
        .await?;
    }
}

async fn setup_consumer(
    consumer: &mut StreamConsumer<FlowConsumerContext>,
    state: CaptureState,
    bindings: &[Binding],
    range: &Option<RangeSpec>,
) -> Result<HashMap<String, BindingInfo>> {
    let meta = consumer.fetch_metadata(None, None)?;

    let extant_partitions: HashMap<String, &[MetadataPartition]> = meta
        .topics()
        .iter()
        .map(|t| (t.name().to_string(), t.partitions()))
        .collect();

    let mut topics_to_bindings: HashMap<String, BindingInfo> = HashMap::new();
    let mut topic_partition_list = TopicPartitionList::new();

    for (idx, binding) in bindings.iter().enumerate() {
        let res: Resource = serde_json::from_str(&binding.resource_config_json)?;

        let state_key = &binding.state_key;
        let topic = &res.topic;

        let default_state = ResourceState::default();
        let resource_state = state.resources.get(state_key).unwrap_or(&default_state);

        let partition_info = extant_partitions
            .get(topic)
            .expect("expected configured topic to exist");

        for partition in partition_info.iter() {
            let partition = partition.id();
            if !responsible_for_partition(range, &res.topic, partition) {
                continue;
            }

            let offset = match resource_state.partitions.get(&partition) {
                Some(o) => Offset::Offset(*o + 1), // Don't read the same offset again.
                None => Offset::Beginning,
            };
            topic_partition_list.add_partition_offset(topic, partition, offset)?;
        }

        topics_to_bindings.insert(
            topic.to_string(),
            BindingInfo {
                binding_index: idx as u32,
                state_key: state_key.to_string(),
            },
        );
    }

    consumer.assign(&topic_partition_list)?;

    Ok(topics_to_bindings)
}

lazy_static! {
    // HIGHWAY_HASH_KEY is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
    // DO NOT MODIFY this value, as it is required to have consistent hash results.
    // This value is copied from the Go connector source-boilerplate.
    static ref HIGHWAY_HASH_KEY: Vec<u8> = {
        decode("332757d16f0fb1cf2d4f676f85e34c6a8b85aa58f42bb081449d8eb2e4ed529f")
            .expect("invalid hex string for HIGHWAY_HASH_KEY")
    };
}

fn bytes_to_key(key: &[u8]) -> Key {
    assert!(key.len() == 32, "The key must be exactly 32 bytes long.");

    Key([
        u64::from_le_bytes(key[0..8].try_into().unwrap()),
        u64::from_le_bytes(key[8..16].try_into().unwrap()),
        u64::from_le_bytes(key[16..24].try_into().unwrap()),
        u64::from_le_bytes(key[24..32].try_into().unwrap()),
    ])
}

fn responsible_for_partition(range: &Option<RangeSpec>, topic: &str, partition: i32) -> bool {
    let range = match range {
        None => return true,
        Some(r) => r,
    };

    let mut hasher = HighwayHasher::new(bytes_to_key(&HIGHWAY_HASH_KEY));
    hasher.append(topic.as_bytes());
    hasher.append(&partition.to_le_bytes());
    let hash = (hasher.finalize64() >> 32) as u32;

    hash >= range.key_begin && hash <= range.key_end
}
