use anyhow::{Context, Result};
use avro::{encode, encode_key};
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use bytes::Bytes;
use proto_flow::{
    flow::RangeSpec,
    materialize::{
        request::{Open, StartCommit, Store},
        response::{Acknowledged, Flushed, Opened, StartedCommit},
        Response,
    },
    RuntimeCheckpoint,
};
use rdkafka::{
    consumer::Consumer,
    error::KafkaError,
    producer::{FutureRecord, Producer},
    util::Timeout,
    Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use tuple::Element;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    binding_info::{get_binding_info, BindingInfo},
    configuration::EndpointConfig,
    Input, Output, FLOW_CHECKPOINTS_TOPIC, KAFKA_TIMEOUT,
};

pub async fn run_transactions(mut input: Input, mut output: Output, open: Open) -> Result<()> {
    let spec = open
        .materialization
        .expect("must have a materialization spec");

    let config: EndpointConfig = serde_json::from_str(&spec.config_json)?;
    let range = open.range.expect("range must be set");

    let txn_id = txn_id_for_mat_and_range(&spec.name, range);
    let producer = config.to_producer(&txn_id)?;

    // Fence off any other active producers for this materialization and key
    // range. This is not completely bulletproof for a materialization that
    // recently had it shard(s) split, since a prior invocation of a larger
    // shard range may update the checkpoint for that range concurrently with
    // the smaller split range coming online. This has the potential to result
    // in duplicate messages, but only in the initial transactions of connector
    // invocations immediately following a shard split.
    tracing::info!(txn_id, "started recovering checkpoint");
    producer.init_transactions(None)?;
    output.send(Response {
        opened: Some(Opened {
            runtime_checkpoint: recover_checkpoint(&config, &spec.name, &range)?,
        }),
        ..Default::default()
    })?;
    tracing::info!("finished recovering checkpoint");

    let bindings = get_binding_info(&spec.bindings, config.schema_registry.as_ref()).await?;

    let mut key_buf = Vec::new();
    let mut payload_buf = Vec::new();
    let mut in_txn = false;
    let mut commit_op: Option<JoinHandle<Result<(i32, i64)>>> = None;

    loop {
        let request = match input.read()? {
            Some(req) => req,
            None => return Ok(()),
        };

        if request.flush.is_some() {
            output.send(Response {
                flushed: Some(Flushed { state: None }),
                ..Default::default()
            })?;
            tracing::debug!("wrote Flushed");
        } else if let Some(store) = request.store {
            if !in_txn {
                producer.begin_transaction()?;
                in_txn = true;
            }
            producer
                .send(
                    msg_for_store(&mut key_buf, &mut payload_buf, store, &bindings)?,
                    Timeout::Never,
                )
                .await
                .map_err(|(err, msg)| {
                    anyhow::anyhow!("sending store message to topic {}: {}", msg.topic(), err)
                })?;
        } else if let Some(StartCommit { runtime_checkpoint }) = request.start_commit {
            if !in_txn {
                // In case there were no store requests for this txn.
                producer.begin_transaction()?;
            }

            let (partition, offset) = producer
                .send(
                    msg_for_checkpoint(
                        &mut key_buf,
                        &mut payload_buf,
                        runtime_checkpoint.expect("StartCommit must have a runtime checkpoint"),
                        &spec.name,
                        &range,
                    )?,
                    Timeout::Never,
                )
                .await
                .map_err(|(err, _)| anyhow::anyhow!("sending checkpoint message: {}", err))?;

            let commit_producer = producer.clone();
            assert!(commit_op
                .replace(tokio::spawn(async move {
                    commit_producer.commit_transaction(Timeout::Never)?;
                    Ok((partition, offset))
                }))
                .is_none());

            output.send(Response {
                started_commit: Some(StartedCommit { state: None }),
                ..Default::default()
            })?;
            tracing::debug!("wrote StartedCommit");
        } else if request.acknowledge.is_some() {
            if let Some(commit_op) = commit_op.take() {
                // commit_op will be None for a recovery commit.
                let (partition, offset) = commit_op.await??;
                tracing::debug!(partition, offset, "committed transaction");
            }
            in_txn = false;

            output.send(Response {
                acknowledged: Some(Acknowledged { state: None }),
                ..Default::default()
            })?;
            tracing::debug!("wrote Acknowledged");
        }

        key_buf.clear();
        payload_buf.clear();
    }
}

fn msg_for_store<'a>(
    key_buf: &'a mut Vec<u8>,
    payload_buf: &'a mut Vec<u8>,
    store: Store,
    bindings: &'a [BindingInfo],
) -> Result<FutureRecord<'a, Vec<u8>, Vec<u8>>> {
    let Store {
        binding,
        key_packed,
        values_packed,
        doc_json,
        delete,
        ..
    } = store;

    let b = &bindings[binding as usize];

    let mut converted = convert_tuple(&key_packed);
    if !values_packed.is_empty() {
        converted.append(&mut convert_tuple(&values_packed));
    }
    if !doc_json.is_empty() {
        // The root document field, if selected, is always materialized as a
        // string.
        converted.push(serde_json::Value::String(doc_json));
    }

    let doc = converted
        .into_iter()
        .enumerate()
        .map(|(i, value)| (b.field_names[i].clone(), value))
        .collect::<serde_json::Value>();

    // TODO(whb): Support JSON serialization without a schema, and JSON
    // serialization with a JSON schema.
    let schema = b.schema.as_ref().unwrap();

    key_buf.push(0);
    key_buf.extend(schema.key_schema_id.to_be_bytes());
    encode_key(key_buf, &schema.key_schema, &doc, &b.key_ptr).context("encoding key")?;
    let mut rec = FutureRecord::to(&b.topic).key(key_buf);

    if !delete {
        payload_buf.push(0);
        payload_buf.extend(schema.schema_id.to_be_bytes());
        encode(payload_buf, &schema.schema, &doc).context("encoding payload")?;
        rec = rec.payload(payload_buf);
    }

    Ok(rec)
}

fn convert_tuple(packed: &Bytes) -> Vec<serde_json::Value> {
    tuple::unpack::<Vec<_>>(packed)
        .expect("tuple must unpack")
        .iter()
        .map(|element| match element {
            Element::Bool(v) => serde_json::json!(v),
            Element::Int(v) => serde_json::json!(v),
            Element::Float(v) => serde_json::json!(v),
            Element::Double(v) => serde_json::json!(v),
            Element::String(v) => serde_json::json!(v),
            Element::Bytes(v) => serde_json::json!(base64.encode(v)),
            Element::Nil => serde_json::json!(null),
            _ => panic!("received unexpected tuple element {:?}", element),
        })
        .collect()
}

#[derive(Serialize, Deserialize)]
struct CheckpointKey {
    materialization: String,
    range: RangeSpec,
}

impl CheckpointKey {
    fn overlaps(&self, other: &CheckpointKey) -> bool {
        self.materialization == other.materialization
            && self.range.key_end >= other.range.key_begin
            && self.range.key_begin <= other.range.key_end
    }
}

fn msg_for_checkpoint<'a>(
    key_buf: &'a mut Vec<u8>,
    payload_buf: &'a mut Vec<u8>,
    checkpoint: RuntimeCheckpoint,
    materialization_name: &str,
    range: &RangeSpec,
) -> Result<FutureRecord<'a, Vec<u8>, Vec<u8>>> {
    serde_json::to_writer(
        &mut *key_buf,
        &CheckpointKey {
            materialization: materialization_name.to_string(),
            range: *range,
        },
    )?;

    serde_json::to_writer(&mut *payload_buf, &checkpoint)?;

    Ok(FutureRecord::to(FLOW_CHECKPOINTS_TOPIC)
        .key(key_buf)
        .payload(payload_buf))
}

fn recover_checkpoint(
    config: &EndpointConfig,
    materialization_name: &str,
    range: &RangeSpec,
) -> Result<Option<RuntimeCheckpoint>> {
    let consumer = config.to_consumer()?;
    let mut assignment = TopicPartitionList::new();
    let topic_metadata = consumer.fetch_metadata(Some(FLOW_CHECKPOINTS_TOPIC), KAFKA_TIMEOUT)?;

    // We must read up until the latest offsets for all partitions of the
    // checkpoint topic to get the latest checkpoint. Any prior producer must
    // have been fenced off to prevent additional relevant offsets from being
    // committed after these are retrieved.
    let mut partition_high_watermarks: HashMap<i32, i64> = HashMap::new();
    assert!(topic_metadata.topics().len() == 1); // sanity check
    for partition in topic_metadata.topics()[0].partitions() {
        let (low, high) =
            consumer.fetch_watermarks(FLOW_CHECKPOINTS_TOPIC, partition.id(), KAFKA_TIMEOUT)?;

        if high > low {
            assignment.add_partition_offset(
                FLOW_CHECKPOINTS_TOPIC,
                partition.id(),
                Offset::Offset(low),
            )?;

            partition_high_watermarks.insert(partition.id(), high);
        }
    }

    if assignment.count() == 0 {
        // No checkpoints have ever been written to the topic.
        return Ok(None);
    }

    let this_key = CheckpointKey {
        materialization: materialization_name.to_string(),
        range: *range,
    };
    let mut out = None;

    consumer.assign(&assignment)?;
    for msg in consumer.iter() {
        match msg {
            Ok(msg) => {
                let read_key: CheckpointKey = serde_json::from_slice(
                    msg.key().expect("checkpoints topic key is always present"),
                )?;

                if this_key.overlaps(&read_key) {
                    out = Some(serde_json::from_slice(
                        msg.payload()
                            .expect("checkpoints topic payload is always present"),
                    )?);
                }

                let partition = msg.partition();
                if msg.offset() >= *partition_high_watermarks.get(&partition).unwrap() {
                    // For this to happen additional checkpoint messages must
                    // have been written to the partition since the high
                    // watermark was fetched, maybe by some other
                    // materialization. The high watermark is the offset that
                    // the next message will be written as, rather than the
                    // offset of the "latest" message.
                    partition_high_watermarks.remove(&partition);
                }
            }
            Err(e) => match e {
                KafkaError::PartitionEOF(partition) => {
                    // Read to the end of the topic without reaching the high
                    // watermark.
                    partition_high_watermarks.remove(&partition);
                }
                _ => return Err(e.into()),
            },
        }

        if partition_high_watermarks.is_empty() {
            break;
        }
    }

    Ok(out)
}

fn txn_id_for_mat_and_range(materialization_name: &str, range: RangeSpec) -> String {
    let sanitized = materialization_name.replace(|c: char| !c.is_alphanumeric(), "_");
    let trimmed = if sanitized.len() > 32 {
        &sanitized[0..32]
    } else {
        &sanitized
    };
    let hashed = xxh3_64(materialization_name.as_bytes());

    // A cleaned up version of the materialization name is used for help with
    // human-readability for debugging, with a hash stuck on the end to prevent
    // inadvertent collisions. Also note that only the key_begin is used for
    // fencing, since there is no way to fence off "overlapping" ranges.
    format!("{}_{:08x}_{:016x}", trimmed, range.key_begin, hashed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlaps() {
        let tests = [
            ("foo/bar", 1, 5, "foo/bar", 3, 7, true),
            ("foo/bar", 1, 5, "foo/bar", 1, 5, true),
            ("foo/bar", 7, 10, "foo/bar", 1, 5, false),
            ("foo/bar", 1, 5, "foo/bar", 5, 10, true),
            ("foo/bar", 5, 10, "foo/bar", 1, 5, true),
            ("foo/bar", 1, 5, "foo/bar", 6, 10, false),
            ("foo/bar", 1, 5, "foo/baz", 3, 7, false),
        ];

        for test in tests {
            assert_eq!(
                CheckpointKey {
                    materialization: test.0.to_string(),
                    range: RangeSpec {
                        key_begin: test.1,
                        key_end: test.2,
                        ..Default::default()
                    }
                }
                .overlaps(&CheckpointKey {
                    materialization: test.3.to_string(),
                    range: RangeSpec {
                        key_begin: test.4,
                        key_end: test.5,
                        ..Default::default()
                    }
                }),
                test.6
            );
        }
    }
}
