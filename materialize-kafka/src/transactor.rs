use anyhow::{Context, Result};
use avro::{encode, encode_key};
use bytes::Bytes;
use proto_flow::materialize::{
    request::{Open, Store},
    response::{Acknowledged, Flushed, Opened, StartedCommit},
    Response,
};
use rdkafka::{
    error::KafkaError,
    producer::{BaseRecord, Producer},
    types::RDKafkaErrorCode,
    util::Timeout,
};
use tuple::Element;

use crate::{
    binding_info::{get_binding_info, BindingInfo},
    configuration::{EndpointConfig, MessageFormat},
    Input, Output,
};

pub async fn run_transactions(mut input: Input, mut output: Output, open: Open) -> Result<()> {
    let spec = open
        .materialization
        .expect("must have a materialization spec");

    let config: EndpointConfig = serde_json::from_str(&spec.config_json)?;
    let producer = config.to_producer()?;
    let producer_context = producer.context();
    let mut txn_docs_counter: usize = 0;

    output.send(Response {
        opened: Some(Opened {
            runtime_checkpoint: None,
        }),
        ..Default::default()
    })?;

    let bindings = get_binding_info(
        &spec.bindings,
        &config.message_format,
        config.schema_registry.as_ref(),
    )
    .await?;

    let mut key_buf = Vec::new();
    let mut payload_buf = Vec::new();

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
            let mut msg = msg_for_store(
                &config.message_format,
                &mut key_buf,
                &mut payload_buf,
                store,
                &bindings,
            )?;

            loop {
                match producer.send(msg) {
                    Ok(_) => {
                        break;
                    }
                    Err((err, original_message)) => match err {
                        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) => {
                            tracing::debug!("polling producer since queue is full");
                            producer.poll(Timeout::Never);
                            msg = original_message;
                        }
                        _ => {
                            anyhow::bail!(
                                "failed to send store message to topic {}: {}",
                                original_message.topic,
                                err
                            )
                        }
                    },
                }
            }

            txn_docs_counter += 1;
        } else if request.start_commit.is_some() {
            producer.flush(Timeout::Never)?;
            producer_context.get_error().context("flushing producer")?;
            let sent = producer_context.get_and_reset_count();
            if sent != txn_docs_counter {
                anyhow::bail!(
                    "successfully delivered {} messages vs. {} expected",
                    sent,
                    txn_docs_counter,
                )
            }
            txn_docs_counter = 0;

            output.send(Response {
                started_commit: Some(StartedCommit { state: None }),
                ..Default::default()
            })?;
            tracing::debug!("wrote StartedCommit");
        } else if request.acknowledge.is_some() {
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
    message_format: &MessageFormat,
    key_buf: &'a mut Vec<u8>,
    payload_buf: &'a mut Vec<u8>,
    store: Store,
    bindings: &'a [BindingInfo],
) -> Result<BaseRecord<'a, Vec<u8>, Vec<u8>>> {
    let Store {
        binding,
        key_packed,
        values_packed,
        doc_json,
        delete,
        ..
    } = store;

    let b = &bindings[binding as usize];

    let converted_key = convert_tuple(&key_packed);
    let mut converted_values = if !values_packed.is_empty() {
        convert_tuple(&values_packed)
    } else {
        vec![]
    };
    if !doc_json.is_empty() {
        // The root document field, if selected, is always materialized as a
        // string.
        converted_values.push(serde_json::Value::String(doc_json));
    }

    Ok(match message_format {
        MessageFormat::JSON => {
            let key_doc = converted_key
                .clone()
                .into_iter()
                .enumerate()
                .map(|(i, value)| (b.field_names[i].clone(), value))
                .collect::<serde_json::Value>();

            let doc = converted_key
                .into_iter()
                .chain(converted_values)
                .enumerate()
                .map(|(i, value)| (b.field_names[i].clone(), value))
                .collect::<serde_json::Value>();

            serde_json::to_writer(&mut *key_buf, &key_doc)?;
            let mut rec = BaseRecord::to(&b.topic).key(key_buf);

            if !delete {
                serde_json::to_writer(&mut *payload_buf, &doc)?;
                rec = rec.payload(payload_buf);
            }

            rec
        }
        MessageFormat::Avro => {
            let doc = converted_key
                .into_iter()
                .chain(converted_values)
                .enumerate()
                .map(|(i, value)| (b.field_names[i].clone(), value))
                .collect::<serde_json::Value>();

            let schema = b
                .schema
                .as_ref()
                .expect("schema must be available for Avro encoding");

            key_buf.push(0);
            key_buf.extend(schema.key_schema_id.to_be_bytes());
            encode_key(key_buf, &schema.key_schema, &doc, &b.key_ptr).context("encoding key")?;
            let mut rec = BaseRecord::to(&b.topic).key(key_buf);

            if !delete {
                payload_buf.push(0);
                payload_buf.extend(schema.schema_id.to_be_bytes());
                encode(payload_buf, &schema.schema, &doc).context("encoding payload")?;
                rec = rec.payload(payload_buf);
            }

            rec
        }
    })
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
            Element::Bytes(v) => serde_json::from_slice(v).expect("bytes must be valid json"),
            Element::Nil => serde_json::json!(null),
            _ => panic!("received unexpected tuple element {:?}", element),
        })
        .collect()
}
