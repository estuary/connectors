use crate::{
    configuration::{EndpointConfig, FlowConsumerContext, Resource, SchemaRegistryConfig},
    document::MergeSerializer,
    protobuf::{decode_protobuf_message, parse_message_indexes, resolve_message_from_indexes},
    schema_registry::{ProtobufSchema, RegisteredSchema, SchemaRegistryClient},
    transcode::{Plans, TranscodeError, Transcoder},
    write_captured,
};
use anyhow::{anyhow, Context, Result};
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use bigdecimal::BigDecimal;
use hex::decode;
use highway::{HighwayHash, HighwayHasher, Key};
use lazy_static::lazy_static;
use prost_reflect::SerializeOptions;
use proto_flow::{
    capture::request::Open,
    flow::{capture_spec::Binding, RangeSpec},
};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::Headers,
    metadata::MetadataPartition,
    Message, Offset, Timestamp, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map};
use std::collections::{hash_map::Entry, HashMap};
use std::io::{BufWriter, Stdout, Write};
use time::{format_description, OffsetDateTime};

/// Feature flags this connector reads, with their defaults. Set through
/// `advanced.feature_flags`; `no_<flag>` disables one.
const FEATURE_FLAG_DEFAULTS: &[(&str, bool)] = &[
    // Transcode registry protobuf payloads straight from the wire instead of
    // through a DynamicMessage. See src/transcode/mod.rs.
    ("protobuf_transcoder", false),
];

#[derive(Debug, Deserialize, Serialize, Default)]
struct CaptureState {
    #[serde(rename = "bindingStateV1")]
    resources: HashMap<String, ResourceState>,
}

impl CaptureState {
    /// The checkpoint `write_checkpoint` writes, as a struct. Used by the tests
    /// as the oracle for the raw bytes.
    #[cfg(test)]
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

#[derive(Serialize, Deserialize, Default)]
struct Meta {
    topic: String,
    partition: i32,
    offset: i64,
    op: String,
    headers: Option<serde_json::Map<String, serde_json::Value>>,
    timestamp: Option<MetaTimestamp>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum MetaTimestamp {
    CreationTime(String),
    LogAppendTime(String),
}

/// The result of parsing a message payload or key. Registry protobuf payloads
/// are left as wire bytes so `do_pull` can transcode them straight to output
/// once the key and `_meta` are known; everything else is parsed to a `Value`.
enum Parsed<'a> {
    Protobuf {
        schema_id: u32,
        /// The whole datum, Confluent framing included.
        datum: &'a [u8],
    },
    Value(serde_json::Value),
}

pub async fn do_pull(req: Open, mut stdout: BufWriter<Stdout>) -> Result<()> {
    let spec = req.capture.expect("open must contain a capture spec");

    let state = if req.state_json == "{}" {
        CaptureState::default()
    } else {
        serde_json::from_slice(&req.state_json)?
    };

    let config: EndpointConfig = serde_json::from_slice(&spec.config_json)?;
    let feature_flags = config.feature_flags(FEATURE_FLAG_DEFAULTS);
    let is_transcoder_enabled = feature_flags["protobuf_transcoder"];
    let mut consumer = config.to_consumer().await?;
    let schema_client = match config.schema_registry {
        SchemaRegistryConfig::ConfluentSchemaRegistry {
            endpoint,
            username,
            password,
        } => Some(SchemaRegistryClient::new(endpoint, username, password)),
        SchemaRegistryConfig::NoSchemaRegistry { .. } => None,
    };
    let mut schema_cache: HashMap<u32, RegisteredSchema> = HashMap::new();
    let mut plans_cache: HashMap<u32, Plans> = HashMap::new();
    let mut transcoder = Transcoder::new();
    tracing::info!(feature_flags = ?feature_flags, "resolved feature flags");

    let topics_to_bindings =
        setup_consumer(&mut consumer, state, &spec.bindings, &req.range).await?;

    loop {
        let msg = consumer
            .poll(None)
            .expect("polling without a timeout should always produce a message")
            .context("receiving next message")?;

        let mut op = "u";
        let payload = match msg.payload() {
            Some(bytes) => parse_datum(bytes, false, &mut schema_cache, schema_client.as_ref())
                .await
                .with_context(|| describe_payload(&msg, bytes))?,
            None => {
                // We interpret an absent message payload as a deletion
                // tombstone. The captured document will otherwise be empty
                // except for the _meta field and the message key (if present).
                op = "d";
                Parsed::Value(json!({}))
            }
        };

        let mut meta = Meta {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            op: op.to_string(),
            ..Default::default()
        };

        if let Some(headers) = msg.headers() {
            meta.headers = Some(
                headers
                    .iter()
                    .map(|h| {
                        let value = match h.value {
                            Some(v) => match std::str::from_utf8(v) {
                                // Prefer capturing header byte values as UTF-8
                                // strings if possible, otherwise base64 encode
                                // them.
                                Ok(v) => json!(v),
                                Err(_) => json!(base64.encode(v)),
                            },
                            None => json!(null),
                        };
                        (h.key.to_string(), value)
                    })
                    .collect(),
            )
        }

        meta.timestamp = match msg.timestamp() {
            Timestamp::NotAvailable => None,
            Timestamp::CreateTime(ts) => {
                Some(MetaTimestamp::CreationTime(unix_millis_to_rfc3339(ts)?))
            }
            Timestamp::LogAppendTime(ts) => {
                Some(MetaTimestamp::LogAppendTime(unix_millis_to_rfc3339(ts)?))
            }
        };

        // Parse the message key into a map so its fields can be merged
        // over the payload.
        let key_fields: Option<Map<String, serde_json::Value>> = match msg.key() {
            Some(key_bytes) => {
                let parsed = parse_datum(key_bytes, true, &mut schema_cache, schema_client.as_ref())
                    .await
                    .with_context(|| format!("parsing message key for topic {}", msg.topic()))?;
                match parsed {
                    Parsed::Value(serde_json::Value::Object(map)) => Some(map),
                    Parsed::Value(other) => anyhow::bail!(
                        "message key for topic {} did not parse to a JSON object: {}",
                        msg.topic(),
                        other
                    ),
                    Parsed::Protobuf { .. } => unreachable!("keys are parsed with is_key = true"),
                }
            }
            None => None,
        };

        // Build the captured document's JSON bytes. Protobuf payloads are
        // transcoded straight into the buffer with the key and _meta merged
        // in. Other payloads are already a Value.
        let doc_bytes: Vec<u8> = match payload {
            Parsed::Protobuf { schema_id, datum } => {
                let Some(RegisteredSchema::Protobuf(proto_schema)) = schema_cache.get(&schema_id)
                else {
                    unreachable!(
                        "parse_datum returns Parsed::Protobuf only for a cached protobuf schema"
                    )
                };
                let framed = &datum[5..];
                if !is_transcoder_enabled {
                    // The default: no plans are built for a capture that
                    // never consults them.
                    dynamic_protobuf_document(proto_schema, framed, key_fields.as_ref(), &meta)
                        .with_context(|| describe_payload(&msg, datum))?
                } else {
                    let plans = match plans_cache.entry(schema_id) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => e.insert(
                            Plans::for_schema(
                                &proto_schema.descriptor_pool,
                                &proto_schema.message_name,
                            )
                            .with_context(|| {
                                format!("building transcoder plans for schema id {schema_id}")
                            })?,
                        ),
                    };
                    let location = (msg.topic(), msg.partition(), msg.offset());
                    transcode_or_fall_back(
                        plans,
                        &mut transcoder,
                        proto_schema,
                        framed,
                        key_fields.as_ref(),
                        &meta,
                        location,
                    )
                    .with_context(|| describe_payload(&msg, datum))?
                }
            }
            Parsed::Value(mut doc) => {
                let captured = doc
                    .as_object_mut()
                    .context("captured document must be a JSON object")?;
                captured.insert("_meta".to_string(), serde_json::to_value(&meta)?);
                if let Some(mut key_fields) = key_fields {
                    // Add key/val pairs from the "key" to root of the captured
                    // document, which will clobber any collisions with keys from
                    // the parsed payload.
                    captured.append(&mut key_fields);
                }
                serde_json::to_vec(&doc)?
            }
        };

        let binding_info = topics_to_bindings
            .get(msg.topic())
            .with_context(|| format!("got a message for unknown topic {}", msg.topic()))?;

        write_captured(binding_info.binding_index, &doc_bytes, &mut stdout)?;
        write_checkpoint(
            &binding_info.state_key,
            msg.partition(),
            msg.offset(),
            &mut stdout,
        )?;
    }
}

/// Write the merge-patch `Checkpoint` response that records `offset` for one
/// partition, as raw bytes. Byte-identical to serializing a `CaptureState`
/// slice through `Response`, which built two maps and serialized the state
/// twice per message. Flushes, pushing the buffered documents to the runtime.
pub fn write_checkpoint<W: Write>(
    state_key: &str,
    partition: i32,
    offset: i64,
    out: &mut W,
) -> Result<()> {
    use serde_json::ser::{CompactFormatter, Formatter};

    out.write_all(b"{\"checkpoint\":{\"state\":{\"updated\":{\"bindingStateV1\":{")?;
    serde_json::to_writer(&mut *out, state_key).context("writing checkpoint state key")?;
    // Map keys are strings in JSON, so the partition number is quoted.
    out.write_all(b":{\"partitions\":{\"")?;
    CompactFormatter.write_i32(out, partition)?;
    out.write_all(b"\":")?;
    CompactFormatter.write_i64(out, offset)?;
    out.write_all(b"}}}},\"mergePatch\":true}}}\n")
        .context("writing checkpoint response")?;
    out.flush().context("flushing output")
}

/// Context for a payload that failed to parse. The first 64 bytes are enough
/// to inspect the schema registry framing (magic byte, schema ID, and protobuf
/// message indexes).
fn describe_payload(msg: &rdkafka::message::BorrowedMessage<'_>, bytes: &[u8]) -> String {
    format!(
        "parsing message payload for topic {} (partition {}, offset {}, payload length {}, first bytes {})",
        msg.topic(),
        msg.partition(),
        msg.offset(),
        bytes.len(),
        hex::encode(&bytes[..bytes.len().min(64)]),
    )
}

/// The DynamicMessage path for a registry protobuf payload. `framed` is the
/// datum after the magic byte and schema id: message indexes, then the message.
fn dynamic_protobuf_document(
    proto_schema: &ProtobufSchema,
    framed: &[u8],
    key: Option<&Map<String, serde_json::Value>>,
    meta: &Meta,
) -> Result<Vec<u8>> {
    let (indexes, payload_offset) = parse_message_indexes(framed)?;
    let descriptor = resolve_message_from_indexes(
        &proto_schema.descriptor_pool,
        &proto_schema.message_name,
        &indexes,
    )?;
    let message = decode_protobuf_message(&descriptor, &framed[payload_offset..])?;
    let mut buf = Vec::new();
    {
        let mut ser = serde_json::Serializer::new(&mut buf);
        message
            .serialize_with_options(
                MergeSerializer::new(&mut ser, key, meta),
                &SerializeOptions::new().use_proto_field_name(true),
            )
            .context("serializing protobuf message")?;
    }
    Ok(buf)
}

/// The transcoder path. The inner `Err` is the reason the transcoder declined
/// the message, which the caller resolves with the DynamicMessage path.
fn transcoded_protobuf_document(
    plans: &Plans,
    transcoder: &mut Transcoder,
    framed: &[u8],
    key: Option<&Map<String, serde_json::Value>>,
    meta: &Meta,
) -> Result<std::result::Result<Vec<u8>, &'static str>> {
    let (plan, consumed) = plans.plan_for_indexes(framed)?;
    let mut buf = Vec::new();
    match transcoder.transcode(plans, plan, &framed[consumed..], key, meta, &mut buf) {
        Ok(()) => Ok(Ok(buf)),
        Err(TranscodeError::Unsupported(why)) => Ok(Err(why)),
        Err(err @ TranscodeError::Decode(_)) => Err(anyhow::Error::new(err)),
    }
}

/// The transcoder, with the DynamicMessage path for the shapes it declines.
fn transcode_or_fall_back(
    plans: &Plans,
    transcoder: &mut Transcoder,
    proto_schema: &ProtobufSchema,
    framed: &[u8],
    key: Option<&Map<String, serde_json::Value>>,
    meta: &Meta,
    (topic, partition, offset): (&str, i32, i64),
) -> Result<Vec<u8>> {
    match transcoded_protobuf_document(plans, transcoder, framed, key, meta)? {
        Ok(buf) => Ok(buf),
        Err(reason) => {
            tracing::debug!(
                topic,
                partition,
                offset,
                reason,
                "protobuf transcoder declined a message; using the DynamicMessage path"
            );
            dynamic_protobuf_document(proto_schema, framed, key, meta)
        }
    }
}

fn unix_millis_to_rfc3339(millis: i64) -> Result<String> {
    let time = OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(millis);
    Ok(time.format(&format_description::well_known::Rfc3339)?)
}

async fn setup_consumer(
    consumer: &mut BaseConsumer<FlowConsumerContext>,
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
        let res: Resource = serde_json::from_slice(&binding.resource_config_json)?;

        let state_key = &binding.state_key;
        let topic = &res.topic;

        let default_state = ResourceState::default();
        let resource_state = state.resources.get(state_key).unwrap_or(&default_state);

        let partition_info = extant_partitions
            .get(topic)
            .ok_or(anyhow!("configured topic {} does not exist", topic))?;

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

    consumer
        .assign(&topic_partition_list)
        .context("could not assign consumer to topic_partition_list")?;

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

async fn parse_datum<'a>(
    datum: &'a [u8],
    is_key: bool,
    schema_cache: &mut HashMap<u32, RegisteredSchema>,
    schema_client: Option<&SchemaRegistryClient>,
) -> Result<Parsed<'a>> {
    match (schema_client, datum[0]) {
        (Some(schema_client), 0) => {
            // Schema registry is available, and this message was encoded with a
            // schema.
            let schema_id = u32::from_be_bytes(datum[1..5].try_into()?);
            if let Entry::Vacant(e) = schema_cache.entry(schema_id) {
                e.insert(schema_client.fetch_schema(schema_id).await?);
            }

            match schema_cache.get(&schema_id).unwrap() {
                RegisteredSchema::Avro(avro_schema) => {
                    let avro_value =
                        apache_avro::from_avro_datum(avro_schema, &mut &datum[5..], None)?;

                    let is_doc = matches!(avro_value, AvroValue::Map(_) | AvroValue::Record(_));
                    let json_value = avro_to_json(avro_value, avro_schema)?;

                    if is_key && !is_doc {
                        // Handle cases where there is an Avro schema, but it's
                        // not a record type. I'm not sure how common this is in
                        // practice but it's the first thing I tried to do.
                        Ok(Parsed::Value(
                            serde_json::Map::from_iter([("_key".to_string(), json_value)]).into(),
                        ))
                    } else {
                        Ok(Parsed::Value(json_value))
                    }
                }
                RegisteredSchema::Json(_) => Ok(Parsed::Value(serde_json::from_slice(&datum[5..])?)),
                RegisteredSchema::Protobuf(proto_schema) => {
                    if !is_key {
                        // Payloads are transcoded straight to output by
                        // do_pull once the key and _meta are known.
                        return Ok(Parsed::Protobuf { schema_id, datum });
                    }

                    // Parse message indexes (bytes after schema ID)
                    let (indexes, payload_offset) = parse_message_indexes(&datum[5..])?;

                    // Resolve message descriptor using indexes
                    let descriptor = resolve_message_from_indexes(
                        &proto_schema.descriptor_pool,
                        &proto_schema.message_name,
                        &indexes,
                    )?;

                    // Decode to DynamicMessage
                    let message =
                        decode_protobuf_message(&descriptor, &datum[5 + payload_offset..])?;

                    // Convert to JSON using proto field names to match discovered schemas.
                    let json_value: serde_json::Value = message.serialize_with_options(
                        serde_json::value::Serializer,
                        &SerializeOptions::new().use_proto_field_name(true),
                    )?;

                    // For keys that are not objects, wrap in a synthetic _key field
                    if json_value.is_object() {
                        Ok(Parsed::Value(json_value))
                    } else {
                        Ok(Parsed::Value(
                            serde_json::Map::from_iter([("_key".to_string(), json_value)]).into(),
                        ))
                    }
                }
            }
        }
        (None, 0) => {
            // Schema registry is not available, but the data was encoded with a
            // schema. We might as well try to see if the data is a valid JSON
            // document.
            Ok(Parsed::Value(serde_json::from_slice(&datum[5..]).context(
                "received a message with a schema magic byte, but schema registry is not configured and the message is not valid JSON"
            )?))
        }
        (_, _) => {
            // If there is no schema information available for how to parse the
            // document, we make our best guess at parsing into something that
            // would be useful. A present key will always be able to be captured
            // as a base64-encoded string of its bytes. The most reasonable
            // thing to do for a "payload" is to try to parse it as a JSON
            // document.
            if is_key {
                Ok(Parsed::Value(
                    serde_json::Map::from_iter([("_key".to_string(), base64.encode(datum).into())])
                        .into(),
                ))
            } else {
                Ok(Parsed::Value(serde_json::from_slice(datum)?))
            }
        }
    }
}

pub fn avro_to_json(value: AvroValue, schema: &AvroSchema) -> Result<serde_json::Value> {
    Ok(match value {
        AvroValue::Null => json!(null),
        AvroValue::Boolean(v) => json!(v),
        AvroValue::Int(v) => json!(v),
        AvroValue::Long(v) => json!(v),
        AvroValue::Float(v) => match v.is_nan() || v.is_infinite() {
            true => json!(v.to_string()),
            false => json!(v),
        },
        AvroValue::Double(v) => match v.is_nan() || v.is_infinite() {
            true => json!(v.to_string()),
            false => json!(v),
        },
        AvroValue::Bytes(v) => json!(base64.encode(v)),
        AvroValue::String(v) => json!(v),
        AvroValue::Fixed(_, v) => json!(base64.encode(v)),
        AvroValue::Enum(_, v) => json!(v),
        AvroValue::Union(idx, v) => match schema {
            AvroSchema::Union(s) => avro_to_json(*v, &s.variants()[idx as usize])
                .context("failed to decode union value")?,
            _ => anyhow::bail!(
                "expected a union schema for a union value but got {}",
                schema
            ),
        },
        AvroValue::Array(v) => match schema {
            AvroSchema::Array(s) => json!(v
                .into_iter()
                .map(|v| avro_to_json(v, &s.items))
                .collect::<Result<Vec<_>>>()?),
            _ => anyhow::bail!(
                "expected an array schema for an array value but got {}",
                schema
            ),
        },
        AvroValue::Map(v) => match schema {
            AvroSchema::Map(s) => json!(v
                .into_iter()
                .map(|(k, v)| Ok((k, avro_to_json(v, &s.types)?)))
                .collect::<Result<Map<_, _>>>()?),
            _ => anyhow::bail!("expected a map schema for a map value but got {}", schema),
        },
        AvroValue::Record(v) => match schema {
            AvroSchema::Record(s) => json!(v
                .into_iter()
                .zip(s.fields.iter())
                .map(|((k, v), field)| {
                    if k != field.name {
                        anyhow::bail!(
                            "expected record field value with name '{}' but schema had name '{}'",
                            k,
                            field.name,
                        )
                    }
                    Ok((k, avro_to_json(v, &field.schema)?))
                })
                .collect::<Result<Map<_, _>>>()?),
            _ => anyhow::bail!(
                "expected a record schema for a record value but got {}",
                schema
            ),
        },
        AvroValue::Date(v) => {
            let date = OffsetDateTime::UNIX_EPOCH + time::Duration::days(v.into());
            json!(format!(
                "{}-{:02}-{:02}",
                date.year(),
                date.month() as u8,
                date.day()
            ))
        }
        AvroValue::Decimal(v) => match schema {
            AvroSchema::Decimal(s) => json!(BigDecimal::new(v.into(), s.scale as i64).to_string()),
            _ => anyhow::bail!(
                "expected a decimal schema for a decimal value but got {}",
                schema
            ),
        },
        AvroValue::BigDecimal(v) => json!(v.to_string()),
        AvroValue::TimeMillis(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(v as i64);
            json!(format!(
                "{:02}:{:02}:{:02}.{:03}",
                time.hour(),
                time.minute(),
                time.second(),
                time.millisecond()
            ))
        }
        AvroValue::TimeMicros(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::microseconds(v);
            json!(format!(
                "{:02}:{:02}:{:02}.{:06}",
                time.hour(),
                time.minute(),
                time.second(),
                time.microsecond()
            ))
        }
        AvroValue::TimestampMillis(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(v);
            json!(time
                .format(&format_description::well_known::Rfc3339)
                .unwrap())
        }
        AvroValue::TimestampMicros(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::microseconds(v);
            json!(time
                .format(&format_description::well_known::Rfc3339)
                .unwrap())
        }
        AvroValue::TimestampNanos(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::nanoseconds(v);
            json!(time
                .format(&format_description::well_known::Rfc3339)
                .unwrap())
        }
        AvroValue::LocalTimestampMillis(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(v);
            json!(time
                .format(&format_description::well_known::Rfc3339)
                .unwrap())
        }
        AvroValue::LocalTimestampMicros(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::microseconds(v);
            json!(time
                .format(&format_description::well_known::Rfc3339)
                .unwrap())
        }
        AvroValue::LocalTimestampNanos(v) => {
            let time = OffsetDateTime::UNIX_EPOCH + time::Duration::nanoseconds(v);
            json!(time
                .format(&format_description::well_known::Rfc3339)
                .unwrap())
        }
        AvroValue::Duration(v) => {
            json!(duration_to_duration_string(
                v.months().into(),
                v.days().into(),
                v.millis().into()
            ))
        }
        AvroValue::Uuid(v) => json!(v.to_string()),
    })
}

fn duration_to_duration_string(months: u32, days: u32, total_milliseconds: u32) -> String {
    let total_seconds = total_milliseconds / 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let milliseconds = total_milliseconds % 1000;

    let mut duration = String::from("P");
    if months > 0 {
        duration.push_str(&format!("{}M", months));
    }
    if days > 0 {
        duration.push_str(&format!("{}D", days));
    }

    if hours > 0 || minutes > 0 || seconds > 0 || milliseconds > 0 {
        duration.push('T');
        if hours > 0 {
            duration.push_str(&format!("{}H", hours));
        }
        if minutes > 0 {
            duration.push_str(&format!("{}M", minutes));
        }
        if seconds > 0 || milliseconds > 0 {
            if milliseconds > 0 {
                duration.push_str(&format!("{}.{:03}S", seconds, milliseconds));
            } else {
                duration.push_str(&format!("{}S", seconds));
            }
        }
    }

    duration
}

#[cfg(test)]
mod tests {
    use core::{f32, f64};
    use std::{collections::HashMap, i64};

    use super::*;
    use crate::write_capture_response;
    use apache_avro::{
        types::{Record, Value as AvroValue},
        Days, Decimal, Duration, Millis, Months,
    };
    use bigdecimal::num_bigint::ToBigInt;
    use insta::assert_json_snapshot;
    use proto_flow::capture::{
        response::{self, Checkpoint},
        Response,
    };
    use proto_flow::flow::ConnectorState;
    use serde_json::json;

    #[test]
    fn test_avro_to_json() {
        let record_schema_raw = json!({
          "type": "record",
          "name": "test",
          "fields": [
            {"name": "nullField", "type": "null"},
            {"name": "boolField", "type": "boolean"},
            {"name": "intField", "type": "int"},
            {"name": "longField", "type": "long"},
            {"name": "floatField", "type": "float"},
            {"name": "floatFieldNaN", "type": "float"},
            {"name": "floatFieldPosInf", "type": "float"},
            {"name": "floatFieldNegInf", "type": "float"},
            {"name": "doubleField", "type": "double"},
            {"name": "doubleFieldNaN", "type": "double"},
            {"name": "doubleFieldPosInf", "type": "double"},
            {"name": "doubleFieldNegInf", "type": "double"},
            {"name": "bytesField", "type": "bytes"},
            {"name": "stringField", "type": "string"},
            {"name": "nullableStringField", "type": ["null", "string"]},
            {"name": "fixedBytesField", "type": {"type": "fixed", "name": "foo", "size": 5}},
            {"name": "enumField", "type": {"type": "enum", "name": "foo", "symbols": ["a", "b", "c"]}},
            {"name": "arrayField", "type": "array", "items": "string"},
            {"name": "mapField", "type": "map", "values": "string"},
            {"name": "dateField", "type": "int", "logicalType": "date"},
            {"name": "decimalField", "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3},
            {"name": "fixedDecimalField", "type":{"type": "fixed", "size": 2, "name": "decimal"}, "logicalType": "decimal", "precision": 4, "scale": 2},
            {"name": "timeMillisField", "type": "int", "logicalType": "time-millis"},
            {"name": "timeMicrosField", "type": "long", "logicalType": "time-micros"},
            {"name": "timestampMillisField", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "timestampMicrosField", "type": "long", "logicalType": "timestamp-micros"},
            {"name": "localTimestampMillisField", "type": "long", "logicalType": "local-timestamp-millis"},
            {"name": "localTimestampMicrosField", "type": "long", "logicalType": "local-timestamp-micros"},
            {"name": "durationField", "type": {"type": "fixed", "size": 12, "name": "duration"}, "logicalType": "duration"},
            {"name": "uuidField", "type": "string", "logicalType": "uuid"},
            {"name": "nestedRecordField", "type": {"type": "record", "name": "nestedRecord", "fields": [
                {"name": "nestedStringField", "type": "string"},
                {"name": "nestedLongField", "type": "long"},
            ]}}
          ]
        });

        let nested_record_schema_raw = json!({
          "type": "record",
          "name": "nestedRecord",
          "fields": [
            {"name": "nestedStringField", "type": "string"},
            {"name": "nestedLongField", "type": "long"},
          ]
        });

        let record_schema_parsed = AvroSchema::parse(&record_schema_raw).unwrap();
        let nested_record_schema_parsed = AvroSchema::parse(&nested_record_schema_raw).unwrap();
        let mut nested_record = Record::new(&nested_record_schema_parsed).unwrap();
        nested_record.put("nestedStringField", "nested string value");
        nested_record.put("nestedLongField", 123);

        let mut record = Record::new(&record_schema_parsed).unwrap();
        record.put("nullField", AvroValue::Null);
        record.put("boolField", true);
        record.put("intField", i32::MAX);
        record.put("longField", i64::MAX);
        record.put("floatField", f32::MAX);
        record.put("floatFieldNaN", f32::NAN);
        record.put("floatFieldPosInf", f32::INFINITY);
        record.put("floatFieldNegInf", f32::NEG_INFINITY);
        record.put("doubleField", f32::MAX);
        record.put("doubleFieldNaN", f64::NAN);
        record.put("doubleFieldPosInf", f64::INFINITY);
        record.put("doubleFieldNegInf", f64::NEG_INFINITY);
        record.put("bytesField", vec![104, 101, 108, 108, 111]);
        record.put("stringField", "hello");
        record.put("nullableStringField", AvroValue::Null);
        record.put("fixedBytesField", vec![104, 101, 108, 108, 111]);
        record.put("enumField", "b");
        record.put(
            "arrayField",
            AvroValue::Array(vec![
                AvroValue::String("first".into()),
                AvroValue::String("second".into()),
            ]),
        );
        record.put(
            "mapField",
            HashMap::from([("key".to_string(), "value".to_string())]),
        );
        record.put("dateField", 123);
        record.put(
            "decimalField",
            Decimal::from((-32442.to_bigint().unwrap()).to_signed_bytes_be()),
        );
        record.put(
            "fixedDecimalField",
            Decimal::from(9936.to_bigint().unwrap().to_signed_bytes_be()),
        );
        record.put("timeMillisField", AvroValue::TimeMillis(73_800_000));
        record.put("timeMicrosField", AvroValue::TimeMicros(73_800_000 * 1000));
        record.put(
            "timestampMillisField",
            AvroValue::TimestampMillis(1_730_233_606 * 1000),
        );
        record.put(
            "timestampMicrosField",
            AvroValue::TimestampMicros(1_730_233_606 * 1000 * 1000),
        );
        record.put(
            "localTimestampMillisField",
            AvroValue::TimestampMillis(1_730_233_606 * 1000),
        );
        record.put(
            "localTimestampMicrosField",
            AvroValue::TimestampMicros(1_730_233_606 * 1000 * 1000),
        );
        record.put(
            "durationField",
            Duration::new(Months::new(6), Days::new(14), Millis::new(73_800_000)),
        );
        record.put("uuidField", uuid::Uuid::nil());
        record.put("nestedRecordField", nested_record);

        assert_json_snapshot!(avro_to_json(record.into(), &record_schema_parsed).unwrap());
    }

    #[test]
    fn test_duration_to_duration_string() {
        let test_cases = [
            (2, 0, 0, "P2M"),
            (0, 5, 0, "P5D"),
            (0, 8, 0, "P8D"),
            (0, 0, 3661001, "PT1H1M1.001S"),
            (1, 2, 3661001, "P1M2DT1H1M1.001S"),
            (1, 2, 3661000, "P1M2DT1H1M1S"),
            (0, 0, 3000, "PT3S"),
            (0, 0, 120000, "PT2M"),
            (0, 0, 3600000, "PT1H"),
        ];

        for (months, days, milliseconds, want) in test_cases {
            assert_eq!(
                duration_to_duration_string(months, days, milliseconds),
                want
            )
        }
    }

    #[derive(Default)]
    struct FlushCountingWriter {
        buf: Vec<u8>,
        flushes: usize,
    }

    impl std::io::Write for FlushCountingWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.buf.extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn test_write_capture_response_buffering() {
        let mut out = FlushCountingWriter::default();

        // A captured document is buffered (not flushed) and written as exactly
        // one newline-terminated line.
        write_capture_response(
            Response {
                captured: Some(response::Captured {
                    binding: 0,
                    doc_json: r#"{"a":1}"#.to_string().into(),
                }),
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        assert_eq!(out.flushes, 0, "captured documents must not flush");
        let text = String::from_utf8(out.buf.clone()).unwrap();
        assert_eq!(text.matches('\n').count(), 1);
        assert!(text.ends_with('\n'));

        // A checkpoint flushes, pushing itself and all preceding buffered
        // documents to the runtime.
        write_capture_response(
            Response {
                checkpoint: Some(Checkpoint {
                    state: Some(ConnectorState {
                        updated_json: "{}".to_string().into(),
                        merge_patch: true,
                    }),
                }),
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        assert_eq!(out.flushes, 1, "a checkpoint must flush");
    }

    fn checkpoint_via_response(state_key: &str, partition: i32, offset: i64) -> Vec<u8> {
        use proto_flow::capture::response::Checkpoint;
        use proto_flow::flow::ConnectorState;
        let state = CaptureState::state_slice(state_key, partition, offset);
        let mut out = Vec::new();
        write_capture_response(
            Response {
                checkpoint: Some(Checkpoint {
                    state: Some(ConnectorState {
                        updated_json: serde_json::to_string(&state).unwrap().into(),
                        merge_patch: true,
                    }),
                }),
                ..Default::default()
            },
            &mut out,
        )
        .unwrap();
        out
    }

    fn checkpoint_via_raw(state_key: &str, partition: i32, offset: i64) -> Vec<u8> {
        let mut out = Vec::new();
        write_checkpoint(state_key, partition, offset, &mut out).unwrap();
        out
    }

    const STATE_KEYS: &[&str] = &[
        "topic",
        "",
        "näme/with \"quotes\" and \\ and a\u{0}control\ttab",
        "☃ 🦀",
    ];

    #[test]
    fn checkpoint_matches_response_serialization() {
        for state_key in STATE_KEYS {
            for partition in [0, 11, i32::MAX, -1] {
                for offset in [0i64, 1_234_567_890, i64::MAX, -1] {
                    assert_eq!(
                        String::from_utf8_lossy(&checkpoint_via_raw(state_key, partition, offset)),
                        String::from_utf8_lossy(&checkpoint_via_response(
                            state_key, partition, offset
                        )),
                        "{state_key:?} {partition} {offset}"
                    );
                }
            }
        }
    }

    /// The runtime's decoder must read back the exact state slice.
    #[test]
    fn checkpoint_round_trips_through_the_response_decoder() {
        for state_key in STATE_KEYS {
            let line = checkpoint_via_raw(state_key, 7, 99);
            let response: Response = serde_json::from_slice(&line).unwrap();
            let state = response.checkpoint.unwrap().state.unwrap();
            assert!(state.merge_patch);
            let parsed: CaptureState = serde_json::from_slice(&state.updated_json).unwrap();
            assert_eq!(parsed.resources.len(), 1);
            assert_eq!(
                parsed.resources[*state_key].partitions,
                HashMap::from([(7, 99)])
            );
        }
    }

    #[test]
    fn checkpoint_flushes() {
        let mut out = FlushCountingWriter::default();
        write_checkpoint("t", 0, 0, &mut out).unwrap();
        assert_eq!(out.flushes, 1, "a checkpoint must flush");
    }

    // --- protobuf wiring ---------------------------------------------------

    fn test_meta() -> Meta {
        Meta {
            topic: "t".to_string(),
            partition: 3,
            offset: 42,
            op: "u".to_string(),
            headers: None,
            timestamp: None,
        }
    }

    fn pb_varint(mut v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while v >= 0x80 {
            out.push((v & 0x7F) as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
        out
    }

    fn pb_len(number: u32, payload: &[u8]) -> Vec<u8> {
        [
            pb_varint(((number as u64) << 3) | 2),
            pb_varint(payload.len() as u64),
            payload.to_vec(),
        ]
        .concat()
    }

    fn pb_vi(number: u32, v: u64) -> Vec<u8> {
        [pb_varint((number as u64) << 3), pb_varint(v)].concat()
    }

    /// Both protobuf paths on the same framed datum: the same bytes, or both
    /// fail.
    fn assert_paths_agree(
        label: &str,
        plans: &Plans,
        transcoder: &mut Transcoder,
        schema: &ProtobufSchema,
        framed: &[u8],
        key: Option<&Map<String, serde_json::Value>>,
    ) {
        let meta = test_meta();
        let expected = dynamic_protobuf_document(schema, framed, key, &meta);
        let got =
            transcode_or_fall_back(plans, transcoder, schema, framed, key, &meta, ("t", 3, 42));
        match (expected, got) {
            (Ok(e), Ok(g)) => assert_eq!(
                String::from_utf8_lossy(&g),
                String::from_utf8_lossy(&e),
                "{label}"
            ),
            (Err(_), Err(_)) => {}
            (e, g) => panic!("{label}: dynamic {e:?} vs transcoder {g:?}"),
        }
    }

    /// Every Confluent message-index framing `do_pull` can see, through the
    /// real wiring rather than the transcoder's own tests. Index arrays are
    /// zigzag varints: a length, then indexes into the file's top-level
    /// messages (Nested, Everything, Kinds, Tree, Sparse) and then into
    /// nested messages.
    #[test]
    fn transcoder_wiring_matches_the_dynamic_path() {
        use crate::document::differential as corpus;
        use prost_reflect::prost::Message as _;
        use prost_reflect::Value as PValue;

        let pool = corpus::pool();
        let schema = ProtobufSchema {
            descriptor_pool: pool.clone(),
            message_name: "differential.Everything".to_string(),
        };
        let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
        let mut transcoder = Transcoder::new();
        // Map-free, so the dynamic path's bytes are deterministic.
        let everything = corpus::everything(
            &pool,
            vec![
                ("f_int32", PValue::I32(7)),
                ("f_string", PValue::String("payload".to_string())),
                ("_meta", PValue::String("dropped".to_string())),
                (
                    "f_message",
                    PValue::Message(corpus::nested(&pool, "n", 0.5, vec![1.5])),
                ),
            ],
        )
        .encode_to_vec();
        let key: Map<String, serde_json::Value> = json!({"f_string": "key-wins", "id": 7})
            .as_object()
            .unwrap()
            .clone();

        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("no-indexes", everything.clone()),
            ("zero-byte", [vec![0x00], everything.clone()].concat()),
            (
                "array-of-zero",
                [vec![0x02, 0x00], everything.clone()].concat(),
            ),
            (
                "padded-zero-length",
                [vec![0x80, 0x00], everything.clone()].concat(),
            ),
            (
                "top-level-index-1",
                [vec![0x02, 0x02], everything.clone()].concat(),
            ),
            (
                "nested-map-entry-type",
                [vec![0x04, 0x02, 0x00], pb_len(1, b"k"), pb_len(2, b"v")].concat(),
            ),
            (
                "top-level-index-3-tree",
                [
                    vec![0x02, 0x06],
                    pb_len(1, b"root"),
                    pb_len(2, &pb_len(1, b"leaf")),
                ]
                .concat(),
            ),
            (
                "index-out-of-bounds",
                [vec![0x02, 0x0A], everything.clone()].concat(),
            ),
            (
                "negative-array-length",
                [vec![0x01], everything.clone()].concat(),
            ),
            (
                "negative-index",
                [vec![0x02, 0x01], everything.clone()].concat(),
            ),
            ("truncated-index-array", vec![0x04, 0x02]),
            ("decode-error", [vec![0x00], pb_vi(17, 1)].concat()),
            ("empty-message", vec![0x00]),
        ];
        for (label, framed) in cases {
            assert_paths_agree(label, &plans, &mut transcoder, &schema, &framed, None);
            assert_paths_agree(
                &format!("{label}/key"),
                &plans,
                &mut transcoder,
                &schema,
                &framed,
                Some(&key),
            );
        }
    }

    /// A declined message runs the dynamic path with the key and `_meta`
    /// merged, exactly as a transcoded one would be.
    #[test]
    fn declined_messages_fall_back_with_the_key_merged() {
        let set = protox::compile(
            ["differential_corpus_proto2.proto"],
            [concat!(env!("CARGO_MANIFEST_DIR"), "/src/testdata")],
        )
        .unwrap();
        let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(set).unwrap();
        let schema = ProtobufSchema {
            descriptor_pool: pool.clone(),
            message_name: "differential2.Plain".to_string(),
        };
        let plans = Plans::for_schema(&pool, "differential2.Plain").unwrap();
        let mut transcoder = Transcoder::new();
        let meta = test_meta();
        let key: Map<String, serde_json::Value> = json!({"a": "key", "_meta": {"k": 1}})
            .as_object()
            .unwrap()
            .clone();
        // Legacy is the file's second message; index array [1].
        let legacy = [vec![0x02, 0x02], pb_vi(1, 5), pb_len(100, b"ext")].concat();

        let declined =
            transcoded_protobuf_document(&plans, &mut transcoder, &legacy, Some(&key), &meta)
                .unwrap();
        assert_eq!(declined, Err("message with extensions"));

        let got = transcode_or_fall_back(
            &plans,
            &mut transcoder,
            &schema,
            &legacy,
            Some(&key),
            &meta,
            ("t", 3, 42),
        )
        .unwrap();
        let expected = dynamic_protobuf_document(&schema, &legacy, Some(&key), &meta).unwrap();
        assert_eq!(got, expected);
        let got = String::from_utf8(got).unwrap();
        assert!(
            got.contains(r#""a":"key""#) && got.contains(r#""_meta":{"k":1}"#),
            "{got}"
        );
    }

    /// The flag as `do_pull` reads it: parsed from the endpoint config JSON
    /// and looked up by the name in `FEATURE_FLAG_DEFAULTS`.
    #[test]
    fn transcoder_flag_resolves_from_the_endpoint_config() {
        for (advanced, expected) in [
            (json!(null), false),
            (json!({}), false),
            (json!({"feature_flags": null}), false),
            (json!({"feature_flags": ""}), false),
            (json!({"feature_flags": "protobuf_transcoder"}), true),
            (
                json!({"feature_flags": " protobuf_transcoder , other"}),
                true,
            ),
            (json!({"feature_flags": "no_protobuf_transcoder"}), false),
            (
                json!({"feature_flags": "protobuf_transcoder,no_protobuf_transcoder"}),
                false,
            ),
            (
                json!({"feature_flags": "no_protobuf_transcoder,protobuf_transcoder"}),
                true,
            ),
            (json!({"feature_flags": "no_no_protobuf_transcoder"}), false),
            (json!({"feature_flags": "other_flag"}), false),
        ] {
            let config: EndpointConfig = serde_json::from_value(json!({
                "bootstrap_servers": "localhost:9092",
                "schema_registry": {
                    "schema_registry_type": "no_schema_registry",
                    "enable_json_only": true
                },
                "advanced": advanced,
            }))
            .unwrap();
            let flags = config.feature_flags(FEATURE_FLAG_DEFAULTS);
            assert_eq!(flags["protobuf_transcoder"], expected, "{advanced}");
        }
    }
}
