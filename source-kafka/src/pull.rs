use crate::{
    configuration::{EndpointConfig, FlowConsumerContext, Resource, SchemaRegistryConfig},
    schema_registry::{RegisteredSchema, SchemaRegistryClient},
    write_capture_response,
};
use anyhow::{anyhow, Context, Result};
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use bigdecimal::BigDecimal;
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
    consumer::{BaseConsumer, Consumer},
    message::Headers,
    metadata::MetadataPartition,
    Message, Offset, Timestamp, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map};
use std::collections::{hash_map::Entry, HashMap};
use time::{format_description, OffsetDateTime};

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

pub async fn do_pull(req: Open, mut stdout: std::io::Stdout) -> Result<()> {
    let spec = req.capture.expect("open must contain a capture spec");

    let state = if req.state_json == "{}" {
        CaptureState::default()
    } else {
        serde_json::from_slice(&req.state_json)?
    };

    let config: EndpointConfig = serde_json::from_slice(&spec.config_json)?;
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

    let topics_to_bindings =
        setup_consumer(&mut consumer, state, &spec.bindings, &req.range).await?;

    loop {
        let msg = consumer
            .poll(None)
            .expect("polling without a timeout should always produce a message")
            .context("receiving next message")?;

        let mut op = "u";
        let mut doc = match msg.payload() {
            Some(bytes) => parse_datum(bytes, false, &mut schema_cache, schema_client.as_ref())
                .await
                .with_context(|| format!("parsing message payload for topic {}", msg.topic()))?,
            None => {
                // We interpret an absent message payload as a deletion
                // tombstone. The captured document will otherwise be empty
                // except for the _meta field and the message key (if present).
                op = "d";
                json!({})
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

        let captured = doc.as_object_mut().unwrap();
        captured.insert("_meta".to_string(), serde_json::to_value(meta).unwrap());

        if let Some(key_bytes) = msg.key() {
            let mut key_parsed =
                parse_datum(key_bytes, true, &mut schema_cache, schema_client.as_ref())
                    .await
                    .with_context(|| format!("parsing message key for topic {}", msg.topic()))?;

            // Add key/val pairs from the "key" to root of the captured
            // document, which will clobber any collisions with keys from
            // the parsed payload.
            captured.append(key_parsed.as_object_mut().unwrap());
        }

        let binding_info = topics_to_bindings
            .get(msg.topic())
            .with_context(|| format!("got a message for unknown topic {}", msg.topic()))?;

        let message = response::Captured {
            binding: binding_info.binding_index,
            doc_json: serde_json::to_string(&captured)?.into(),
        };

        let checkpoint =
            CaptureState::state_slice(&binding_info.state_key, msg.partition(), msg.offset());

        write_capture_response(
            Response {
                captured: Some(message),
                ..Default::default()
            },
            &mut stdout,
        )?;

        write_capture_response(
            Response {
                checkpoint: Some(Checkpoint {
                    state: Some(ConnectorState {
                        updated_json: serde_json::to_string(&checkpoint)?.into(),
                        merge_patch: true,
                    }),
                }),
                ..Default::default()
            },
            &mut stdout,
        )?;
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

async fn parse_datum(
    datum: &[u8],
    is_key: bool,
    schema_cache: &mut HashMap<u32, RegisteredSchema>,
    schema_client: Option<&SchemaRegistryClient>,
) -> Result<serde_json::Value> {
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
                        Ok(serde_json::Map::from_iter([("_key".to_string(), json_value)]).into())
                    } else {
                        Ok(json_value)
                    }
                }
                RegisteredSchema::Json(_) => Ok(serde_json::from_slice(&datum[5..])?),
                RegisteredSchema::Protobuf(proto_schema) => {
                    // Parse message indexes (bytes after schema ID)
                    let (indexes, payload_offset) = crate::protobuf::parse_message_indexes(&datum[5..])?;

                    // Resolve message descriptor using indexes
                    let descriptor = crate::protobuf::resolve_message_from_indexes(
                        &proto_schema.descriptor_pool,
                        &proto_schema.message_name,
                        &indexes,
                    )?;

                    // Decode to DynamicMessage
                    let message = crate::protobuf::decode_protobuf_message(&descriptor, &datum[5 + payload_offset..])?;

                    // Convert to JSON using proto field names (snake_case) to match discovered schemas
                    let json_value: serde_json::Value = {
                        let mut buf = Vec::new();
                        let mut serializer = serde_json::Serializer::new(&mut buf);
                        message.serialize_with_options(
                            &mut serializer,
                            &prost_reflect::SerializeOptions::new().use_proto_field_name(true),
                        )?;
                        serde_json::from_slice(&buf)?
                    };

                    // For keys that are not objects, wrap in a synthetic _key field
                    if is_key && !json_value.is_object() {
                        Ok(serde_json::Map::from_iter([("_key".to_string(), json_value)]).into())
                    } else {
                        Ok(json_value)
                    }
                }
            }
        }
        (None, 0) => {
            // Schema registry is not available, but the data was encoded with a
            // schema. We might as well try to see if the data is a valid JSON
            // document.
            Ok(serde_json::from_slice(&datum[5..]).context(
                "received a message with a schema magic byte, but schema registry is not configured and the message is not valid JSON"
            )?)
        }
        (_, _) => {
            // If there is no schema information available for how to parse the
            // document, we make our best guess at parsing into something that
            // would be useful. A present key will always be able to be captured
            // as a base64-encoded string of its bytes. The most reasonable
            // thing to do for a "payload" is to try to parse it as a JSON
            // document.
            if is_key {
                Ok(
                    serde_json::Map::from_iter([("_key".to_string(), base64.encode(datum).into())])
                        .into(),
                )
            } else {
                Ok(serde_json::from_slice(datum)?)
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
    use apache_avro::{
        types::{Record, Value as AvroValue},
        Days, Decimal, Duration, Millis, Months,
    };
    use bigdecimal::num_bigint::ToBigInt;
    use insta::assert_json_snapshot;
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
}
