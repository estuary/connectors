use anyhow::{Context, Result};
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;
use bigdecimal::BigDecimal;
use core::str;
use rdkafka::{
    admin::{AdminClient, AdminOptions},
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message, TopicPartitionList,
};
use serde_json::{json, Map};
use std::collections::HashMap;
use time::{format_description, OffsetDateTime};

const BOOTSTRAP_SERVERS: &str = "localhost:9092";
const SCHEMA_REGISTRY_ENDPOINT: &str = "http://localhost:8081";
static KAFKA_INTERNAL_TOPICS: [&str; 4] = [
    "__consumer_offsets",
    "__amazon_msk_canary",
    "_schemas",
    "__transaction_state",
];

#[test]
fn test_spec() {
    let output = std::process::Command::new("flowctl")
        .args([
            "raw",
            "spec",
            "--source",
            "tests/test.flow.yaml",
            "--name",
            "acmeCo/materialize-kafka/avro",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let got: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    insta::assert_snapshot!(serde_json::to_string_pretty(&got).unwrap());
}

#[tokio::test]
async fn test_materialization() {
    drop_topics().await;

    for name in [
        "acmeCo/materialize-kafka/avro",
        "acmeCo/materialize-kafka/json",
    ] {
        let output = std::process::Command::new("flowctl")
            .args([
                "preview",
                "--source",
                "tests/test.flow.yaml",
                "--fixture",
                "tests/fixture.json",
                "--name",
                name,
            ])
            .output()
            .unwrap();

        println!("{}", str::from_utf8(&output.stderr).unwrap());
        assert!(output.status.success());
    }

    insta::assert_snapshot!(snapshot_topics().await);
}

async fn drop_topics() {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .create()
        .unwrap();

    let topics = list_topics().await;
    let topics = topics.iter().map(String::as_str).collect::<Vec<&str>>();
    if !topics.is_empty() {
        admin
            .delete_topics(&topics, &AdminOptions::default())
            .await
            .unwrap();
    }
}

async fn list_topics() -> Vec<String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .create()
        .unwrap();

    let meta = consumer.fetch_metadata(None, None).unwrap();
    let mut topics = meta
        .topics()
        .iter()
        .filter(|t| !KAFKA_INTERNAL_TOPICS.contains(&t.name()))
        .map(|t| t.name().to_string())
        .collect::<Vec<_>>();
    topics.sort();
    topics
}

async fn snapshot_topics() -> String {
    let mut out = String::new();

    for topic in list_topics().await {
        if !out.is_empty() {
            out.push_str("\n");
        }
        out.push_str("************\n");
        out.push_str(format!("Topic: {}\n", topic).as_str());
        out.push_str("************\n");
        out.push_str(&snapshot_topic(&topic).await);
        out.push_str("\n");
    }

    out
}

async fn snapshot_topic(topic: &str) -> String {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .set("enable.auto.commit", "false")
        .set("group.id", "materialize-kafka")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .create()
        .unwrap();

    let meta = consumer.fetch_metadata(Some(topic), None).unwrap();
    assert!(meta.topics()[0].partitions().len() != 0);
    let assignment =
        meta.topics()[0]
            .partitions()
            .iter()
            .fold(TopicPartitionList::new(), |mut tpl, p| {
                tpl.add_partition(topic, p.id());
                tpl
            });
    consumer.assign(&assignment).unwrap();
    let mut n = consumer.assignment().unwrap().count();

    let mut schema_cache: HashMap<u32, apache_avro::Schema> = HashMap::new();

    let mut rows = Vec::new();
    for msg in consumer.iter() {
        match msg {
            Ok(msg) => {
                let key = parse_datum(msg.key().unwrap(), &mut schema_cache).await;
                let value = parse_datum(msg.payload().unwrap(), &mut schema_cache).await;
                let row = format!("key: {}, value: {}", key, value);
                rows.push(row);
            }
            _ => {
                n -= 1;
                if n == 0 {
                    break;
                }
            }
        }
    }
    rows.sort();
    rows.join("\n")
}

async fn parse_datum(datum: &[u8], schema_cache: &mut HashMap<u32, apache_avro::Schema>) -> String {
    match datum[0] {
        0 => {
            let schema_id = u32::from_be_bytes(datum[1..5].try_into().unwrap());

            if !schema_cache.contains_key(&schema_id) {
                schema_cache.insert(schema_id, fetch_schema(schema_id).await);
            }

            let schema = schema_cache.get(&schema_id).unwrap();
            let avro_value = apache_avro::from_avro_datum(schema, &mut &datum[5..], None).unwrap();

            avro_to_json(avro_value, schema).unwrap().to_string()
        }
        _ => str::from_utf8(datum).unwrap().to_owned(),
    }
}

async fn fetch_schema(id: u32) -> apache_avro::Schema {
    let schema_string = reqwest::get(format!(
        "{SCHEMA_REGISTRY_ENDPOINT}/schemas/ids/{id}/schema"
    ))
    .await
    .unwrap()
    .error_for_status()
    .unwrap()
    .text()
    .await
    .unwrap();

    apache_avro::Schema::parse_str(&schema_string).unwrap()
}

// TODO(whb): This is copied entirely from source-kafka. Consider a better way
// to share code between the two connectors.
fn avro_to_json(value: AvroValue, schema: &AvroSchema) -> Result<serde_json::Value> {
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
