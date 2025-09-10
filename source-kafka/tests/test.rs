use std::time::Duration;

use anyhow::Result;
use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::message::{Header, OwnedHeaders, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::json::JsonEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde_json::json;

#[test]
fn test_spec() {
    let output = std::process::Command::new("flowctl")
        .args(["raw", "spec", "--source", "tests/test.flow.yaml"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let got: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    insta::assert_snapshot!(serde_json::to_string_pretty(&got).unwrap());
}

#[tokio::test]
#[serial_test::serial]
async fn test_discover() {
    setup_test().await;

    let output = std::process::Command::new("flowctl")
        .args([
            "raw",
            "discover",
            "--source",
            "tests/test.flow.yaml",
            "-o",
            "json",
            "--emit-raw",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());

    let snap = std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .map(|line| serde_json::to_string_pretty(&line).unwrap())
        .reduce(|snap, line| format!("{}\n{}", snap, line))
        .unwrap();

    insta::assert_snapshot!(snap);
}

#[tokio::test]
#[serial_test::serial]
async fn test_capture() {
    setup_test().await;

    let output = std::process::Command::new("flowctl")
        .args([
            "preview",
            "--source",
            "tests/test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "2s",
            "--output-state",
        ])
        .output()
        .unwrap();

    println!("{}", std::str::from_utf8(&output.stderr).unwrap());

    assert!(output.status.success());

    let snap = std::str::from_utf8(&output.stdout).unwrap();

    insta::assert_snapshot!(snap);
}

#[tokio::test]
#[serial_test::serial]
async fn test_capture_resume() {
    setup_test().await;

    let initial_state = json!({
      "bindingStateV1": {
        "avro-topic": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        },
        "json-schema-topic": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        },
        "json-raw-topic": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        }
      }
    });

    let output = std::process::Command::new("flowctl")
        .args([
            "preview",
            "--source",
            "tests/test.flow.yaml",
            "--sessions",
            "1",
            "--delay",
            "2s",
            "--output-state",
            "--initial-state",
            &initial_state.to_string(),
        ])
        .output()
        .unwrap();

    assert!(output.status.success());

    // Filter all but the last connectorState line, as they're non-deterministic:
    // Messages across topics may be polled in any order, but the final state is consistent.
    let lines: Vec<&str> = std::str::from_utf8(&output.stdout).unwrap().lines().collect();
    let last_connector_idx = lines.iter().rposition(|l| l.starts_with(r#"["connectorState","#));
    
    let snap = lines.iter().enumerate()
        .filter(|(i, l)| !l.starts_with(r#"["connectorState","#) || Some(*i) == last_connector_idx)
        .map(|(_, l)| *l)
        .collect::<Vec<_>>()
        .join("\n");

    insta::assert_snapshot!(snap);
}

async fn setup_test() {
    let bootstrap_servers = "localhost:9092";
    let schema_registry_endpoint = "http://localhost:8081";
    let num_messages = 9;
    let num_partitions = 3;
    let topic_replication = 1;

    let test_cases: &[(&dyn TestDataEncoder, &str)] = &[
        (&AvroTestDataEncoder::new(), "avro-topic"),
        (&JsonSchemaTestDataEncoder::new(), "json-schema-topic"),
        (&JsonRawTestDataEncoder::new(), "json-raw-topic"),
    ];

    let http = reqwest::Client::default();

    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();

    let opts = AdminOptions::default().request_timeout(Some(Duration::from_secs(1)));

    for (enc, topic) in test_cases {
        admin.delete_topics(&[topic], &opts).await.unwrap();
        admin
            .create_topics(
                &[NewTopic::new(
                    topic,
                    num_partitions,
                    TopicReplication::Fixed(topic_replication),
                )],
                &opts,
            )
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Registry schemas if the encoder uses schemas.
        if let Some(schema_type) = enc.schema_type_string() {
            for (topic, suffix, schema) in [
                (topic, "key", &enc.key_schema_string()),
                (topic, "value", &enc.payload_schema_string()),
            ] {
                // Try to delete the existing schema if it exists, ignoring any errors
                // that are returned, which may be 404's.
                http.delete(format!(
                    "{}/subjects/{}-{}",
                    schema_registry_endpoint, topic, suffix
                ))
                .send()
                .await
                .unwrap();

                // You have to do a "soft" delete before a permanent "hard" delete.
                http.delete(format!(
                    "{}/subjects/{}-{}?permanent=true",
                    schema_registry_endpoint, topic, suffix
                ))
                .send()
                .await
                .unwrap();

                // Register the schema, which must be successful.
                assert!(http
                    .post(format!(
                        "{}/subjects/{}-{}/versions",
                        schema_registry_endpoint, topic, suffix
                    ))
                    .json(&json!({"schema": schema, "schemaType": schema_type}))
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success());
            }
        }

        // Populate regular "data" records.
        for idx in 0..num_messages {
            send_message(
                topic,
                &enc.key_for_idx(idx, topic).await,
                Some(&enc.payload_for_idx(idx, topic).await),
                idx,
                num_partitions,
                &producer,
            )
            .await;
        }

        // Populate deletion records.
        for idx in 0..num_partitions {
            send_message(
                topic,
                &enc.key_for_idx(idx as usize, topic).await,
                None::<&[u8]>,
                idx as usize,
                num_partitions,
                &producer,
            )
            .await;
        }
    }
}

async fn send_message<K, P>(
    topic: &str,
    key: &K,
    payload: Option<&P>,
    idx: usize,
    num_partitions: i32,
    producer: &FutureProducer,
) where
    K: ToBytes + ?Sized,
    P: ToBytes + ?Sized,
{
    let mut rec = FutureRecord::to(topic)
        .partition(idx as i32 % num_partitions)
        .key(key)
        .timestamp(unix_millis_fixture(idx))
        .headers(OwnedHeaders::new().insert(Header {
            key: "header-key",
            value: Some(&format!("header-value-{}", idx)),
        }));

    if let Some(payload) = payload {
        rec = rec.payload(payload);
    }

    producer.send(rec, None).await.unwrap();
}

fn unix_millis_fixture(idx: usize) -> i64 {
    ((idx + 1) * 86_400_000) as i64
}

#[async_trait::async_trait]
trait TestDataEncoder {
    async fn key_for_idx<'a>(&'a self, idx: usize, topic: &'a str) -> Vec<u8>;
    async fn payload_for_idx<'a>(&'a self, idx: usize, topic: &'a str) -> Vec<u8>;
    fn key_schema_string(&self) -> String;
    fn payload_schema_string(&self) -> String;
    fn schema_type_string(&self) -> Option<String>;
}

struct AvroTestDataEncoder {}

impl AvroTestDataEncoder {
    fn new() -> Self {
        AvroTestDataEncoder {}
    }
}

#[async_trait::async_trait]
impl TestDataEncoder for AvroTestDataEncoder {
    async fn key_for_idx<'a>(&'a self, idx: usize, topic: &'a str) -> Vec<u8> {
        let enc = AvroEncoder::new(SrSettings::new(String::from("http://localhost:8081")));
        let schema =
            Schema::parse(&serde_json::from_str(&self.key_schema_string()).unwrap()).unwrap();

        let mut key = Record::new(&schema).unwrap();
        key.put("idx", Value::Int(idx as i32));
        key.put(
            "nested",
            Value::Record(vec![("sub_id".to_string(), Value::Int(idx as i32))]),
        );

        enc.encode_value(
            key.into(),
            &SubjectNameStrategy::TopicNameStrategy(topic.to_string(), true),
        )
        .await
        .unwrap()
    }

    async fn payload_for_idx<'a>(&'a self, idx: usize, topic: &'a str) -> Vec<u8> {
        let enc = AvroEncoder::new(SrSettings::new(String::from("http://localhost:8081")));
        let schema =
            Schema::parse(&serde_json::from_str(&self.payload_schema_string()).unwrap()).unwrap();

        let mut value = Record::new(&schema).unwrap();
        value.put("value", Value::String(format!("value-{}", idx)));

        enc.encode_value(
            value.into(),
            &SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false),
        )
        .await
        .unwrap()
    }

    fn key_schema_string(&self) -> String {
        let parsed = Schema::parse(&json!({
          "type": "record",
          "name": "AvroKey",
          "fields": [
            {
              "name": "idx",
              "type": "int"
            },
            {
              "name": "nested",
              "type": {
                "type": "record",
                "name": "NestedAvroKeyRecord",
                "fields": [
                  {
                    "name": "sub_id",
                    "type": "int"
                  }
                ]
              }
            }
          ]
        }))
        .unwrap();

        parsed.canonical_form()
    }

    fn payload_schema_string(&self) -> String {
        let parsed = Schema::parse(&json!({
          "type": "record",
          "name": "AvroValue",
          "fields": [
            {
              "name": "value",
              "type": "string"
            }
          ]
        }))
        .unwrap();

        parsed.canonical_form()
    }

    fn schema_type_string(&self) -> Option<String> {
        Some("AVRO".to_string())
    }
}

struct JsonSchemaTestDataEncoder {}

impl JsonSchemaTestDataEncoder {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TestDataEncoder for JsonSchemaTestDataEncoder {
    async fn key_for_idx<'a>(&'a self, idx: usize, topic: &'a str) -> Vec<u8> {
        let enc = JsonEncoder::new(SrSettings::new(String::from("http://localhost:8081")));
        enc.encode(
            &json!({
                "idx": idx,
                "nested": {
                    "sub_id": idx
                },
            }),
            SubjectNameStrategy::TopicNameStrategy(topic.to_string(), true),
        )
        .await
        .unwrap()
    }

    async fn payload_for_idx<'a>(&'a self, idx: usize, topic: &'a str) -> Vec<u8> {
        let enc = JsonEncoder::new(SrSettings::new(String::from("http://localhost:8081")));
        enc.encode(
            &json!({
                "value": format!("value-{}", idx),
            }),
            SubjectNameStrategy::TopicNameStrategy(topic.to_string(), false),
        )
        .await
        .unwrap()
    }

    fn key_schema_string(&self) -> String {
        serde_json::to_string(&json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "JsonKey",
            "type": "object",
            "properties": {
              "idx": {
                "type": "integer"
              },
              "nested": {
                "type": "object",
                "title": "NestedJsonKeyRecord",
                "properties": {
                  "sub_id": {
                    "type": "integer"
                  }
                },
                "required": ["sub_id"]
              }
            },
            "required": ["idx", "nested"],
            "additionalProperties": false
        }))
        .unwrap()
    }

    fn payload_schema_string(&self) -> String {
        serde_json::to_string(&json!({
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "JsonValue",
          "type": "object",
          "properties": {
            "value": {
              "type": "string"
            }
          },
          "required": ["value"],
          "additionalProperties": false
        }))
        .unwrap()
    }

    fn schema_type_string(&self) -> Option<String> {
        Some("JSON".to_string())
    }
}

struct JsonRawTestDataEncoder {}

impl JsonRawTestDataEncoder {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TestDataEncoder for JsonRawTestDataEncoder {
    async fn key_for_idx<'a>(&'a self, idx: usize, _: &'a str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "key": idx,
        }))
        .unwrap()
    }

    async fn payload_for_idx<'a>(&'a self, idx: usize, _: &'a str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "payload": idx,
        }))
        .unwrap()
    }

    fn key_schema_string(&self) -> String {
        panic!("not implemented")
    }

    fn payload_schema_string(&self) -> String {
        panic!("not implemented")
    }

    fn schema_type_string(&self) -> Option<String> {
        None
    }
}
