use std::time::Duration;

use anyhow::Result;
use apache_avro::Schema;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::Serialize;
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
async fn test_discover() {
    setup_topics("localhost:9092").await;

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
async fn test_capture() {
    setup_topics("localhost:9092").await;

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

    assert!(output.status.success());

    let snap = std::str::from_utf8(&output.stdout).unwrap();

    insta::assert_snapshot!(snap);
}

#[tokio::test]
async fn test_capture_resume() {
    setup_topics("localhost:9092").await;

    let initial_state = json!({
      "bindingStateV1": {
        "test-topic-1": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        },
        "test-topic-2": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        },
        "test-topic-3": {
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

    let snap = std::str::from_utf8(&output.stdout).unwrap();

    insta::assert_snapshot!(snap);
}

#[derive(Serialize)]
struct AvroKey {
    idx: i32,
    nested: NestedAvroRecord,
}

#[derive(Serialize)]
struct NestedAvroRecord {
    sub_id: i32,
}

async fn setup_topics(bootstrap_servers: &str) {
    let avro_key_schema = json!({
      "type": "record",
      "name": "AvroKey",
      "fields": [
        {"name": "idx", "type": "int"},
        {
          "name": "nested",
          "type": {
            "type": "record",
            "name": "NestedAvroRecord",
            "fields": [
              {"name": "sub_id", "type": "int"}
            ]
          }
        }
      ]
    });

    let avro_key_schema = Schema::parse(&avro_key_schema).unwrap();

    let http = reqwest::Client::default();
    assert!(http
        .post("http://localhost:8081/subjects/test-topic-1-key/versions")
        .json(&json!({"schema": avro_key_schema.canonical_form()}))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());

    let avro_encoder = AvroEncoder::new(SrSettings::new(String::from("http://localhost:8081")));

    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();

    let opts = AdminOptions::default().request_timeout(Some(Duration::from_secs(1)));

    admin
        .delete_topics(&vec!["test-topic-1", "test-topic-2", "test-topic-3"], &opts)
        .await
        .unwrap();

    admin
        .create_topics(
            vec![
                &NewTopic::new("test-topic-1", 3, TopicReplication::Fixed(1)),
                &NewTopic::new("test-topic-2", 3, TopicReplication::Fixed(1)),
                &NewTopic::new("test-topic-3", 3, TopicReplication::Fixed(1)),
            ],
            &opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();

    for idx in 0..9 {
        let topic = "test-topic-1";
        let key = avro_encoder
            .encode_struct(
                AvroKey {
                    idx,
                    nested: NestedAvroRecord { sub_id: idx },
                },
                &SubjectNameStrategy::TopicNameStrategy(topic.to_string(), true),
            )
            .await
            .unwrap();

        producer
            .send(
                FutureRecord::to(topic)
                    .partition(idx % 3)
                    .key(&key)
                    .payload(&json!({"payload": idx}).to_string())
                    .headers(OwnedHeaders::new().insert(Header {
                        key: "header-key",
                        value: Some(&format!("header-value-{}", idx)),
                    })),
                None,
            )
            .await
            .unwrap();
    }

    for topic in ["test-topic-2", "test-topic-3"] {
        for idx in 0..9 {
            producer
                .send(
                    FutureRecord::to(topic)
                        .partition(idx % 3)
                        .key(&json!({"key": idx}).to_string())
                        .payload(&json!({"payload": idx}).to_string())
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header-key",
                            value: Some(&format!("header-value-{}", idx)),
                        })),
                    None,
                )
                .await
                .unwrap();
        }
    }
}
