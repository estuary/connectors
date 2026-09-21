use std::io::Write;
use std::process::{Command, Stdio};
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

// Waits in this file are bounded and named. An unbounded one does not fail the
// job, it holds it until GitHub's six-hour ceiling, reporting nothing.

/// Await `fut`, failing with `what` if it outlives `secs`.
async fn deadline<F: std::future::Future>(what: &str, secs: u64, fut: F) -> F::Output {
    match tokio::time::timeout(Duration::from_secs(secs), fut).await {
        Ok(v) => v,
        Err(_) => panic!("timed out after {secs}s waiting for: {what}"),
    }
}

/// reqwest's default client has no timeout at all.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

/// tests/test.flow.yaml, with the connector command rewritten to the binary
/// cargo built for this run.
///
/// The checked-in `cargo run` would nest a second cargo inside `cargo test`,
/// which resolves features differently and so rebuilds what the other just
/// built, while holding target/debug/.cargo-lock. Generating beside the
/// original keeps its relative `import:` valid.
fn catalog() -> &'static str {
    static PATH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PATH.get_or_init(|| {
        const GENERATED: &str = "tests/test.generated.flow.yaml";
        let source = std::fs::read_to_string("tests/test.flow.yaml").unwrap();

        let anchor = "        command:\n          - cargo\n          - run\n";
        assert!(
            source.contains(anchor),
            "tests/test.flow.yaml no longer declares `cargo run` as the connector \
             command; update catalog() to match",
        );
        let rewritten = source.replace(
            anchor,
            &format!(
                "        command:\n          - {}\n",
                env!("CARGO_BIN_EXE_source-kafka")
            ),
        );
        std::fs::write(GENERATED, rewritten).unwrap();
        GENERATED.to_string()
    })
}

/// Run `program` under coreutils `timeout`, capturing output to files.
///
/// `timeout` bounds the child and signals its whole process group. Files rather
/// than pipes because `Command::output` waits for EOF, not for exit, so a
/// grandchild holding stderr blocks the read; flowctl spawns the connector, so
/// grandchildren are the norm.
fn bounded_command(what: &str, secs: u64, program: &str, args: &[&str]) -> std::process::Output {
    let dir = std::env::temp_dir();
    let stem = uuid::Uuid::new_v4();
    let (out_path, err_path) = (
        dir.join(format!("kafka-test-{stem}.out")),
        dir.join(format!("kafka-test-{stem}.err")),
    );
    let out_file = std::fs::File::create(&out_path).unwrap();
    let err_file = std::fs::File::create(&err_path).unwrap();

    let status = Command::new("timeout")
        .args(["--signal=TERM", "--kill-after=10", &secs.to_string(), program])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn {program} for {what}: {e}"))
        .wait()
        .unwrap_or_else(|e| panic!("failed to wait for {program} during {what}: {e}"));

    let stdout = std::fs::read(&out_path).unwrap();
    let stderr = std::fs::read(&err_path).unwrap();
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(&err_path);

    // `timeout` exits 124 when it had to fire.
    if status.code() == Some(124) {
        panic!(
            "`{program}` exceeded {secs}s during {what}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&stderr)
        );
    }

    std::process::Output { status, stdout, stderr }
}

#[test]
#[serial_test::serial]
fn test_spec() {
    let output = bounded_command(
        "test_spec: flowctl raw spec",
        120,
        "flowctl",
        &["raw", "spec", "--source", catalog()],
    );

    assert!(output.status.success());
    let got: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    insta::assert_snapshot!(serde_json::to_string_pretty(&got).unwrap());
}

#[tokio::test]
#[serial_test::serial]
async fn test_discover() {
    setup_test().await;

    let output = bounded_command(
        "test_discover: flowctl raw discover",
        300,
        "flowctl",
        &[
            "--profile",
            "local",
            "raw",
            "discover",
            "--source",
            catalog(),
            "-o",
            "json",
            "--emit-raw",
        ],
    );

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

    let output = bounded_command(
        "test_capture: flowctl raw preview-next",
        300,
        "flowctl",
        &[
            "--profile",
            "local",
            "raw",
            "preview-next",
            "--source",
            catalog(),
            "--sessions",
            "1",
            "--delay",
            "2s",
            "--output-state",
        ],
    );

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
        },
        "protobuf-topic": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        },
        "protobuf-ref-topic": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        },
        "protobuf-wkt-topic": {
          "partitions": {
            "1": 1,
            "2": 1
          }
        }
      }
    });

    let initial_state = initial_state.to_string();
    let output = bounded_command(
        "test_capture_resume: flowctl raw preview-next",
        300,
        "flowctl",
        &[
            "--profile",
            "local",
            "raw",
            "preview-next",
            "--source",
            catalog(),
            "--sessions",
            "1",
            "--delay",
            "2s",
            "--output-state",
            "--initial-state",
            &initial_state,
        ],
    );

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

    // Test cases using the TestDataEncoder trait
    let test_cases: &[(&dyn TestDataEncoder, &str)] = &[
        (&AvroTestDataEncoder::new(), "avro-topic"),
        (&JsonSchemaTestDataEncoder::new(), "json-schema-topic"),
        (&JsonRawTestDataEncoder::new(), "json-raw-topic"),
    ];

    let http = http_client();

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

        // Register schemas if the encoder uses schemas.
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

    // Protobuf test case: uses Confluent's official kafka-protobuf-console-producer
    // to validate our decoder against Confluent's actual wire format implementation.
    setup_protobuf_test(
        &admin,
        &opts,
        &http,
        schema_registry_endpoint,
        num_messages,
        num_partitions,
        topic_replication,
        &producer,
    )
    .await;

    // Protobuf with schema references test case: registers a shared proto as a separate
    // subject, then registers the main schema with a references array pointing to it.
    // Verifies the connector can recursively resolve references and decode messages.
    setup_protobuf_ref_test(
        &admin,
        &opts,
        &http,
        schema_registry_endpoint,
        num_messages,
        num_partitions,
        topic_replication,
        &producer,
    )
    .await;

    // Protobuf schema importing a google well-known type (timestamp.proto).
    // Regression test for the DescriptorPool well-known types fix.
    setup_protobuf_wkt_test(
        &admin,
        &opts,
        &http,
        schema_registry_endpoint,
        num_messages,
        num_partitions,
        topic_replication,
        &producer,
    )
    .await;

    // Avro schema with references (unsupported). Regression test for discovery
    // resilience: one unsupported schema must not fail discovery of the others.
    setup_avro_ref_test(&admin, &opts, &http, schema_registry_endpoint, num_partitions, topic_replication).await;
}

async fn setup_protobuf_test(
    admin: &AdminClient<rdkafka::client::DefaultClientContext>,
    opts: &AdminOptions,
    http: &reqwest::Client,
    schema_registry_endpoint: &str,
    num_messages: usize,
    num_partitions: i32,
    topic_replication: i32,
    producer: &FutureProducer,
) {
    let topic = "protobuf-topic";
    let temp_topic = format!("protobuf-temp-{}", std::process::id());
    let enc = ProtobufTestDataEncoder::new();

    // Delete and recreate the main topic
    admin.delete_topics(&[topic], opts).await.unwrap();
    admin
        .create_topics(
            &[NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(topic_replication),
            )],
            opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Delete and recreate temp topic for capturing Confluent-encoded bytes
    let _ = admin.delete_topics(&[&temp_topic], opts).await;
    admin
        .create_topics(
            &[NewTopic::new(
                &temp_topic,
                1, // Single partition for predictable ordering
                TopicReplication::Fixed(topic_replication),
            )],
            opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Delete existing schemas
    for suffix in ["key", "value"] {
        http.delete(format!(
            "{}/subjects/{}-{}",
            schema_registry_endpoint, topic, suffix
        ))
        .send()
        .await
        .unwrap();

        http.delete(format!(
            "{}/subjects/{}-{}?permanent=true",
            schema_registry_endpoint, topic, suffix
        ))
        .send()
        .await
        .unwrap();
    }

    // Also delete temp topic schemas
    for suffix in ["key", "value"] {
        http.delete(format!(
            "{}/subjects/{}-{}",
            schema_registry_endpoint, temp_topic, suffix
        ))
        .send()
        .await
        .unwrap();

        http.delete(format!(
            "{}/subjects/{}-{}?permanent=true",
            schema_registry_endpoint, temp_topic, suffix
        ))
        .send()
        .await
        .unwrap();
    }

    // Produce messages to temp topic using Confluent's official protobuf serializer
    produce_to_temp_topic(&enc, &temp_topic, num_messages);

    // Copy schemas from temp topic to main topic.
    // The Confluent producer registered schemas with the temp topic name.
    // Retry in a loop since the producer may not have registered schemas yet.
    for suffix in ["key", "value"] {
        let schema_resp = deadline(
            &format!("schema registry to publish {temp_topic}-{suffix}"),
            60,
            async {
                loop {
                    let resp = http
                        .get(format!(
                            "{}/subjects/{}-{}/versions/latest",
                            schema_registry_endpoint, temp_topic, suffix
                        ))
                        .send()
                        .await
                        .unwrap();

                    if resp.status().is_success() {
                        break resp.json::<serde_json::Value>().await.unwrap();
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        )
        .await;

        let schema = schema_resp["schema"].as_str().unwrap();

        // Register the same schema for the main topic
        let register_result = http
            .post(format!(
                "{}/subjects/{}-{}/versions",
                schema_registry_endpoint, topic, suffix
            ))
            .json(&json!({"schema": schema, "schemaType": "PROTOBUF"}))
            .send()
            .await
            .unwrap();

        assert!(
            register_result.status().is_success(),
            "Failed to register {} schema for {}: {:?}",
            suffix,
            topic,
            register_result.text().await
        );
    }

    // Consume the raw Confluent-encoded bytes from temp topic
    let encoded_messages = consume_raw_messages(&temp_topic, num_messages).await;

    // Re-produce the Confluent-encoded bytes to the main topic with controlled metadata
    for (idx, (key, value)) in encoded_messages.into_iter().enumerate() {
        send_message(
            topic,
            &key,
            Some(&value),
            idx,
            num_partitions,
            producer,
        )
        .await;
    }

    // Produce tombstone (deletion) records.
    // We need to use Confluent-encoded keys for tombstones too.
    // Re-use the key bytes from the first num_partitions messages.
    let tombstone_keys = consume_raw_messages(&temp_topic, num_partitions as usize).await;
    for (idx, (key, _)) in tombstone_keys.into_iter().enumerate() {
        send_message(
            topic,
            &key,
            None::<&[u8]>,
            idx,
            num_partitions,
            producer,
        )
        .await;
    }

    // Clean up temp topic
    let _ = admin.delete_topics(&[&temp_topic], opts).await;
}

fn produce_to_temp_topic(enc: &ProtobufTestDataEncoder, temp_topic: &str, num_messages: usize) {
    // Build input lines in the format: key_json|value_json
    let mut input_lines = String::new();
    for idx in 0..num_messages {
        let key_json = format!(r#"{{"idx":{},"nested":{{"sub_id":{}}}}}"#, idx, idx);
        let value_json = format!(r#"{{"value":"value-{}"}}"#, idx);
        input_lines.push_str(&format!("{}|{}\n", key_json, value_json));
    }

    let key_schema = enc.key_schema_string().replace('\n', " ");
    let value_schema = enc.payload_schema_string().replace('\n', " ");

    let mut child = Command::new("timeout")
        .args([
            "--signal=TERM",
            "--kill-after=10",
            "120",
            "docker",
            "exec",
            "-i",
            "schema-registry",
            "kafka-protobuf-console-producer",
            "--broker-list",
            "db:29092",
            "--topic",
            temp_topic,
            "--property",
            "schema.registry.url=http://schema-registry:8081",
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=|",
            "--property",
            &format!("key.schema={}", key_schema),
            "--property",
            &format!("value.schema={}", value_schema),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kafka-protobuf-console-producer");

    let stdin = child.stdin.as_mut().expect("failed to get stdin");
    stdin
        .write_all(input_lines.as_bytes())
        .expect("failed to write to stdin");
    drop(child.stdin.take());

    let output = child
        .wait_with_output()
        .expect("failed to wait for producer");

    if output.status.code() == Some(124) {
        panic!(
            "kafka-protobuf-console-producer exceeded 120s producing to {temp_topic}\n\
             --- stderr ---\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if !output.status.success() {
        panic!(
            "kafka-protobuf-console-producer failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

async fn setup_protobuf_ref_test(
    admin: &AdminClient<rdkafka::client::DefaultClientContext>,
    opts: &AdminOptions,
    http: &reqwest::Client,
    schema_registry_endpoint: &str,
    num_messages: usize,
    num_partitions: i32,
    topic_replication: i32,
    producer: &FutureProducer,
) {
    let topic = "protobuf-ref-topic";

    // Delete and recreate the topic.
    admin.delete_topics(&[topic], opts).await.unwrap();
    admin
        .create_topics(
            &[NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(topic_replication),
            )],
            opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Delete existing schemas for the topic and the shared subject.
    for subject in [
        format!("{}-key", topic),
        format!("{}-value", topic),
        "protobuf-ref-common".to_string(),
    ] {
        http.delete(format!(
            "{}/subjects/{}",
            schema_registry_endpoint, subject
        ))
        .send()
        .await
        .unwrap();

        http.delete(format!(
            "{}/subjects/{}?permanent=true",
            schema_registry_endpoint, subject
        ))
        .send()
        .await
        .unwrap();
    }

    // Register the shared/common proto as its own subject.
    let common_schema =
        r#"syntax = "proto3"; message CommonType { string common_field = 1; }"#;
    let resp = http
        .post(format!(
            "{}/subjects/protobuf-ref-common/versions",
            schema_registry_endpoint
        ))
        .json(&json!({"schema": common_schema, "schemaType": "PROTOBUF"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register common schema: {:?}",
        resp.text().await
    );

    // Register key schema (simple, no references).
    let key_schema = r#"syntax = "proto3"; message RefKey { int32 idx = 1; }"#;
    let resp = http
        .post(format!(
            "{}/subjects/{}-key/versions",
            schema_registry_endpoint, topic
        ))
        .json(&json!({"schema": key_schema, "schemaType": "PROTOBUF"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register key schema: {:?}",
        resp.text().await
    );

    // Register value schema WITH a reference to the common schema.
    let value_schema = r#"syntax = "proto3"; import "common.proto"; message RefValue { CommonType common = 1; string value = 2; }"#;
    let resp = http
        .post(format!(
            "{}/subjects/{}-value/versions",
            schema_registry_endpoint, topic
        ))
        .json(&json!({
            "schema": value_schema,
            "schemaType": "PROTOBUF",
            "references": [{
                "name": "common.proto",
                "subject": "protobuf-ref-common",
                "version": 1
            }]
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register value schema with references: {:?}",
        resp.text().await
    );

    // Get the assigned schema IDs.
    let key_info: serde_json::Value = http
        .get(format!(
            "{}/subjects/{}-key/versions/latest",
            schema_registry_endpoint, topic
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key_schema_id = key_info["id"].as_u64().unwrap() as u32;

    let value_info: serde_json::Value = http
        .get(format!(
            "{}/subjects/{}-value/versions/latest",
            schema_registry_endpoint, topic
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let value_schema_id = value_info["id"].as_u64().unwrap() as u32;

    // Produce messages with manually constructed Confluent wire-format bytes.
    for idx in 0..num_messages {
        let key = encode_confluent_protobuf(key_schema_id, &encode_ref_key(idx as i32));
        let value = encode_confluent_protobuf(
            value_schema_id,
            &encode_ref_value(idx),
        );
        send_message(topic, &key, Some(&value), idx, num_partitions, producer).await;
    }

    // Produce tombstone (deletion) records.
    for idx in 0..num_partitions {
        let key =
            encode_confluent_protobuf(key_schema_id, &encode_ref_key(idx));
        send_message(
            topic,
            &key,
            None::<&[u8]>,
            idx as usize,
            num_partitions,
            producer,
        )
        .await;
    }
}

async fn setup_protobuf_wkt_test(
    admin: &AdminClient<rdkafka::client::DefaultClientContext>,
    opts: &AdminOptions,
    http: &reqwest::Client,
    schema_registry_endpoint: &str,
    num_messages: usize,
    num_partitions: i32,
    topic_replication: i32,
    producer: &FutureProducer,
) {
    let topic = "protobuf-wkt-topic";

    // Delete and recreate the topic.
    admin.delete_topics(&[topic], opts).await.unwrap();
    admin
        .create_topics(
            &[NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(topic_replication),
            )],
            opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Delete existing schemas for the topic.
    for suffix in ["key", "value"] {
        http.delete(format!(
            "{}/subjects/{}-{}",
            schema_registry_endpoint, topic, suffix
        ))
        .send()
        .await
        .unwrap();

        http.delete(format!(
            "{}/subjects/{}-{}?permanent=true",
            schema_registry_endpoint, topic, suffix
        ))
        .send()
        .await
        .unwrap();
    }

    // Register a simple key schema (no references).
    let key_schema = r#"syntax = "proto3"; message WktKey { int32 idx = 1; }"#;
    let resp = http
        .post(format!(
            "{}/subjects/{}-key/versions",
            schema_registry_endpoint, topic
        ))
        .json(&json!({"schema": key_schema, "schemaType": "PROTOBUF"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register wkt key schema: {:?}",
        resp.text().await
    );

    // Value schema imports the google well-known Timestamp type, resolved
    // natively rather than via a Schema Registry reference.
    let value_schema = r#"syntax = "proto3"; import "google/protobuf/timestamp.proto"; message WktValue { string value = 1; google.protobuf.Timestamp ts = 2; }"#;
    let resp = http
        .post(format!(
            "{}/subjects/{}-value/versions",
            schema_registry_endpoint, topic
        ))
        .json(&json!({"schema": value_schema, "schemaType": "PROTOBUF"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register wkt value schema: {:?}",
        resp.text().await
    );

    // Get the assigned schema IDs.
    let key_info: serde_json::Value = http
        .get(format!(
            "{}/subjects/{}-key/versions/latest",
            schema_registry_endpoint, topic
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let key_schema_id = key_info["id"].as_u64().unwrap() as u32;

    let value_info: serde_json::Value = http
        .get(format!(
            "{}/subjects/{}-value/versions/latest",
            schema_registry_endpoint, topic
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let value_schema_id = value_info["id"].as_u64().unwrap() as u32;

    // Produce messages with manually constructed Confluent wire-format bytes.
    for idx in 0..num_messages {
        let key = encode_confluent_protobuf(key_schema_id, &encode_ref_key(idx as i32));
        let value = encode_confluent_protobuf(value_schema_id, &encode_wkt_value(idx));
        send_message(topic, &key, Some(&value), idx, num_partitions, producer).await;
    }

    // Produce tombstone (deletion) records.
    for idx in 0..num_partitions {
        let key = encode_confluent_protobuf(key_schema_id, &encode_ref_key(idx));
        send_message(
            topic,
            &key,
            None::<&[u8]>,
            idx as usize,
            num_partitions,
            producer,
        )
        .await;
    }
}

async fn setup_avro_ref_test(
    admin: &AdminClient<rdkafka::client::DefaultClientContext>,
    opts: &AdminOptions,
    http: &reqwest::Client,
    schema_registry_endpoint: &str,
    num_partitions: i32,
    topic_replication: i32,
) {
    let topic = "avro-ref-topic";

    // Delete and recreate the topic.
    admin.delete_topics(&[topic], opts).await.unwrap();
    admin
        .create_topics(
            &[NewTopic::new(
                topic,
                num_partitions,
                TopicReplication::Fixed(topic_replication),
            )],
            opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Delete existing schemas for the topic value and the shared subject.
    for subject in [format!("{}-value", topic), "avro-ref-common".to_string()] {
        http.delete(format!("{}/subjects/{}", schema_registry_endpoint, subject))
            .send()
            .await
            .unwrap();
        http.delete(format!(
            "{}/subjects/{}?permanent=true",
            schema_registry_endpoint, subject
        ))
        .send()
        .await
        .unwrap();
    }

    // Register a shared/common Avro record as its own subject.
    let common_schema = r#"{"type":"record","name":"Common","namespace":"acme","fields":[{"name":"common_field","type":"string"}]}"#;
    let resp = http
        .post(format!(
            "{}/subjects/avro-ref-common/versions",
            schema_registry_endpoint
        ))
        .json(&json!({"schema": common_schema, "schemaType": "AVRO"}))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register common avro schema: {:?}",
        resp.text().await
    );

    // Register the topic value schema WITH an Avro reference to the common schema.
    // The connector does not support Avro references and will error fetching this
    // schema; discovery must remain resilient and still surface the topic.
    let value_schema = r#"{"type":"record","name":"AvroRefValue","namespace":"acme","fields":[{"name":"common","type":"acme.Common"},{"name":"value","type":"string"}]}"#;
    let resp = http
        .post(format!(
            "{}/subjects/{}-value/versions",
            schema_registry_endpoint, topic
        ))
        .json(&json!({
            "schema": value_schema,
            "schemaType": "AVRO",
            "references": [{
                "name": "acme.Common",
                "subject": "avro-ref-common",
                "version": 1
            }]
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Failed to register avro value schema with references: {:?}",
        resp.text().await
    );

    // Intentionally produce no messages: this case exercises discovery resilience only.
}

/// Encode WktValue { value: "value-N", ts: google.protobuf.Timestamp { seconds } }
/// as raw protobuf bytes.
fn encode_wkt_value(idx: usize) -> Vec<u8> {
    let value_str = format!("value-{}", idx);
    // Deterministic, non-zero seconds so the Timestamp serializes to a stable RFC3339.
    let seconds = ((idx + 1) * 86_400) as u64;

    // Inner google.protobuf.Timestamp { seconds = <n> } (field 1, varint).
    let mut inner = Vec::new();
    inner.push(0x08);
    encode_unsigned_varint(seconds, &mut inner);

    let mut buf = Vec::new();
    // Field 1: string value (length-delimited).
    buf.push(0x0A);
    encode_unsigned_varint(value_str.len() as u64, &mut buf);
    buf.extend_from_slice(value_str.as_bytes());
    // Field 2: Timestamp message (length-delimited).
    buf.push(0x12);
    encode_unsigned_varint(inner.len() as u64, &mut buf);
    buf.extend_from_slice(&inner);

    buf
}

/// Wrap protobuf bytes in Confluent wire format:
/// [magic:0x00][schema_id:4 bytes BE][msg_index_array_len:0x00][proto_bytes]
fn encode_confluent_protobuf(schema_id: u32, proto_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(6 + proto_bytes.len());
    buf.push(0x00); // magic byte
    buf.extend_from_slice(&schema_id.to_be_bytes());
    buf.push(0x00); // message index array length 0 = first message
    buf.extend_from_slice(proto_bytes);
    buf
}

/// Encode RefKey { idx: n } as raw protobuf bytes.
fn encode_ref_key(idx: i32) -> Vec<u8> {
    if idx == 0 {
        return Vec::new(); // default value, no fields encoded
    }
    let mut buf = Vec::new();
    buf.push(0x08); // field 1, wire type 0 (varint)
    encode_unsigned_varint(idx as u64, &mut buf);
    buf
}

/// Encode RefValue { common: CommonType { common_field: "common-N" }, value: "value-N" }
/// as raw protobuf bytes.
fn encode_ref_value(idx: usize) -> Vec<u8> {
    let common_field_str = format!("common-{}", idx);
    let value_str = format!("value-{}", idx);

    // Inner: CommonType { common_field: "common-N" }
    let mut inner = Vec::new();
    inner.push(0x0A); // field 1, wire type 2 (length-delimited)
    encode_unsigned_varint(common_field_str.len() as u64, &mut inner);
    inner.extend_from_slice(common_field_str.as_bytes());

    let mut buf = Vec::new();
    // Field 1: CommonType (message, length-delimited)
    buf.push(0x0A); // field 1, wire type 2
    encode_unsigned_varint(inner.len() as u64, &mut buf);
    buf.extend_from_slice(&inner);

    // Field 2: string value
    buf.push(0x12); // field 2, wire type 2 (length-delimited)
    encode_unsigned_varint(value_str.len() as u64, &mut buf);
    buf.extend_from_slice(value_str.as_bytes());

    buf
}

fn encode_unsigned_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        if value < 0x80 {
            buf.push(value as u8);
            return;
        }
        buf.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
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

    // `None` here would be rdkafka's `Timeout::Never`: an unbounded block when
    // the producer queue is full.
    producer
        .send(rec, Duration::from_secs(30))
        .await
        .expect("producing test message");
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

struct ProtobufTestDataEncoder {}

impl ProtobufTestDataEncoder {
    fn new() -> Self {
        Self {}
    }

    fn key_schema_string(&self) -> String {
        r#"syntax = "proto3";

message ProtoKey {
  int32 idx = 1;
  NestedKey nested = 2;
}

message NestedKey {
  int32 sub_id = 1;
}"#
        .to_string()
    }

    fn payload_schema_string(&self) -> String {
        r#"syntax = "proto3";

message ProtoValue {
  string value = 1;
}"#
        .to_string()
    }
}

/// Consume raw bytes from a Kafka topic using rdkafka
async fn consume_raw_messages(topic: &str, num_messages: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::Message;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", format!("test-consumer-{}", std::process::id()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    let mut messages = Vec::with_capacity(num_messages);
    let timeout = Duration::from_secs(10);
    let start = std::time::Instant::now();

    while messages.len() < num_messages && start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await {
            Ok(Ok(msg)) => {
                let key = msg.key().map(|k| k.to_vec()).unwrap_or_default();
                let value = msg.payload().map(|v| v.to_vec()).unwrap_or_default();
                messages.push((key, value));
            }
            _ => continue,
        }
    }

    messages
}
