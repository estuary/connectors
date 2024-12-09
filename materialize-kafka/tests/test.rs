use core::str;
use std::collections::HashMap;

use rdkafka::{
    admin::{AdminClient, AdminOptions},
    consumer::{BaseConsumer, Consumer},
    ClientConfig, Message, TopicPartitionList,
};

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

            source_kafka::pull::avro_to_json(avro_value, schema)
                .unwrap()
                .to_string()
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
