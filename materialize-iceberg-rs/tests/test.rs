use core::str;
use std::process::Command;
use std::sync::Once;

use apache_avro::types::Value as AvroValue;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::ProvideErrorMetadata;

const RUSTFS_ENDPOINT: &str = "http://localhost:9603";
const BUCKET: &str = "test-bucket";

// The connector binds a fixed port for its embedded catalog server
// (`MATERIALIZE_ICEBERG_RS_PORT`, defaulting to 8080), regardless of the
// request it's handling (Spec included). `cargo test`'s default parallelism
// runs tests in this binary concurrently, so each gets its own port to avoid
// colliding on the same one.
const TEST_SPEC_PORT: u16 = 8081;
const TEST_MATERIALIZATION_PORT: u16 = 8082;
const TEST_RESTART_PORT: u16 = 8083;
const TEST_UPDATE_PORT: u16 = 8084;

#[test]
fn test_spec() {
    let output = std::process::Command::new("flowctl")
        .env("MATERIALIZE_ICEBERG_RS_PORT", TEST_SPEC_PORT.to_string())
        .args([
            "raw",
            "spec",
            "--source",
            "tests/test.flow.yaml",
            "--name",
            "acmeCo/materialize-iceberg-rs",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let got: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    insta::assert_snapshot!(serde_json::to_string_pretty(&got).unwrap());
}

// Brings up (and leaves running) the docker-compose stack backing the local
// rustfs S3-compatible store, shared across every test in this binary.
fn ensure_services_up() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = Command::new("docker")
            .args(["network", "create", "flow-test"])
            .output();

        let status = Command::new("docker")
            .args(["compose", "-f", "docker-compose.yaml", "up", "--wait"])
            .status()
            .expect("failed to invoke docker compose");
        if !status.success() {
            let logs = Command::new("docker")
                .args(["compose", "-f", "docker-compose.yaml", "logs"])
                .output();
            if let Ok(logs) = logs {
                eprintln!("--- docker compose logs ---");
                eprintln!("{}", String::from_utf8_lossy(&logs.stdout));
                eprintln!("{}", String::from_utf8_lossy(&logs.stderr));
            }
            panic!("docker compose up --wait failed");
        }
    });
}

async fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region("us-east-1")
        .credentials_provider(Credentials::new(
            "test_key",
            "test_secret",
            None,
            None,
            "test",
        ))
        .endpoint_url(RUSTFS_ENDPOINT)
        .load()
        .await;

    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build(),
    )
}

async fn ensure_bucket(s3: &aws_sdk_s3::Client) {
    match s3.create_bucket().bucket(BUCKET).send().await {
        Ok(_) => {}
        Err(err) if err.code() == Some("BucketAlreadyOwnedByYou") => {}
        Err(err) => panic!("failed to create test bucket: {err}"),
    }
}

// Deletes every object under a table's prefix, so each test starts from a
// clean table regardless of what earlier runs against this rustfs instance
// left behind.
async fn clear_table(s3: &aws_sdk_s3::Client, table: &str) {
    let listed = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(format!("warehouse/{table}/"))
        .send()
        .await
        .unwrap();

    for object in listed.contents() {
        let Some(key) = object.key() else { continue };
        s3.delete_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
    }
}

fn run_preview(fixture: &str, name: &str, port: u16) {
    let output = Command::new("flowctl")
        .env("MATERIALIZE_ICEBERG_RS_PORT", port.to_string())
        .args([
            "raw",
            "preview-next",
            "--source",
            "tests/test.flow.yaml",
            "--fixture",
            fixture,
            "--name",
            name,
        ])
        .output()
        .unwrap();

    println!("{}", str::from_utf8(&output.stderr).unwrap());
    assert!(output.status.success());
}

#[tokio::test]
async fn test_materialization() {
    ensure_services_up();

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;
    clear_table(&s3, "test_table").await;

    run_preview(
        "tests/fixture.json",
        "acmeCo/materialize-iceberg-rs",
        TEST_MATERIALIZATION_PORT,
    );

    let listed = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("warehouse/test_table/data/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = listed
        .contents()
        .iter()
        .filter_map(|o| o.key())
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    assert_eq!(
        keys.len(),
        1,
        "expected exactly one parquet data file, found {keys:?}"
    );

    let object = s3
        .get_object()
        .bucket(BUCKET)
        .key(keys[0])
        .send()
        .await
        .unwrap();
    let bytes = object.body.collect().await.unwrap().into_bytes();

    let reader = parquet::file::reader::SerializedFileReader::new(bytes).unwrap();
    let rows: Vec<serde_json::Value> =
        parquet::file::reader::FileReader::get_row_iter(&reader, None)
            .unwrap()
            .map(|row| row.unwrap().to_json_value())
            .collect();

    assert_eq!(rows.len(), 3, "expected the fixture's 3 rows: {rows:#?}");

    // `value` is a selected non-key field (via `fields.recommended: true`),
    // so it must land as its own column, not only inside flow_document.
    let mut by_id: Vec<(i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row["id"].as_i64().unwrap(),
                row["value"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    by_id.sort();
    assert_eq!(
        by_id,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
        ],
        "expected the value column to carry each row's fixture value: {rows:#?}"
    );

    let metadata = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("warehouse/test_table/metadata/")
        .send()
        .await
        .unwrap();
    let metadata_keys: Vec<&str> = metadata.contents().iter().filter_map(|o| o.key()).collect();

    let manifest_keys: Vec<&&str> = metadata_keys
        .iter()
        .filter(|k| k.ends_with("-m0.avro"))
        .collect();
    assert_eq!(
        manifest_keys.len(),
        1,
        "expected exactly one manifest file, found {metadata_keys:?}"
    );

    let manifest_list_keys: Vec<&&str> = metadata_keys
        .iter()
        .filter(|k| k.rsplit('/').next().unwrap().starts_with("snap-"))
        .collect();
    assert_eq!(
        manifest_list_keys.len(),
        1,
        "expected exactly one manifest-list file, found {metadata_keys:?}"
    );

    let manifest_entries = avro_records(&s3, manifest_list_keys[0]).await;
    assert_eq!(manifest_entries.len(), 1, "expected one manifest-list row");
    assert_eq!(
        avro_str(&manifest_entries[0], "manifest_path"),
        format!("s3://{BUCKET}/{}", manifest_keys[0]),
    );
    assert_eq!(avro_int(&manifest_entries[0], "added_files_count"), 1);
    assert_eq!(avro_long(&manifest_entries[0], "added_rows_count"), 3);

    let manifest_entries = avro_records(&s3, manifest_keys[0]).await;
    assert_eq!(manifest_entries.len(), 1, "expected one manifest entry");
    let data_file = avro_field(&manifest_entries[0], "data_file");
    assert_eq!(
        avro_str(data_file, "file_path"),
        format!("s3://{BUCKET}/{}", keys[0]),
    );
    assert_eq!(avro_long(data_file, "record_count"), 3);

    let version_hint = get_object_string(&s3, "warehouse/test_table/metadata/version-hint.text")
        .await
        .trim()
        .to_string();
    assert_eq!(
        version_hint, "1",
        "expected the table's first metadata version"
    );

    let metadata_json_keys: Vec<&&str> = metadata_keys
        .iter()
        .filter(|k| k.ends_with(".metadata.json"))
        .collect();
    assert_eq!(
        metadata_json_keys.len(),
        1,
        "expected exactly one table metadata file, found {metadata_keys:?}"
    );
    let metadata_json_filename = metadata_json_keys[0].rsplit('/').next().unwrap();
    assert!(
        metadata_json_filename.starts_with("00001-")
            && metadata_json_filename.ends_with(".metadata.json"),
        "expected a 00001-<uuid>.metadata.json filename, got {metadata_json_filename}"
    );

    let table_metadata: serde_json::Value =
        serde_json::from_str(&get_object_string(&s3, metadata_json_keys[0]).await).unwrap();
    assert_eq!(table_metadata["format-version"], 2);
    assert_eq!(table_metadata["schemas"].as_array().unwrap().len(), 1);
    assert_eq!(table_metadata["snapshots"].as_array().unwrap().len(), 1);

    let snapshot_id = table_metadata["current-snapshot-id"].as_i64().unwrap();
    assert_eq!(table_metadata["snapshots"][0]["snapshot-id"], snapshot_id,);
    assert!(
        table_metadata["snapshots"][0]
            .as_object()
            .unwrap()
            .get("parent-snapshot-id")
            .is_none(),
        "the table's first snapshot must omit parent-snapshot-id entirely, not set it to null: {}",
        table_metadata["snapshots"][0]
    );
    assert_eq!(
        table_metadata["snapshots"][0]["manifest-list"],
        format!("s3://{BUCKET}/{}", manifest_list_keys[0]),
    );
    assert_eq!(table_metadata["refs"]["main"]["snapshot-id"], snapshot_id,);
    assert_eq!(
        table_metadata["location"],
        format!("s3://{BUCKET}/warehouse/test_table"),
    );
}

// Runs two completely independent `flowctl raw preview-next` processes
// against the same table, each with no knowledge of the other's connector
// state (unlike `--sessions`, which carries a checkpoint forward across
// session boundaries within one process). This reproduces the scenario that
// exposed a bug where the connector, started with an empty checkpoint,
// would commit a snapshot that collided with and orphaned a table's
// already-written data: snapshot ids must stay unique and chain correctly,
// and the second commit's manifest-list must still carry the first commit's
// manifest forward, even though the second process never saw the first
// commit's checkpoint.
#[tokio::test]
async fn test_materialization_restart_reconciles_checkpoint_with_catalog() {
    ensure_services_up();

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;
    clear_table(&s3, "test_table_restart").await;

    run_preview(
        "tests/restart_fixture_1.json",
        "acmeCo/materialize-iceberg-rs/restart",
        TEST_RESTART_PORT,
    );
    run_preview(
        "tests/restart_fixture_2.json",
        "acmeCo/materialize-iceberg-rs/restart",
        TEST_RESTART_PORT,
    );

    let listed = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("warehouse/test_table_restart/data/")
        .send()
        .await
        .unwrap();
    let data_keys: Vec<&str> = listed
        .contents()
        .iter()
        .filter_map(|o| o.key())
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    assert_eq!(
        data_keys.len(),
        2,
        "expected one parquet data file per commit, found {data_keys:?}"
    );

    // Every row from both independent commits must carry real document
    // data: a connector restart must never result in an empty flow_document.
    let mut total_rows = 0;
    for key in &data_keys {
        let object = s3
            .get_object()
            .bucket(BUCKET)
            .key(*key)
            .send()
            .await
            .unwrap();
        let bytes = object.body.collect().await.unwrap().into_bytes();
        let reader = parquet::file::reader::SerializedFileReader::new(bytes).unwrap();
        for row in parquet::file::reader::FileReader::get_row_iter(&reader, None).unwrap() {
            let row = row.unwrap().to_json_value();
            let doc = row["flow_document"].as_str().unwrap();
            assert!(
                !doc.is_empty() && serde_json::from_str::<serde_json::Value>(doc).is_ok(),
                "expected a non-empty JSON flow_document, got {doc:?}"
            );
            total_rows += 1;
        }
    }
    assert_eq!(
        total_rows, 3,
        "expected 2 rows from the first commit plus 1 from the second"
    );

    let metadata = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("warehouse/test_table_restart/metadata/")
        .send()
        .await
        .unwrap();
    let metadata_keys: Vec<&str> = metadata.contents().iter().filter_map(|o| o.key()).collect();

    let metadata_json_keys: Vec<&&str> = metadata_keys
        .iter()
        .filter(|k| k.ends_with(".metadata.json"))
        .collect();
    assert_eq!(
        metadata_json_keys.len(),
        2,
        "expected one metadata.json per independent commit, found {metadata_keys:?}"
    );

    // The second, independent process's metadata.json must show both
    // snapshots chained together under unique ids, never a collision.
    let second_metadata_key = metadata_json_keys
        .iter()
        .find(|k| k.starts_with("warehouse/test_table_restart/metadata/00002-"))
        .expect("expected a 00002-<uuid>.metadata.json file");
    let table_metadata: serde_json::Value =
        serde_json::from_str(&get_object_string(&s3, second_metadata_key).await).unwrap();

    let snapshots = table_metadata["snapshots"].as_array().unwrap();
    assert_eq!(
        snapshots.len(),
        2,
        "expected both commits' snapshots to be present: {snapshots:#?}"
    );
    let first_snapshot_id = snapshots[0]["snapshot-id"].as_i64().unwrap();
    let second_snapshot_id = snapshots[1]["snapshot-id"].as_i64().unwrap();
    assert_ne!(
        first_snapshot_id, second_snapshot_id,
        "a restarted connector must never reuse a snapshot id that already exists"
    );
    assert_eq!(
        snapshots[1]["parent-snapshot-id"].as_i64().unwrap(),
        first_snapshot_id,
        "the second commit's snapshot must chain onto the first, even though \
         the second process started with no checkpoint of its own"
    );
    assert_eq!(
        table_metadata["current-snapshot-id"].as_i64().unwrap(),
        second_snapshot_id
    );

    // The current snapshot's manifest-list must carry both commits' manifests
    // forward: a restarted connector must not lose track of data the prior
    // process already committed.
    let manifest_list_path = snapshots[1]["manifest-list"]
        .as_str()
        .unwrap()
        .strip_prefix(&format!("s3://{BUCKET}/"))
        .unwrap()
        .to_string();
    let manifest_entries = avro_records(&s3, &manifest_list_path).await;
    assert_eq!(
        manifest_entries.len(),
        2,
        "expected the manifest-list to carry both commits' manifests forward"
    );
    let added_rows: i64 = manifest_entries
        .iter()
        .map(|entry| avro_long(entry, "added_rows_count"))
        .sum();
    assert_eq!(
        added_rows, 3,
        "expected all 3 rows across both commits to be reachable"
    );
}

// Exercises real Load -> Store reduction within a single flowctl process:
// two commits updating the same key, in one preview-next invocation, so the
// second commit's Load finds the first commit's row and the runtime's
// document reduction must carry `_meta.file_path`/`_meta.positional_delete`
// from the Loaded document through to the Store call for this to work at
// all. If that assumption doesn't hold, the connector errors loudly (see
// src/transactor.rs's Store handling) rather than silently skipping the
// delete.
#[tokio::test]
async fn test_materialization_update_writes_positional_delete() {
    ensure_services_up();

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;
    clear_table(&s3, "test_table_update").await;

    run_preview(
        "tests/update_fixture.json",
        "acmeCo/materialize-iceberg-rs/update",
        TEST_UPDATE_PORT,
    );

    let data = s3
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("warehouse/test_table_update/data/")
        .send()
        .await
        .unwrap();
    let data_keys: Vec<&str> = data
        .contents()
        .iter()
        .filter_map(|o| o.key())
        .filter(|k| k.ends_with(".parquet") && !k.ends_with("-deletes.parquet"))
        .collect();
    assert_eq!(
        data_keys.len(),
        2,
        "expected one data file per commit, found {data_keys:?}"
    );

    let delete_keys: Vec<&str> = data
        .contents()
        .iter()
        .filter_map(|o| o.key())
        .filter(|k| k.ends_with("-deletes.parquet"))
        .collect();
    assert_eq!(
        delete_keys.len(),
        1,
        "expected exactly one positional-delete file, found {delete_keys:?}: if this \
         is 0, _meta.file_path/_meta.positional_delete did not survive Flow's document \
         reduction from Load to Store"
    );

    // The delete file masks exactly one row: the first commit's.
    let delete_object = s3
        .get_object()
        .bucket(BUCKET)
        .key(delete_keys[0])
        .send()
        .await
        .unwrap();
    let delete_bytes = delete_object.body.collect().await.unwrap().into_bytes();
    let reader = parquet::file::reader::SerializedFileReader::new(delete_bytes).unwrap();
    let delete_rows: Vec<serde_json::Value> =
        parquet::file::reader::FileReader::get_row_iter(&reader, None)
            .unwrap()
            .map(|row| row.unwrap().to_json_value())
            .collect();
    assert_eq!(
        delete_rows.len(),
        1,
        "expected exactly one deleted position: {delete_rows:#?}"
    );
    assert_eq!(delete_rows[0]["pos"].as_i64().unwrap(), 0);
    let deleted_file_path = delete_rows[0]["file_path"].as_str().unwrap().to_string();
    assert!(
        data_keys
            .iter()
            .any(|k| format!("s3://{BUCKET}/{k}") == deleted_file_path),
        "deleted file_path {deleted_file_path} should reference one of the data files {data_keys:?}"
    );

    // The second (current) data file's row carries the final value, with no
    // leftover _meta.file_path/_meta.positional_delete from Load.
    let second_data_key = data_keys
        .iter()
        .find(|k| format!("s3://{BUCKET}/{k}") != deleted_file_path)
        .expect("expected a surviving data file distinct from the deleted one");
    let object = s3
        .get_object()
        .bucket(BUCKET)
        .key(*second_data_key)
        .send()
        .await
        .unwrap();
    let bytes = object.body.collect().await.unwrap().into_bytes();
    let reader = parquet::file::reader::SerializedFileReader::new(bytes).unwrap();
    let rows: Vec<serde_json::Value> =
        parquet::file::reader::FileReader::get_row_iter(&reader, None)
            .unwrap()
            .map(|row| row.unwrap().to_json_value())
            .collect();
    assert_eq!(
        rows.len(),
        1,
        "expected the surviving file's one row: {rows:#?}"
    );
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 501);
    assert_eq!(rows[0]["value"].as_str().unwrap(), "v2");

    let doc: serde_json::Value =
        serde_json::from_str(rows[0]["flow_document"].as_str().unwrap()).unwrap();
    assert!(
        doc.get("_meta").and_then(|m| m.get("file_path")).is_none(),
        "the stored flow_document should not retain the synthetic _meta.file_path: {doc:#?}"
    );
    assert!(
        doc.get("_meta")
            .and_then(|m| m.get("positional_delete"))
            .is_none(),
        "the stored flow_document should not retain the synthetic _meta.positional_delete: {doc:#?}"
    );
}

async fn get_object_string(s3: &aws_sdk_s3::Client, key: &str) -> String {
    let object = s3
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    let bytes = object.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn avro_records(s3: &aws_sdk_s3::Client, key: &str) -> Vec<AvroValue> {
    let object = s3
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    let bytes = object.body.collect().await.unwrap().into_bytes();

    apache_avro::Reader::new(&bytes[..])
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn avro_field<'a>(value: &'a AvroValue, name: &str) -> &'a AvroValue {
    match value {
        AvroValue::Record(fields) => {
            &fields
                .iter()
                .find(|(field, _)| field == name)
                .unwrap_or_else(|| panic!("missing field {name} in {fields:?}"))
                .1
        }
        other => panic!("expected an Avro record, got {other:?}"),
    }
}

fn avro_str(value: &AvroValue, name: &str) -> String {
    match avro_field(value, name) {
        AvroValue::String(s) => s.clone(),
        other => panic!("expected field {name} to be a string, got {other:?}"),
    }
}

fn avro_int(value: &AvroValue, name: &str) -> i32 {
    match avro_field(value, name) {
        AvroValue::Int(v) => *v,
        other => panic!("expected field {name} to be an int, got {other:?}"),
    }
}

fn avro_long(value: &AvroValue, name: &str) -> i64 {
    match avro_field(value, name) {
        AvroValue::Long(v) => *v,
        other => panic!("expected field {name} to be a long, got {other:?}"),
    }
}
