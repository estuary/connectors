use core::str;
use std::process::Command;

use apache_avro::types::Value as AvroValue;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::error::ProvideErrorMetadata;

const RUSTFS_ENDPOINT: &str = "http://localhost:9603";
const BUCKET: &str = "test-bucket";

#[test]
fn test_spec() {
    let output = std::process::Command::new("flowctl")
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

// Starts (and, on drop, tears down) the docker-compose stack backing the
// local rustfs S3-compatible store used by `test_materialization`.
struct ComposeGuard;

impl Drop for ComposeGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["compose", "-f", "docker-compose.yaml", "down", "-v"])
            .status();
    }
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

#[tokio::test]
async fn test_materialization() {
    let out = Command::new("docker")
        .args(["compose", "-f", "docker-compose.yaml", "up", "--wait"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "docker compose up failed: {}",
        str::from_utf8(&out.stderr).unwrap()
    );
    let _guard = ComposeGuard;

    let s3 = s3_client().await;
    match s3.create_bucket().bucket(BUCKET).send().await {
        Ok(_) => {}
        Err(err) if err.code() == Some("BucketAlreadyOwnedByYou") => {}
        Err(err) => panic!("failed to create test bucket: {err}"),
    }

    let output = Command::new("flowctl")
        .args([
            "raw",
            "preview-next",
            "--source",
            "tests/test.flow.yaml",
            "--fixture",
            "tests/fixture.json",
            "--name",
            "acmeCo/materialize-iceberg-rs",
        ])
        .output()
        .unwrap();

    println!("{}", str::from_utf8(&output.stderr).unwrap());
    assert!(output.status.success());

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
    assert_eq!(
        *metadata_json_keys[0],
        "warehouse/test_table/metadata/v1.metadata.json"
    );

    let table_metadata: serde_json::Value =
        serde_json::from_str(&get_object_string(&s3, metadata_json_keys[0]).await).unwrap();
    assert_eq!(table_metadata["format-version"], 2);
    assert_eq!(table_metadata["schemas"].as_array().unwrap().len(), 1);
    assert_eq!(table_metadata["snapshots"].as_array().unwrap().len(), 1);

    let snapshot_id = table_metadata["current-snapshot-id"].as_i64().unwrap();
    assert_eq!(table_metadata["snapshots"][0]["snapshot-id"], snapshot_id,);
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
