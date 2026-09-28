use core::str;
use std::process::Command;

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
}
