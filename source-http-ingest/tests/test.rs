use anyhow::Context;
use proto_flow::capture::{
    response::{Captured, Opened},
    Response,
};
use reqwest::header::HeaderMap;

use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::{
    io::BufReader,
    process::{ChildStdin, ChildStdout, Command},
};

#[tokio::test]
async fn test_discover() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_source-http-ingest"))
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        // This is just easier than having it listen on a random port and then
        // needing to figure out what it is by parsing log output.
        .env("SOURCE_HTTP_INGEST_PORT", "27173")
        .env("LOG_LEVEL", "info")
        .spawn()
        .expect("spawning child process");

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    let discover_req = serde_json::json!({
        "discover": {
            "config": {}
        }
    });
    write_capture_request(&discover_req, &mut stdin)
        .await
        .expect("failed to wrtie req");

    let mut buf = String::new();
    stdout
        .read_line(&mut buf)
        .await
        .expect("failed to read stdout");
    let result: serde_json::Value = serde_json::from_str(&buf).expect("deserialize response");
    insta::assert_json_snapshot!(result);
}

#[tokio::test]
async fn test_http_request_processing() -> Result<(), anyhow::Error> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_source-http-ingest"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        // This is just easier than having it listen on a random port and then
        // needing to figure out what it is by parsing log output.
        .env("SOURCE_HTTP_INGEST_PORT", "27172")
        .env("LOG_LEVEL", "info")
        .spawn()?;

    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let result = run_http_request_processing(stdin, stdout).await;
    let kill_result = child.kill().await;
    result?;
    kill_result?;
    Ok(())
}

async fn run_http_request_processing(
    mut stdin: ChildStdin,
    stdout: ChildStdout,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stdout);

    // Write open and expect to read opened.
    write_capture_request(&open_json(), &mut stdin).await?;
    match read_response(&mut reader).await.context("reading opened")? {
        Response {
            opened: Some(Opened {
                explicit_acknowledgements,
            }),
            ..
        } => assert!(explicit_acknowledgements),
        _ => anyhow::bail!("unexpected repsonse type, expected opened"),
    };
    // The connector sends the opened response before it actually starts listening
    // on the port. This is an (admittedly hacky) way of dealing with that.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Use two separate clients to fire requests off in separate background tasks.
    let client_a = reqwest::ClientBuilder::new().build()?;
    let client_b = client_a.clone();

    let headers_a = HeaderMap::new();
    let mut headers_b = HeaderMap::new();
    headers_b.insert(
        "X-Webhook-Id",
        reqwest::header::HeaderValue::from_str("same-id").unwrap(),
    );

    let handle_a = tokio::task::spawn(write_docs(
        128,
        "http://localhost:27172/aliceCo/test/webhook-data".to_string(),
        client_a,
        headers_a,
    ));
    let handle_b = tokio::task::spawn(write_docs(
        128,
        "http://localhost:27172/another.json".to_string(),
        client_b,
        headers_b,
    ));

    // We'll re-use this value to write acknowledgements as we read commits
    let ack_request = serde_json::json!({"acknowledge": {}});

    // Expect to read 256 documents in total, each followed immediately by a checkpoint.
    // As checkpoints are read, write an ack to unblock responses.
    let mut a_docs = 0;
    let mut b_docs = 0;
    for i in 0..512 {
        let expect_commit = i % 2 == 1;
        let expect_document = !expect_commit;

        let response = read_response(&mut reader).await?;
        let Response {
            checkpoint,
            captured,
            ..
        } = &response;

        match (checkpoint.as_ref(), captured.as_ref()) {
            (Some(_), None) if expect_commit => {
                write_capture_request(&ack_request, &mut stdin)
                    .await
                    .context("writing acknowledge")?;
            }
            (None, Some(Captured { binding, doc_json })) if expect_document => {
                if *binding == 0 {
                    a_docs += 1;
                } else if *binding == 1 {
                    b_docs += 1;
                } else {
                    anyhow::bail!(
                        "got document with unexpected binding {binding}: {}",
                        doc_json
                    );
                }
            }
            _ => {
                anyhow::bail!(
                    "got unexpected response on round {i}, expect_commit: {expect_commit}: {:?}",
                    response
                );
            }
        }
    }

    // ensure that the background tasks all completed successfully
    handle_a.await.context("result_a")??;
    handle_b.await.context("result_b")??;

    anyhow::ensure!(a_docs == 128, "expected 128 a_docs, got: {a_docs}");
    anyhow::ensure!(b_docs == 128, "expected 128 b_docs, got: {b_docs}");

    Ok(())
}

async fn read_response(stdout: &mut BufReader<ChildStdout>) -> anyhow::Result<Response> {
    let mut line = String::with_capacity(256);

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stdout.read_line(&mut line),
    )
    .await??;
    if line.is_empty() {
        anyhow::bail!("stdout line was empty");
    }
    let resp = serde_json::from_str::<Response>(&line)?;
    Ok(resp)
}

async fn write_docs(
    count: usize,
    url: String,
    client: reqwest::Client,
    headers: HeaderMap,
) -> anyhow::Result<()> {
    let expected = serde_json::json!({"published": 1});
    for i in 0..count {
        if let Err(err) = post_doc(i, url.as_str(), &client, &expected, headers.clone()).await {
            eprintln!("failed to post doc at index: {i}: error: {:?}", err);
            anyhow::bail!("failed to post document at index: {i}: error: {:?}", err);
        }
    }
    Ok(())
}

async fn post_doc(
    i: usize,
    url: &str,
    client: &reqwest::Client,
    expected_resp: &serde_json::Value,
    headers: HeaderMap,
) -> anyhow::Result<()> {
    let resp = client
        .post(url)
        .headers(headers)
        .json(&serde_json::json!({"docIndex": i, "url": url}))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    if &resp != expected_resp {
        anyhow::bail!("actual response: '{resp}' differed from expected: '{expected_resp}'");
    }
    Ok(())
}

async fn write_capture_request(
    req: &serde_json::Value,
    stdin: &mut ChildStdin,
) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let buf = serde_json::to_vec(req).unwrap();

    stdin.write_all(&buf).await?;
    stdin.write_all(b"\n").await?;
    Ok(())
}

fn open_json() -> serde_json::Value {
    serde_json::json!({
        "open": {
            "version": "canary-version",
            "range": {
                "keyBegin": 0,
                "keyEnd": 4294967295i64,
            },
            "state_json": "{}",
            "capture": {
                "name": "aliceCo/test/ingest",
                "config": {},
                "intervalSeconds": 10,
                "bindings": [
                    {
                        "collection": {
                            "name": "aliceCo/test/webhook-data",
                            "write_schema_json": {
                                "type": "object",
                                "properties": {
                                    "_meta": {
                                        "type": "object",
                                        "properties": { "webhookId": {"type": "string"}},
                                        "required": ["webhookId"]
                                    }
                                },
                                "required": ["_meta"]
                            },
                            "key": ["/_meta/webhookId"],
                            "partitionFields": [],
                            "projections": []
                        },
                        "resourceConfig": {},
                        "resourcePath": ["aliceCo/test/webhook-data"]
                    },
                    {
                        "collection": {
                            "name": "aliceCo/test/webhook-data",
                            "write_schema_json": {
                                "type": "object",
                                "properties": {
                                    "_meta": {
                                        "type": "object",
                                        "properties": { "webhookId": {"type": "string"}},
                                        "required": ["webhookId"]
                                    }
                                },
                                "required": ["_meta"]
                            },
                            "key": ["/_meta/webhookId"],
                            "partitionFields": [],
                            "projections": []
                        },
                        "resourceConfig": {
                            "path": "another.json",
                            "idFromHeader": "X-Webhook-Id"
                        },
                        "resourcePath": ["/another.json"]
                    }
                ]
            }
        }
    })
}
