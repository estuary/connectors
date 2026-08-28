use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::Result;
use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
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

/// A test catalog with the connector command rewritten to the binary cargo
/// built for this run. `catalog()` is tests/test.flow.yaml; `catalog_for` takes
/// any of the fixtures.
///
/// The checked-in `cargo run` would nest a second cargo inside `cargo test`,
/// which resolves features differently and so rebuilds what the other just
/// built, while holding target/debug/.cargo-lock. Generating beside the
/// original keeps its relative `import:` valid.
fn catalog() -> &'static str {
    catalog_for("tests/test.flow.yaml")
}

/// Memoized per source path, so a fixture is rewritten once per test binary
/// however many tests ask for it.
fn catalog_for(source_path: &str) -> &'static str {
    static PATHS: std::sync::OnceLock<std::sync::Mutex<HashMap<String, &'static str>>> =
        std::sync::OnceLock::new();
    let paths = PATHS.get_or_init(|| std::sync::Mutex::new(HashMap::new()));

    let mut paths = paths.lock().unwrap();
    if let Some(generated) = paths.get(source_path) {
        return generated;
    }

    let generated = source_path.replace(".flow.yaml", ".generated.flow.yaml");
    assert_ne!(
        generated, source_path,
        "expected a fixture named *.flow.yaml, got {source_path}",
    );
    let source = std::fs::read_to_string(source_path).unwrap();

    let anchor = "        command:\n          - cargo\n          - run\n";
    assert!(
        source.contains(anchor),
        "{source_path} no longer declares `cargo run` as the connector command; \
         update catalog_for() to match",
    );
    let rewritten = source.replace(
        anchor,
        &format!(
            "        command:\n          - {}\n",
            env!("CARGO_BIN_EXE_source-kafka")
        ),
    );
    std::fs::write(&generated, rewritten).unwrap();

    // Leaked so the path can outlive this lock and be handed out as &'static.
    // One leak per fixture per test binary.
    let generated: &'static str = Box::leak(generated.into_boxed_str());
    paths.insert(source_path.to_string(), generated);
    generated
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

/// A child spawned by `spawn_bounded`, with its output going to files.
///
/// Files rather than pipes for the same reason `bounded_command` uses them: a
/// grandchild holding the pipe blocks the read, and flowctl always spawns the
/// connector as one.
struct BoundedChild {
    child: std::process::Child,
    out_path: std::path::PathBuf,
    err_path: std::path::PathBuf,
    what: String,
}

impl BoundedChild {
    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.err_path).unwrap_or_default()
    }

    /// Blocks until the stderr file holds an assignment line per partition.
    ///
    /// A fixed sleep is not enough: producing before the assignment lands puts
    /// the messages behind the end offset, where the capture correctly skips
    /// them and then has nothing to read.
    fn wait_for_assignment(&self, partitions: i32, timeout: Duration) {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // `--log-json` is what forwards the connector's own logs here. Match
            // the parsed message rather than a substring, so a topic or error
            // that happens to contain the phrase cannot trip the count.
            let seen = self
                .stderr()
                .lines()
                .filter(|l| {
                    serde_json::from_str::<serde_json::Value>(l)
                        .is_ok_and(|v| v["message"] == "assigned partition")
                })
                .count();
            if seen >= partitions as usize {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{} did not assign all {} partitions within {:?}:\n{}",
                self.what,
                partitions,
                timeout,
                self.stderr()
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Waits for exit and returns stdout, panicking if `timeout` had to fire.
    fn wait(mut self) -> String {
        let status = self
            .child
            .wait()
            .unwrap_or_else(|e| panic!("failed to wait for {}: {e}", self.what));

        let stdout = std::fs::read_to_string(&self.out_path).unwrap_or_default();
        let stderr = self.stderr();
        let _ = std::fs::remove_file(&self.out_path);
        let _ = std::fs::remove_file(&self.err_path);

        // `timeout` exits 124 when it had to fire.
        if status.code() == Some(124) {
            panic!("{} exceeded its timeout\n--- stderr ---\n{}", self.what, stderr);
        }
        assert!(status.success(), "{} exited {}:\n{}", self.what, status, stderr);
        stdout
    }
}

/// Spawn `program` under coreutils `timeout` without blocking, so the test can
/// act while it runs. The blocking counterpart is `bounded_command`.
fn spawn_bounded(what: &str, secs: u64, program: &str, args: &[&str]) -> BoundedChild {
    let dir = std::env::temp_dir();
    let stem = uuid::Uuid::new_v4();
    let (out_path, err_path) = (
        dir.join(format!("kafka-test-{stem}.out")),
        dir.join(format!("kafka-test-{stem}.err")),
    );
    let out_file = std::fs::File::create(&out_path).unwrap();
    let err_file = std::fs::File::create(&err_path).unwrap();

    let child = Command::new("timeout")
        .args(["--signal=TERM", "--kill-after=10", &secs.to_string(), program])
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out_file))
        .stderr(Stdio::from(err_file))
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn {program} for {what}: {e}"));

    BoundedChild { child, out_path, err_path, what: what.to_string() }
}

const JSON_RAW_DOC_PREFIX: &str = r#"["acmeCo/json-raw-topic","#;
const ONLY_CHANGES_DOC_PREFIX: &str = r#"["acmeCo/only-changes-topic","#;
const ONLY_CHANGES_TOPIC: &str = "only-changes-topic";

fn kafka_clients() -> (AdminClient<DefaultClientContext>, FutureProducer, BaseConsumer) {
    let config = || {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", "localhost:9092")
            .set("group.id", "source-kafka-tests");
        c
    };
    (
        config().create().unwrap(),
        config().create().unwrap(),
        config().create().unwrap(),
    )
}


/// Deletion is asynchronous, so a recreate issued immediately can be serviced
/// first and inherit the old messages. Hence the sleep.
async fn recreate_topic(admin: &AdminClient<DefaultClientContext>, topic: &str, partitions: i32) {
    let opts = AdminOptions::default().request_timeout(Some(Duration::from_secs(5)));

    admin.delete_topics(&[topic], &opts).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &opts,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
}

fn end_offsets(consumer: &BaseConsumer, topic: &str, partitions: i32) -> HashMap<i32, i64> {
    (0..partitions)
        .map(|p| {
            let (_low, high) = consumer
                .fetch_watermarks(topic, p, Duration::from_secs(10))
                .unwrap();
            (p, high)
        })
        .collect()
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

/// A binding with no saved state must skip everything already retained in the
/// topic and capture only what arrives after it starts.
///
/// The other capture tests block on `.output()`, which cannot express this: a
/// capture starting at the end of the partition has nothing to read from the
/// fixture, so a run-to-completion invocation snapshots an empty document set
/// whether or not the feature works. This spawns preview, produces while it
/// runs, and asserts that exactly those messages come back.
///
/// It owns its topic rather than borrowing a shared fixture one, because the
/// messages it produces would otherwise change what the other snapshot tests
/// see depending on test order.
#[tokio::test]
#[serial_test::serial]
async fn test_capture_only_changes() {
    let topic = ONLY_CHANGES_TOPIC;
    let partitions = 3;

    let (admin, producer, consumer) = kafka_clients();
    recreate_topic(&admin, topic, partitions).await;

    // The retained history this capture must skip.
    let enc = JsonRawTestDataEncoder::new();
    for idx in 0..9 {
        let (key, payload) = (
            enc.key_for_idx(idx, topic).await,
            enc.payload_for_idx(idx, topic).await,
        );
        send_message(topic, &key, Some(&payload), idx, partitions, &producer).await;
    }

    // Every emitted document must sit at or beyond its partition's end offset
    // here. That is what "skipped the retained history" means, and it holds
    // however long assignment takes.
    let fixture_end = end_offsets(&consumer, topic, partitions);
    assert!(
        fixture_end.values().all(|&o| o > 0),
        "the fixture was not produced, so this test could not detect a binding \
         that failed to skip it: {:?}",
        fixture_end
    );

    let child = spawn_bounded(
        "test_capture_only_changes: flowctl raw preview-next",
        180,
        "flowctl",
        &[
            "--profile",
            "local",
            "raw",
            "preview-next",
            "--source",
            catalog_for("tests/test.only-changes.flow.yaml"),
            "--sessions",
            "1",
            "--delay",
            "10s",
            "--output-state",
            // Without this the connector's own logs never reach stderr, so there
            // is no assignment to wait on and nothing to assert about.
            "--log-json",
        ],
    );

    child.wait_for_assignment(partitions, Duration::from_secs(60));

    for partition in 0..partitions {
        let doc = json!({ "id": format!("post-start-{}", partition) }).to_string();
        send_message(
            topic,
            doc.as_bytes(),
            Some(doc.as_bytes()),
            partition as usize,
            partitions,
            &producer,
        )
        .await;
    }

    // These messages are what ends the capture's single session, so a lost
    // produce would hang the suite. Fail with a name instead.
    let after = end_offsets(&consumer, topic, partitions);
    assert!(
        (0..partitions).all(|p| after[&p] > fixture_end[&p]),
        "the post-start messages never reached the broker, so the capture has \
         nothing to read and would not finish its session: {:?} then {:?}",
        fixture_end,
        after
    );

    let log = child.stderr();
    let stdout = child.wait();
    let stdout = stdout.as_str();

    let docs: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|l| l.starts_with(ONLY_CHANGES_DOC_PREFIX))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(
        docs.len(),
        partitions as usize,
        "expected one document per partition:\n{}",
        stdout
    );

    for doc in &docs {
        let meta = &doc[1]["_meta"];
        let (partition, offset) = (
            meta["partition"].as_i64().unwrap() as i32,
            meta["offset"].as_i64().unwrap(),
        );
        assert!(
            offset >= fixture_end[&partition],
            "captured partition {} at offset {}, behind its pre-capture end \
             offset of {}, so retained history was not skipped:\n{}",
            partition,
            offset,
            fixture_end[&partition],
            doc
        );
    }

    // Support reads these lines to confirm where a capture actually started, so
    // the resolved number has to be in them, not just the symbolic end.
    let assignments: HashMap<i32, serde_json::Value> = log
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v["message"] == "assigned partition")
        .map(|v| (v["fields"]["partition"].as_i64().unwrap() as i32, v))
        .collect();

    for partition in 0..partitions {
        let fields = &assignments
            .get(&partition)
            .unwrap_or_else(|| {
                panic!(
                    "no assignment log for partition {}, so support cannot confirm \
                     where this capture started:\n{}",
                    partition, log
                )
            })["fields"];
        assert_eq!(
            fields["start_offset"].as_i64(),
            Some(fixture_end[&partition]),
            "partition {} logged start_offset {} rather than its resolved end offset \
             of {}: {}",
            partition,
            fields["start_offset"],
            fixture_end[&partition],
            fields
        );
        assert_eq!(
            fields["mode"], "OnlyChanges",
            "the mode belongs in the assignment log: {}",
            fields
        );
    }

    // The final state must also sit past the fixture, so a resumed task does
    // not re-read the history this run skipped.
    let state: serde_json::Value = stdout
        .lines()
        .filter(|l| l.starts_with(r#"["connectorState","#))
        .last()
        .map(|l| serde_json::from_str(l).unwrap())
        .expect("preview must emit a connectorState line");

    let saved = &state[1]["updated"]["bindingStateV1"][topic]["partitions"];
    for partition in 0..partitions {
        let offset = saved[partition.to_string()].as_i64().unwrap();
        assert!(
            offset >= fixture_end[&partition],
            "saved offset {} for partition {} is behind the fixture end of {}:\n{}",
            offset,
            partition,
            fixture_end[&partition],
            state
        );
    }
}

/// Runs a preview of the only-changes source, optionally from a supplied state,
/// and returns its stdout. Used by the tests that care about the state a run
/// emits rather than the documents it captures.
fn preview_only_changes(initial_state: Option<&str>, delay: &str) -> String {
    let mut cmd = Command::new("flowctl");
    cmd.args([
        "--profile",
        "local",
        "preview",
        "--source",
        "tests/test.only-changes.flow.yaml",
        "--sessions",
        "1",
        "--delay",
        delay,
        "--output-state",
    ]);
    if let Some(state) = initial_state {
        cmd.args(["--initial-state", state]);
    }

    let output = cmd.output().unwrap();
    assert!(
        output.status.success(),
        "the capture exited {}:\n{}",
        output.status,
        std::str::from_utf8(&output.stderr).unwrap()
    );
    std::str::from_utf8(&output.stdout).unwrap().to_string()
}

/// The last connector state a preview emitted, as a map of partition to offset.
fn saved_partitions(stdout: &str, topic: &str) -> HashMap<i32, i64> {
    let state: serde_json::Value = stdout
        .lines()
        .filter(|l| l.starts_with(r#"["connectorState","#))
        .last()
        .map(|l| serde_json::from_str(l).unwrap())
        .unwrap_or_else(|| panic!("preview emitted no connectorState line:\n{}", stdout));

    match &state[1]["updated"]["bindingStateV1"][topic]["partitions"] {
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(p, o)| (p.parse().unwrap(), o.as_i64().unwrap()))
            .collect(),
        other => panic!("expected a partitions map, got {}:\n{}", other, stdout),
    }
}

/// A restart before the first message must not skip it. `Offset::End` is
/// re-resolved by the broker on every startup, so a run that checkpoints nothing
/// leaves the next run free to jump past whatever arrived in between.
#[tokio::test]
#[serial_test::serial]
async fn test_capture_only_changes_checkpoints_before_first_message() {
    let topic = ONLY_CHANGES_TOPIC;
    let partitions = 3;

    let (admin, producer, consumer) = kafka_clients();
    recreate_topic(&admin, topic, partitions).await;

    // The history this capture must skip, and must also not resume from.
    let enc = JsonRawTestDataEncoder::new();
    for idx in 0..9 {
        let (key, payload) = (
            enc.key_for_idx(idx, topic).await,
            enc.payload_for_idx(idx, topic).await,
        );
        send_message(topic, &key, Some(&payload), idx, partitions, &producer).await;
    }
    let fixture_end = end_offsets(&consumer, topic, partitions);

    // No message arrives during this run, so the only state it can emit is the
    // one written at assignment.
    let first = preview_only_changes(None, "1s");
    assert!(
        !first
            .lines()
            .any(|l| l.starts_with(ONLY_CHANGES_DOC_PREFIX)),
        "the idle run captured a document, so it did not skip the history:\n{}",
        first
    );

    let saved = saved_partitions(&first, topic);
    for partition in 0..partitions {
        let offset = *saved.get(&partition).unwrap_or_else(|| {
            panic!(
                "the idle run saved no offset for partition {}, so a restart would \
                 re-resolve the end of the partition and skip whatever arrived first:\n{}",
                partition, first
            )
        });
        // One less than the end offset, because state records the last message
        // read and the next startup adds one to it.
        assert_eq!(
            offset,
            fixture_end[&partition] - 1,
            "partition {} saved {}, which does not resume at its end offset of {}",
            partition,
            offset,
            fixture_end[&partition]
        );
    }

    // Now the restart. These messages landed while nothing was running, which is
    // exactly the window in which they used to be lost.
    for partition in 0..partitions {
        let doc = json!({ "id": format!("after-restart-{}", partition) }).to_string();
        send_message(
            topic,
            doc.as_bytes(),
            Some(doc.as_bytes()),
            partition as usize,
            partitions,
            &producer,
        )
        .await;
    }

    let state = json!({ "bindingStateV1": { topic: { "partitions": saved } } });
    let second = preview_only_changes(Some(&state.to_string()), "10s");

    let offsets: Vec<i64> = second
        .lines()
        .filter(|l| l.starts_with(ONLY_CHANGES_DOC_PREFIX))
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()[1]["_meta"]["offset"]
                .as_i64()
                .unwrap()
        })
        .collect();

    assert_eq!(
        offsets.len(),
        partitions as usize,
        "expected the message produced to each partition across the restart:\n{}",
        second
    );
    for partition in 0..partitions {
        assert!(
            offsets.contains(&fixture_end[&partition]),
            "the message at offset {} of partition {} was produced while nothing was \
             running and was never captured: {:?}",
            fixture_end[&partition],
            partition,
            offsets
        );
    }
}

/// The empty-partition boundary. A partition with no messages has a watermark of
/// 0, so the saved offset is -1, and the first message ever produced to it must
/// still be captured.
#[tokio::test]
#[serial_test::serial]
async fn test_capture_only_changes_on_empty_topic() {
    let topic = ONLY_CHANGES_TOPIC;
    let partitions = 3;

    let (admin, producer, _consumer) = kafka_clients();
    recreate_topic(&admin, topic, partitions).await;

    let first = preview_only_changes(None, "1s");
    let saved = saved_partitions(&first, topic);
    for partition in 0..partitions {
        assert_eq!(
            saved.get(&partition),
            Some(&-1),
            "an empty partition must save -1 so that it resumes at offset 0:\n{}",
            first
        );
    }

    for partition in 0..partitions {
        let doc = json!({ "id": format!("first-ever-{}", partition) }).to_string();
        send_message(
            topic,
            doc.as_bytes(),
            Some(doc.as_bytes()),
            partition as usize,
            partitions,
            &producer,
        )
        .await;
    }

    let state = json!({ "bindingStateV1": { topic: { "partitions": saved } } });
    let second = preview_only_changes(Some(&state.to_string()), "10s");

    let offsets: Vec<i64> = second
        .lines()
        .filter(|l| l.starts_with(ONLY_CHANGES_DOC_PREFIX))
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()[1]["_meta"]["offset"]
                .as_i64()
                .unwrap()
        })
        .collect();

    assert_eq!(
        offsets,
        vec![0, 0, 0],
        "every partition's first message sits at offset 0 and must be captured:\n{}",
        second
    );
}

/// Saved state takes precedence over the mode. A binding that has already run
/// resumes from its stored offsets, which is what makes the setting safe to add
/// to a running capture.
#[tokio::test]
#[serial_test::serial]
async fn test_capture_only_changes_respects_saved_state() {
    setup_test().await;

    // Mid-fixture offsets, in the manner of test_capture_resume. If the mode
    // were consulted here the connector would jump to the end of each partition
    // and capture nothing.
    let initial_state = json!({
      "bindingStateV1": {
        "json-raw-topic": {
          "partitions": {
            "0": 1,
            "1": 1,
            "2": 1
          }
        }
      }
    });

    let output = bounded_command(
        "test_capture_only_changes_respects_saved_state: flowctl raw preview-next",
        180,
        "flowctl",
        &[
            "--profile",
            "local",
            "raw",
            "preview-next",
            "--source",
            catalog_for("tests/test.only-changes-saved-state.flow.yaml"),
            "--sessions",
            "1",
            "--delay",
            "2s",
            "--output-state",
            "--initial-state",
            &initial_state.to_string(),
        ],
    );

    assert!(
        output.status.success(),
        "the capture exited {}:
{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // The supplied state stops at offset 1 of each partition, so resuming reads
    // offsets 2 and 3. Under the mode the binding would have jumped to the end
    // and captured nothing at all.
    let offsets: Vec<i64> = stdout
        .lines()
        .filter(|l| l.starts_with(JSON_RAW_DOC_PREFIX))
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()[1]["_meta"]["offset"]
                .as_i64()
                .unwrap()
        })
        .collect();

    assert_eq!(
        offsets.len(),
        6,
        "expected the remaining two messages from each of the three partitions, \
         so the mode was ignored once state existed:\n{}",
        stdout
    );
    assert!(
        offsets.iter().all(|&o| o > 1),
        "every resumed document must sit past the supplied offset of 1: {:?}",
        offsets
    );
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

    // test_capture_only_changes owns this topic and produces into it while
    // running. Drop it here rather than at the end of that test, so a panicking
    // run cannot leave it behind for test_discover to enumerate.
    let _ = admin.delete_topics(&[ONLY_CHANGES_TOPIC], &opts).await;

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
