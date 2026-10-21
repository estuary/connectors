//! Stage-by-stage timing of the per-message CPU path for registry protobuf
//! payloads so each stage's cost is explicit.
//!
//! Run with `cargo bench --bench protobuf_path`. Kafka I/O runs on librdkafka's
//! own threads and is not modeled here; this is the work the single tokio
//! worker does for every message, from framed payload bytes to the bytes
//! handed to the runtime.
//!
//! Stages mirror `do_pull` / `parse_datum` in src/pull.rs:
//!   meta        build the `_meta` object (topic string, RFC3339 timestamp, headers)
//!   key         parse the message key to a `serde_json::Map`
//!   resolve     parse message indexes + resolve the descriptor from the pool
//!   decode      `DynamicMessage::decode`
//!   serialize   stream the message through `MergeSerializer` into a fresh Vec
//!   envelope    wrap `doc_json` in a `Captured` response via `write_capture_response`
//!   checkpoint  serialize the per-message checkpoint response
//!   flush       write both lines through the 256 KiB `BufWriter` and flush, as
//!               the pull loop does after every checkpoint; measured against
//!               /dev/null, so a floor for the write(2) to the runtime's pipe
//!
//! The table's other columns: `wire` is the encoded protobuf body in bytes,
//! `json` the captured document in bytes (key fields and `_meta` included),
//! and `total` the sum of the stages. Times are ns per message, the median of
//! seven rounds. Later changes add a column per path they introduce and report
//! against these same names.
//!
//! Not modeled: the schema-cache and binding lookups (a few hash probes per
//! message), the nested-message index path (`resolve` sees the common
//! zero-index framing), and Kafka I/O. The bench links the connector's
//! jemalloc allocator so allocation costs match production.

// The connector binary installs jemalloc as the global allocator; the bench
// must too or allocation-heavy stages measure glibc malloc instead.
extern crate allocator;

use std::collections::HashMap;
use std::hint::black_box;
use std::io::Write;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;

use prost_reflect::{
    bytes::Bytes, prost::Message as _, DescriptorPool, DynamicMessage, MapKey, SerializeOptions,
    Value as PValue,
};
use proto_flow::capture::{
    response::{Captured, Checkpoint},
    Response,
};
use proto_flow::flow::ConnectorState;
use serde::Serialize;
use serde_json::{json, Map, Value};
use source_kafka::document::MergeSerializer;
use source_kafka::protobuf::{
    decode_protobuf_message, parse_message_indexes, resolve_message_from_indexes,
};
use source_kafka::pull::write_checkpoint;
use source_kafka::{write_capture_response, write_captured};

/// Same shape as the private `Meta` in src/pull.rs.
#[derive(Serialize, Default)]
struct Meta {
    topic: String,
    partition: i32,
    offset: i64,
    op: String,
    headers: Option<Map<String, Value>>,
    timestamp: Option<MetaTimestamp>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum MetaTimestamp {
    CreationTime(String),
}

#[derive(Serialize)]
struct CaptureState {
    #[serde(rename = "bindingStateV1")]
    resources: HashMap<String, ResourceState>,
}

#[derive(Serialize)]
struct ResourceState {
    partitions: HashMap<i32, i64>,
}

struct Fixture {
    name: &'static str,
    message_name: &'static str,
    /// Confluent-framed payload: magic byte, schema id, message indexes, body.
    framed: Vec<u8>,
    /// Confluent-framed key.
    framed_key: Vec<u8>,
}

fn pool() -> DescriptorPool {
    let set = protox::compile(
        ["bench_corpus.proto"],
        [concat!(env!("CARGO_MANIFEST_DIR"), "/src/testdata")],
    )
    .expect("bench corpus must compile");
    DescriptorPool::from_file_descriptor_set(set).expect("bench descriptors must load")
}

fn frame(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 6);
    out.push(0);
    out.extend_from_slice(&7u32.to_be_bytes());
    // A zero-length index array selects the file's first message.
    out.push(0);
    out.extend_from_slice(body);
    out
}

fn message(pool: &DescriptorPool, name: &str, fields: Vec<(&str, PValue)>) -> DynamicMessage {
    let desc = pool
        .get_message_by_name(name)
        .unwrap_or_else(|| panic!("missing {name}"));
    let mut msg = DynamicMessage::new(desc);
    for (field, value) in fields {
        msg.set_field_by_name(field, value);
    }
    msg
}

fn s(v: &str) -> PValue {
    PValue::String(v.to_string())
}

fn fixtures(pool: &DescriptorPool) -> Vec<Fixture> {
    let key = message(
        pool,
        "bench.Key",
        vec![
            ("request_id", s("3f1c2a9e-77b4-4c1e-9d2a-5b6e7f8a9b0c")),
            ("shard", PValue::I32(7)),
        ],
    )
    .encode_to_vec();

    let usage = |p: i32, c: i32, cost: f32| {
        PValue::Message(message(
            pool,
            "bench.Usage",
            vec![
                ("reads", PValue::I32(p)),
                ("writes", PValue::I32(c)),
                ("cost", PValue::F32(cost)),
            ],
        ))
    };

    let ts_desc = pool
        .get_message_by_name("google.protobuf.Timestamp")
        .unwrap();
    let ts = prost_types::Timestamp {
        seconds: 1_759_320_000,
        nanos: 123_456_000,
    };
    let ts =
        PValue::Message(DynamicMessage::decode(ts_desc, ts.encode_to_vec().as_slice()).unwrap());

    let tiny = message(
        pool,
        "bench.Tiny",
        vec![
            ("id", PValue::I64(9_007_199_254)),
            ("kind", s("purchase")),
            ("n", PValue::I32(42)),
            ("ok", PValue::Bool(true)),
        ],
    );

    let event = message(
        pool,
        "bench.Event",
        vec![
            ("request_id", s("3f1c2a9e-77b4-4c1e-9d2a-5b6e7f8a9b0c")),
            ("service", s("search-api/v3")),
            ("region", s("us-west-2")),
            ("user_id", PValue::I64(184_467_440_737)),
            ("status", PValue::EnumNumber(1)),
            ("ts", ts.clone()),
            ("latency_ms", PValue::F64(812.375)),
            ("bytes_in", PValue::I32(1_024)),
            ("bytes_out", PValue::I32(256)),
            ("cached", PValue::Bool(false)),
            (
                "labels",
                PValue::Map(HashMap::from([
                    (MapKey::String("tier".into()), s("enterprise")),
                    (MapKey::String("zone".into()), s("us-east-1a")),
                    (MapKey::String("client".into()), s("python-sdk/1.4.2")),
                ])),
            ),
            ("usage", usage(1_024, 256, 0.0031)),
            (
                "tags",
                PValue::List(vec![s("retry"), s("cached"), s("canary")]),
            ),
        ],
    );

    let wide_fields: Vec<(String, PValue)> = (1..=40)
        .map(|i| {
            let name = format!("f{i:02}");
            let value = match i % 8 {
                1 => PValue::I32(i * 11),
                2 => PValue::I64(i as i64 * 1_000_003),
                3 => s(&format!("value-{i:02}-abc")),
                4 => PValue::F64(i as f64 * 1.25),
                5 => PValue::Bool(i % 2 == 1),
                6 => PValue::U32(i as u32 * 7),
                7 => PValue::F32(i as f32 * 0.5),
                _ => PValue::I32(-(i * 3)),
            };
            (name, value)
        })
        .collect();
    let wide = message(
        pool,
        "bench.Wide",
        wide_fields
            .iter()
            .map(|(n, v)| (n.as_str(), v.clone()))
            .collect(),
    );

    let paragraph = "The quick brown fox jumps over the lazy dog while the connector streams \
                     protobuf messages into JSON documents for the runtime to validate and reduce. ";
    let text = message(
        pool,
        "bench.Text",
        vec![
            ("title", s("Throughput investigation for source-kafka")),
            ("body", s(&paragraph.repeat(14))),
            (
                "footer",
                s("generated for benchmarking; contains \"quotes\" and a\ttab"),
            ),
            ("id", PValue::I64(77)),
        ],
    );

    let nested = message(
        pool,
        "bench.Nested",
        vec![
            (
                "items",
                PValue::List(
                    (0..10)
                        .map(|i| usage(100 + i, 10 + i, i as f32 * 0.01))
                        .collect(),
                ),
            ),
            (
                "samples",
                PValue::List((0..20).map(|i| PValue::F64(i as f64 * 0.37)).collect()),
            ),
            ("name", s("batch-0042")),
        ],
    );

    [
        ("tiny", "bench.Tiny", tiny),
        ("event", "bench.Event", event),
        ("wide", "bench.Wide", wide),
        ("text", "bench.Text", text),
        ("nested", "bench.Nested", nested),
    ]
    .into_iter()
    .map(|(name, message_name, msg)| Fixture {
        name,
        message_name,
        framed: frame(&msg.encode_to_vec()),
        framed_key: frame(&key),
    })
    .collect()
}

/// ns per call: median over rounds of ~60ms each, after a short warm-up.
fn time(mut f: impl FnMut()) -> f64 {
    let warm = Instant::now();
    let mut n = 0u64;
    while warm.elapsed() < Duration::from_millis(50) {
        f();
        n += 1;
    }
    let per_call = warm.elapsed().as_nanos() as f64 / n.max(1) as f64;
    let iters = ((60_000_000.0 / per_call).ceil() as u64).max(10);

    let mut samples = Vec::with_capacity(7);
    for _ in 0..7 {
        let start = Instant::now();
        for _ in 0..iters {
            f();
        }
        samples.push(start.elapsed().as_nanos() as f64 / iters as f64);
    }
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

/// Builds `_meta` the way `do_pull` does: the header value decoded from bytes,
/// the timestamp formatted from the message's millisecond clock.
fn meta_for(topic: &str, header_value: &[u8], millis: i64) -> Meta {
    let header = match std::str::from_utf8(header_value) {
        Ok(v) => json!(v),
        Err(_) => json!(BASE64.encode(header_value)),
    };
    Meta {
        topic: topic.to_string(),
        partition: 11,
        offset: 1_234_567_890,
        op: "u".to_string(),
        headers: Some(Map::from_iter([("header-key".to_string(), header)])),
        timestamp: Some(MetaTimestamp::CreationTime(unix_millis_to_rfc3339(millis))),
    }
}

/// Same as the private helper in src/pull.rs.
fn unix_millis_to_rfc3339(millis: i64) -> String {
    let time = time::OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(millis);
    time.format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

fn key_fields(pool: &DescriptorPool, framed_key: &[u8]) -> Map<String, Value> {
    let (indexes, off) = parse_message_indexes(&framed_key[5..]).unwrap();
    let desc = resolve_message_from_indexes(pool, "bench.Key", &indexes).unwrap();
    let msg = decode_protobuf_message(&desc, &framed_key[5 + off..]).unwrap();
    let value: Value = msg
        .serialize_with_options(
            serde_json::value::Serializer,
            &SerializeOptions::new().use_proto_field_name(true),
        )
        .unwrap();
    match value {
        Value::Object(map) => map,
        _ => unreachable!(),
    }
}

fn serialize_streaming(
    msg: &DynamicMessage,
    key: Option<&Map<String, Value>>,
    meta: &Meta,
) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut ser = serde_json::Serializer::new(&mut buf);
        msg.serialize_with_options(
            MergeSerializer::new(&mut ser, key, meta),
            &SerializeOptions::new().use_proto_field_name(true),
        )
        .unwrap();
    }
    buf
}

struct Row {
    name: &'static str,
    wire_bytes: usize,
    json_bytes: usize,
    meta: f64,
    key: f64,
    resolve: f64,
    decode: f64,
    serialize: f64,
    envelope: f64,
    checkpoint: f64,
    flush: f64,
    raw_envelope: f64,
    raw_checkpoint: f64,
}

impl Row {
    fn current_total(&self) -> f64 {
        self.meta
            + self.key
            + self.resolve
            + self.decode
            + self.serialize
            + self.envelope
            + self.checkpoint
            + self.flush
    }
    /// Writing the Captured and Checkpoint responses as raw bytes instead of
    /// through `Response` serialization, on its own.
    fn raw_total(&self) -> f64 {
        self.current_total() - self.envelope - self.checkpoint
            + self.raw_envelope
            + self.raw_checkpoint
    }
}

fn main() {
    let pool = pool();
    let fixtures = fixtures(&pool);
    let mut rows = Vec::new();

    for fixture in &fixtures {
        let framed = fixture.framed.as_slice();
        let (indexes, off) = parse_message_indexes(&framed[5..]).unwrap();
        let desc = resolve_message_from_indexes(&pool, fixture.message_name, &indexes).unwrap();
        let body = &framed[5 + off..];
        let decoded = decode_protobuf_message(&desc, body).unwrap();
        let meta = meta_for("service-events", b"header-value", 1_759_320_000_123);
        let key = key_fields(&pool, &fixture.framed_key);
        let json = serialize_streaming(&decoded, Some(&key), &meta);

        let meta_ns = time(|| {
            black_box(meta_for(
                black_box("service-events"),
                black_box(b"header-value"),
                black_box(1_759_320_000_123),
            ));
        });
        let key_ns = time(|| {
            black_box(key_fields(&pool, black_box(&fixture.framed_key)));
        });
        let resolve_ns = time(|| {
            let (indexes, _) = parse_message_indexes(black_box(&framed[5..])).unwrap();
            black_box(resolve_message_from_indexes(&pool, fixture.message_name, &indexes).unwrap());
        });
        let decode_ns = time(|| {
            black_box(decode_protobuf_message(&desc, black_box(body)).unwrap());
        });
        let serialize_ns = time(|| {
            black_box(serialize_streaming(black_box(&decoded), Some(&key), &meta));
        });

        let doc_json: Bytes = json.clone().into();
        let mut sink: Vec<u8> = Vec::with_capacity(json.len() * 2);
        let envelope_ns = time(|| {
            sink.clear();
            write_capture_response(
                Response {
                    captured: Some(Captured {
                        binding: 3,
                        doc_json: doc_json.clone(),
                    }),
                    ..Default::default()
                },
                &mut sink,
            )
            .unwrap();
            black_box(&sink);
        });
        let checkpoint_ns = time(|| {
            sink.clear();
            let mut partitions = HashMap::new();
            partitions.insert(11, 1_234_567_890i64);
            let mut resources = HashMap::new();
            resources.insert("service-events".to_string(), ResourceState { partitions });
            let state = CaptureState { resources };
            write_capture_response(
                Response {
                    checkpoint: Some(Checkpoint {
                        state: Some(ConnectorState {
                            updated_json: serde_json::to_string(&state).unwrap().into(),
                            merge_patch: true,
                        }),
                    }),
                    ..Default::default()
                },
                &mut sink,
            )
            .unwrap();
            black_box(&sink);
        });

        // The write(2) the pull loop pays per message: both lines through the
        // same BufWriter the connector uses, flushed, into /dev/null.
        let mut lines = Vec::new();
        write_captured(3, &json, &mut lines).unwrap();
        write_checkpoint("service-events", 11, 1_234_567_890, &mut lines).unwrap();
        let mut pipe = std::io::BufWriter::with_capacity(
            256 * 1024,
            std::fs::File::create("/dev/null").unwrap(),
        );
        let flush_ns = time(|| {
            pipe.write_all(black_box(&lines)).unwrap();
            pipe.flush().unwrap();
        });

        // The raw writers the connector now uses, checked byte-identical
        // against `Response` serialization here as well as in the unit tests.
        let mut raw = Vec::with_capacity(json.len() * 2);
        write_captured(3, &json, &mut raw).unwrap();
        sink.clear();
        write_capture_response(
            Response {
                captured: Some(Captured {
                    binding: 3,
                    doc_json: doc_json.clone(),
                }),
                ..Default::default()
            },
            &mut sink,
        )
        .unwrap();
        assert_eq!(raw, sink, "raw envelope must match Response serialization");
        let raw_envelope_ns = time(|| {
            raw.clear();
            write_captured(3, black_box(&json), &mut raw).unwrap();
            black_box(&raw);
        });

        raw.clear();
        write_checkpoint("service-events", 11, 1_234_567_890, &mut raw).unwrap();
        sink.clear();
        {
            let mut partitions = HashMap::new();
            partitions.insert(11, 1_234_567_890i64);
            let mut resources = HashMap::new();
            resources.insert("service-events".to_string(), ResourceState { partitions });
            write_capture_response(
                Response {
                    checkpoint: Some(Checkpoint {
                        state: Some(ConnectorState {
                            updated_json: serde_json::to_string(&CaptureState { resources })
                                .unwrap()
                                .into(),
                            merge_patch: true,
                        }),
                    }),
                    ..Default::default()
                },
                &mut sink,
            )
            .unwrap();
        }
        assert_eq!(
            String::from_utf8_lossy(&raw),
            String::from_utf8_lossy(&sink),
            "raw checkpoint must match Response serialization"
        );
        let raw_checkpoint_ns = time(|| {
            raw.clear();
            write_checkpoint(
                "service-events",
                black_box(11),
                black_box(1_234_567_890),
                &mut raw,
            )
            .unwrap();
            black_box(&raw);
        });

        rows.push(Row {
            name: fixture.name,
            wire_bytes: body.len(),
            json_bytes: json.len(),
            meta: meta_ns,
            key: key_ns,
            resolve: resolve_ns,
            decode: decode_ns,
            serialize: serialize_ns,
            envelope: envelope_ns,
            checkpoint: checkpoint_ns,
            flush: flush_ns,
            raw_envelope: raw_envelope_ns,
            raw_checkpoint: raw_checkpoint_ns,
        });
    }

    println!();
    println!(
        "{:<8} {:>5} {:>5} | {:>6} {:>6} {:>7} {:>7} {:>9} {:>8} {:>10} {:>6} | {:>7} | {:>7} {:>7} | {:>7}",
        "shape", "wire", "json", "meta", "key", "resolve", "decode", "serialize", "envelope",
        "checkpoint", "flush", "total", "raw-env", "raw-ckp", "raw"
    );
    println!(
        "{:<8} {:>5} {:>5} | {:>6} {:>6} {:>7} {:>7} {:>9} {:>8} {:>10} {:>6} | {:>7} | {:>7} {:>7} | {:>7}",
        "", "bytes", "bytes", "ns", "ns", "ns", "ns", "ns", "ns", "ns", "ns", "ns/msg", "ns", "ns", "x"
    );
    for r in &rows {
        let total = r.current_total();
        let raw = total / r.raw_total();
        println!(
            "{:<8} {:>5} {:>5} | {:>6.0} {:>6.0} {:>7.0} {:>7.0} {:>9.0} {:>8.0} {:>10.0} {:>6.0} | {:>7.0} | {:>7.0} {:>7.0} | {:>6.2}x",
            r.name, r.wire_bytes, r.json_bytes, r.meta, r.key, r.resolve, r.decode, r.serialize,
            r.envelope, r.checkpoint, r.flush, total, r.raw_envelope, r.raw_checkpoint, raw
        );
    }
    println!();
    println!("wire, json   bytes of the encoded protobuf body and of the captured document (key fields and _meta included)");
    println!("stages       ns per message, median of 7 rounds; total = their sum, the cost of one message on the pull loop");
    println!("flush        the per-message write(2), measured into /dev/null: a floor for the write to the runtime's pipe");
    println!("raw-env/ckp  ns to write the Captured and Checkpoint responses as raw bytes, replacing envelope and checkpoint");
    println!("raw          speedup of total with raw-env and raw-ckp in place of envelope and checkpoint");
    println!();
    println!("Implied single-core throughput at 100% CPU, captured document bytes per second:");
    for r in &rows {
        let now = r.json_bytes as f64 / r.current_total() * 1e9 / (1024.0 * 1024.0);
        println!("  {:<8} {:>8.1} MiB/s", r.name, now);
    }
}
