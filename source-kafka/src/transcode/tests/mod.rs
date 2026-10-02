use super::plan::{Card, PlanId};
use super::wire::{
    read_len_prefixed, read_tag, read_varint, skip_value, WT_END_GROUP, WT_FIXED32, WT_FIXED64,
    WT_LEN, WT_START_GROUP, WT_VARINT,
};
use super::wkt::{MAX_TIMESTAMP_SECONDS, MIN_TIMESTAMP_SECONDS};
use super::{Plans, TranscodeError, Transcoder};
use crate::document::differential as corpus;
use crate::protobuf::decode_protobuf_message;
use prost_reflect::{
    bytes::Bytes, prost::Message as _, DescriptorPool, DynamicMessage, MapKey, MessageDescriptor,
    ReflectMessage as _, Value as PValue,
};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

// Field numbers of differential.Everything used when hand-building wire bytes.
const F_DOUBLE: u32 = 1;
const F_FLOAT: u32 = 2;
const F_INT32: u32 = 3;
const F_INT64: u32 = 4;
const F_UINT32: u32 = 5;
const F_SINT32: u32 = 7;
const F_BOOL: u32 = 13;
const F_STRING: u32 = 14;
const F_BYTES: u32 = 15;
const F_ENUM: u32 = 16;
const F_MESSAGE: u32 = 17;
const R_STRING: u32 = 18;
const R_FLOAT: u32 = 19;
const M_STRING: u32 = 21;
const M_BOOL: u32 = 25;
const OPT_INT32: u32 = 26;
const ONE_STRING: u32 = 27;
const ONE_MESSAGE: u32 = 28;
const WKT_TIMESTAMP: u32 = 29;
const WKT_DURATION: u32 = 30;
const WKT_STRUCT: u32 = 31;
const WKT_VALUE: u32 = 32;
const WKT_LIST: u32 = 33;
const WRAP_DOUBLE: u32 = 34;
const WRAP_FLOAT: u32 = 35;
const WRAP_INT64: u32 = 36;
const WRAP_STRING: u32 = 39;
const WKT_MASK: u32 = 41;
const F_ANY: u32 = 43;
const R_SINT32: u32 = 45;
const M_SINT32: u32 = 46;

// Field numbers of differential.Kinds.
const K_R_INT32: u32 = 2;
const K_R_BOOL: u32 = 11;
const K_R_ENUM: u32 = 13;
const K_R_TIMESTAMP: u32 = 14;
const K_MV_DURATION: u32 = 36;
const K_WKT_EMPTY: u32 = 39;
const K_ONE_INT32: u32 = 40;
const K_ONE_ENUM: u32 = 41;
const K_ONE_TIMESTAMP: u32 = 43;
const K_OPT_STRING: u32 = 44;
const K_OPT_ENUM: u32 = 45;
const K_TREE: u32 = 46;
const K_OPT_MESSAGE: u32 = 53;
const K_F_NULL_PLAIN: u32 = 54;
const K_F_SIGNED: u32 = 55;
const K_R_SIGNED: u32 = 56;
const K_F_ALIASED: u32 = 57;

fn everything_desc(pool: &DescriptorPool) -> MessageDescriptor {
    pool.get_message_by_name("differential.Everything").unwrap()
}

/// Today's production path for a protobuf payload, as `pull.rs` drives it.
fn oracle(
    desc: &MessageDescriptor,
    body: &[u8],
    key: Option<&Map<String, Value>>,
    meta: &Value,
) -> std::result::Result<Vec<u8>, String> {
    let message = decode_protobuf_message(desc, body).map_err(|e| format!("{e:#}"))?;
    corpus::merge_via_streaming(&message, key, meta).map_err(|e| e.to_string())
}

/// The transcoder as `pull.rs` wires it: fast path first, the production
/// path when the fast path declines. Returns whether it fell back.
fn transcode_with_fallback(
    plans: &Plans,
    desc: &MessageDescriptor,
    body: &[u8],
    key: Option<&Map<String, Value>>,
    meta: &Value,
    transcoder: &mut Transcoder,
) -> std::result::Result<(Vec<u8>, bool), String> {
    let plan = plans.plan_for(desc).unwrap();
    let mut out = Vec::new();
    match transcoder.transcode(plans, plan, body, key, meta, &mut out) {
        Ok(()) => Ok((out, false)),
        Err(TranscodeError::Unsupported(_)) => oracle(desc, body, key, meta).map(|b| (b, true)),
        Err(TranscodeError::Decode(e)) => Err(e),
    }
}

/// Whether any map in the decoded message has two or more entries, which
/// is the only case where the production path's output bytes are not
/// deterministic (HashMap order).
fn has_unordered_map(message: &DynamicMessage) -> bool {
    if message.descriptor().full_name() == "google.protobuf.Any" {
        // The payload is still bytes here; the production serializer decodes
        // it, so look inside the same way.
        let url = message
            .get_field_by_number(1)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let name = url.rsplit_once('/').map_or("", |(_, name)| name);
        let payload = message
            .get_field_by_number(2)
            .unwrap()
            .as_bytes()
            .unwrap()
            .to_vec();
        return message
            .descriptor()
            .parent_pool()
            .get_message_by_name(name)
            .and_then(|desc| decode_protobuf_message(&desc, &payload).ok())
            .is_some_and(|payload| has_unordered_map(&payload));
    }
    fn value_has(value: &PValue) -> bool {
        match value {
            PValue::Map(m) => m.len() >= 2 || m.values().any(value_has),
            PValue::List(l) => l.iter().any(value_has),
            PValue::Message(m) => has_unordered_map(m),
            _ => false,
        }
    }
    message.fields().any(|(_, v)| value_has(v))
}

fn assert_equivalent(
    label: &str,
    oracle: std::result::Result<Vec<u8>, String>,
    ours: std::result::Result<Vec<u8>, String>,
    require_identical_bytes: bool,
) {
    match (oracle, ours) {
        (Ok(expected), Ok(got)) => {
            if require_identical_bytes {
                assert_eq!(
                    String::from_utf8_lossy(&got),
                    String::from_utf8_lossy(&expected),
                    "{label}: bytes differ"
                );
            } else {
                let keys = corpus::top_level_keys(&got);
                let unique: std::collections::HashSet<&String> = keys.iter().collect();
                assert_eq!(
                    unique.len(),
                    keys.len(),
                    "{label}: duplicate top-level keys"
                );
                let expected: Value = serde_json::from_slice(&expected).unwrap();
                let got: Value = serde_json::from_slice(&got).unwrap();
                assert_eq!(got, expected, "{label}: documents differ");
            }
        }
        (Err(_), Err(_)) => {}
        (Ok(expected), Err(e)) => panic!(
            "{label}: transcoder failed ({e}) where production succeeded with {}",
            String::from_utf8_lossy(&expected)
        ),
        (Err(e), Ok(got)) => panic!(
            "{label}: transcoder produced {} where production failed with {e}",
            String::from_utf8_lossy(&got)
        ),
    }
}

/// Runs both paths on `body` under every key variant.
fn check_body(plans: &Plans, desc: &MessageDescriptor, label: &str, body: &[u8]) -> bool {
    let meta = corpus::meta();
    let mut transcoder = Transcoder::new();
    let require_identical_bytes = match decode_protobuf_message(desc, body) {
        Ok(message) => !has_unordered_map(&message),
        Err(_) => false,
    };
    let mut fell_back = false;
    for (key_label, key) in corpus::key_variants() {
        let expected = oracle(desc, body, key.as_ref(), &meta);
        let got = transcode_with_fallback(plans, desc, body, key.as_ref(), &meta, &mut transcoder)
            .map(|(bytes, fallback)| {
                fell_back |= fallback;
                bytes
            });
        assert_equivalent(
            &format!("{label}/{key_label}"),
            expected,
            got,
            require_identical_bytes,
        );
    }
    fell_back
}

// --- wire-building helpers -------------------------------------------

fn varint(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return out;
        }
        out.push((v & 0x7F) as u8 | 0x80);
        v >>= 7;
    }
}

fn tag(number: u32, wt: u8) -> Vec<u8> {
    varint(((number as u64) << 3) | wt as u64)
}

fn vi(number: u32, v: u64) -> Vec<u8> {
    [tag(number, WT_VARINT), varint(v)].concat()
}

fn ld(number: u32, payload: &[u8]) -> Vec<u8> {
    [
        tag(number, WT_LEN),
        varint(payload.len() as u64),
        payload.to_vec(),
    ]
    .concat()
}

fn f32f(number: u32, v: f32) -> Vec<u8> {
    [
        tag(number, WT_FIXED32).as_slice(),
        &v.to_bits().to_le_bytes(),
    ]
    .concat()
}

fn f64f(number: u32, v: f64) -> Vec<u8> {
    [
        tag(number, WT_FIXED64).as_slice(),
        &v.to_bits().to_le_bytes(),
    ]
    .concat()
}

/// Splits a message into its top-level field encodings.
fn chunks(buf: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let (number, wt, after) = read_tag(buf, pos).unwrap();
        let end = skip_value(buf, after, wt, number).unwrap();
        out.push(buf[pos..end].to_vec());
        pos = end;
    }
    out
}

/// A varint for `v` padded with `extra` redundant continuation bytes.
fn padded_varint(v: u64, extra: usize) -> Vec<u8> {
    let mut out = varint(v);
    let last = out.pop().unwrap();
    out.push(last | 0x80);
    out.extend(std::iter::repeat_n(0x80, extra - 1));
    out.push(0x00);
    out
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
    fn shuffle<T>(&mut self, xs: &mut [T]) {
        for i in (1..xs.len()).rev() {
            let j = self.below(i + 1);
            xs.swap(i, j);
        }
    }
}

const STRINGS: &[&str] = &[
    "",
    "a",
    "héllo \"world\"\n☃",
    "\u{0}\u{1}\u{1f}\u{7f}",
    "back\\slash/solidus",
    "emoji 🦀 and 中文",
    "_meta",
    "0123456789abcdef0123456789abcdef",
];

fn random_string(rng: &mut Rng) -> String {
    if rng.chance(70) {
        return rng.pick(STRINGS).to_string();
    }
    let len = rng.below(40);
    (0..len)
        .map(|_| {
            let c = rng.below(0x80) as u8 as char;
            if c.is_control() && !rng.chance(20) {
                'x'
            } else {
                c
            }
        })
        .collect()
}

fn random_f32(rng: &mut Rng) -> f32 {
    *rng.pick(&[
        0.0,
        -0.0,
        0.5,
        -1.25,
        0.1,
        1e-45,
        f32::MAX,
        f32::MIN_POSITIVE,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        16_777_216.0,
        3.5,
    ])
}

fn random_f64(rng: &mut Rng) -> f64 {
    *rng.pick(&[
        0.0,
        -0.0,
        0.1,
        2.5,
        1e300,
        5e-324,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -123.456,
    ])
}

fn random_i32(rng: &mut Rng) -> i32 {
    *rng.pick(&[0, 1, -1, 42, i32::MAX, i32::MIN, 1 << 20])
}

fn random_i64(rng: &mut Rng) -> i64 {
    *rng.pick(&[0, 1, -1, i64::MAX, i64::MIN, 1 << 40, -(1 << 33)])
}

/// A random instance of any corpus message: each field set with probability
/// one half, scalars drawn from edge-case pools, message values recursing to
/// a bounded depth. Well-known types stay inside the ranges the production
/// path accepts, so most random messages exercise output, not errors.
fn random_message(
    pool: &DescriptorPool,
    desc: &MessageDescriptor,
    rng: &mut Rng,
    depth: u32,
) -> DynamicMessage {
    use prost_reflect::Kind;
    if desc.full_name() == "google.protobuf.Any" {
        let target = *rng.pick(&[
            "differential.Nested",
            "differential.Tree",
            "google.protobuf.Timestamp",
            "google.protobuf.Struct",
        ]);
        let payload = random_message(pool, &pool.get_message_by_name(target).unwrap(), rng, depth);
        let any = prost_types::Any {
            type_url: format!("type.googleapis.com/{target}"),
            value: payload.encode_to_vec(),
        };
        return corpus::from_prost(pool, "google.protobuf.Any", &any);
    }
    let mut msg = DynamicMessage::new(desc.clone());
    for field in desc.fields() {
        if !rng.chance(50) {
            continue;
        }
        let value = if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                unreachable!("map fields are messages")
            };
            let key_kind = entry.map_entry_key_field().kind();
            let value_field = entry.map_entry_value_field();
            let map: HashMap<MapKey, PValue> = (0..rng.below(4))
                .map(|_| {
                    (
                        random_map_key(&key_kind, rng),
                        random_single(pool, &value_field, rng, depth),
                    )
                })
                .collect();
            PValue::Map(map)
        } else if field.is_list() {
            PValue::List(
                (0..rng.below(4))
                    .map(|_| random_single(pool, &field, rng, depth))
                    .collect(),
            )
        } else {
            random_single(pool, &field, rng, depth)
        };
        msg.set_field(&field, value);
    }
    msg
}

fn random_map_key(kind: &prost_reflect::Kind, rng: &mut Rng) -> MapKey {
    use prost_reflect::Kind;
    match kind {
        Kind::Bool => MapKey::Bool(rng.chance(50)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => MapKey::I32(random_i32(rng)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => MapKey::I64(random_i64(rng)),
        Kind::Uint32 | Kind::Fixed32 => MapKey::U32(*rng.pick(&[0, 1, 7, u32::MAX])),
        Kind::Uint64 | Kind::Fixed64 => MapKey::U64(*rng.pick(&[0, 1, 8, u64::MAX])),
        Kind::String => MapKey::String(random_string(rng)),
        other => unreachable!("{other:?} is not a map key kind"),
    }
}

/// One value for a field, or one element of a repeated or map field.
fn random_single(
    pool: &DescriptorPool,
    field: &prost_reflect::FieldDescriptor,
    rng: &mut Rng,
    depth: u32,
) -> PValue {
    use prost_reflect::Kind;
    let owner = field.parent_message();
    match (owner.full_name(), field.name()) {
        ("google.protobuf.Timestamp", "seconds") => {
            return PValue::I64(*rng.pick(&[
                0,
                1_730_233_606,
                -1,
                MIN_TIMESTAMP_SECONDS,
                MAX_TIMESTAMP_SECONDS,
            ]))
        }
        ("google.protobuf.Timestamp", "nanos") => {
            return PValue::I32(*rng.pick(&[0, 123_000_000, 123_456_000, 123_456_789, 999_999_999]))
        }
        ("google.protobuf.Duration", "seconds") => {
            return PValue::I64(*rng.pick(&[0, 3600, -1, 315_576_000_000]))
        }
        ("google.protobuf.Duration", "nanos") => {
            return PValue::I32(*rng.pick(&[0, 500_000_000, 1_000, 1, -500_000_000]))
        }
        ("google.protobuf.Value", "number_value") => {
            return PValue::F64(*rng.pick(&[0.0, 1.5, -2.0, 1e10]))
        }
        ("google.protobuf.FieldMask", "paths") => {
            return PValue::String(rng.pick(&["a.b_c", "snake_case", "x"]).to_string())
        }
        _ => {}
    }
    match field.kind() {
        Kind::Double => PValue::F64(random_f64(rng)),
        Kind::Float => PValue::F32(random_f32(rng)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => PValue::I32(random_i32(rng)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => PValue::I64(random_i64(rng)),
        Kind::Uint32 | Kind::Fixed32 => PValue::U32(*rng.pick(&[0, 1, 7, u32::MAX])),
        Kind::Uint64 | Kind::Fixed64 => PValue::U64(*rng.pick(&[0, 1, 8, u64::MAX])),
        Kind::Bool => PValue::Bool(rng.chance(50)),
        Kind::String => PValue::String(random_string(rng)),
        Kind::Bytes => {
            let bytes: Vec<u8> = (0..rng.below(8)).map(|_| rng.next() as u8).collect();
            PValue::Bytes(Bytes::from(bytes))
        }
        Kind::Enum(e) => {
            // Declared numbers, plus ones the enum does not declare.
            let mut numbers: Vec<i32> = e.values().map(|v| v.number()).collect();
            numbers.extend([42, -1]);
            PValue::EnumNumber(*rng.pick(&numbers))
        }
        Kind::Message(m) => {
            if depth >= 3 {
                PValue::Message(DynamicMessage::new(m))
            } else {
                PValue::Message(random_message(pool, &m, rng, depth + 1))
            }
        }
    }
}

/// `differential.Kinds` with every field set, for the byte snapshot and the
/// truncation sweep.
fn kinds_fixture(pool: &DescriptorPool) -> DynamicMessage {
    let s = |v: &str| PValue::String(v.to_string());
    let strs = |pairs: Vec<(&str, PValue)>| {
        PValue::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (MapKey::String(k.to_string()), v))
                .collect::<HashMap<_, _>>(),
        )
    };
    let ts = |seconds: i64, nanos: i32| {
        PValue::Message(corpus::from_prost(
            pool,
            "google.protobuf.Timestamp",
            &prost_types::Timestamp { seconds, nanos },
        ))
    };
    let dur = |seconds: i64, nanos: i32| {
        PValue::Message(corpus::from_prost(
            pool,
            "google.protobuf.Duration",
            &prost_types::Duration { seconds, nanos },
        ))
    };
    let tree = |label: &str, children: Vec<PValue>, named: Vec<(&str, PValue)>| {
        PValue::Message(corpus::message_of(
            pool,
            "differential.Tree",
            vec![
                ("label", s(label)),
                ("children", PValue::List(children)),
                ("named", strs(named)),
            ],
        ))
    };
    corpus::message_of(
        pool,
        "differential.Kinds",
        vec![
            (
                "r_double",
                PValue::List(vec![
                    PValue::F64(0.1),
                    PValue::F64(-0.0),
                    PValue::F64(f64::INFINITY),
                ]),
            ),
            (
                "r_int32",
                PValue::List(vec![PValue::I32(i32::MIN), PValue::I32(0), PValue::I32(1)]),
            ),
            (
                "r_int64",
                PValue::List(vec![PValue::I64(i64::MAX), PValue::I64(-1)]),
            ),
            (
                "r_uint32",
                PValue::List(vec![PValue::U32(u32::MAX), PValue::U32(0)]),
            ),
            (
                "r_uint64",
                PValue::List(vec![PValue::U64(u64::MAX), PValue::U64(0)]),
            ),
            (
                "r_sint64",
                PValue::List(vec![PValue::I64(i64::MIN), PValue::I64(1)]),
            ),
            (
                "r_fixed32",
                PValue::List(vec![PValue::U32(7), PValue::U32(u32::MAX)]),
            ),
            (
                "r_fixed64",
                PValue::List(vec![PValue::U64(8), PValue::U64(u64::MAX)]),
            ),
            (
                "r_sfixed32",
                PValue::List(vec![PValue::I32(-7), PValue::I32(i32::MAX)]),
            ),
            (
                "r_sfixed64",
                PValue::List(vec![PValue::I64(-8), PValue::I64(i64::MIN)]),
            ),
            (
                "r_bool",
                PValue::List(vec![PValue::Bool(true), PValue::Bool(false)]),
            ),
            (
                "r_bytes",
                PValue::List(vec![
                    PValue::Bytes(Bytes::from_static(b"")),
                    PValue::Bytes(Bytes::from_static(&[0xFF, 0x00])),
                ]),
            ),
            (
                "r_enum",
                PValue::List(vec![
                    PValue::EnumNumber(1),
                    PValue::EnumNumber(0),
                    PValue::EnumNumber(42),
                ]),
            ),
            (
                "r_timestamp",
                PValue::List(vec![ts(0, 0), ts(1_730_233_606, 123_456_789)]),
            ),
            (
                "m_uint32",
                PValue::Map(HashMap::from([
                    (MapKey::U32(u32::MAX), s("max")),
                    (MapKey::U32(0), s("zero")),
                ])),
            ),
            (
                "m_sint64",
                PValue::Map(HashMap::from([
                    (MapKey::I64(i64::MIN), s("min")),
                    (MapKey::I64(1), s("one")),
                ])),
            ),
            (
                "m_fixed32",
                PValue::Map(HashMap::from([(MapKey::U32(7), s("seven"))])),
            ),
            (
                "m_fixed64",
                PValue::Map(HashMap::from([(MapKey::U64(u64::MAX), s("max"))])),
            ),
            (
                "m_sfixed32",
                PValue::Map(HashMap::from([(MapKey::I32(-7), s("neg"))])),
            ),
            (
                "m_sfixed64",
                PValue::Map(HashMap::from([(MapKey::I64(-8), s("neg"))])),
            ),
            (
                "mv_double",
                strs(vec![("a", PValue::F64(0.1)), ("b", PValue::F64(f64::NAN))]),
            ),
            (
                "mv_float",
                strs(vec![("a", PValue::F32(0.5)), ("b", PValue::F32(0.0))]),
            ),
            (
                "mv_int32",
                strs(vec![("a", PValue::I32(i32::MIN)), ("b", PValue::I32(0))]),
            ),
            ("mv_int64", strs(vec![("a", PValue::I64(i64::MAX))])),
            ("mv_uint32", strs(vec![("a", PValue::U32(u32::MAX))])),
            ("mv_uint64", strs(vec![("a", PValue::U64(u64::MAX))])),
            ("mv_sint32", strs(vec![("a", PValue::I32(-42))])),
            ("mv_sint64", strs(vec![("a", PValue::I64(i64::MIN))])),
            ("mv_fixed32", strs(vec![("a", PValue::U32(7))])),
            ("mv_fixed64", strs(vec![("a", PValue::U64(8))])),
            ("mv_sfixed32", strs(vec![("a", PValue::I32(-7))])),
            ("mv_sfixed64", strs(vec![("a", PValue::I64(-8))])),
            (
                "mv_bool",
                strs(vec![("t", PValue::Bool(true)), ("f", PValue::Bool(false))]),
            ),
            (
                "mv_bytes",
                strs(vec![
                    ("a", PValue::Bytes(Bytes::from_static(b"hi"))),
                    ("e", PValue::Bytes(Bytes::from_static(b""))),
                ]),
            ),
            (
                "mv_enum",
                strs(vec![
                    ("a", PValue::EnumNumber(2)),
                    ("z", PValue::EnumNumber(0)),
                    ("u", PValue::EnumNumber(42)),
                ]),
            ),
            (
                "mv_duration",
                strs(vec![("a", dur(1, 500_000_000)), ("z", dur(0, 0))]),
            ),
            (
                "wrap_int32",
                corpus::wrapper(pool, "google.protobuf.Int32Value", PValue::I32(i32::MIN)),
            ),
            (
                "wrap_uint32",
                corpus::wrapper(pool, "google.protobuf.UInt32Value", PValue::U32(0)),
            ),
            (
                "wkt_empty",
                PValue::Message(corpus::message_of(pool, "google.protobuf.Empty", vec![])),
            ),
            ("one_enum", PValue::EnumNumber(0)),
            ("opt_string", s("")),
            ("opt_enum", PValue::EnumNumber(0)),
            (
                "r_wrap_float",
                PValue::List(vec![
                    corpus::wrapper(pool, "google.protobuf.FloatValue", PValue::F32(0.5)),
                    corpus::wrapper(pool, "google.protobuf.FloatValue", PValue::F32(f32::NAN)),
                ]),
            ),
            (
                "mv_wrap_int64",
                strs(vec![(
                    "a",
                    corpus::wrapper(pool, "google.protobuf.Int64Value", PValue::I64(i64::MIN)),
                )]),
            ),
            (
                "r_any",
                PValue::List(vec![PValue::Message(corpus::from_prost(
                    pool,
                    "google.protobuf.Any",
                    &prost_types::Any {
                        type_url: "type.googleapis.com/google.protobuf.Empty".to_string(),
                        value: vec![],
                    },
                ))]),
            ),
            (
                "r_struct",
                PValue::List(vec![PValue::Message(corpus::from_prost(
                    pool,
                    "google.protobuf.Struct",
                    &prost_types::Struct {
                        fields: std::collections::BTreeMap::from([(
                            "k".to_string(),
                            corpus::pv(prost_types::value::Kind::BoolValue(true)),
                        )]),
                    },
                ))]),
            ),
            (
                "mv_value",
                strs(vec![(
                    "n",
                    PValue::Message(corpus::from_prost(
                        pool,
                        "google.protobuf.Value",
                        &corpus::pv(prost_types::value::Kind::NumberValue(1.5)),
                    )),
                )]),
            ),
            (
                "r_list",
                PValue::List(vec![PValue::Message(corpus::from_prost(
                    pool,
                    "google.protobuf.ListValue",
                    &prost_types::ListValue {
                        values: vec![corpus::pv(prost_types::value::Kind::NullValue(0))],
                    },
                ))]),
            ),
            // Present and empty: proto3 optional keeps it.
            (
                "opt_message",
                PValue::Message(corpus::nested(pool, "", 0.0, vec![])),
            ),
            // NullValue's only value is its default, so this is omitted.
            ("f_null_plain", PValue::EnumNumber(0)),
            ("f_signed", PValue::EnumNumber(-1)),
            (
                "r_signed",
                PValue::List(vec![
                    PValue::EnumNumber(7),
                    PValue::EnumNumber(-1),
                    PValue::EnumNumber(0),
                ]),
            ),
            ("f_aliased", PValue::EnumNumber(1)),
            (
                "tree",
                tree(
                    "root",
                    vec![
                        tree("leaf", vec![], vec![]),
                        tree("", vec![tree("deep", vec![], vec![])], vec![]),
                    ],
                    vec![("n", tree("named", vec![], vec![]))],
                ),
            ),
        ],
    )
}

/// Splits a packed repeated field into one encoding per element, if `part`
/// is one; otherwise returns it unchanged.
fn unpacked(plans: &Plans, plan: PlanId, part: &[u8]) -> Vec<Vec<u8>> {
    let (number, wt, after) = read_tag(part, 0).unwrap();
    let field = plans.messages[plan as usize]
        .lookup
        .get(number)
        .map(|idx| &plans.messages[plan as usize].fields[idx]);
    let Some(field) = field else {
        return vec![part.to_vec()];
    };
    if wt != WT_LEN || !matches!(field.card, Card::Repeated) || !field.leaf.is_packable() {
        return vec![part.to_vec()];
    }
    let element_wt = field.leaf.wire_type();
    let (s, e) = read_len_prefixed(part, after).unwrap();
    let mut out = Vec::new();
    let mut pos = s;
    while pos < e {
        let next = skip_value(&part[..e], pos, element_wt, number).unwrap();
        out.push([tag(number, element_wt).as_slice(), &part[pos..next]].concat());
        pos = next;
    }
    out
}

mod differential;
mod malformed;
mod proto2;
mod wkt;

/// A sparse-numbered corpus message exercises the binary-search field lookup
/// that messages with small field numbers never reach.
#[test]
fn sparse_field_numbers_match_production_path() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let sparse = pool.get_message_by_name("differential.Sparse").unwrap();
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("low-only", vi(1, 5)),
        ("high-string", ld(2000, b"far")),
        (
            "packed-high",
            ld(3000, &[varint(1), varint(2), varint(300)].concat()),
        ),
        ("unpacked-high", [vi(3000, 7), vi(3000, 8)].concat()),
        (
            "unknown-between",
            [vi(1, 1), vi(2500, 9), ld(2000, b"x")].concat(),
        ),
        ("wrong-wire-type-high", vi(2000, 1)),
    ];
    for (label, body) in cases {
        check_body(&plans, &sparse, label, &body);
    }
}
