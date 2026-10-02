//! Malformed input must fail on both paths, and the shapes the transcoder
//! declines must fall back and still match.

use super::*;

#[test]
fn malformed_inputs_fail_on_both_paths() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let meta = corpus::meta();

    // Every truncation of fully populated messages. Many are valid shorter
    // messages; those must agree too.
    let (_, everything) = corpus::corpus(&pool).remove(1);
    let kinds = kinds_fixture(&pool);
    for (label, message) in [("everything", everything), ("kinds", kinds)] {
        let full = message.encode_to_vec();
        for len in 0..full.len() {
            check_body(
                &plans,
                &message.descriptor(),
                &format!("{label}-truncated@{len}"),
                &full[..len],
            );
        }
    }

    let deep = {
        // ListValue holding a Value holding a ListValue ... 120 levels.
        let mut inner: Vec<u8> = Vec::new();
        for _ in 0..120 {
            inner = ld(6, &ld(1, &inner));
        }
        ld(WKT_VALUE, &inner)
    };
    let shallow = {
        let mut inner: Vec<u8> = Vec::new();
        for _ in 0..20 {
            inner = ld(6, &ld(1, &inner));
        }
        ld(WKT_VALUE, &inner)
    };

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("wire-type-6", vec![0x0E, 0x00]),
        ("wire-type-7", vec![0x0F]),
        ("tag-zero", vec![0x00, 0x00]),
        ("stray-end-group", tag(5, WT_END_GROUP)),
        (
            "unterminated-group",
            [tag(250, WT_START_GROUP), vi(1, 1)].concat(),
        ),
        (
            "mismatched-end-group",
            [tag(250, WT_START_GROUP), tag(251, WT_END_GROUP)].concat(),
        ),
        (
            "varint-11-bytes",
            [tag(F_INT64, WT_VARINT), vec![0xFF; 10], vec![0x01]].concat(),
        ),
        (
            "varint-10th-byte-too-big",
            [tag(F_INT64, WT_VARINT), vec![0xFF; 9], vec![0x02]].concat(),
        ),
        (
            "varint-truncated",
            [tag(F_INT64, WT_VARINT), vec![0xFF]].concat(),
        ),
        (
            "length-overrun",
            [tag(F_STRING, WT_LEN), varint(100), b"short".to_vec()].concat(),
        ),
        (
            "length-huge",
            [tag(F_STRING, WT_LEN), varint(u64::MAX)].concat(),
        ),
        ("invalid-utf8", ld(F_STRING, &[0xFF, 0xFE, b'a'])),
        (
            "invalid-utf8-in-map-key",
            ld(M_STRING, &[ld(1, &[0xC0]), ld(2, b"v")].concat()),
        ),
        ("invalid-utf8-in-nested", ld(F_MESSAGE, &ld(1, &[0xFF]))),
        ("wire-type-mismatch-scalar", ld(F_INT32, b"abc")),
        ("wire-type-mismatch-message", vi(F_MESSAGE, 1)),
        ("wire-type-mismatch-repeated-string", vi(R_STRING, 1)),
        ("wire-type-mismatch-map", vi(M_STRING, 1)),
        (
            "wire-type-mismatch-map-key",
            ld(M_STRING, &[vi(1, 1), ld(2, b"v")].concat()),
        ),
        (
            "wire-type-mismatch-map-value",
            ld(M_STRING, &[ld(1, b"k"), vi(2, 1)].concat()),
        ),
        (
            "wire-type-mismatch-timestamp",
            ld(WKT_TIMESTAMP, &ld(1, b"x")),
        ),
        ("wire-type-mismatch-wrapper", ld(WRAP_FLOAT, &vi(1, 1))),
        ("wire-type-mismatch-value", ld(WKT_VALUE, &vi(2, 1))),
        ("wire-type-mismatch-any", ld(F_ANY, &vi(1, 1))),
        (
            "fixed32-truncated",
            [tag(F_FLOAT, WT_FIXED32), vec![1, 2]].concat(),
        ),
        (
            "fixed64-truncated",
            [tag(F_DOUBLE, WT_FIXED64), vec![1, 2, 3]].concat(),
        ),
        ("packed-float-odd-length", ld(R_FLOAT, &[1, 2, 3, 4, 5])),
        (
            "nested-truncated-inside",
            ld(F_MESSAGE, &[tag(1, WT_LEN), varint(50)].concat()),
        ),
        ("deep-nesting", deep),
        ("shallow-nesting", shallow),
    ];
    for (label, body) in cases {
        check_body(&plans, &desc, label, &body);
    }

    // Values the document never prints are still decoded by the production
    // path, so invalid UTF-8 in them must fail here too.
    let bad = b"\xff\xfe";
    let value_str = |s: &[u8]| ld(3, s);
    let struct_entry = |key: &[u8], value: &[u8]| ld(1, &[ld(1, key), ld(2, value)].concat());
    let superseded: Vec<(&str, Vec<u8>)> = vec![
        (
            "scalar-superseded",
            [ld(F_STRING, bad), ld(F_STRING, b"ok")].concat(),
        ),
        (
            "map-key-superseded",
            [
                ld(M_STRING, &[ld(1, bad), ld(2, b"v")].concat()),
                ld(M_STRING, &[ld(1, b"k"), ld(2, b"v")].concat()),
            ]
            .concat(),
        ),
        (
            "map-value-superseded",
            [
                ld(M_STRING, &[ld(1, b"k"), ld(2, bad)].concat()),
                ld(M_STRING, &[ld(1, b"k"), ld(2, b"v")].concat()),
            ]
            .concat(),
        ),
        (
            "map-entry-dup-value-superseded",
            ld(M_STRING, &[ld(1, b"k"), ld(2, bad), ld(2, b"v")].concat()),
        ),
        (
            "wrapper-superseded",
            ld(WRAP_STRING, &[ld(1, bad), ld(1, b"ok")].concat()),
        ),
        (
            "any-url-superseded",
            ld(
                F_ANY,
                &[
                    ld(1, bad),
                    ld(1, b"type.googleapis.com/differential.Nested"),
                ]
                .concat(),
            ),
        ),
        (
            "value-member-superseded",
            ld(WKT_VALUE, &[value_str(bad), f64f(2, 1.0)].concat()),
        ),
        (
            "struct-dup-key-superseded",
            ld(
                WKT_STRUCT,
                &[
                    struct_entry(b"k", &value_str(bad)),
                    struct_entry(b"k", &value_str(b"ok")),
                ]
                .concat(),
            ),
        ),
        (
            "oneof-member-superseded",
            [ld(ONE_STRING, bad), ld(ONE_STRING, b"ok")].concat(),
        ),
        (
            "oneof-other-member-superseded",
            [ld(ONE_STRING, bad), ld(ONE_MESSAGE, &ld(1, b"n"))].concat(),
        ),
        (
            "key-overridden-field",
            // Overridden by the colliding key variant, dropped under `_meta`.
            [ld(F_STRING, bad), ld(44, bad)].concat(),
        ),
        (
            "nested-superseded-by-merge",
            [ld(F_MESSAGE, &ld(1, bad)), ld(F_MESSAGE, &ld(1, b"ok"))].concat(),
        ),
    ];
    for (label, body) in superseded {
        assert!(
            oracle(&desc, &body, None, &meta).is_err(),
            "{label}: production must reject this"
        );
        check_body(&plans, &desc, label, &body);
    }
}

/// A singular message field that occurs more than once decodes as the
/// concatenation of its occurrences; inside a oneof, only the run after the
/// last other member counts. Map entries with the same key merge the same way.
#[test]
fn merged_occurrences_match_production() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let nested = |name: &str| corpus::nested(&pool, name, 0.5, vec![1.5]).encode_to_vec();
    let value_str = |s: &str| ld(3, s.as_bytes());
    let struct_entry =
        |key: &str, value: &[u8]| ld(1, &[ld(1, key.as_bytes()), ld(2, value)].concat());

    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "singular-message-twice",
            [
                ld(F_MESSAGE, &nested("a")),
                vi(F_INT32, 1),
                ld(F_MESSAGE, &ld(3, &2.5f64.to_le_bytes())),
            ]
            .concat(),
        ),
        (
            "singular-message-thrice-with-repeated-inside",
            [
                ld(F_MESSAGE, &ld(3, &1.0f64.to_le_bytes())),
                ld(
                    F_MESSAGE,
                    &[ld(1, b"x"), ld(3, &2.0f64.to_le_bytes())].concat(),
                ),
                ld(F_MESSAGE, &ld(1, b"y")),
            ]
            .concat(),
        ),
        (
            "oneof-message-twice",
            [ld(ONE_MESSAGE, &nested("a")), ld(ONE_MESSAGE, &nested("b"))].concat(),
        ),
        (
            "oneof-message-string-message",
            [
                ld(ONE_MESSAGE, &nested("a")),
                ld(ONE_STRING, b"s"),
                ld(ONE_MESSAGE, &nested("b")),
            ]
            .concat(),
        ),
        (
            "oneof-message-string-message-message",
            [
                ld(ONE_MESSAGE, &nested("a")),
                ld(ONE_STRING, b"s"),
                ld(ONE_MESSAGE, &ld(1, b"b")),
                ld(ONE_MESSAGE, &ld(2, &0.25f32.to_le_bytes())),
            ]
            .concat(),
        ),
        (
            "map-value-message-twice",
            ld(
                22,
                &[vi(1, 1), ld(2, &nested("a")), ld(2, &nested("b"))].concat(),
            ),
        ),
        (
            "map-dup-key-message-values",
            [
                ld(22, &[vi(1, 1), ld(2, &nested("a"))].concat()),
                ld(22, &[vi(1, 1), ld(2, &nested("b"))].concat()),
            ]
            .concat(),
        ),
        (
            "map-entry-key-twice",
            ld(M_STRING, &[ld(1, b"a"), ld(2, b"v"), ld(1, b"b")].concat()),
        ),
        (
            "value-struct-twice",
            ld(
                WKT_VALUE,
                &[
                    ld(5, &struct_entry("a", &value_str("1"))),
                    ld(5, &struct_entry("b", &value_str("2"))),
                ]
                .concat(),
            ),
        ),
        (
            "value-list-twice",
            ld(
                WKT_VALUE,
                &[
                    ld(6, &ld(1, &value_str("1"))),
                    ld(6, &ld(1, &value_str("2"))),
                ]
                .concat(),
            ),
        ),
        (
            "struct-twice",
            [
                ld(WKT_STRUCT, &struct_entry("a", &value_str("1"))),
                ld(WKT_STRUCT, &struct_entry("a", &value_str("2"))),
            ]
            .concat(),
        ),
        (
            "list-twice",
            [
                ld(WKT_LIST, &ld(1, &value_str("1"))),
                ld(WKT_LIST, &ld(1, &value_str("2"))),
            ]
            .concat(),
        ),
        (
            "timestamp-split",
            [ld(WKT_TIMESTAMP, &vi(1, 5)), ld(WKT_TIMESTAMP, &vi(2, 7))].concat(),
        ),
        (
            "any-split",
            [
                ld(F_ANY, &ld(1, b"type.googleapis.com/differential.Nested")),
                ld(F_ANY, &ld(2, &nested("a"))),
            ]
            .concat(),
        ),
        (
            "any-value-twice-last-wins",
            ld(
                F_ANY,
                &[
                    ld(1, b"type.googleapis.com/differential.Nested"),
                    ld(2, &nested("a")),
                    ld(2, &nested("b")),
                ]
                .concat(),
            ),
        ),
    ];
    for (label, body) in cases {
        assert!(
            !check_body(&plans, &desc, label, &body),
            "{label}: must not fall back"
        );
    }
}

/// Well-known types as the whole payload are left to the production path,
/// which emits objects for some and errors for others.
#[test]
fn well_known_type_payloads_fall_back() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let meta = corpus::meta();
    for name in [
        "google.protobuf.Struct",
        "google.protobuf.Any",
        "google.protobuf.Timestamp",
    ] {
        let top = pool.get_message_by_name(name).unwrap();
        let plan = plans.plan_for(&top).unwrap();
        let mut out = Vec::new();
        let err = Transcoder::new()
            .transcode(&plans, plan, &[], None, &meta, &mut out)
            .unwrap_err();
        assert!(
            matches!(err, TranscodeError::Unsupported(_)),
            "{name}: {err}"
        );
        assert!(out.is_empty(), "{name}: output must be restored on error");
        check_body(&plans, &top, name, &[]);
    }
}

/// `levels` nested `Value { list_value }` pairs under `wkt_value`, with `tail`
/// placed inside the deepest ListValue. Each level is two messages deep.
fn value_chain(levels: usize, tail: &[u8]) -> Vec<u8> {
    let mut value = ld(6, tail);
    for _ in 1..levels {
        value = ld(6, &ld(1, &value));
    }
    ld(WKT_VALUE, &value)
}

/// `levels` nested `Value { struct_value { fields["k"] } }` under `wkt_value`,
/// with `tail` placed inside the deepest map entry. Each level is two
/// messages deep for the dynamic decode and three for prost, which counts
/// the map entry.
fn struct_chain(levels: usize, tail: &[u8]) -> Vec<u8> {
    let mut value = ld(5, &ld(1, &[ld(1, b"k"), tail.to_vec()].concat()));
    for _ in 1..levels {
        value = ld(5, &ld(1, &[ld(1, b"k"), ld(2, &value)].concat()));
    }
    ld(WKT_VALUE, &value)
}

/// prost's recursion limit is 100 message levels. Rather than pin the exact
/// boundary, sweep across it and require both paths to agree at every depth,
/// for plain nesting, for an unknown field and an unknown group at the deepest
/// level, for the same chain inside an `Any`, whose payload gets a fresh
/// budget, and for a chain through `Struct` map entries, which only prost
/// counts.
#[test]
fn recursion_limit_boundary_matches_production() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let meta = corpus::meta();
    let any = |payload: &[u8]| {
        ld(
            F_ANY,
            &[
                ld(1, b"type.googleapis.com/differential.Everything"),
                ld(2, payload),
            ]
            .concat(),
        )
    };
    let group = [tag(99, WT_START_GROUP), vi(1, 1), tag(99, WT_END_GROUP)].concat();
    type Shape = Box<dyn Fn(usize) -> Vec<u8>>;
    let shapes: Vec<(&str, Shape)> = vec![
        ("plain", Box::new(|n| value_chain(n, &[]))),
        ("unknown-field", Box::new(|n| value_chain(n, &vi(99, 1)))),
        ("unknown-group", Box::new(move |n| value_chain(n, &group))),
        ("in-any", Box::new(move |n| any(&value_chain(n, &[])))),
        ("struct-chain", Box::new(|n| struct_chain(n, &[]))),
        (
            "struct-chain-unknown-in-entry",
            Box::new(|n| struct_chain(n, &vi(99, 1))),
        ),
        (
            "struct-chain-value-in-entry",
            Box::new(|n| struct_chain(n, &ld(2, &ld(3, b"v")))),
        ),
    ];
    for (label, shape) in shapes {
        let mut saw_ok = false;
        let mut saw_err = false;
        let sweep = if label.starts_with("struct") {
            30..=36
        } else {
            45..=55
        };
        for levels in sweep {
            let body = shape(levels);
            match oracle(&desc, &body, None, &meta) {
                Ok(_) => saw_ok = true,
                Err(_) => saw_err = true,
            }
            check_body(&plans, &desc, &format!("{label}@{levels}"), &body);
        }
        assert!(saw_ok && saw_err, "{label}: the sweep must cross the limit");
    }
}

/// `levels` nested `Tree { children }` under `Kinds.tree`, with `tail` inside
/// the deepest Tree. One message level each.
fn children_chain(levels: usize, tail: &[u8]) -> Vec<u8> {
    let mut tree = tail.to_vec();
    for _ in 0..levels {
        tree = ld(2, &tree);
    }
    ld(K_TREE, &tree)
}

/// `levels` nested `Tree { named["k"] }` under `Kinds.tree`, with `tail`
/// inside the deepest map entry beside its key. The map entry is where the
/// dynamic decoder applies `skip_field`'s check without counting a level.
fn named_chain(levels: usize, tail: &[u8]) -> Vec<u8> {
    let mut entry = [ld(1, b"k"), tail.to_vec()].concat();
    for _ in 1..levels {
        entry = [ld(1, b"k"), ld(2, &ld(3, &entry))].concat();
    }
    ld(K_TREE, &ld(3, &entry))
}

/// The same sweep through a user-defined recursive message, where only the
/// dynamic decoder's count applies and map entries do not count.
#[test]
fn recursion_limit_through_user_messages_matches_production() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let kinds = pool.get_message_by_name("differential.Kinds").unwrap();
    let meta = corpus::meta();
    let group = [tag(99, WT_START_GROUP), vi(1, 1), tag(99, WT_END_GROUP)].concat();
    type Shape = Box<dyn Fn(usize) -> Vec<u8>>;
    let shapes: Vec<(&str, Shape)> = vec![
        ("children", Box::new(|n| children_chain(n, &[]))),
        (
            "children-unknown-field",
            Box::new(|n| children_chain(n, &vi(99, 1))),
        ),
        ("children-unknown-group", {
            let group = group.clone();
            Box::new(move |n| children_chain(n, &group))
        }),
        ("named", Box::new(|n| named_chain(n, &[]))),
        (
            "named-unknown-in-entry",
            Box::new(|n| named_chain(n, &vi(99, 1))),
        ),
        ("named-group-in-entry", {
            let group = group.clone();
            Box::new(move |n| named_chain(n, &group))
        }),
        (
            "named-value-in-entry",
            Box::new(|n| named_chain(n, &ld(2, &ld(1, b"v")))),
        ),
    ];
    for (label, shape) in shapes {
        let mut saw_ok = false;
        let mut saw_err = false;
        for levels in 94..=104 {
            let body = shape(levels);
            match oracle(&kinds, &body, None, &meta) {
                Ok(_) => saw_ok = true,
                Err(_) => saw_err = true,
            }
            check_body(&plans, &kinds, &format!("tree-{label}@{levels}"), &body);
        }
        assert!(
            saw_ok && saw_err,
            "tree-{label}: the sweep must cross the limit"
        );
    }
}

/// Production reuses one `Transcoder` for the life of the capture. Its scratch
/// space must not leak between messages, success or failure, and a failure in
/// either pass must leave the output buffer as it was.
#[test]
fn one_transcoder_serves_a_sequence_of_successes_and_failures() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let plan = plans.plan_for(&desc).unwrap();
    let meta = corpus::meta();
    let any =
        |url: &str, payload: &[u8]| ld(F_ANY, &[ld(1, url.as_bytes()), ld(2, payload)].concat());
    let (_, everything) = corpus::corpus(&pool).remove(1);
    let sequence: Vec<(&str, Vec<u8>)> = vec![
        ("ok-everything", everything.encode_to_vec()),
        ("index-error-wire-type", vi(F_MESSAGE, 1)),
        (
            "ok-small",
            [
                ld(F_STRING, b"s"),
                ld(M_STRING, &[ld(1, b"k"), ld(2, b"v")].concat()),
            ]
            .concat(),
        ),
        (
            "format-error-any-payload",
            any("type.googleapis.com/differential.Nested", &[0xFF]),
        ),
        (
            "ok-map-and-nested",
            [
                ld(F_MESSAGE, &ld(1, b"n")),
                ld(M_STRING, &[ld(1, b"b"), ld(2, b"2")].concat()),
                ld(M_STRING, &[ld(1, b"a"), ld(2, b"1")].concat()),
            ]
            .concat(),
        ),
        (
            "format-error-timestamp-range",
            ld(WKT_TIMESTAMP, &vi(1, (MAX_TIMESTAMP_SECONDS + 1) as u64)),
        ),
        ("ok-everything-again", everything.encode_to_vec()),
    ];
    let mut shared = Transcoder::new();
    for (label, body) in &sequence {
        let mut from_shared = b"prefix".to_vec();
        let mut from_fresh = b"prefix".to_vec();
        let shared_result = shared.transcode(&plans, plan, body, None, &meta, &mut from_shared);
        let fresh_result =
            Transcoder::new().transcode(&plans, plan, body, None, &meta, &mut from_fresh);
        assert_eq!(shared_result.is_ok(), fresh_result.is_ok(), "{label}");
        assert_eq!(from_shared, from_fresh, "{label}: reuse changed the output");
        if shared_result.is_err() {
            assert_eq!(from_shared, b"prefix", "{label}: output not restored");
        } else {
            assert!(from_shared.starts_with(b"prefix{"), "{label}");
        }
        check_body(&plans, &desc, label, body);
    }
}
