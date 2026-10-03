//! Hand-built wire bytes for every well-known type and the awkward scalar, map,
//! repeated, and oneof encodings.

use super::*;

#[test]
fn hand_built_wkt_edge_cases_match() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);

    let ts = |seconds: i64, nanos: i64| {
        let mut m = Vec::new();
        if seconds != 0 {
            m.extend(vi(1, seconds as u64));
        }
        if nanos != 0 {
            m.extend(vi(2, nanos as u64));
        }
        m
    };
    let struct_entry =
        |key: &str, value: &[u8]| ld(1, &[ld(1, key.as_bytes()), ld(2, value)].concat());
    let value_str = |s: &str| ld(3, s.as_bytes());
    let value_num = |f: f64| f64f(2, f);

    let any = |url: &str, payload: &[u8]| [ld(1, url.as_bytes()), ld(2, payload)].concat();
    let nested_bytes = corpus::nested(&pool, "in-any", 0.5, vec![1.5]).encode_to_vec();

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("ts-epoch", ld(WKT_TIMESTAMP, &ts(0, 0))),
        (
            "ts-millis",
            ld(WKT_TIMESTAMP, &ts(1_730_233_606, 123_000_000)),
        ),
        (
            "ts-micros",
            ld(WKT_TIMESTAMP, &ts(1_730_233_606, 123_456_000)),
        ),
        (
            "ts-nanos",
            ld(WKT_TIMESTAMP, &ts(1_730_233_606, 123_456_789)),
        ),
        ("ts-negative", ld(WKT_TIMESTAMP, &ts(-1, 0))),
        ("ts-min", ld(WKT_TIMESTAMP, &ts(MIN_TIMESTAMP_SECONDS, 0))),
        (
            "ts-max",
            ld(WKT_TIMESTAMP, &ts(MAX_TIMESTAMP_SECONDS, 999_999_999)),
        ),
        (
            "ts-out-of-range",
            ld(WKT_TIMESTAMP, &ts(MAX_TIMESTAMP_SECONDS + 1, 0)),
        ),
        ("ts-negative-nanos", ld(WKT_TIMESTAMP, &ts(10, -1))),
        (
            "ts-dup-fields",
            ld(WKT_TIMESTAMP, &[ts(5, 0), ts(9, 7)].concat()),
        ),
        (
            "ts-unknown-field",
            ld(WKT_TIMESTAMP, &[ts(5, 0), vi(9, 1)].concat()),
        ),
        ("dur-zero", ld(WKT_DURATION, &ts(0, 0))),
        ("dur-frac", ld(WKT_DURATION, &ts(1, 500_000_000))),
        ("dur-negative", ld(WKT_DURATION, &ts(-1, -500_000_000))),
        (
            "dur-negative-frac-only",
            ld(WKT_DURATION, &ts(0, -500_000_000)),
        ),
        (
            "dur-out-of-range",
            ld(WKT_DURATION, &ts(315_576_000_001, 0)),
        ),
        (
            "dur-nanos-out-of-range",
            ld(WKT_DURATION, &ts(0, 1_000_000_000)),
        ),
        ("wrap-float-nan", ld(WRAP_FLOAT, &f32f(1, f32::NAN))),
        ("wrap-float-inf", ld(WRAP_FLOAT, &f32f(1, f32::INFINITY))),
        (
            "wrap-double-neg-inf",
            ld(WRAP_DOUBLE, &f64f(1, f64::NEG_INFINITY)),
        ),
        ("wrap-float-explicit-zero", ld(WRAP_FLOAT, &f32f(1, 0.0))),
        ("wrap-float-empty", ld(WRAP_FLOAT, &[])),
        ("wrap-int64-empty", ld(WRAP_INT64, &[])),
        ("wrap-string-empty", ld(WRAP_STRING, &[])),
        (
            "wrap-string-dup",
            ld(WRAP_STRING, &[ld(1, b"first"), ld(1, b"second")].concat()),
        ),
        ("value-empty", ld(WKT_VALUE, &[])),
        ("value-null", ld(WKT_VALUE, &vi(1, 0))),
        ("value-nan", ld(WKT_VALUE, &value_num(f64::NAN))),
        ("value-number", ld(WKT_VALUE, &value_num(2.5))),
        (
            "value-last-wins",
            ld(
                WKT_VALUE,
                &[value_str("a"), value_num(1.0), value_str("b")].concat(),
            ),
        ),
        (
            "value-struct-then-string",
            ld(
                WKT_VALUE,
                &[ld(5, &struct_entry("k", &value_str("v"))), value_str("s")].concat(),
            ),
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
            "struct-dup-keys",
            ld(
                WKT_STRUCT,
                &[
                    struct_entry("k", &value_str("first")),
                    struct_entry("z", &value_num(1.0)),
                    struct_entry("k", &value_str("last")),
                ]
                .concat(),
            ),
        ),
        (
            "struct-entry-no-value",
            ld(WKT_STRUCT, &ld(1, &ld(1, b"k"))),
        ),
        (
            "struct-entry-no-key",
            ld(WKT_STRUCT, &ld(1, &ld(2, &value_str("v")))),
        ),
        (
            "struct-nested-meta",
            ld(WKT_STRUCT, &struct_entry("_meta", &value_str("kept"))),
        ),
        ("list-empty", ld(WKT_LIST, &[])),
        (
            "list-values",
            ld(
                WKT_LIST,
                &[ld(1, &value_num(1.0)), ld(1, &[]), ld(1, &value_str("x"))].concat(),
            ),
        ),
        (
            "mask-camel",
            ld(
                WKT_MASK,
                &[ld(1, b"a.b_c"), ld(1, b"snake_case_name")].concat(),
            ),
        ),
        ("mask-bad-upper", ld(WKT_MASK, &ld(1, b"fooBar"))),
        ("mask-bad-double-underscore", ld(WKT_MASK, &ld(1, b"a__b"))),
        ("mask-empty", ld(WKT_MASK, &[])),
        (
            "any-nested",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/differential.Nested",
                    &corpus::nested(&pool, "in-any", 0.5, vec![1.5]).encode_to_vec(),
                ),
            ),
        ),
        (
            "any-wkt",
            ld(
                F_ANY,
                &any("type.googleapis.com/google.protobuf.Timestamp", &ts(5, 0)),
            ),
        ),
        (
            "any-empty-wkt",
            ld(
                F_ANY,
                &any("type.googleapis.com/google.protobuf.Empty", &[]),
            ),
        ),
        (
            "any-unknown-type",
            ld(F_ANY, &any("type.googleapis.com/no.Such", &[])),
        ),
        ("any-no-slash", ld(F_ANY, &any("nope", &[]))),
        ("any-empty", ld(F_ANY, &[])),
        (
            "any-of-any",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/google.protobuf.Any",
                    &any(
                        "type.googleapis.com/differential.Nested",
                        &corpus::nested(&pool, "deep", 0.5, vec![]).encode_to_vec(),
                    ),
                ),
            ),
        ),
        (
            "any-everything-with-meta",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/differential.Everything",
                    &[ld(44, b"inner-meta"), vi(F_INT32, 3)].concat(),
                ),
            ),
        ),
        ("enum-unknown", vi(F_ENUM, 42)),
        ("enum-negative", vi(F_ENUM, (-1i64) as u64)),
        ("int32-ten-byte-negative", vi(F_INT32, (-5i64) as u64)),
        ("int64-max", vi(F_INT64, i64::MAX as u64)),
        ("int64-min", vi(F_INT64, i64::MIN as u64)),
        ("float-negative-zero", f32f(F_FLOAT, -0.0)),
        ("float-nan", f32f(F_FLOAT, f32::NAN)),
        ("double-inf", f64f(F_DOUBLE, f64::INFINITY)),
        (
            "string-escapes",
            ld(
                F_STRING,
                "\u{0}\u{8}\t\n\u{c}\r\"\\/\u{7f}\u{80}".as_bytes(),
            ),
        ),
        (
            "map-dup-keys",
            [
                ld(M_STRING, &[ld(1, b"k"), ld(2, b"first")].concat()),
                ld(M_STRING, &[ld(1, b"j"), ld(2, b"j")].concat()),
                ld(M_STRING, &[ld(1, b"k"), ld(2, b"last")].concat()),
            ]
            .concat(),
        ),
        ("map-entry-missing-key", ld(M_STRING, &ld(2, b"v"))),
        ("map-entry-missing-value", ld(M_STRING, &ld(1, b"k"))),
        ("map-entry-empty", ld(M_STRING, &[])),
        (
            "map-entry-unknown-field",
            ld(M_STRING, &[ld(1, b"k"), vi(7, 1), ld(2, b"v")].concat()),
        ),
        // prost-reflect reads a map entry's length whatever the wire type says.
        (
            "map-varint-wire-type-empty",
            [tag(M_STRING, WT_VARINT), vec![0x00]].concat(),
        ),
        (
            "map-varint-wire-type-entry",
            [
                tag(M_STRING, WT_VARINT),
                vec![0x06],
                ld(1, b"k"),
                ld(2, b"v"),
            ]
            .concat(),
        ),
        (
            "map-fixed32-wire-type-entry",
            [
                tag(M_STRING, WT_FIXED32),
                vec![0x06],
                ld(1, b"k"),
                ld(2, b"v"),
            ]
            .concat(),
        ),
        (
            "struct-varint-wire-type",
            ld(WKT_STRUCT, &[tag(1, WT_VARINT), vec![0x00]].concat()),
        ),
        // sint32 narrows to 32 bits before zigzag decoding.
        ("sint32-overlong-negative-one", vi(F_SINT32, 0x1_0000_0001)),
        ("sint32-overlong-zero", vi(F_SINT32, 0x1_0000_0000)),
        (
            "sint32-map-key-overlong",
            ld(M_SINT32, &[vi(1, 0x1_0000_0001), ld(2, b"v")].concat()),
        ),
        (
            "sint32-packed-overlong",
            ld(R_SINT32, &[varint(0x1_0000_0001), varint(3)].concat()),
        ),
        // Empty FieldMask paths add no separator.
        (
            "mask-empty-then-path",
            ld(WKT_MASK, &[ld(1, b""), ld(1, b"a")].concat()),
        ),
        (
            "mask-two-empty",
            ld(WKT_MASK, &[ld(1, b""), ld(1, b"")].concat()),
        ),
        (
            "map-int32-keys-order",
            [
                ld(22, &[vi(1, (-2i64) as u64), ld(2, &[])].concat()),
                ld(
                    22,
                    &[
                        vi(1, 1),
                        ld(
                            2,
                            &corpus::nested(&pool, "one", 0.5, vec![]).encode_to_vec(),
                        ),
                    ]
                    .concat(),
                ),
                ld(22, &[vi(1, 0), ld(2, &[])].concat()),
            ]
            .concat(),
        ),
        (
            "map-bool-keys",
            [
                ld(25, &[vi(1, 1), ld(2, b"t")].concat()),
                ld(25, &[vi(1, 0), ld(2, b"f")].concat()),
            ]
            .concat(),
        ),
        (
            "repeated-string-empty-elems",
            [ld(R_STRING, b""), ld(R_STRING, b"x"), ld(R_STRING, b"")].concat(),
        ),
        ("repeated-float-packed-empty", ld(R_FLOAT, &[])),
        (
            "repeated-float-packed-then-unpacked",
            [
                ld(
                    R_FLOAT,
                    &[0.5f32.to_le_bytes(), 1.5f32.to_le_bytes()].concat(),
                ),
                f32f(R_FLOAT, 2.5),
            ]
            .concat(),
        ),
        ("nested-empty", ld(F_MESSAGE, &[])),
        ("nested-unknown-only", ld(F_MESSAGE, &vi(99, 1))),
        (
            "oneof-string-message-string",
            [
                ld(ONE_STRING, b"a"),
                ld(ONE_MESSAGE, &ld(1, b"n")),
                ld(ONE_STRING, b"b"),
            ]
            .concat(),
        ),
        (
            "oneof-message-string",
            [ld(ONE_MESSAGE, &ld(1, b"n")), ld(ONE_STRING, b"b")].concat(),
        ),
        (
            "oneof-string-message",
            [ld(ONE_STRING, b"b"), ld(ONE_MESSAGE, &ld(1, b"n"))].concat(),
        ),
        // Any: every well-known-type target form, URL variants, and payloads
        // that fail inside the target.
        (
            "any-int32-wrapper",
            ld(
                F_ANY,
                &any("type.googleapis.com/google.protobuf.Int32Value", &vi(1, 5)),
            ),
        ),
        (
            "any-float-wrapper",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/google.protobuf.FloatValue",
                    &f32f(1, 1.5),
                ),
            ),
        ),
        (
            "any-bytes-wrapper",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/google.protobuf.BytesValue",
                    &ld(1, b"hi"),
                ),
            ),
        ),
        (
            "any-value-string",
            ld(
                F_ANY,
                &any("type.googleapis.com/google.protobuf.Value", &ld(3, b"v")),
            ),
        ),
        (
            "any-value-empty",
            ld(
                F_ANY,
                &any("type.googleapis.com/google.protobuf.Value", &[]),
            ),
        ),
        (
            "any-list",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/google.protobuf.ListValue",
                    &ld(1, &ld(3, b"x")),
                ),
            ),
        ),
        (
            "any-duration",
            ld(
                F_ANY,
                &any("type.googleapis.com/google.protobuf.Duration", &vi(1, 5)),
            ),
        ),
        (
            "any-mask",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/google.protobuf.FieldMask",
                    &ld(1, b"a_b"),
                ),
            ),
        ),
        (
            "any-kinds-maps",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/differential.Kinds",
                    &[
                        ld(15, &[vi(1, 9), ld(2, b"nine")].concat()),
                        ld(15, &[vi(1, 1), ld(2, b"one")].concat()),
                    ]
                    .concat(),
                ),
            ),
        ),
        (
            "any-map-entry-type",
            ld(
                F_ANY,
                &any(
                    "type.googleapis.com/differential.Everything.MStringEntry",
                    &[ld(1, b"k"), ld(2, b"v")].concat(),
                ),
            ),
        ),
        (
            "any-url-trailing-slash",
            ld(F_ANY, &any("type.googleapis.com/differential.Nested/", &[])),
        ),
        (
            "any-url-two-slashes",
            ld(F_ANY, &any("a/b/differential.Nested", &nested_bytes)),
        ),
        (
            "any-url-leading-slash",
            ld(F_ANY, &any("/differential.Nested", &nested_bytes)),
        ),
        (
            "any-unknown-field-beside",
            ld(
                F_ANY,
                &[
                    any("type.googleapis.com/differential.Nested", &nested_bytes),
                    vi(9, 1),
                ]
                .concat(),
            ),
        ),
        (
            "any-bad-payload",
            ld(
                F_ANY,
                &any("type.googleapis.com/differential.Nested", &[0xFF]),
            ),
        ),
        (
            "any-payload-wire-type-mismatch",
            ld(
                F_ANY,
                &any("type.googleapis.com/differential.Nested", &vi(1, 1)),
            ),
        ),
        // Time ranges at their edges.
        (
            "ts-nanos-overflow",
            ld(WKT_TIMESTAMP, &ts(5, 1_000_000_000)),
        ),
        (
            "ts-max-nanos-overflow",
            ld(WKT_TIMESTAMP, &ts(MAX_TIMESTAMP_SECONDS, 1_000_000_000)),
        ),
        (
            "ts-min-negative-nanos",
            ld(WKT_TIMESTAMP, &ts(MIN_TIMESTAMP_SECONDS, -1)),
        ),
        (
            "ts-below-min",
            ld(WKT_TIMESTAMP, &ts(MIN_TIMESTAMP_SECONDS - 1, 0)),
        ),
        ("dur-mixed-signs", ld(WKT_DURATION, &ts(1, -1))),
        ("dur-i64-min", ld(WKT_DURATION, &ts(i64::MIN, 0))),
        (
            "dur-nanos-i32-min",
            ld(WKT_DURATION, &ts(0, i32::MIN as i64)),
        ),
        (
            "dur-nanos-min-valid",
            ld(WKT_DURATION, &ts(0, -999_999_999)),
        ),
        // Value kinds at their edges.
        ("value-negative-zero", ld(WKT_VALUE, &value_num(-0.0))),
        ("value-1e300", ld(WKT_VALUE, &value_num(1e300))),
        (
            "value-neg-inf",
            ld(WKT_VALUE, &value_num(f64::NEG_INFINITY)),
        ),
        ("value-bool-2", ld(WKT_VALUE, &vi(4, 2))),
        ("value-null-7", ld(WKT_VALUE, &vi(1, 7))),
        ("value-unknown-only", ld(WKT_VALUE, &vi(9, 1))),
        // Varints wider than their field.
        ("bool-2", vi(F_BOOL, 2)),
        ("bool-overlong", vi(F_BOOL, 1 << 32)),
        ("uint32-overlong-zero", vi(F_UINT32, 1 << 32)),
        ("int32-overlong-zero", vi(F_INT32, 1 << 32)),
        ("enum-overlong-zero", vi(F_ENUM, 1 << 32)),
        (
            "map-bool-key-2-then-1",
            [
                ld(M_BOOL, &[vi(1, 2), ld(2, b"a")].concat()),
                ld(M_BOOL, &[vi(1, 1), ld(2, b"b")].concat()),
            ]
            .concat(),
        ),
        // FieldMask paths that do or do not round-trip.
        ("mask-trailing-underscore", ld(WKT_MASK, &ld(1, b"a_"))),
        ("mask-leading-underscore", ld(WKT_MASK, &ld(1, b"_a"))),
        ("mask-digit-after-underscore", ld(WKT_MASK, &ld(1, b"a_1"))),
        (
            "mask-non-ascii-after-underscore",
            ld(WKT_MASK, &ld(1, "a_\u{e9}".as_bytes())),
        ),
        ("mask-dot-only", ld(WKT_MASK, &ld(1, b"."))),
        ("mask-trailing-dot", ld(WKT_MASK, &ld(1, b"a."))),
        // Wrappers with more than their value.
        (
            "wrap-int64-unknown-field",
            ld(WRAP_INT64, &[vi(1, 5), vi(9, 1)].concat()),
        ),
        ("wrap-string-invalid-utf8", ld(WRAP_STRING, &ld(1, b"\xff"))),
        // Nested explicit defaults and noise.
        (
            "nested-explicit-defaults",
            ld(F_MESSAGE, &[ld(1, b""), f32f(2, -0.0), ld(3, &[])].concat()),
        ),
        (
            "nested-unknown-group-padded-tag",
            ld(
                F_MESSAGE,
                &[
                    tag(99, WT_START_GROUP),
                    vi(1, 1),
                    tag(99, WT_END_GROUP),
                    padded_varint((1 << 3) | WT_LEN as u64, 2),
                    varint(1),
                    b"x".to_vec(),
                ]
                .concat(),
            ),
        ),
    ];

    for (label, body) in cases {
        check_body(&plans, &desc, label, &body);
    }
}

/// The one value-level difference kept on purpose. The DynamicMessage path
/// serializes a wrapper by re-encoding it, and `-0.0` is dropped there as a
/// default, so the sign is lost; the transcoder keeps it, as the reference
/// JSON printer does. Both parse to the same f64.
#[test]
fn wrapper_negative_zero_keeps_its_sign() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let meta = corpus::meta();
    for (body, ours, production) in [
        (
            ld(WRAP_FLOAT, &f32f(1, -0.0)),
            r#""wrap_float":-0.0"#,
            r#""wrap_float":0.0"#,
        ),
        (
            ld(WRAP_DOUBLE, &f64f(1, -0.0)),
            r#""wrap_double":-0.0"#,
            r#""wrap_double":0.0"#,
        ),
    ] {
        let mut out = Vec::new();
        Transcoder::new()
            .transcode(&plans, plans.root(), &body, None, &meta, &mut out)
            .unwrap();
        let out = String::from_utf8(out).unwrap();
        let expected = String::from_utf8(oracle(&desc, &body, None, &meta).unwrap()).unwrap();
        assert!(out.contains(ours), "{out}");
        assert!(expected.contains(production), "{expected}");
        let a: Value = serde_json::from_str(&out).unwrap();
        let b: Value = serde_json::from_str(&expected).unwrap();
        assert_eq!(a, b);
    }
}

/// The `Kinds` root: repeated, map, oneof, and enum shapes `Everything`
/// does not declare, at their awkward encodings.
#[test]
fn kinds_edge_cases_match() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let kinds = pool.get_message_by_name("differential.Kinds").unwrap();
    let tree = |label: &str| ld(1, label.as_bytes());
    let entry = |key: &str, value: &[u8]| ld(3, &[ld(1, key.as_bytes()), ld(2, value)].concat());

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("packed-bool-2", ld(K_R_BOOL, &[2])),
        ("packed-bool-overlong", ld(K_R_BOOL, &varint(1 << 32))),
        (
            "packed-enum-negative",
            ld(K_R_ENUM, &varint((-1i64) as u64)),
        ),
        ("packed-enum-overlong", ld(K_R_ENUM, &varint(1 << 32))),
        (
            "packed-enum-unknown",
            ld(K_R_ENUM, &[varint(1), varint(42)].concat()),
        ),
        (
            "oneof-timestamp-three-pieces",
            [
                ld(K_ONE_TIMESTAMP, &vi(1, 5)),
                ld(K_ONE_TIMESTAMP, &vi(2, 7)),
                ld(K_ONE_TIMESTAMP, &vi(1, 9)),
            ]
            .concat(),
        ),
        (
            "oneof-timestamp-interrupted",
            [
                ld(K_ONE_TIMESTAMP, &vi(1, 5)),
                vi(K_ONE_INT32, 1),
                ld(K_ONE_TIMESTAMP, &vi(2, 7)),
                ld(K_ONE_TIMESTAMP, &vi(1, 9)),
            ]
            .concat(),
        ),
        (
            "tree-named-dup-key-merge",
            ld(
                K_TREE,
                &[entry("k", &tree("a")), entry("k", &ld(2, &tree("c")))].concat(),
            ),
        ),
        (
            "tree-children-split",
            [
                ld(K_TREE, &ld(2, &tree("a"))),
                ld(K_TREE, &ld(2, &tree("b"))),
            ]
            .concat(),
        ),
        (
            "map-duration-mixed-signs",
            ld(
                K_MV_DURATION,
                &[
                    ld(1, b"a"),
                    ld(2, &[vi(1, 1), vi(2, (-1i64) as u64)].concat()),
                ]
                .concat(),
            ),
        ),
        (
            "map-duration-out-of-range",
            ld(
                K_MV_DURATION,
                &[ld(1, b"a"), ld(2, &vi(1, 315_576_000_001))].concat(),
            ),
        ),
        ("empty-with-unknown-field", ld(K_WKT_EMPTY, &vi(9, 1))),
        (
            "repeated-timestamp-one-out-of-range",
            [
                ld(K_R_TIMESTAMP, &vi(1, 5)),
                ld(K_R_TIMESTAMP, &vi(1, (MAX_TIMESTAMP_SECONDS + 1) as u64)),
            ]
            .concat(),
        ),
        ("opt-message-empty", ld(K_OPT_MESSAGE, &[])),
        (
            "opt-message-twice",
            [
                ld(K_OPT_MESSAGE, &ld(1, b"a")),
                ld(K_OPT_MESSAGE, &ld(2, &0.5f32.to_le_bytes())),
            ]
            .concat(),
        ),
        ("null-value-plain-42", vi(K_F_NULL_PLAIN, 42)),
        ("signed-enum-negative", vi(K_F_SIGNED, (-1i64) as u64)),
        (
            "signed-enum-packed-negative",
            ld(K_R_SIGNED, &[varint((-1i64) as u64), varint(7)].concat()),
        ),
        ("aliased-enum", vi(K_F_ALIASED, 1)),
        ("aliased-enum-unknown", vi(K_F_ALIASED, 2)),
    ];
    for (label, body) in cases {
        check_body(&plans, &kinds, label, &body);
    }
}
