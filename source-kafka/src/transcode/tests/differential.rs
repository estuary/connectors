//! Equivalence with the production path over the corpus, random messages, mutated
//! wire encodings, and the key/`_meta` merge.

use super::*;

#[test]
fn corpus_matches_production_path() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let mut messages = corpus::corpus(&pool);
    messages.push(("kinds", kinds_fixture(&pool)));
    for (label, message) in messages {
        let desc = message.descriptor();
        check_body(&plans, &desc, label, &message.encode_to_vec());
    }
}

#[test]
fn random_messages_match_production_path() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let meta = corpus::meta();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for root in ["differential.Everything", "differential.Kinds"] {
        let desc = pool.get_message_by_name(root).unwrap();
        let mut succeeded = 0;
        for i in 0..500 {
            let body = random_message(&pool, &desc, &mut rng, 0).encode_to_vec();
            if oracle(&desc, &body, None, &meta).is_ok() {
                succeeded += 1;
            }
            check_body(&plans, &desc, &format!("{root}/random#{i}"), &body);
        }
        // Agreeing on errors is cheap; most random messages must print.
        assert!(
            succeeded >= 400,
            "{root}: only {succeeded} of 500 serialized"
        );
    }
}

/// Mutates serializer output into the legal-but-unusual encodings a
/// serializer never produces: reordered fields, fields split and repeated
/// across the message, unpacked repeated scalars, unknown fields, padded
/// varints, and explicitly encoded defaults.
#[test]
fn wire_mutations_match_production_path() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);

    for root in ["differential.Everything", "differential.Kinds"] {
        let desc = pool.get_message_by_name(root).unwrap();
        let plan = plans.plan_for(&desc).unwrap();
        for i in 0..300 {
            let a = random_message(&pool, &desc, &mut rng, 0).encode_to_vec();
            let b = random_message(&pool, &desc, &mut rng, 0).encode_to_vec();
            let mut parts = chunks(&a);

            // Interleaving a second message's fields makes scalars repeat
            // with different values, splits repeated fields, and makes
            // singular message fields and map entries merge.
            if rng.chance(50) {
                parts.extend(chunks(&b));
            }
            rng.shuffle(&mut parts);

            // Packed repeated fields as unpacked elements.
            if rng.chance(50) {
                parts = parts
                    .iter()
                    .flat_map(|part| unpacked(&plans, plan, part))
                    .collect();
            }

            // Unknown fields of every wire type, including a group.
            if rng.chance(50) {
                let group = [
                    tag(250, WT_START_GROUP),
                    vi(1, 5),
                    ld(2, b"grp"),
                    tag(250, WT_END_GROUP),
                ]
                .concat();
                for unknown in [
                    vi(200, 77),
                    f32f(201, 1.5),
                    f64f(202, 2.5),
                    ld(203, b"zzz"),
                    group,
                ] {
                    let at = rng.below(parts.len() + 1);
                    parts.insert(at, unknown);
                }
            }

            // Padded varints in tags and varint values.
            if rng.chance(50) {
                for part in parts.iter_mut() {
                    let (number, wt, after) = read_tag(part, 0).unwrap();
                    let mut rebuilt =
                        padded_varint(((number as u64) << 3) | wt as u64, 1 + rng.below(3));
                    if wt == WT_VARINT {
                        let (v, _) = read_varint(part, after).unwrap();
                        rebuilt.extend(padded_varint(v, 1 + rng.below(2)));
                    } else {
                        rebuilt.extend_from_slice(&part[after..]);
                    }
                    *part = rebuilt;
                }
            }

            // Explicit defaults: omitted for implicit presence, kept for
            // explicit presence.
            if rng.chance(50) {
                let defaults = match root {
                    "differential.Everything" => vec![
                        vi(F_INT32, 0),
                        ld(F_STRING, b""),
                        vi(F_BOOL, 0),
                        f64f(F_DOUBLE, 0.0),
                        f32f(F_FLOAT, -0.0),
                        vi(F_ENUM, 0),
                        ld(F_BYTES, b""),
                        vi(OPT_INT32, 0),
                        ld(ONE_STRING, b""),
                        ld(R_FLOAT, b""),
                    ],
                    _ => vec![
                        ld(K_R_INT32, b""),
                        vi(K_ONE_INT32, 0),
                        vi(K_ONE_ENUM, 0),
                        ld(K_OPT_STRING, b""),
                        vi(K_OPT_ENUM, 0),
                        ld(K_WKT_EMPTY, b""),
                    ],
                };
                parts.extend(defaults);
                rng.shuffle(&mut parts);
            }

            check_body(
                &plans,
                &desc,
                &format!("{root}/mutation#{i}"),
                &parts.concat(),
            );
        }
    }
}

/// Pins the transcoder's exact bytes for a fully populated document. The
/// transcoder is deterministic even with maps, so this is a byte snapshot.
#[test]
fn everything_bytes_snapshot() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let (_, message) = corpus::corpus(&pool).remove(1);
    let key = corpus::key_variants().remove(1).1;
    let mut out = Vec::new();
    Transcoder::new()
        .transcode(
            &plans,
            plans.root(),
            &message.encode_to_vec(),
            key.as_ref(),
            &corpus::meta(),
            &mut out,
        )
        .unwrap();
    insta::assert_snapshot!("everything_transcoded", String::from_utf8(out).unwrap());
}

#[test]
fn kinds_bytes_snapshot() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let message = kinds_fixture(&pool);
    let plan = plans.plan_for(&message.descriptor()).unwrap();
    let mut out = Vec::new();
    Transcoder::new()
        .transcode(
            &plans,
            plan,
            &message.encode_to_vec(),
            None,
            &corpus::meta(),
            &mut out,
        )
        .unwrap();
    insta::assert_snapshot!("kinds_transcoded", String::from_utf8(out).unwrap());
}

#[test]
fn key_and_meta_merge_like_merge_serializer() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let meta = json!({"op": "u"});
    let body = [
        ld(F_STRING, b"payload"),
        vi(F_INT32, 1),
        ld(44, b"payload-meta"),
    ]
    .concat();
    let key: Map<String, Value> = json!({"f_string": "key-wins", "id": 7})
        .as_object()
        .unwrap()
        .clone();
    let mut out = Vec::new();
    Transcoder::new()
        .transcode(&plans, plans.root(), &body, Some(&key), &meta, &mut out)
        .unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        r#"{"f_int32":1,"f_string":"key-wins","id":7,"_meta":{"op":"u"}}"#
    );
    assert_eq!(
        oracle(&desc, &body, Some(&key), &meta).unwrap(),
        r#"{"f_int32":1,"f_string":"key-wins","id":7,"_meta":{"op":"u"}}"#.as_bytes()
    );
}

/// Key shapes beyond the four corpus variants. The key is merged by name
/// after the payload, so a key naming a payload field drops that field before
/// it is serialized. For an `Any` that means its payload is never decoded,
/// on either path: the first two cases pin that laziness.
#[test]
fn key_shapes_match_merge_serializer() {
    let pool = corpus::pool();
    let plans = Plans::for_schema(&pool, "differential.Everything").unwrap();
    let desc = everything_desc(&pool);
    let meta = corpus::meta();
    let mut transcoder = Transcoder::new();
    let obj = |v: Value| v.as_object().unwrap().clone();
    let any =
        |url: &str, payload: &[u8]| ld(F_ANY, &[ld(1, url.as_bytes()), ld(2, payload)].concat());
    let oneof_both = [ld(ONE_STRING, b"a"), ld(ONE_MESSAGE, &ld(1, b"n"))].concat();

    type KeyCase = (&'static str, Map<String, Value>, Vec<u8>, bool);
    let cases: Vec<KeyCase> = vec![
        (
            "any-overridden-bad-payload",
            obj(json!({"f_any": 1})),
            any("type.googleapis.com/differential.Nested", &[0xFF]),
            true,
        ),
        (
            "any-overridden-bad-url",
            obj(json!({"f_any": null})),
            any("nope", &[]),
            true,
        ),
        (
            "empty-key",
            obj(json!({})),
            [ld(F_STRING, b"s"), vi(F_INT32, 1)].concat(),
            true,
        ),
        (
            "key-names-winning-oneof",
            obj(json!({"one_message": "k"})),
            oneof_both.clone(),
            true,
        ),
        (
            "key-names-losing-oneof",
            obj(json!({"one_string": "k"})),
            oneof_both.clone(),
            true,
        ),
        (
            "key-meta-null",
            obj(json!({"_meta": null, "id": 7})),
            [ld(44, b"m"), ld(F_STRING, b"s")].concat(),
            true,
        ),
        (
            "key-names-every-field-set",
            obj(
                json!({"f_string": 1, "f_int32": "x", "f_message": [], "wkt_struct": 0, "r_string": {}}),
            ),
            [
                ld(F_STRING, b"s"),
                vi(F_INT32, 1),
                ld(F_MESSAGE, &ld(1, b"n")),
                ld(
                    WKT_STRUCT,
                    &ld(1, &[ld(1, b"k"), ld(2, &ld(3, b"v"))].concat()),
                ),
                ld(R_STRING, b"r"),
            ]
            .concat(),
            true,
        ),
    ];
    for (label, key, body, must_succeed) in cases {
        let expected = oracle(&desc, &body, Some(&key), &meta);
        if must_succeed {
            assert!(expected.is_ok(), "{label}: production must succeed");
        }
        let got = transcode_with_fallback(&plans, &desc, &body, Some(&key), &meta, &mut transcoder)
            .map(|(bytes, _)| bytes);
        assert_equivalent(label, expected, got, true);
    }
}
