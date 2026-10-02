//! proto2 presence, packed/unpacked, enum aliases, and the group/extension
//! fallbacks.

use super::*;

#[test]
fn proto2_presence_groups_and_extensions() {
    let set = protox::compile(
        ["differential_corpus_proto2.proto"],
        [concat!(env!("CARGO_MANIFEST_DIR"), "/src/testdata")],
    )
    .unwrap();
    let pool = DescriptorPool::from_file_descriptor_set(set).unwrap();
    let plans = Plans::for_schema(&pool, "differential2.Plain").unwrap();
    let plain = pool.get_message_by_name("differential2.Plain").unwrap();
    let legacy = pool.get_message_by_name("differential2.Legacy").unwrap();

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        // Explicit presence: defaults set on the wire are emitted.
        (
            "explicit-defaults",
            [vi(1, 0), ld(2, b""), vi(3, 0), vi(4, 0)].concat(),
        ),
        (
            "explicit-declared-default",
            [vi(1, 7), ld(2, b"dflt")].concat(),
        ),
        (
            "values",
            [vi(1, 9), ld(2, b"x"), vi(3, 1), vi(4, (-3i64) as u64)].concat(),
        ),
        (
            "unpacked-as-packed",
            ld(5, &[varint(1), varint(2)].concat()),
        ),
        ("packed-as-unpacked", [vi(6, 1), vi(6, 2)].concat()),
        ("packed-empty", ld(6, &[])),
        ("alias", vi(7, 1)),
        ("alias-unknown", vi(7, 99)),
        (
            "aliases-packed",
            ld(8, &[varint(1), varint(2), varint(0)].concat()),
        ),
        // A missing map value is the enum's first declared value, not 0.
        ("nonzero-map-missing-value", ld(9, &ld(1, b"k"))),
        (
            "nonzero-map-value",
            ld(9, &[ld(1, b"k"), vi(2, 6)].concat()),
        ),
    ];
    let mut fell_back = false;
    for (label, body) in cases {
        fell_back |= check_body(&plans, &plain, label, &body);
    }
    assert!(!fell_back, "Plain must run on the fast path");

    let group = [
        tag(2, WT_START_GROUP),
        ld(1, b"inner"),
        tag(2, WT_END_GROUP),
    ]
    .concat();
    for (label, body) in [
        ("legacy-plain", vi(1, 5)),
        ("legacy-group", [vi(1, 5), group.clone()].concat()),
        ("legacy-extension", [vi(1, 5), ld(100, b"ext")].concat()),
    ] {
        assert!(
            check_body(&plans, &legacy, label, &body),
            "{label}: must fall back"
        );
    }

    // Legacy declines for its extensions before any tag is read, so the
    // group fallback itself only fires on a message without them.
    let group_only = pool.get_message_by_name("differential2.GroupOnly").unwrap();
    assert!(
        !check_body(&plans, &group_only, "group-only-plain", &vi(1, 5)),
        "GroupOnly without its group must run on the fast path"
    );
    let with_group = [vi(1, 5), group].concat();
    assert!(
        check_body(&plans, &group_only, "group-only-group", &with_group),
        "group-only-group: must fall back"
    );
    // The group's field number with another wire type: declined before the
    // wire type is checked, and production rejects it.
    let group_as_len = [vi(1, 5), ld(2, b"x")].concat();
    check_body(
        &plans,
        &group_only,
        "group-only-number-as-len",
        &group_as_len,
    );
    for body in [&with_group, &group_as_len] {
        let plan = plans.plan_for(&group_only).unwrap();
        let mut out = Vec::new();
        let err = Transcoder::new()
            .transcode(&plans, plan, body, None, &corpus::meta(), &mut out)
            .unwrap_err();
        assert!(
            matches!(err, TranscodeError::Unsupported("proto2 group field")),
            "{err}"
        );
    }
}
