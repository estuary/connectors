import json
from typing import Any

from estuary_cdk.flow import CollectionSpec, Inference


def collection(projections: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "name": "acmeCo/a",
        "key": ["/id"],
        "writeSchema": {"type": "object"},
        "projections": projections,
    }


def test_known_variants_parse_as_their_enum():
    spec = CollectionSpec.model_validate(
        collection(
            [
                {
                    "field": "f",
                    "inference": {
                        "exists": "MUST",
                        "reduce": "REDUCE_SUM",
                        "redact": "REDACT_BLOCK",
                    },
                }
            ]
        )
    )
    inference = spec.projections[0].inference
    assert inference.exists is Inference.Exists.MUST
    assert inference.reduce is Inference.Reduce.SUM
    assert inference.redact is Inference.Redact.BLOCK
    # As StrEnums, they also compare equal to their wire values.
    assert inference.reduce == "REDUCE_SUM"


def test_unknown_variants_parse_as_their_raw_string():
    # A variant added to `flow.proto` after this CDK was built must not fail
    # the message which carries it.
    spec = CollectionSpec.model_validate(
        collection(
            [
                {
                    "field": "f",
                    "inference": {
                        "exists": "SOMEDAY",
                        "reduce": "REDUCE_BRAND_NEW",
                        "redact": "REDACT_AES",
                    },
                }
            ]
        )
    )
    inference = spec.projections[0].inference
    assert inference.exists == "SOMEDAY"
    assert inference.reduce == "REDUCE_BRAND_NEW"
    assert inference.redact == "REDACT_AES"


def test_omitted_variants_default_to_unset():
    spec = CollectionSpec.model_validate(collection([{"field": "f"}]))
    inference = spec.projections[0].inference
    assert inference.exists is Inference.Exists.INVALID
    assert inference.reduce is Inference.Reduce.UNSET
    assert inference.redact is Inference.Redact.UNSET


def test_variants_re_serialize_unchanged():
    # A linked message re-serializes inline, so both forms must round-trip.
    projections = [
        {
            "field": "known",
            "inference": {
                "exists": "MAY",
                "reduce": "REDUCE_MERGE",
                "redact": "REDACT_SHA256",
            },
        },
        {
            "field": "future",
            "inference": {
                "exists": "SOMEDAY",
                "reduce": "REDUCE_BRAND_NEW",
                "redact": "REDACT_AES",
            },
        },
    ]
    spec = CollectionSpec.model_validate(collection(projections))
    out = json.loads(spec.model_dump_json(exclude_defaults=True))
    assert out["projections"] == projections
