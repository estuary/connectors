from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from estuary_cdk.capture import request as capture_request
from estuary_cdk.derive import request as derive_request
from estuary_cdk.flow import CaptureSpec, DerivedCollectionSpec


class ResourceConfig(BaseModel):
    name: str = ""
    readOnly: bool = False


def collection(name: str) -> dict[str, Any]:
    return {"name": name, "key": ["/id"], "writeSchema": {"type": "object"}}


def capture(bindings: list[dict[str, Any]], linked: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "name": "acmeCo/source-acme",
        "connectorType": "IMAGE",
        "config": {},
        "intervalSeconds": 0,
        "bindings": [
            {"resourceConfig": {}, "resourcePath": ["r"], "stateKey": "r", **b}
            for b in bindings
        ],
        "linkedCollections": linked,
    }


def test_indirect_bindings_share_their_linked_collection():
    spec = CaptureSpec[dict, ResourceConfig].model_validate(
        capture(
            [{"collectionIndex": 1}, {}, {"collectionIndex": 1}],
            [collection("acmeCo/a"), collection("acmeCo/b")],
        )
    )
    names = [b.collection.name for b in spec.bindings]
    assert names == ["acmeCo/b", "acmeCo/a", "acmeCo/b"]
    assert spec.bindings[0].collection is spec.bindings[2].collection
    assert spec.bindings[0].collection is spec.linkedCollections[1]


def test_inline_bindings_keep_their_own_collection():
    spec = CaptureSpec[dict, ResourceConfig].model_validate(
        capture([{"collection": collection("acmeCo/a")}], [])
    )
    assert spec.bindings[0].collection.name == "acmeCo/a"


def test_malformed_forms_are_errors():
    with pytest.raises(ValidationError, match="binding 0 is missing its collection"):
        CaptureSpec[dict, ResourceConfig].model_validate(capture([{}], []))

    with pytest.raises(ValidationError, match="collectionIndex 2, but there are 1"):
        CaptureSpec[dict, ResourceConfig].model_validate(
            capture([{"collectionIndex": 2}], [collection("acmeCo/a")])
        )


def test_nested_messages_are_linked():
    validate = capture_request.Validate[dict, ResourceConfig].model_validate(
        {
            "name": "acmeCo/source-acme",
            "connectorType": "IMAGE",
            "config": {},
            "bindings": [{"resourceConfig": {}}],
            "linkedCollections": [collection("acmeCo/a")],
            "lastCapture": {
                "name": "acmeCo/source-acme",
                "connectorType": "IMAGE",
                "config": {"config": {}},
                "bindings": [{"resourceConfig": {}, "collectionIndex": 0}],
                "linkedCollections": [collection("acmeCo/last")],
            },
        }
    )
    assert validate.bindings[0].collection.name == "acmeCo/a"
    assert validate.lastCapture is not None
    assert validate.lastCapture.bindings[0].collection.name == "acmeCo/last"

    open = capture_request.Open[dict, ResourceConfig, dict].model_validate(
        {
            "capture": capture([{}], [collection("acmeCo/a")]),
            "version": "v1",
            "range": {},
            "state": {},
        }
    )
    assert open.capture.bindings[0].collection.name == "acmeCo/a"


def test_derivations_are_linked_and_omitted_lambdas_are_empty():
    derived = {
        **collection("acmeCo/derived"),
        "projections": [
            {
                "ptr": "/id",
                "field": "id",
                "isPrimaryKey": True,
                "inference": {
                    "types": ["string"],
                    "exists": "MUST",
                    "string": {"format": "uuid", "maxLength": 36},
                },
            }
        ],
        "derivation": {
            "config": {"region": "north"},
            "transforms": [
                {"name": "fromA", "collectionIndex": 0, "lambdaConfig": None},
                {"name": "fromB", "collectionIndex": 0, "lambdaConfig": {"readOnly": True}},
            ],
            "shuffleKeyTypes": ["STRING"],
            "linkedCollections": [collection("acmeCo/a")],
        },
    }
    spec = DerivedCollectionSpec[dict, ResourceConfig].model_validate(derived)
    transforms = spec.derivation.transforms

    assert transforms[0].collection is transforms[1].collection
    assert [t.lambdaConfig.readOnly for t in transforms] == [False, True]
    assert spec.projections[0].inference.exists == "MUST"
    assert spec.projections[0].inference.string is not None
    assert spec.projections[0].inference.string.maxLength == 36

    open = derive_request.Open[dict, ResourceConfig, dict].model_validate(
        {"collection": derived, "version": "v1", "range": {}, "state": {"k": 1}}
    )
    assert open.collection.derivation.config == {"region": "north"}
    assert open.collection.derivation.transforms[1].collection.name == "acmeCo/a"

    validate = derive_request.Validate[dict, ResourceConfig].model_validate(
        {
            "config": {},
            "collection": collection("acmeCo/derived"),
            "transforms": [{"name": "fromA", "collection": collection("acmeCo/a"), "lambdaConfig": None}],
        }
    )
    assert validate.transforms[0].collection.name == "acmeCo/a"
    assert validate.transforms[0].lambdaConfig == ResourceConfig()
