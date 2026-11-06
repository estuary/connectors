from typing import Any, NamedTuple

from pydantic import BaseModel

from estuary_cdk.materialize import request, response


class ResourceConfig(BaseModel):
    table: str
    delta_updates: bool = False


class Key(NamedTuple):
    id: str
    version: int


class Doc(BaseModel):
    id: str
    version: int


def collection(name: str) -> dict[str, Any]:
    return {"name": name, "key": ["/id"], "writeSchema": {"type": "object"}}


def materialization(bindings: list[dict[str, Any]], linked: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "name": "acmeCo/materialize-acme",
        "connectorType": "IMAGE",
        "config": {"region": "north"},
        "bindings": [{"resourceConfig": {"table": "t"}, "resourcePath": ["t"], **b} for b in bindings],
        "linkedCollections": linked,
    }


def test_validate_is_linked_and_its_last_materialization_is_unmodeled():
    validate = request.Validate[dict[str, Any], ResourceConfig].model_validate(
        {
            "name": "acmeCo/materialize-acme",
            "connectorType": "IMAGE",
            "config": {},
            "bindings": [{"resourceConfig": {"table": "contacts"}, "collectionIndex": 0, "groupBy": ["id"]}],
            "linkedCollections": [collection("acmeCo/contacts")],
            "lastMaterialization": materialization(
                [{"resourceConfig": {"renamed": "contacts"}, "collectionIndex": 0}],
                [collection("acmeCo/last")],
            ),
        }
    )
    assert validate.bindings[0].collection.name == "acmeCo/contacts"
    assert validate.bindings[0].groupBy == ["id"]

    last = validate.lastMaterialization
    assert last is not None
    assert last.bindings[0].collection.name == "acmeCo/last"
    assert last.bindings[0].resourceConfig.model_extra == {"renamed": "contacts"}


def test_omitted_state_is_empty():
    open = request.Open[dict[str, Any], ResourceConfig, dict[str, Any]].model_validate(
        {
            "materialization": materialization([{"collectionIndex": 0}], [collection("acmeCo/a")]),
            "version": "v1",
            "range": {},
        }
    )
    assert open.state == {}
    assert open.materialization.bindings[0].collection.name == "acmeCo/a"
    assert open.materialization.bindings[0].resourceConfig.table == "t"

    class State(BaseModel):
        cursor: int = 0

    apply = request.Apply[dict[str, Any], ResourceConfig, State].model_validate(
        {
            "materialization": materialization([{"collection": collection("acmeCo/a")}], []),
            "version": "v2",
        }
    )
    assert apply.state == State()
    assert apply.lastMaterialization is None


def test_transaction_messages_parse_typed_keys_and_documents():
    load = request.Load[Key].model_validate({"key": ["a", 3]})
    assert load.binding == 0
    assert load.key.version == 3

    store = request.Store[Key, Doc].model_validate(
        {"binding": 2, "key": ["a", 3], "doc": {"id": "a", "version": 3}, "exists": True}
    )
    assert store.key == Key("a", 3)
    assert store.doc.version == 3
    assert (store.exists, store.delete, store.values) == (True, False, [])

    flush = request.Flush.model_validate(
        {
            "statePatches": [{"cursor": 1}],
            "backfillCompletes": [{"binding": 1, "timestamp": "2023-11-14T22:21:40.123+00:00"}],
        }
    )
    assert flush.backfillCompletes[0].binding == 1
    assert flush.backfillBegins == []


def test_responses_serialize_as_the_protocol_expects():
    loaded = response.Loaded[Doc](binding=1, doc=Doc(id="a", version=3))
    assert loaded.model_dump_json() == '{"binding":1,"doc":{"id":"a","version":3}}'

    validated = response.Validated(
        bindings=[
            response.ValidatedBinding(
                resourcePath=["contacts"],
                projectionConstraints=[
                    response.ProjectionConstraint(
                        field="flow_document",
                        constraint=response.Constraint(type="LOCATION_REQUIRED", reason="the root document"),
                    )
                ],
            )
        ]
    )
    assert validated.model_dump(exclude_defaults=True) == {
        "bindings": [
            {
                "resourcePath": ["contacts"],
                "projectionConstraints": [
                    {
                        "field": "flow_document",
                        "constraint": {"type": "LOCATION_REQUIRED", "reason": "the root document"},
                    }
                ],
            }
        ]
    }
