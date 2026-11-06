from typing import Any

import pytest
from pydantic import BaseModel

from estuary_cdk import flow
from estuary_cdk.flow import CaptureSpec


class ResourceConfig(BaseModel):
    pass


def capture() -> dict[str, Any]:
    return {
        "name": "acmeCo/source-acme",
        "connectorType": "IMAGE",
        "config": {},
        "intervalSeconds": 0,
        "bindings": [
            {
                "collectionIndex": 0,
                "resourceConfig": {},
                "resourcePath": ["r"],
                "stateKey": "r",
            }
        ],
        "linkedCollections": [
            {
                "name": "acmeCo/a",
                "key": ["/id"],
                "writeSchema": {"type": "object"},
                "projections": [{"field": "f", "inference": {"exists": "MUST"}}],
            }
        ],
    }


@pytest.fixture
def retain_projections():
    # The flag is process-wide, as a process serves one connector.
    def _set(retain: bool):
        flow.set_retain_projections(retain)

    yield _set
    flow.set_retain_projections(True)


def test_projections_are_retained_by_default():
    spec = CaptureSpec[dict, ResourceConfig].model_validate(capture())
    assert len(spec.bindings[0].collection.projections) == 1


def test_projections_are_dropped_when_not_retained(retain_projections):
    retain_projections(False)
    spec = CaptureSpec[dict, ResourceConfig].model_validate(capture())
    assert spec.bindings[0].collection.projections == []
    # The rest of the collection is untouched.
    assert spec.bindings[0].collection.name == "acmeCo/a"
    assert spec.bindings[0].collection.key == ["/id"]


def test_connectors_retain_projections_unless_they_opt_out():
    from estuary_cdk import BaseConnector

    assert BaseConnector.retain_projections is True


@pytest.mark.asyncio
async def test_serve_applies_the_connectors_choice(retain_projections):
    # The flag has to be set before the first line is parsed, so drive the real
    # `serve()` and parse from within the request generator it drives.
    import json
    from collections.abc import AsyncGenerator

    from estuary_cdk import BaseConnector

    raw = json.dumps(capture()).encode()
    Spec = CaptureSpec[dict, ResourceConfig]
    seen: list[int] = []

    class Connector(BaseConnector[Spec]):
        retain_projections = False

        def request_class(self) -> type[Spec]:
            return Spec

        async def handle(self, log: Any, request: Spec) -> None:
            pass

    async def requests(cls: type[Spec]) -> AsyncGenerator[Spec, None]:
        spec = cls.model_validate_json(raw)
        seen.append(len(spec.bindings[0].collection.projections))
        if False:
            yield spec

    await Connector().serve(requests=requests)
    assert seen == [0]
