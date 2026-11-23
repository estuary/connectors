from logging import Logger
from unittest.mock import MagicMock

import pytest
from estuary_cdk.http import HTTPMixin

from source_sage_intacct import resources
from source_sage_intacct.models import EndpointConfig, SagePermissionError


class FakeSage:
    """Stands in for `Sage` in `all_resources`. It makes no request."""

    unreadable: set[str] = set()

    def __init__(self, log: Logger, http: HTTPMixin, config: EndpointConfig):
        self.config = config

    async def setup(self):
        pass

    async def probe_query_permission(self, obj: str):
        if obj in self.unreadable:
            raise SagePermissionError(f"no access to {obj}")

    async def get_model(self, obj: str):
        return MagicMock()


@pytest.fixture
def fake_sage(monkeypatch: pytest.MonkeyPatch) -> type[FakeSage]:
    monkeypatch.setattr(resources, "Sage", FakeSage)
    monkeypatch.setattr(FakeSage, "unreadable", set())
    return FakeSage
