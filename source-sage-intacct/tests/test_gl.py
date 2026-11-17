from datetime import UTC, date, datetime
from logging import Logger
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from estuary_cdk.capture.backfill_signals import BackfillCoverage

from source_sage_intacct import resources
from source_sage_intacct.models import (
    EndpointConfig,
    FieldDefinition,
    ObjectDefinition,
    SagePermissionError,
)
from source_sage_intacct.resources import (
    GL_OBJECTS,
    LOOKBACK,
    LOOKBACK_CREATIONS,
    LOOKBACK_DELETIONS,
    REALTIME,
    REALTIME_CREATIONS,
    REALTIME_DELETIONS,
    all_resources,
)
from source_sage_intacct.sage import Sage


def make_config(**advanced: Any) -> EndpointConfig:
    return EndpointConfig(
        sender_id="sender_id",
        sender_password="sender_password",
        company_id="company_id",
        user_id="user_id",
        password="password",
        advanced=EndpointConfig.Advanced(**advanced),
    )


class FakeSage:
    """Stands in for `Sage` in `all_resources`. It makes no request."""

    unreadable: set[str] = set()

    def __init__(self, log, http, config):
        self.config = config

    async def setup(self):
        pass

    async def probe_query_permission(self, obj: str):
        if obj in self.unreadable:
            raise SagePermissionError(f"no access to {obj}")

    async def get_model(self, obj: str):
        return MagicMock()


@pytest.fixture
def fake_sage(monkeypatch):
    monkeypatch.setattr(resources, "Sage", FakeSage)
    monkeypatch.setattr(FakeSage, "unreadable", set())
    return FakeSage


async def open_subtasks(monkeypatch, resource) -> dict[str, Any]:
    """Opens the resource and returns what it passes to `open_binding`."""
    opened: dict[str, Any] = {}

    def open_binding(binding, binding_index, state, task, **kwargs):
        opened.update(kwargs)

    monkeypatch.setattr(resources.common, "open_binding", open_binding)

    task = MagicMock()
    task.checkpoint = AsyncMock()
    await resource.open(MagicMock(), 0, resource.initial_state, task, [])
    return opened


@pytest.mark.asyncio
async def test_only_gl_resources_declare_a_backfill_coverage(fake_sage):
    found = await all_resources(MagicMock(spec=Logger), MagicMock(), make_config())
    by_name = {r.name: r for r in found}

    for obj in GL_OBJECTS:
        assert by_name[obj].backfill_coverage == BackfillCoverage.FULL
        assert by_name[obj].key == ["/RECORDNO"]
        assert not by_name[obj].disable

    others = [r for r in found if r.name not in GL_OBJECTS | {"GLDETAIL"}]
    assert others
    assert all(r.backfill_coverage is None for r in others)


@pytest.mark.asyncio
@pytest.mark.parametrize("obj", sorted(GL_OBJECTS))
async def test_gl_binding_opens_all_six_subtasks(monkeypatch, fake_sage, obj):
    found = await all_resources(MagicMock(spec=Logger), MagicMock(), make_config())
    resource = next(r for r in found if r.name == obj)

    opened = await open_subtasks(monkeypatch, resource)

    assert set(opened["fetch_changes"]) == {
        REALTIME,
        LOOKBACK,
        REALTIME_CREATIONS,
        LOOKBACK_CREATIONS,
        REALTIME_DELETIONS,
        LOOKBACK_DELETIONS,
    }
    # A backfill that sends backfill signals needs the backfill function.
    assert opened["fetch_page"] is not None


@pytest.mark.asyncio
async def test_gl_binding_without_audit_trail_has_no_deletion_subtasks(
    monkeypatch, fake_sage
):
    monkeypatch.setattr(FakeSage, "unreadable", {"AUDITHISTORY"})
    log = MagicMock(spec=Logger)
    found = await all_resources(log, MagicMock(), make_config())
    resource = next(r for r in found if r.name == "GLBATCH")

    opened = await open_subtasks(monkeypatch, resource)

    assert set(opened["fetch_changes"]) == {
        REALTIME,
        LOOKBACK,
        REALTIME_CREATIONS,
        LOOKBACK_CREATIONS,
    }
    assert "next backfill" in log.warning.call_args.args[0]


@pytest.mark.asyncio
async def test_unreadable_gl_object_is_omitted(fake_sage, monkeypatch):
    monkeypatch.setattr(FakeSage, "unreadable", {"GLENTRY"})
    found = await all_resources(MagicMock(spec=Logger), MagicMock(), make_config())

    names = {r.name for r in found}
    assert "GLENTRY" not in names
    assert "GLBATCH" in names


def make_sage(config: EndpointConfig, lookup: dict[str, list[tuple[str, str]]]) -> Sage:
    """A `Sage` whose `lookup` answers come from `lookup`, and whose queries
    are recorded in `sage.sent` and return no records."""
    sage = Sage(MagicMock(spec=Logger), MagicMock(), config)
    sage.session_id = "session_id"
    sage.tz_dt = datetime(2024, 1, 1, tzinfo=UTC)
    sage.sent = []  # type: ignore[attr-defined]

    async def req_xml(cls, data, skip_refresh=False):
        obj = data.split("<object>")[1].split("</object>")[0]
        return ObjectDefinition(
            Type=ObjectDefinition.Type_(
                Fields=ObjectDefinition.Type_.Fields_(
                    Field=[
                        FieldDefinition(ID=name, DATATYPE=datatype)
                        for name, datatype in lookup[obj]
                    ]
                )
            )
        )

    async def req_json(cls, data):
        sage.sent.append(data)  # type: ignore[attr-defined]
        return
        yield

    sage._req_xml = req_xml  # type: ignore[method-assign]
    sage._req_json = req_json  # type: ignore[method-assign]
    return sage


# A company can add its own fields and books. The streams select each field
# that `lookup` returns and add no filter on book or state.
GLENTRY_LOOKUP = [
    ("RECORDNO", "INTEGER"),
    ("BATCHNO", "INTEGER"),
    ("BATCH_NUMBER", "INTEGER"),
    ("ENTRY_DATE", "DATE"),
    ("BOOKID", "TEXT"),
    ("STATE", "TEXT"),
    ("AMOUNT", "DECIMAL"),
    ("WHENCREATED", "TIMESTAMP"),
    ("WHENMODIFIED", "TIMESTAMP"),
    ("A_CUSTOM_FIELD", "TEXT"),
]


@pytest.mark.asyncio
async def test_gl_queries_select_every_lookup_field():
    sage = make_sage(make_config(), {"GLENTRY": GLENTRY_LOOKUP})
    since = datetime(2024, 5, 1, tzinfo=UTC)

    async for _ in sage.fetch_since("GLENTRY", since):
        pass
    async for _ in sage.fetch_all("GLENTRY", None):
        pass

    for sent in sage.sent:  # type: ignore[attr-defined]
        for name, _ in GLENTRY_LOOKUP:
            assert f"<field>{name}</field>" in sent
        assert "<value>ACCRUAL</value>" not in sent
        assert "<field>BOOKID</field>\n                <value>" not in sent
        assert "<field>STATE</field>\n                <value>" not in sent

    model = await sage.get_model("GLENTRY")
    schema = model.sourced_schema()["properties"]
    assert schema["A_CUSTOM_FIELD"] == {"type": "string"}
    assert schema["BATCHNO"] == {"type": "integer"}
    assert schema["BATCH_NUMBER"] == {"type": "integer"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "obj,date_field", [("GLBATCH", "BATCH_DATE"), ("GLENTRY", "ENTRY_DATE")]
)
async def test_gl_backfill_filters_from_1900(obj: str, date_field: str):
    sage = make_sage(make_config(), {obj: GLENTRY_LOOKUP})

    async for _ in sage.fetch_all(obj, None):
        pass
    async for _ in sage.fetch_all(obj, 77):
        pass

    first, resumed = sage.sent  # type: ignore[attr-defined]
    for sent in (first, resumed):
        assert f"<field>{date_field}</field>" in sent
        assert "<value>01/01/1900</value>" in sent
        assert "<greaterthanorequalto>" in sent
    assert "<and>" not in first
    assert "<and>" in resumed
    assert "<value>77</value>" in resumed


@pytest.mark.asyncio
async def test_other_backfills_send_no_start_date():
    sage = make_sage(make_config(), {"VENDOR": GLENTRY_LOOKUP})

    async for _ in sage.fetch_all("VENDOR", None):
        pass

    (sent,) = sage.sent  # type: ignore[attr-defined]
    assert "<filter>" not in sent
