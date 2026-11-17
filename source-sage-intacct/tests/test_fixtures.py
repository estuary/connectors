"""Runs the responses in `tests/sample_xml_responses` through the connector."""

from datetime import UTC, datetime
from logging import Logger
from unittest.mock import MagicMock

import pytest

from source_sage_intacct.api import fetch_page
from source_sage_intacct.models import IncrementalResource
from source_sage_intacct.sage import DATATYPE_MAP, PAGE_SIZE, Sage

from .sage_fixtures import fixture_rows, lookup_fields
from .test_gl import make_config, make_sage

GL_ROWS = [
    ("GLBATCH", "sample-glbatch"),
    ("GLBATCH", "json-glbatch"),
    ("GLENTRY", "full-glentry"),
    ("GLENTRY", "json-glentry"),
]


def serve_rows(sage: Sage, rows: list[dict[str, str | None]]) -> None:
    """Makes each query of `sage` return `rows`, parsed as Sage's JSON is."""

    async def req_json(cls, data):
        sage.sent.append(data)  # type: ignore[attr-defined]
        for row in rows:
            yield cls.model_validate(row)

    sage._req_json = req_json  # type: ignore[method-assign]


@pytest.mark.parametrize("obj", ["GLBATCH", "GLENTRY"])
def test_lookup_types_are_known(obj: str):
    fields = lookup_fields(obj)

    assert {datatype for _, datatype in fields} <= set(DATATYPE_MAP)
    assert dict(fields)["RECORDNO"] == "INTEGER"
    assert dict(fields)["WHENMODIFIED"] == "TIMESTAMP"


def test_backfill_date_fields_are_dates():
    assert dict(lookup_fields("GLBATCH"))["BATCH_DATE"] == "DATE"
    assert dict(lookup_fields("GLENTRY"))["ENTRY_DATE"] == "DATE"


@pytest.mark.asyncio
@pytest.mark.parametrize("obj,fixture", GL_ROWS)
async def test_backfill_emits_each_fixture_row(obj: str, fixture: str):
    rows = fixture_rows(fixture, obj)
    sage = make_sage(make_config(), {obj: lookup_fields(obj)})
    serve_rows(sage, rows)
    cutoff = datetime(2099, 1, 1, tzinfo=UTC)

    got = [
        item
        async for item in fetch_page(
            obj, sage, PAGE_SIZE, MagicMock(spec=Logger), None, cutoff
        )
    ]

    assert len(got) == len(rows) > 0
    assert all(isinstance(doc, IncrementalResource) for doc in got)
    assert {doc.RECORDNO for doc in got} == {int(row["RECORDNO"]) for row in rows}  # type: ignore[arg-type,union-attr]
    (sent,) = sage.sent  # type: ignore[attr-defined]
    for name, _ in lookup_fields(obj):
        assert f"<field>{name}</field>" in sent


@pytest.mark.asyncio
async def test_glentry_documents_carry_both_batch_references():
    rows = fixture_rows("full-glentry", "GLENTRY")
    sage = make_sage(make_config(), {"GLENTRY": lookup_fields("GLENTRY")})
    model = await sage.get_model("GLENTRY")

    for row in rows:
        doc = model.model_validate(row).model_dump()
        assert isinstance(doc["BATCHNO"], int)
        assert isinstance(doc["BATCH_NUMBER"], int)
        assert doc["BATCHNO"] == int(row["BATCHNO"])  # type: ignore[arg-type]


def test_fixtures_have_empty_booleans():
    """Sage's JSON writes an empty BOOLEAN as "", which the model must drop."""
    types = dict(lookup_fields("GLENTRY"))
    rows = fixture_rows("json-glentry", "GLENTRY")

    assert any(
        value == "" for row in rows for f, value in row.items() if types[f] == "BOOLEAN"
    )
