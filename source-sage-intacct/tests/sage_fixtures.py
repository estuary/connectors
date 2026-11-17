"""Reads the Sage responses in `tests/sample_xml_responses`. See the README
there."""

import gzip
from pathlib import Path
from typing import Any

import xmltodict

FIXTURES = Path(__file__).parent / "sample_xml_responses"


def read_fixture(name: str) -> dict[str, Any]:
    """The `data` element of the response in `<name>.xml` or `<name>.xml.gz`."""
    path = FIXTURES / f"{name}.xml"
    if path.exists():
        text = path.read_text()
    else:
        text = gzip.decompress((FIXTURES / f"{name}.xml.gz").read_bytes()).decode()

    return xmltodict.parse(text)["response"]["operation"]["result"]["data"]


def lookup_fields(obj: str) -> list[tuple[str, str]]:
    """Each field of `obj` with its DATATYPE, in the order `lookup` returns."""
    fields = read_fixture(f"lookup-{obj.lower()}")["Type"]["Fields"]["Field"]
    return [(field["ID"], field["DATATYPE"]) for field in fields]


# In Sage's JSON, an empty value of these types is "" or null. An empty value
# of each other type is always null.
EMPTY_STRING_TYPES = {"BOOLEAN", "ENUM", "TEXT"}


def fixture_rows(name: str, obj: str) -> list[dict[str, str | None]]:
    """The rows of `obj` in the fixture, with each value as Sage's JSON has
    it: a string, or for an empty value "" or None by the type of its field."""
    rows = read_fixture(name)[obj]
    types = dict(lookup_fields(obj))
    return [
        {
            field: "" if value is None and types[field] in EMPTY_STRING_TYPES else value
            for field, value in row.items()
        }
        for row in (rows if isinstance(rows, list) else [rows])
    ]
