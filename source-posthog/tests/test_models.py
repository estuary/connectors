import json
from decimal import Decimal

import pytest

from source_posthog.models import HogQLRow, ProjectIdValidationContext, Session

# HogQLRow
#
# ijson parses every JSON float as a Decimal. The dev account has no float in
# any HogQL column — `vitals_lcp` is null throughout — so the snapshot tests
# cannot cover this.


def test_hogql_row_accepts_decimal():
    row = HogQLRow.model_validate([1, Decimal("400.0"), None])
    assert row.root[1] == Decimal("400.0")


def test_hogql_row_keeps_decimal_precision():
    value = Decimal("400.12345678901234567890123")
    assert HogQLRow.model_validate([value]).root[0] == value


@pytest.mark.parametrize(
    "value, expected_type",
    [(7, int), (4.5, float), (True, bool), ("s", str), (None, type(None))],
)
def test_hogql_row_does_not_coerce_other_types(value, expected_type):
    assert type(HogQLRow.model_validate([value]).root[0]) is expected_type


def test_session_serializes_decimal_without_losing_precision():
    value = Decimal("400.12345678901234567890123")
    session = Session.model_validate(
        {
            "session_id": "s1",
            "end_timestamp": "2026-10-01T00:00:00Z",
            "max_inserted_at": "2026-10-01T00:00:00Z",
            "vitals_lcp": value,
        },
        context=ProjectIdValidationContext(project_id=7),
    )
    serialized = json.loads(session.model_dump_json(by_alias=True))
    assert serialized["vitals_lcp"] == str(value)
