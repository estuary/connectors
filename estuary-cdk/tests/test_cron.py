from datetime import datetime, UTC

import pytest

from estuary_cdk.cron import next_fire


def utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=UTC)


@pytest.mark.parametrize("start, end, expected", [
    # start falls in the minute after one fire time, and end in the minute
    # before the next.
    ("2026-11-02T00:01:05", "2026-11-03T00:00:22", None),
    # A fire time at exactly end is included.
    ("2026-11-02T00:01:05", "2026-11-03T00:01:00", "2026-11-03T00:01:00"),
])
def test_next_fire_window(start: str, end: str, expected: str | None):
    assert next_fire("1 0 * * *", utc(start), utc(end)) == (utc(expected) if expected else None)
