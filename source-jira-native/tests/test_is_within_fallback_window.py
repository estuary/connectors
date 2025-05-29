import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from source_jira_native.api import _is_within_dst_fallback_window

LONDON = ZoneInfo("Europe/London")

def parse_jira_timestamp(ts: str) -> datetime:
    """
    Parse a Jira-style timestamp like '2024-10-27T00:15:00+0100'
    into a timezone-aware datetime.
    """
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")

@pytest.mark.parametrize("timestamp_str,expected", [
    # Before the fallback hour
    ("2024-10-27T00:15:00+0100", False),
    ("2024-10-27T00:59:00+0100", False),
    # Within fallback hour
    ("2024-10-27T01:00:00+0100", True),
    ("2024-10-27T01:59:00+0100", True),
    # After fallback hour
    ("2024-10-27T01:30:00+0000", False),
    ("2024-10-27T02:15:00+0000", False),
])
def test_is_within_dst_fallback_window(timestamp_str: str, expected: bool):
    dt = parse_jira_timestamp(timestamp_str)
    assert _is_within_dst_fallback_window(dt, LONDON) is expected


@pytest.mark.parametrize("timestamp_str", [
    # Before the spring forward transition.
    "2025-03-30T00:15:00+0000",
    "2025-03-30T00:59:00+0000",
    # At the spring forward transition.
    "2025-03-30T01:00:00+0000",
    # After springing forward.
    "2025-03-30T01:30:00+0100",
    "2025-03-30T02:15:00+0100",
])
def test_spring_forward_returns_false(timestamp_str: str):
    dt = parse_jira_timestamp(timestamp_str)
    assert not _is_within_dst_fallback_window(dt, LONDON)


def test_naive_datetime_raises():
    naive_dt = datetime(2024, 10, 27, 1, 0)  # No tzinfo
    with pytest.raises(ValueError, match="timezone-aware"):
        _is_within_dst_fallback_window(naive_dt, LONDON)
