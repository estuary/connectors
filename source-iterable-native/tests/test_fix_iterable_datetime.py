from datetime import datetime

import pytest

from source_iterable_native.models import _fix_iterable_datetime


@pytest.mark.parametrize(
    "input_value,expected",
    [
        # Space-separated datetime with colon in timezone
        ("2026-01-14 06:20:22 +00:00", "2026-01-14T06:20:22+00:00"),
        ("2025-12-01 15:45:30 -05:00", "2025-12-01T15:45:30-05:00"),
        ("2024-06-15 23:59:59 +12:00", "2024-06-15T23:59:59+12:00"),
        # Space-separated datetime without colon in timezone
        ("2026-01-14 06:20:22 +0000", "2026-01-14T06:20:22+00:00"),
        ("2025-12-01 15:45:30 -0500", "2025-12-01T15:45:30-05:00"),
        # Double fractional seconds
        ("2025-12-19T22:43:34.134105.000Z", "2025-12-19T22:43:34.134105Z"),
        ("2026-01-01T00:00:00.123.456Z", "2026-01-01T00:00:00.123Z"),
        # Already valid ISO 8601 - should pass through unchanged
        ("2026-01-14T06:20:22+00:00", "2026-01-14T06:20:22+00:00"),
        ("2025-12-19T22:43:34.134105Z", "2025-12-19T22:43:34.134105Z"),
        ("2025-12-19T22:43:34Z", "2025-12-19T22:43:34Z"),
        # Non-datetime strings - should pass through unchanged
        ("hello world", "hello world"),
        ("user@example.com", "user@example.com"),
        ("12345", "12345"),
        ("", ""),
        # Strings with - and : that aren't datetimes - should pass through unchanged
        ("2026-01-14", "2026-01-14"),
        ("10:30:00", "10:30:00"),
        ("10:00-12:00", "10:00-12:00"),
        ("192.168.1.1:8080", "192.168.1.1:8080"),
        ("550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440000"),
        ("v2.0.0-beta:latest", "v2.0.0-beta:latest"),
        ("00:1A:2B:3C:4D:5E", "00:1A:2B:3C:4D:5E"),
        ("key:value-pair", "key:value-pair"),
        ("2026-01-14 06:20:22", "2026-01-14 06:20:22"),
    ],
)
def test_fix_iterable_datetime_transformation(input_value: str, expected: str):
    """Test that known Iterable datetime formats are correctly transformed."""
    assert _fix_iterable_datetime(input_value) == expected


@pytest.mark.parametrize(
    "input_value",
    [
        # Space-separated formats
        "2026-01-14 06:20:22 +00:00",
        "2025-12-01 15:45:30 -0500",
        # Double fractional seconds
        "2025-12-19T22:43:34.134105.000Z",
        # Already valid
        "2026-01-14T06:20:22+00:00",
        "2025-12-19T22:43:34Z",
    ],
)
def test_fix_iterable_datetime_produces_valid_iso8601(input_value: str):
    """Test that repaired datetimes are valid ISO 8601 and can be parsed."""
    result = _fix_iterable_datetime(input_value)
    datetime.fromisoformat(result)  # Raises ValueError if invalid
