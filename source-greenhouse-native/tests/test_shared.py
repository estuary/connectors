from datetime import datetime, timezone

import pytest

from source_greenhouse_native.shared import extract_next_cursor, updated_at_param


def test_extracts_cursor_from_next_link():
    headers = {
        "Link": '<https://harvest.greenhouse.io/v3/interviewers?cursor=abc123>; rel="next"',
    }
    assert extract_next_cursor(headers) == "abc123"


def test_returns_none_when_no_link_header():
    assert extract_next_cursor({}) is None


def test_returns_none_when_link_header_is_empty():
    assert extract_next_cursor({"Link": ""}) is None


def test_returns_none_when_no_next_rel():
    headers = {
        "Link": '<https://harvest.greenhouse.io/v3/interviewers?cursor=abc>; rel="prev"',
    }
    assert extract_next_cursor(headers) is None


def test_extracts_cursor_from_multiple_links():
    headers = {
        "Link": '<https://harvest.greenhouse.io/v3/interviewers?cursor=abc>; rel="prev", '
                '<https://harvest.greenhouse.io/v3/interviewers?cursor=def>; rel="next"',
    }
    assert extract_next_cursor(headers) == "def"


def test_handles_extra_whitespace():
    headers = {
        "Link": '<https://harvest.greenhouse.io/v3/interviewers?cursor=xyz>;  rel="next"',
    }
    assert extract_next_cursor(headers) == "xyz"


def test_returns_none_when_next_link_has_no_cursor_param():
    headers = {
        "Link": '<https://harvest.greenhouse.io/v3/interviewers?page=2>; rel="next"',
    }
    assert extract_next_cursor(headers) is None


def test_updated_at_param_gte_only():
    dt = datetime(2025, 3, 20, tzinfo=timezone.utc)
    assert updated_at_param(gte=dt) == "gte|2025-03-20T00:00:00Z"


def test_updated_at_param_lte_only():
    dt = datetime(2025, 3, 20, tzinfo=timezone.utc)
    assert updated_at_param(lte=dt) == "lte|2025-03-20T00:00:00Z"


def test_updated_at_param_gte_and_lte():
    start = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end = datetime(2025, 3, 20, tzinfo=timezone.utc)
    assert updated_at_param(gte=start, lte=end) == "gte|2025-03-01T00:00:00Z|lte|2025-03-20T00:00:00Z"


def test_updated_at_param_gt_only():
    dt = datetime(2025, 3, 20, tzinfo=timezone.utc)
    assert updated_at_param(gt=dt) == "gt|2025-03-20T00:00:00Z"


def test_updated_at_param_lt_only():
    dt = datetime(2025, 3, 20, tzinfo=timezone.utc)
    assert updated_at_param(lt=dt) == "lt|2025-03-20T00:00:00Z"


def test_updated_at_param_no_args_raises():
    with pytest.raises(ValueError, match="requires at least one bound"):
        updated_at_param()


def test_updated_at_param_gt_and_gte_raises():
    dt = datetime(2025, 3, 20, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="Cannot specify both gt and gte"):
        updated_at_param(gt=dt, gte=dt)


def test_updated_at_param_lt_and_lte_raises():
    dt = datetime(2025, 3, 20, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="Cannot specify both lt and lte"):
        updated_at_param(lt=dt, lte=dt)
