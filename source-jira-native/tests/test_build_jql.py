import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from source_jira_native.api import _build_jql

UTC = ZoneInfo("UTC")


@pytest.mark.parametrize("projects,expected_fragment", [
    # Single project key should be quoted
    ("ACME", 'project in ("ACME")'),
    # Multiple project keys should each be quoted
    ("ACME,DEV", 'project in ("ACME","DEV")'),
    # Reserved JQL keywords like "TO" must be quoted to work
    ("TO", 'project in ("TO")'),
    ("TO,ACME", 'project in ("TO","ACME")'),
    ("AND,OR,NOT", 'project in ("AND","OR","NOT")'),
    # Numeric project IDs should also be quoted
    ("10000", 'project in ("10000")'),
    ("10000,10004", 'project in ("10000","10004")'),
    # Mix of project keys and IDs
    ("ACME,10000,TO", 'project in ("ACME","10000","TO")'),
])
def test_build_jql_quotes_projects(projects: str, expected_fragment: str):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    jql = _build_jql(start, end, UTC, projects)

    assert expected_fragment in jql


def test_build_jql_no_projects():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    jql = _build_jql(start, end, UTC, projects=None)

    assert "project in" not in jql


def test_build_jql_empty_projects():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    jql = _build_jql(start, end, UTC, projects="")

    assert "project in" not in jql
