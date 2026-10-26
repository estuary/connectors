"""Unit tests for the contact_list_memberships incremental stream."""

import json
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any

import estuary_cdk.emitted_changes_cache as cache
import pytest

from source_hubspot_native.api.contact_list_memberships import (
    fetch_contact_list_memberships,
)
from source_hubspot_native.models import ContactListMembership, Names

log = getLogger("test_contact_list_memberships")

LIST_ID = 100
LOG_CURSOR = datetime.now(tz=UTC) - timedelta(hours=1)
JOINED_AT = LOG_CURSOR + timedelta(minutes=10, milliseconds=123)


class FakeHTTP:
    """Returns canned response bodies in FIFO order."""

    def __init__(self, responses: list[bytes]):
        self._responses = list(responses)

    async def request(
        self, log, url, method="GET", params=None, json=None, **kwargs
    ) -> bytes:
        assert self._responses, f"unexpected request: {url}"
        return self._responses.pop(0)


def _list_search_page(lists: dict[int, str] | None = None) -> bytes:
    """`lists` maps list id to processing type."""
    if lists is None:
        lists = {LIST_ID: "DYNAMIC"}
    last_modified_ms = int(JOINED_AT.timestamp() * 1000)
    return json.dumps(
        {
            "lists": [
                {
                    "listId": list_id,
                    "objectTypeId": "0-1",
                    "processingType": processing_type,
                    "updatedAt": JOINED_AT.isoformat(),
                    "additionalProperties": {
                        "hs_lastmodifieddate": str(last_modified_ms)
                    },
                }
                for list_id, processing_type in lists.items()
            ],
            "hasMore": False,
            "offset": 1,
        }
    ).encode()


def _memberships_page(record_ids: list[str]) -> bytes:
    body: dict[str, Any] = {
        "results": [
            {"recordId": record_id, "membershipTimestamp": JOINED_AT.isoformat()}
            for record_id in record_ids
        ],
    }
    return json.dumps(body).encode()


@pytest.mark.asyncio
async def test_memberships_joining_one_list_in_the_same_millisecond_are_all_emitted():
    cache.emitted_cache.pop(Names.contact_list_memberships, None)
    http = FakeHTTP([_list_search_page(), _memberships_page(["1", "2"])])

    emitted = [
        item
        async for item in fetch_contact_list_memberships(http, log, LOG_CURSOR)  # type: ignore[arg-type]
        if isinstance(item, ContactListMembership)
    ]

    assert sorted(m.recordId for m in emitted) == ["1", "2"]


@pytest.mark.asyncio
async def test_adhoc_lists_are_not_requested():
    cache.emitted_cache.pop(Names.contact_list_memberships, None)
    # Only one memberships page is queued; requesting the ADHOC list would
    # exhaust the fake and fail the test.
    http = FakeHTTP(
        [
            _list_search_page({LIST_ID: "DYNAMIC", LIST_ID + 1: "ADHOC"}),
            _memberships_page(["1"]),
        ]
    )

    emitted = [
        item
        async for item in fetch_contact_list_memberships(http, log, LOG_CURSOR)  # type: ignore[arg-type]
        if isinstance(item, ContactListMembership)
    ]

    assert [(m.listId, m.recordId) for m in emitted] == [(LIST_ID, "1")]
