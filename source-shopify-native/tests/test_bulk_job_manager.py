"""Tests for how BulkJobManager marks the bulk jobs it submits and which jobs it cancels on startup.

Shopify's concurrency limit applies per app per shop, and `bulkOperations` lists every running job
for the app, including jobs submitted by other systems that share the connector's credentials. The
connector tags each query it submits with a marker comment and only cancels running jobs that carry
it. Only the GraphQL client is faked, so the real query building and response parsing run.
"""

import asyncio
import re
from logging import Logger
from typing import Any
from unittest.mock import MagicMock

import pytest

from source_shopify_native.graphql.bulk_job_manager import BulkJobManager

# Pinned as a literal rather than imported: jobs submitted by one connector version are cancelled
# by the next, so changing the marker is a compatibility break this test should flag.
MARKER = "# Estuary Flow Managed Bulk Query"
STORE = "teststore"


def _job(job_id: str, status: str = "RUNNING", query: str = "") -> dict[str, Any]:
    return {
        "type": "QUERY",
        "id": job_id,
        "status": status,
        "createdAt": "2026-01-01T00:00:00Z",
        "completedAt": None,
        "url": None,
        "errorCode": None,
        "query": query,
    }


MARKED_JOB = _job("gid://shopify/BulkOperation/1", query=f"{MARKER}\n{{ products {{ edges {{ node {{ id }} }} }} }}")
UNMARKED_JOB = _job("gid://shopify/BulkOperation/2", query="{ orders { edges { node { id } } } }")


class FakeClient:
    """Stands in for ShopifyGraphQLClient, routing each request by the operation it contains.

    Responses are the `data` payloads Shopify returns, validated into the data model the manager
    asks for, so the manager's own response models are exercised.
    """

    def __init__(
        self,
        running_jobs: list[dict[str, Any]],
        final_status: str = "CANCELED",
        keeps_comments: bool = True,
    ):
        self.store = STORE
        # Whether the query reported back for a submitted job keeps its comment lines.
        self.keeps_comments = keeps_comments
        self.running_jobs = running_jobs
        self.final_status = final_status
        self.cancelled: list[str] = []
        self.polled: list[str] = []
        self.submitted: list[str] = []

    async def request(self, query: str, data_model: type, log: Logger, context: Any = None):
        if "bulkOperationRunQuery(" in query:
            self.submitted.append(query)
            reported_query = _submitted_inner_query(query)
            if not self.keeps_comments:
                reported_query = "\n".join(
                    line for line in reported_query.splitlines() if not line.strip().startswith("#")
                )
            job_id = f"gid://shopify/BulkOperation/{100 + len(self.submitted)}"
            payload = {
                "bulkOperationRunQuery": {
                    "bulkOperation": _job(job_id, status="CREATED", query=reported_query),
                    "userErrors": [],
                }
            }
        elif "bulkOperationCancel(" in query:
            job_id = _job_id(query)
            self.cancelled.append(job_id)
            payload = {
                "bulkOperationCancel": {
                    "bulkOperation": _job(job_id, status="CANCELING"),
                    "userErrors": [],
                }
            }
        elif "bulkOperations(" in query:
            payload = {"bulkOperations": {"edges": [{"node": job} for job in self.running_jobs]}}
        elif "node(id:" in query:
            job_id = _job_id(query)
            self.polled.append(job_id)
            node = _job(job_id, status=self.final_status)
            if self.final_status == "COMPLETED":
                node["url"] = "https://example.com/results.jsonl"
            node.pop("query")
            payload = {"node": node}
        else:
            raise AssertionError(f"unexpected query: {query}")

        return data_model.model_validate(payload)


def _job_id(query: str) -> str:
    match = re.search(r'id: "([^"]+)"', query)
    assert match, query
    return match.group(1)


def _submitted_inner_query(mutation: str) -> str:
    """The query Shopify runs and later reports in `BulkOperation.query`: the triple-quoted argument."""
    match = re.search(r'query: """(.*?)"""', mutation, re.DOTALL)
    assert match, mutation
    return match.group(1)


@pytest.fixture
def log():
    return MagicMock(spec=Logger)


def _manager(client: FakeClient, log) -> BulkJobManager:
    return BulkJobManager(client, log)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_cancel_current_cancels_only_marked_jobs(log):
    """A running job without the marker belongs to another system and is left alone."""
    client = FakeClient(running_jobs=[MARKED_JOB, UNMARKED_JOB])
    manager = _manager(client, log)

    await manager.cancel_current()
    # Let the background wait for the cancel finish, so it isn't left pending when the test ends.
    await asyncio.gather(*manager._cancel_tasks)

    assert client.cancelled == [MARKED_JOB["id"]]


@pytest.mark.asyncio
async def test_cancel_current_ignores_unmarked_jobs(log):
    """With only other systems' jobs running, nothing is cancelled or waited on.

    This is also the first restart after the marker ships: jobs left over from the previous
    version carry no marker and run to completion.
    """
    client = FakeClient(running_jobs=[UNMARKED_JOB])

    await _manager(client, log).cancel_current()

    assert client.cancelled == []
    assert client.polled == []


@pytest.mark.asyncio
async def test_execute_submits_marked_query(log):
    """Every submitted query carries the marker, inside the query Shopify reports back."""
    client = FakeClient(running_jobs=[], final_status="COMPLETED")

    url = await _manager(client, log).execute(MagicMock(NAME="products"), "{ products { edges { node { id } } } }")

    assert url == "https://example.com/results.jsonl"
    assert len(client.submitted) == 1
    inner_lines = [line.strip() for line in _submitted_inner_query(client.submitted[0]).splitlines()]
    assert MARKER in inner_lines
    log.warning.assert_not_called()


@pytest.mark.asyncio
async def test_execute_warns_when_shopify_drops_marker(log):
    """If Shopify stops keeping comments, cancel_current can't find the connector's jobs, so say so."""
    client = FakeClient(running_jobs=[], final_status="COMPLETED", keeps_comments=False)

    await _manager(client, log).execute(MagicMock(NAME="products"), "{ products { edges { node { id } } } }")

    warnings = [call.args[0] for call in log.warning.call_args_list]
    assert any("did not keep the bulk query marker" in w for w in warnings), warnings
