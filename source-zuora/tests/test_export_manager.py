"""Unit tests for ExportManager: AQuA submit/poll/download lifecycle, status
handling (documented statuses plus tap-zuora's observed "failed"), file
segmentation, the 403 -> ExportTooLargeError detection, CSV prefix stripping,
and the retry-only-on-ExportError policy.

A FakeHTTP stands in for the CDK HTTPSession, routing by URL.
"""

import logging

import pytest

from estuary_cdk.http import HTTPError

from source_zuora import export_manager as em
from source_zuora.export_manager import ExportError, ExportManager, ExportTooLargeError
from source_zuora.models import AquaJobStatus
from source_zuora.shared import ZUORA_API_VERSION

_LOG = logging.getLogger(__name__)
_BASE = "https://rest.zuora.com"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    # Collapse polling/backoff delays so tests don't actually wait.
    monkeypatch.setattr(em, "POLL_INTERVAL", 0)
    monkeypatch.setattr(em, "RETRY_BACKOFF_BASE_SECONDS", 0)


class FakeHTTP:
    def __init__(self, *, submit=None, polls=None, downloads=None):
        # Each of submit/downloads entries may be bytes or an Exception (raised).
        # polls is a list of bytes/Exception; the last entry repeats. downloads
        # is keyed by file id.
        self.submit_response = submit
        self.poll_responses = list(polls or [])
        self.downloads = downloads or {}
        self.submit_calls = 0
        self.submit_payloads: list = []
        self.poll_calls = 0
        self.request_headers: list = []
        self.stream_headers = None
        self.downloaded_file_ids: list = []

    async def request(self, log, url, method=None, json=None, headers=None):
        self.request_headers.append(headers)
        if url.endswith("/v1/batch-query/") and method == "POST":
            self.submit_calls += 1
            self.submit_payloads.append(json)
            return _resolve(self.submit_response)
        if "/v1/batch-query/jobs/" in url:
            i = min(self.poll_calls, len(self.poll_responses) - 1)
            self.poll_calls += 1
            return _resolve(self.poll_responses[i])
        raise AssertionError(f"unexpected request URL: {url}")

    async def request_stream(self, log, url, headers=None):
        assert "/v1/files/" in url, url
        file_id = url.rsplit("/", 1)[-1]
        self.downloaded_file_ids.append(file_id)
        self.stream_headers = headers
        data = _resolve(self.downloads[file_id])

        async def body():
            yield data

        return {}, body


def _resolve(value):
    if isinstance(value, Exception):
        raise value
    return value


def _mgr(http) -> ExportManager:
    return ExportManager(http, _LOG, _BASE)


_SUBMIT_OK = b'{"id": "job1", "status": "submitted"}'


# --- _submit -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_returns_job_id_on_success():
    mgr = _mgr(FakeHTTP(submit=_SUBMIT_OK))
    assert await mgr._submit("SELECT Id FROM Account") == "job1"


@pytest.mark.asyncio
async def test_submit_validation_failure_raises_export_error_with_detail():
    # AQuA reports a rejected submission via a top-level message, not an HTTP
    # error status.
    http = FakeHTTP(
        submit=b'{"message": "There is a syntax error in one of the queries in the AQuA input", "status": "error"}'
    )
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._submit("SELECT X FROM Account")
    assert "syntax error" in str(exc.value)
    assert exc.value.status is AquaJobStatus.ERROR


@pytest.mark.asyncio
async def test_submit_sends_stateless_zoqlexport_payload():
    http = FakeHTTP(submit=_SUBMIT_OK)
    await _mgr(http)._submit("SELECT Id FROM Account")
    (payload,) = http.submit_payloads
    assert payload["format"] == "csv"
    assert payload["version"] == em.AQUA_VERSION
    assert payload["dateTimeUtc"] == "true"
    # Without useQueryLabels, CSV headers are "Object: Field Label" display
    # labels and no row key matches the models' field names.
    assert payload["useQueryLabels"] == "true"
    # partner/project must be absent: their presence would flip the job into
    # stateful mode and change the query's incremental semantics.
    assert "partner" not in payload and "project" not in payload
    (query,) = payload["queries"]
    assert query["query"] == "SELECT Id FROM Account"
    assert query["type"] == "zoqlexport"


# --- _poll_until_complete ------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_completed_returns_file_id():
    http = FakeHTTP(
        polls=[b'{"id": "job1", "status": "completed", "batches": [{"fileId": "file1"}]}']
    )
    assert await _mgr(http)._poll_until_complete("job1") == ["file1"]


@pytest.mark.asyncio
async def test_poll_completed_prefers_segments_over_file_id():
    # Tenants with AQuA file segmentation enabled return the result as multiple
    # files; all of them must be surfaced or rows are silently lost.
    http = FakeHTTP(
        polls=[
            b'{"id": "job1", "status": "completed",'
            b' "batches": [{"fileId": "f0", "segments": ["s1", "s2"]}]}'
        ]
    )
    assert await _mgr(http)._poll_until_complete("job1") == ["s1", "s2"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["error", "aborted", "cancelled", "failed"])
async def test_poll_terminal_failure_raises_export_error(status):
    http = FakeHTTP(
        polls=[
            f'{{"id": "job1", "status": "{status}",'
            f' "batches": [{{"message": "boom"}}]}}'.encode()
        ]
    )
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._poll_until_complete("job1")
    assert exc.value.status is AquaJobStatus(status)
    assert exc.value.job_id == "job1"
    assert "boom" in str(exc.value)


@pytest.mark.asyncio
async def test_poll_completed_without_file_ids_raises():
    http = FakeHTTP(polls=[b'{"id": "job1", "status": "completed", "batches": [{}]}'])
    with pytest.raises(ExportError):
        await _mgr(http)._poll_until_complete("job1")


@pytest.mark.asyncio
async def test_poll_pending_then_completed_loops():
    http = FakeHTTP(
        polls=[
            b'{"id": "job1", "status": "pending"}',
            b'{"id": "job1", "status": "completed", "batches": [{"fileId": "f"}]}',
        ]
    )
    assert await _mgr(http)._poll_until_complete("job1") == ["f"]
    assert http.poll_calls == 2


@pytest.mark.asyncio
async def test_poll_times_out(monkeypatch):
    monkeypatch.setattr(em, "MAX_POLL_ATTEMPTS", 3)
    http = FakeHTTP(polls=[b'{"id": "job1", "status": "executing"}'])
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._poll_until_complete("job1")
    assert "timed out" in str(exc.value)
    assert http.poll_calls == 3


# --- _fetch_results ------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_results_strips_object_prefix():
    http = FakeHTTP(downloads={"file1": b"Account.Id,Account.Name\n1,Acme\n"})
    rows = [r async for r in _mgr(http)._fetch_results("file1")]
    assert rows == [{"Id": "1", "Name": "Acme"}]


# A too-large 403 carries the max-object-size XML marker in its body, which the
# CDK folds into HTTPError.message for 4xx responses.
_TOO_LARGE_403 = HTTPError(
    "Encountered HTTP error status 403 which cannot be retried.\nResponse:\n"
    "<?xml version='1.0'?><error>"
    "<security:max-object-size>2047MB</security:max-object-size></error>",
    403,
)


@pytest.mark.asyncio
async def test_fetch_results_403_with_marker_raises_too_large():
    http = FakeHTTP(downloads={"file1": _TOO_LARGE_403})
    with pytest.raises(ExportTooLargeError):
        [r async for r in _mgr(http)._fetch_results("file1")]


@pytest.mark.asyncio
async def test_fetch_results_403_without_marker_propagates():
    # A 403 that isn't the size-overflow marker — most often the download user
    # lacks the "Enable DataSource Exports Reporting" permission — must propagate
    # as HTTPError, not be misdiagnosed as a too-large export.
    http = FakeHTTP(
        downloads={
            "file1": HTTPError(
                "Encountered HTTP error status 403 which cannot be retried.\nResponse:\n"
                "You do not have permission to perform this action.",
                403,
            )
        }
    )
    with pytest.raises(HTTPError) as exc:
        [r async for r in _mgr(http)._fetch_results("file1")]
    assert not isinstance(exc.value, ExportTooLargeError)


@pytest.mark.asyncio
async def test_fetch_results_other_http_error_propagates():
    http = FakeHTTP(downloads={"file1": HTTPError("server error", 500)})
    with pytest.raises(HTTPError):
        [r async for r in _mgr(http)._fetch_results("file1")]


# --- export_rows (end to end) --------------------------------------------------


@pytest.mark.asyncio
async def test_export_rows_submit_poll_stream():
    http = FakeHTTP(
        submit=_SUBMIT_OK,
        polls=[b'{"id": "job1", "status": "completed", "batches": [{"fileId": "file1"}]}'],
        downloads={"file1": b"Account.Id\n7\n"},
    )
    rows = [r async for r in _mgr(http).export_rows("SELECT Id FROM Account")]
    assert rows == [{"Id": "7"}]


@pytest.mark.asyncio
async def test_export_rows_concatenates_segments_in_order():
    # Each segment is a self-contained CSV with its own header row.
    http = FakeHTTP(
        submit=_SUBMIT_OK,
        polls=[
            b'{"id": "job1", "status": "completed",'
            b' "batches": [{"segments": ["s1", "s2"]}]}'
        ],
        downloads={
            "s1": b"Account.Id\n1\n2\n",
            "s2": b"Account.Id\n3\n",
        },
    )
    rows = [r async for r in _mgr(http).export_rows("SELECT Id FROM Account")]
    assert rows == [{"Id": "1"}, {"Id": "2"}, {"Id": "3"}]
    assert http.downloaded_file_ids == ["s1", "s2"]


@pytest.mark.asyncio
async def test_export_rows_pins_api_version_header():
    http = FakeHTTP(
        submit=_SUBMIT_OK,
        polls=[b'{"id": "job1", "status": "completed", "batches": [{"fileId": "file1"}]}'],
        downloads={"file1": b"Account.Id\n7\n"},
    )
    _ = [r async for r in _mgr(http).export_rows("SELECT Id FROM Account")]
    # submit + poll (request) and download (request_stream) all carry the pin
    assert http.request_headers  # submit + poll happened
    assert all(
        h.get("Zuora-Version") == ZUORA_API_VERSION for h in http.request_headers
    )
    assert http.stream_headers.get("Zuora-Version") == ZUORA_API_VERSION


# --- _run_job retry policy -----------------------------------------------------


@pytest.mark.asyncio
async def test_run_job_retries_export_error_then_raises():
    # Job keeps aborting -> ExportError -> retried MAX_RETRIES times.
    http = FakeHTTP(
        submit=_SUBMIT_OK,
        polls=[b'{"id": "job1", "status": "aborted"}'],
    )
    with pytest.raises(ExportError):
        await _mgr(http)._run_job("SELECT Id FROM Account")
    assert http.submit_calls == em.MAX_RETRIES


@pytest.mark.asyncio
async def test_run_job_does_not_retry_http_error():
    # A transport HTTPError isn't an ExportError, so it propagates after one try
    # (the HTTP layer already handled its own retries).
    http = FakeHTTP(submit=HTTPError("bad gateway", 502))
    with pytest.raises(HTTPError):
        await _mgr(http)._run_job("SELECT Id FROM Account")
    assert http.submit_calls == 1
