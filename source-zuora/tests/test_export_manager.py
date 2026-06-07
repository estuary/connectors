"""Unit tests for ExportManager: submit/poll/download lifecycle, status handling
(including the `Canceled` one-l spelling), the 403 -> ExportTooLargeError
detection, CSV prefix stripping, and the retry-only-on-ExportError policy.

A FakeHTTP stands in for the CDK HTTPSession, routing by URL.
"""

import logging

import pytest

from estuary_cdk.http import HTTPError

from source_zuora import export_manager as em
from source_zuora.export_manager import ExportError, ExportManager, ExportTooLargeError
from source_zuora.shared import ZUORA_API_VERSION

_LOG = logging.getLogger(__name__)
_BASE = "https://rest.zuora.com"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    # Collapse polling/backoff delays so tests don't actually wait.
    monkeypatch.setattr(em, "POLL_INTERVAL", 0)
    monkeypatch.setattr(em, "RETRY_BACKOFF_BASE_SECONDS", 0)


class FakeHTTP:
    def __init__(self, *, submit=None, polls=None, download=None):
        # Each of submit/download may be bytes or an Exception (raised).
        # polls is a list of bytes/Exception; the last entry repeats.
        self.submit_response = submit
        self.poll_responses = list(polls or [])
        self.download = download
        self.submit_calls = 0
        self.poll_calls = 0
        self.request_headers: list = []
        self.stream_headers = None

    async def request(self, log, url, method=None, json=None, headers=None):
        self.request_headers.append(headers)
        if url.endswith("/v1/object/export") and method == "POST":
            self.submit_calls += 1
            return _resolve(self.submit_response)
        if "/v1/object/export/" in url:
            i = min(self.poll_calls, len(self.poll_responses) - 1)
            self.poll_calls += 1
            return _resolve(self.poll_responses[i])
        raise AssertionError(f"unexpected request URL: {url}")

    async def request_stream(self, log, url, headers=None):
        assert "/v1/files/" in url, url
        self.stream_headers = headers
        data = _resolve(self.download)

        async def body():
            yield data

        return {}, body


def _resolve(value):
    if isinstance(value, Exception):
        raise value
    return value


def _mgr(http) -> ExportManager:
    return ExportManager(http, _LOG, _BASE)


# --- _submit -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_returns_job_id_on_success():
    mgr = _mgr(FakeHTTP(submit=b'{"Success": true, "Id": "job1"}'))
    assert await mgr._submit("SELECT Id FROM Account") == "job1"


@pytest.mark.asyncio
async def test_submit_failure_raises_export_error_with_detail():
    http = FakeHTTP(
        submit=b'{"Success": false, "Errors": [{"Code": "C", "Message": "There is no field named X."}]}'
    )
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._submit("SELECT X FROM Account")
    assert "There is no field named X." in str(exc.value)


# --- _poll_until_complete ------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_completed_returns_file_id():
    http = FakeHTTP(polls=[b'{"Status": "Completed", "FileId": "file1"}'])
    assert await _mgr(http)._poll_until_complete("job1") == "file1"


@pytest.mark.asyncio
async def test_poll_canceled_raises_export_error():
    # Guards the "Canceled" (one-l) spelling fix: "Cancelled" would ValidationError.
    http = FakeHTTP(polls=[b'{"Status": "Canceled", "StatusReason": "aborted"}'])
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._poll_until_complete("job1")
    assert exc.value.status is em.ExportStatus.CANCELED
    assert exc.value.job_id == "job1"


@pytest.mark.asyncio
async def test_poll_failed_raises_export_error():
    http = FakeHTTP(polls=[b'{"Status": "Failed", "StatusReason": "boom"}'])
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._poll_until_complete("job1")
    assert exc.value.status is em.ExportStatus.FAILED


@pytest.mark.asyncio
async def test_poll_completed_without_file_id_raises():
    http = FakeHTTP(polls=[b'{"Status": "Completed"}'])
    with pytest.raises(ExportError):
        await _mgr(http)._poll_until_complete("job1")


@pytest.mark.asyncio
async def test_poll_pending_then_completed_loops():
    http = FakeHTTP(
        polls=[b'{"Status": "Pending"}', b'{"Status": "Completed", "FileId": "f"}']
    )
    assert await _mgr(http)._poll_until_complete("job1") == "f"
    assert http.poll_calls == 2


@pytest.mark.asyncio
async def test_poll_times_out(monkeypatch):
    monkeypatch.setattr(em, "MAX_POLL_ATTEMPTS", 3)
    http = FakeHTTP(polls=[b'{"Status": "Processing"}'])
    with pytest.raises(ExportError) as exc:
        await _mgr(http)._poll_until_complete("job1")
    assert "timed out" in str(exc.value)
    assert http.poll_calls == 3


# --- _fetch_results ------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_results_strips_object_prefix():
    http = FakeHTTP(download=b"Account.Id,Account.Name\n1,Acme\n")
    rows = [r async for r in _mgr(http)._fetch_results("file1")]
    assert rows == [{"Id": "1", "Name": "Acme"}]


@pytest.mark.asyncio
async def test_fetch_results_403_raises_too_large():
    http = FakeHTTP(download=HTTPError("forbidden", 403))
    with pytest.raises(ExportTooLargeError):
        [r async for r in _mgr(http)._fetch_results("file1")]


@pytest.mark.asyncio
async def test_fetch_results_other_http_error_propagates():
    http = FakeHTTP(download=HTTPError("server error", 500))
    with pytest.raises(HTTPError):
        [r async for r in _mgr(http)._fetch_results("file1")]


# --- export_rows (end to end) --------------------------------------------------


@pytest.mark.asyncio
async def test_export_rows_submit_poll_stream():
    http = FakeHTTP(
        submit=b'{"Success": true, "Id": "job1"}',
        polls=[b'{"Status": "Completed", "FileId": "file1"}'],
        download=b"Account.Id\n7\n",
    )
    rows = [r async for r in _mgr(http).export_rows("SELECT Id FROM Account")]
    assert rows == [{"Id": "7"}]


@pytest.mark.asyncio
async def test_export_rows_pins_api_version_header():
    http = FakeHTTP(
        submit=b'{"Success": true, "Id": "job1"}',
        polls=[b'{"Status": "Completed", "FileId": "file1"}'],
        download=b"Account.Id\n7\n",
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
    # Job keeps Failing -> ExportError -> retried MAX_RETRIES times.
    http = FakeHTTP(
        submit=b'{"Success": true, "Id": "job1"}',
        polls=[b'{"Status": "Failed"}'],
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
