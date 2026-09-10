import gzip
import logging

import pytest
from multidict import CIMultiDict, CIMultiDictProxy

from estuary_cdk.http import format_error_body, read_error_body

SMARTSHEET_ERROR = b'{"errorCode":1006,"message":"Not Found","refId":"abc123"}'


class FakeResponse:
    """The surface `read_error_body` touches."""

    def __init__(self, body: bytes, content_encoding: str | None = None) -> None:
        self._body: bytes = body
        raw: CIMultiDict[str] = CIMultiDict()
        if content_encoding is not None:
            raw["Content-Encoding"] = content_encoding
        self.headers: CIMultiDictProxy[str] = CIMultiDictProxy(raw)

    async def read(self) -> bytes:
        return self._body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body, content_encoding",
    [
        # Uncompressed, and the single-coding case aiohttp already decoded.
        (SMARTSHEET_ERROR, None),
        (SMARTSHEET_ERROR, "gzip"),
        (SMARTSHEET_ERROR, "identity"),
        # A chained coding aiohttp's parser skips, so the error body reaches us
        # gzipped twice.
        (gzip.compress(gzip.compress(SMARTSHEET_ERROR)), "gzip,gzip"),
        (gzip.compress(gzip.compress(SMARTSHEET_ERROR)), "gzip, gzip"),
    ],
)
async def test_recovers_the_providers_error_message(
    body: bytes, content_encoding: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    log = logging.getLogger(__name__)

    with caplog.at_level(logging.WARNING):
        decoded = await read_error_body(log, FakeResponse(body, content_encoding))  # type: ignore[arg-type]

    assert format_error_body(decoded) == SMARTSHEET_ERROR.decode()
    assert not caplog.records


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body, content_encoding",
    [
        # Truncated mid-stream.
        (gzip.compress(SMARTSHEET_ERROR)[:20], "gzip,gzip"),
        # Header claims a coding the body doesn't actually carry.
        (SMARTSHEET_ERROR, "gzip,gzip"),
        # Header claims a coding we have no decoder for.
        (SMARTSHEET_ERROR, "br,gzip"),
        # Header chains more codings than MAX_CODING_CHAIN_DEPTH allows.
        (SMARTSHEET_ERROR, "gzip,gzip,gzip,gzip,gzip"),
    ],
)
async def test_reports_rather_than_raises_when_decoding_fails(
    body: bytes, content_encoding: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A secondary failure here would destroy the HTTP error being reported,
    so an undecodable body is passed through instead -- with a warning, so the
    mangled body in the resulting error message is explained somewhere."""
    log = logging.getLogger(__name__)

    with caplog.at_level(logging.WARNING):
        decoded = await read_error_body(log, FakeResponse(body, content_encoding))  # type: ignore[arg-type]

    assert decoded == body
    assert format_error_body(decoded)
    assert any(
        "could not undo content codings" in record.getMessage()
        for record in caplog.records
    )
