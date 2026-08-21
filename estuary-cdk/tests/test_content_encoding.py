import gzip
import zlib
from collections.abc import AsyncGenerator, AsyncIterable

import pytest

from estuary_cdk.content_encoding import (
    ContentCodingChainTooDeep,
    StreamDecompressor,
    TruncatedContentStream,
    UnsupportedContentCoding,
    decompress,
    decompress_stream,
    parse_content_codings,
)
from estuary_cdk.gunzip_stream import GunzipStream

PAYLOAD = b'{"totalRowCount": 2, "rows": [{"id": 1}, {"id": 2}]}'


@pytest.mark.parametrize(
    "header_value, expected",
    [
        # Nothing to undo
        ("", []),
        ("   ", []),
        ("identity", []),
        # A lone coding aiohttp claims: not ours either way
        ("gzip", []),
        (" gzip ", []),
        ("deflate", []),
        # Claimed case-insensitively, then decoded with the original casing —
        # so aiohttp fails the request and the body never reaches us
        ("GZIP", []),
        # Chains: aiohttp's parser skips them wholesale.
        ("gzip,gzip", ["gzip", "gzip"]),
        ("GZIP,Gzip", ["gzip", "gzip"]),
        ("gzip,gzip,gzip", ["gzip", "gzip", "gzip"]),
        # Codings are listed as applied, so they're undone in reverse
        ("gzip, deflate", ["deflate", "gzip"]),
        ("deflate, gzip", ["gzip", "deflate"]),
        # `identity` is a no-op
        ("gzip, identity", ["gzip"]),
        ("identity, gzip", ["gzip"]),
        ("identity, identity", []),
        # Codings we can't undo still parse; the decoders are what reject them.
        ("gzip, br", ["br", "gzip"]),
    ],
)
def test_parses_what_is_left_to_undo(header_value: str, expected: list[str]) -> None:
    assert parse_content_codings(header_value) == expected


async def chunked(data: bytes) -> AsyncGenerator[bytes, None]:
    """Chunk finely enough that each coding must be undone incrementally"""

    for i in range(0, len(data), 8):
        yield data[i : i + 8]


async def drain(stream: AsyncIterable[bytes]) -> bytes:
    return b"".join([chunk async for chunk in stream])


def apply_codings(body: bytes, header_value: str) -> bytes:
    for coding in header_value.split(","):
        match coding.strip().lower():
            case "gzip":
                body = gzip.compress(body)
            case "deflate":
                body = zlib.compress(body)
            case "identity":
                pass
            case other:
                raise AssertionError(f"test cannot apply coding {other!r}")
    return body


ROUND_TRIPS = [
    "gzip",
    "deflate",
    "gzip,gzip",
    "gzip,gzip,gzip,gzip",  # Exactly MAX_CODING_CHAIN_DEPTH.
    "gzip,deflate",
    "deflate,gzip",
    "gzip,identity,gzip",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("header_value", ROUND_TRIPS)
async def test_stream_round_trips_a_chain(header_value: str) -> None:
    body = apply_codings(PAYLOAD, header_value)
    codings = parse_content_codings(header_value) or [header_value]

    assert await drain(decompress_stream(chunked(body), codings)) == PAYLOAD


@pytest.mark.asyncio
@pytest.mark.parametrize("header_value", ROUND_TRIPS)
async def test_bytes_round_trip_a_chain(header_value: str) -> None:
    body = apply_codings(PAYLOAD, header_value)
    codings = parse_content_codings(header_value) or [header_value]

    assert await decompress(body, codings) == PAYLOAD


@pytest.mark.asyncio
async def test_raw_deflate_is_decoded_despite_the_missing_zlib_header() -> None:
    """Servers commonly send bare RFC 1951 data under the `deflate` coding."""
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    raw = compressor.compress(PAYLOAD) + compressor.flush()

    assert await decompress(raw, ["deflate"]) == PAYLOAD


@pytest.mark.asyncio
async def test_raw_deflate_is_decoded_when_streamed() -> None:
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    raw = compressor.compress(PAYLOAD) + compressor.flush()

    assert await drain(StreamDecompressor(chunked(raw), "deflate")) == PAYLOAD


@pytest.mark.asyncio
async def test_truncated_deflate_raises_rather_than_yield_partial_data() -> None:
    """Truncated zlib data decodes without error; only the missing
    end-of-stream marker reveals the loss. deflate is the one coding aiohttp
    checks it for, and this module matches that."""
    truncated = zlib.compress(PAYLOAD)[:20]

    with pytest.raises(TruncatedContentStream):
        _ = await decompress(truncated, ["deflate"])

    with pytest.raises(TruncatedContentStream):
        _ = await drain(decompress_stream(chunked(truncated), ["deflate"]))


@pytest.mark.asyncio
async def test_truncated_gzip_yields_what_decoded() -> None:
    """aiohttp performs no end-of-stream check for gzip, and neither does this
    module: a truncated gzip stream yields whatever decoded, without error."""
    truncated = gzip.compress(PAYLOAD)[:20]

    assert PAYLOAD.startswith(await decompress(truncated, ["gzip"]))


@pytest.mark.asyncio
async def test_empty_body_is_not_mistaken_for_a_truncated_one() -> None:
    """The deflate check is gated on input having been consumed, mirroring
    aiohttp's `size > 0` gate."""
    assert await decompress(b"", ["deflate"]) == b""


@pytest.mark.asyncio
async def test_bytes_past_the_gzip_stream_end_are_ignored() -> None:
    """zlib hoards post-stream input in `unused_data` with no error, and
    aiohttp never inspects it -- so trailing bytes are dropped. This module
    matches that behavior."""
    body = gzip.compress(PAYLOAD) + b"trailing garbage"

    assert await decompress(body, ["gzip"]) == PAYLOAD


@pytest.mark.asyncio
@pytest.mark.parametrize("coding", ["compress", "x-gzip", "bogus"])
async def test_undecodable_codings_raise_rather_than_pass_bytes_through(
    coding: str,
) -> None:
    with pytest.raises(UnsupportedContentCoding):
        _ = await decompress(gzip.compress(PAYLOAD), [coding])

    with pytest.raises(UnsupportedContentCoding):
        _ = StreamDecompressor(chunked(b""), coding)


@pytest.mark.asyncio
async def test_gunzip_stream_still_decodes_a_payload_no_header_describes() -> None:
    """`GunzipStream` is for gzip data that *is* the payload -- a downloaded
    `.gz` report -- which no `Content-Encoding` covers."""
    assert parse_content_codings("") == []
    assert await drain(GunzipStream(chunked(gzip.compress(PAYLOAD)))) == PAYLOAD


def test_chains_deeper_than_the_cap_are_rejected() -> None:
    """`identity` stacks nothing and so doesn't count against the cap."""
    assert len(parse_content_codings("gzip," * 4 + "identity")) == 4

    with pytest.raises(ContentCodingChainTooDeep):
        _ = parse_content_codings("gzip," * 5 + "identity")


@pytest.mark.asyncio
@pytest.mark.parametrize("coding", ["gzip", "GZIP", " gzip "])
async def test_stream_normalizes_its_coding(coding: str) -> None:
    """Casing and whitespace are normalized at construction, so every
    comparison downstream -- including the raw-deflate check -- sees the
    canonical spelling."""
    stream = StreamDecompressor(chunked(gzip.compress(PAYLOAD)), coding)

    assert stream.coding == "gzip"
    assert await drain(stream) == PAYLOAD
