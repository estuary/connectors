"""
Undo the HTTP content codings that aiohttp leaves on a response body.

aiohttp auto-decompresses only when `Content-Encoding` holds a single coding. A
chained value like `gzip,gzip` matches nothing in its parser, so the body
arrives with every layer still on it.
"""

from collections.abc import AsyncGenerator, AsyncIterable

from aiohttp import ClientRequest, hdrs
from aiohttp.compression_utils import (
    HAS_BROTLI,
    HAS_ZSTD,
    BrotliDecompressor,
    DecompressionBaseHandler,
    ZLibDecompressor,
    ZSTDDecompressor,
)
from aiohttp.web_response import ContentCoding

GZIP = ContentCoding.gzip.value
DEFLATE = ContentCoding.deflate.value
IDENTITY = ContentCoding.identity.value

# The codings aiohttp has already undone and we must not touch. Its parser
# matches the whole header value, so this only ever applies to a lone coding.
#
# Derived from the `Accept-Encoding` value aiohttp computes at import time,
# which advertises exactly the codings it can decode (`br`/`zstd` only when
# their libraries are installed).
AIOHTTP_AUTO_DECOMPRESSED = frozenset(
    coding.strip()
    for coding in ClientRequest.DEFAULT_HEADERS[hdrs.ACCEPT_ENCODING].split(",")
)

MAX_CODING_CHAIN_DEPTH = 4


class UnsupportedContentCoding(RuntimeError):
    """A response declared a coding we have no decoder for."""


class ContentCodingChainTooDeep(RuntimeError):
    """A response chained more codings than `MAX_CODING_CHAIN_DEPTH` allows."""


class TruncatedContentStream(RuntimeError):
    """A `deflate` stream ended before its end-of-stream marker. This is the
    truncation check aiohttp's `DeflateBuffer.feed_eof` performs, and the only
    one this module performs: gzip streams go unchecked, exactly as they do in
    aiohttp."""


def parse_content_codings(header_value: str) -> list[str]:
    """
    The codings still on the body, in the order they must be undone.
    `Content-Encoding` lists codings in the order they were applied, so undoing
    them runs back to front.

    Raises `ContentCodingChainTooDeep` for a chain deeper than
    `MAX_CODING_CHAIN_DEPTH`."""

    header_value = header_value.strip().lower()

    if not header_value or header_value in AIOHTTP_AUTO_DECOMPRESSED:
        return []

    codings = [coding.strip().lower() for coding in header_value.split(",")]
    remaining = [
        coding for coding in reversed(codings) if coding and coding != IDENTITY
    ]

    if len(remaining) > MAX_CODING_CHAIN_DEPTH:
        raise ContentCodingChainTooDeep(
            (
                f"Content-Encoding {header_value!r} chains more than "
                f"{MAX_CODING_CHAIN_DEPTH} codings."
            )
        )

    return remaining


def _decompressor(coding: str) -> DecompressionBaseHandler:
    """
    aiohttp's decompressor for one canonical coding. This is the dispatch
    `DeflateBuffer.__init__` performs.
    """
    if coding == "br":
        if not HAS_BROTLI:
            raise UnsupportedContentCoding(
                "Cannot decode content coding 'br'. Please install `Brotli`."
            )
        return BrotliDecompressor()

    if coding == "zstd":
        if not HAS_ZSTD:
            raise UnsupportedContentCoding(
                "Cannot decode content coding 'zstd'. Please install `backports.zstd`."
            )
        return ZSTDDecompressor()

    if coding in (GZIP, DEFLATE):
        return ZLibDecompressor(encoding=coding)

    raise UnsupportedContentCoding(f"Cannot decode content coding {coding!r}.")


class StreamDecompressor:
    """
    Incrementally undo one content coding over a stream of bytes.

    Usage:
      async for chunk in StreamDecompressor(async_bytes, "gzip"):
          ... # process decompressed bytes
    """

    input: AsyncIterable[bytes]
    coding: str
    decompressor: DecompressionBaseHandler
    _input_iter: AsyncGenerator[bytes, None] | None

    def __init__(self, input: AsyncIterable[bytes], coding: str = "gzip"):
        self.input = input
        # Normalized once here, so `coding` is directly comparable everywhere
        self.coding = coding.strip().lower()
        self.decompressor = _decompressor(self.coding)
        self._input_iter = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._input_iter is None:
            self._input_iter = self._stream()

        return await anext(self._input_iter)

    async def _stream(self) -> AsyncGenerator[bytes, None]:
        is_first_chunk = True

        async for chunk in self.input:
            if not chunk:
                continue

            if is_first_chunk:
                is_first_chunk = False

                self._maybe_switch_to_raw_deflate(chunk)

            data = await self.decompressor.decompress(chunk)
            if data:
                yield data

        # Flush any buffered data at the end of the stream.
        leftover = self.decompressor.flush()
        if leftover:
            yield leftover

        # Mirrors `DeflateBuffer.feed_eof`: deflate is the one coding aiohttp
        # checks for a missing end-of-stream marker, gated on any input having
        # been consumed.
        if (
            not is_first_chunk
            and self.coding == DEFLATE
            and isinstance(self.decompressor, ZLibDecompressor)
            and not self.decompressor.eof
        ):
            raise TruncatedContentStream(
                (
                    f"Content coding {self.coding!r} stream ended before its "
                    "end-of-stream marker."
                )
            )

    async def aclose(self) -> None:
        if self._input_iter is not None:
            await self._input_iter.aclose()

        input_aclose = getattr(self.input, "aclose", None)
        if input_aclose is not None:
            await input_aclose()

    def _maybe_switch_to_raw_deflate(self, first_chunk: bytes) -> None:
        """
        Mirrors aiohttp's own fallback for non-compliant `deflate` bodies.

        RFC 1950's first byte carries the compression method in its low nibble,
        always 8 for a real zlib header. Anything else means the server sent
        bare RFC 1951 data under the `deflate` coding, which is common enough
        that aiohttp carries this same switch.
        """
        if self.coding == DEFLATE and first_chunk[0] & 0x0F != 8:
            self.decompressor = ZLibDecompressor(
                encoding=DEFLATE, suppress_deflate_header=True
            )


def decompress_stream(
    input: AsyncIterable[bytes], codings: list[str]
) -> AsyncIterable[bytes]:
    """
    Undo `codings` over a stream of bytes, in the order given.

    The one place a coding chain becomes a decoder stack: `decompress` and the
    streaming response path both decode through here, so a chained body decodes
    identically whether it was streamed or read whole.
    """
    for coding in codings:
        input = StreamDecompressor(input, coding)
    return input


async def decompress(body: bytes, codings: list[str]) -> bytes:
    """
    Undo `codings` over a body already read whole, in the order given.

    Error responses are the only bodies read whole.
    """

    async def single_chunk() -> AsyncGenerator[bytes, None]:
        yield body

    stream = decompress_stream(single_chunk(), codings)
    chunks: list[bytes] = []
    try:
        async for chunk in stream:
            chunks.append(chunk)
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            await aclose()

    return b"".join(chunks)
