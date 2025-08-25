from typing import AsyncGenerator
import gzip
import pytest
from estuary_cdk.gunzip_stream import GunzipStream


async def bytes_gen(data: bytes) -> AsyncGenerator[bytes, None]:
    chunk_size = 10
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def create_gzip_content(content: bytes) -> bytes:
    return gzip.compress(content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content, expected",
    [
        (b"line1\nline2\nline3\n", b"line1\nline2\nline3\n"),
        (b"line1\nline2\nline3", b"line1\nline2\nline3"),
        (b"", b""),
        (b"single line without newline", b"single line without newline"),
        (
            b'{"id": 1, "value": "test1"}\n{"id": 2, "value": "test2"}\n',
            b'{"id": 1, "value": "test1"}\n{"id": 2, "value": "test2"}\n',
        ),
        (b"no newline at end", b"no newline at end"),
        (b"\n\nline with empty lines\n\n", b"\n\nline with empty lines\n\n"),
        (b"A" * 1000, b"A" * 1000),  # Test larger content
        (
            b"Mixed content with\nnewlines and\ttabs\rand\x00null bytes",
            b"Mixed content with\nnewlines and\ttabs\rand\x00null bytes",
        ),
    ],
)
async def test_gunzip_stream(content, expected):
    gzipped_bytes = create_gzip_content(content)

    actual = b""
    async for chunk in GunzipStream(bytes_gen(gzipped_bytes)):
        actual += chunk

    assert actual == expected


@pytest.mark.asyncio
async def test_gunzip_stream_empty_content():
    gzipped_bytes = create_gzip_content(b"")

    actual = b""
    async for chunk in GunzipStream(bytes_gen(gzipped_bytes)):
        actual += chunk

    assert actual == b""


@pytest.mark.asyncio
async def test_gunzip_stream_large_content():
    large_content = b"This is a test line.\n" * 1000
    gzipped_bytes = create_gzip_content(large_content)

    actual = b""
    chunk_count = 0
    async for chunk in GunzipStream(bytes_gen(gzipped_bytes)):
        actual += chunk
        chunk_count += 1

    assert actual == large_content
    assert chunk_count > 1  # Ensure we got multiple chunks


@pytest.mark.asyncio
async def test_gunzip_stream_single_byte_chunks():
    content = b"Hello, World!\nThis is a test.\n"
    gzipped_bytes = create_gzip_content(content)

    async def single_byte_gen(data: bytes) -> AsyncGenerator[bytes, None]:
        for byte in data:
            yield bytes([byte])

    actual = b""
    async for chunk in GunzipStream(single_byte_gen(gzipped_bytes)):
        actual += chunk

    assert actual == content
