from typing import AsyncGenerator
import io
import zipfile
import pytest
from estuary_cdk.unzip_stream import UnzipStream


async def bytes_gen(data: bytes) -> AsyncGenerator[bytes, None]:
    chunk_size = 10
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def create_zip_with_content(files: dict[str, bytes]) -> bytes:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for filename, content in files.items():
            zip_file.writestr(filename, content)
    return zip_buffer.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "zip_content, expected_files_lines",
    [
        (
            {"file1.txt": b"line1\nline2\nline3\n"},
            {"file1.txt": [b"line1", b"line2", b"line3"]},
        ),
        (
            {"file1.txt": b"line1\nline2\nline3"},
            {"file1.txt": [b"line1", b"line2", b"line3"]},
        ),
        (
            {"file1.txt": b""},
            {"file1.txt": []},
        ),
        (
            {
                "file1.txt": b"first file line 1\nfirst file line 2\n",
                "file2.txt": b"second file line 1\nsecond file line 2\n",
            },
            {
                "file1.txt": [b"first file line 1", b"first file line 2"],
                "file2.txt": [b"second file line 1", b"second file line 2"],
            },
        ),
        (
            {
                "file1.jsonl": b'{"id": 1, "value": "test1"}\n{"id": 2, "value": "test2"}\n'
            },
            {
                "file1.jsonl": [
                    b'{"id": 1, "value": "test1"}',
                    b'{"id": 2, "value": "test2"}',
                ]
            },
        ),
        (
            {"file1.txt": b"no newline at end"},
            {"file1.txt": [b"no newline at end"]},
        ),
        (
            {"file1.txt": b"\n\nline with empty lines\n\n"},
            {"file1.txt": [b"", b"", b"line with empty lines", b""]},
        ),
        (
            {
                "dir1/file1.txt": b"nested file content\n",
                "dir2/file2.txt": b"another nested file\n",
            },
            {
                "dir1/file1.txt": [b"nested file content"],
                "dir2/file2.txt": [b"another nested file"],
            },
        ),
    ],
)
async def test_unzip_stream(zip_content, expected_files_lines):
    zip_bytes = create_zip_with_content(zip_content)
    expected = b""
    for filename, lines in expected_files_lines.items():
        joined = b"\n".join(lines)
        # Add trailing newline if the original file content ends with a newline
        if zip_content[filename].endswith(b"\n"):
            joined += b"\n"
        expected += joined
    actual = b""
    async for chunk in UnzipStream(bytes_gen(zip_bytes)):
        actual += chunk
    assert actual == expected


@pytest.mark.asyncio
async def test_unzip_stream_empty_zip():
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w"):
        pass
    files = [f async for f in UnzipStream(bytes_gen(zip_buffer.getvalue()))]
    assert files == []
