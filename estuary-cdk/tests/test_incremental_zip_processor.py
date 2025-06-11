from typing import AsyncGenerator
import io
import zipfile

import pytest

from estuary_cdk.incremental_zip_processor import IncrementalZipProcessor


async def bytes_gen(data: bytes) -> AsyncGenerator[bytes, None]:
    chunk_size = 10
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def create_zip_with_content(files: dict[str, bytes]) -> bytes:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename, content in files.items():
            zip_file.writestr(filename, content)
    return zip_buffer.getvalue()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "zip_content, expected_lines",
    [
        (
            {"file1.txt": b"line1\nline2\nline3\n"},
            [b"line1", b"line2", b"line3"],
        ),
        (
            {"file1.txt": b"line1\nline2\nline3"},
            [b"line1", b"line2", b"line3"],
        ),
        (
            {"file1.txt": b""},
            [],
        ),
        (
            {
                "file1.txt": b"first file line 1\nfirst file line 2\n",
                "file2.txt": b"second file line 1\nsecond file line 2\n",
            },
            [
                b"first file line 1",
                b"first file line 2",
                b"second file line 1",
                b"second file line 2",
            ],
        ),
        (
            {"file1.jsonl": b'{"id": 1, "value": "test1"}\n{"id": 2, "value": "test2"}\n'},
            [b'{"id": 1, "value": "test1"}', b'{"id": 2, "value": "test2"}'],
        ),
        (
            {"file1.txt": b"no newline at end"},
            [b"no newline at end"],
        ),
        (
            {"file1.txt": b"\n\nline with empty lines\n\n"},
            [b"", b"", b"line with empty lines", b""],
        ),
        (
            {
                "dir1/file1.txt": b"nested file content\n",
                "dir2/file2.txt": b"another nested file\n",
            },
            [b"nested file content", b"another nested file"],
        ),
    ],
)
async def test_incremental_zip_processor(zip_content, expected_lines):
    zip_bytes = create_zip_with_content(zip_content)
    
    processor = IncrementalZipProcessor(bytes_gen(zip_bytes))
    
    lines = []
    async for line in processor:
        lines.append(line)
    
    assert lines == expected_lines
    assert processor._done is True


@pytest.mark.asyncio
async def test_incremental_zip_processor_with_large_lines():
    large_line = b"x" * 1000
    zip_content = {
        "file1.txt": large_line + b"\n" + b"small line\n" + large_line + b"\n"
    }
    
    zip_bytes = create_zip_with_content(zip_content)
    processor = IncrementalZipProcessor(bytes_gen(zip_bytes), chunk_size=100)
    
    lines = []
    async for line in processor:
        lines.append(line)
    
    assert lines == [large_line, b"small line", large_line]


@pytest.mark.asyncio
async def test_incremental_zip_processor_empty_zip():
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w'):
        pass
    
    processor = IncrementalZipProcessor(bytes_gen(zip_buffer.getvalue()))
    
    lines = []
    async for line in processor:
        lines.append(line)
    
    assert lines == []


@pytest.mark.asyncio
async def test_incremental_zip_processor_with_json_parsing():
    import json
    from pydantic import BaseModel
    
    class Record(BaseModel):
        id: int
        value: str
    
    zip_content = {
        "data.jsonl": b'{"id": 1, "value": "test1"}\n{"id": 2, "value": "test2"}\n'
    }
    
    zip_bytes = create_zip_with_content(zip_content)
    
    records = []
    async for line in IncrementalZipProcessor(bytes_gen(zip_bytes)):
        data = json.loads(line)
        record = Record.model_validate(data)
        records.append(record)
    
    assert len(records) == 2
    assert records[0].id == 1
    assert records[0].value == "test1"
    assert records[1].id == 2
    assert records[1].value == "test2"