from typing import AsyncGenerator

import pytest
from pydantic import BaseModel, TypeAdapter

from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor


async def bytes_gen(data: bytes) -> AsyncGenerator[bytes, None]:
    chunk_size = 10
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


class SimpleMeta(BaseModel):
    count: int
    total: int
    next_page: str | None = None


class SimpleRecord(BaseModel):
    id: int
    value: str


class ComplexMeta(BaseModel):
    class Cursor(BaseModel):
        after: str

    class Paging(BaseModel):
        next: "ComplexMeta.Cursor"

    paging: Paging
    total: int


class ComplexRecord(BaseModel):
    class Properties(BaseModel):
        class Timestamp(BaseModel):
            value: str

        lastmodifieddate: Timestamp

    vid: int
    properties: Properties


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "input_data, prefix, record_cls, remainder_cls, want_records, want_remainder",
    [
        (
            b"""{
                "count": 2,
                "total": 10,
                "next_page": "asdf",
                "records": []
            }""",
            "records.item",
            SimpleRecord,
            SimpleMeta,
            [],
            SimpleMeta(count=2, total=10, next_page="asdf"),
        ),
        (
            b"""{
                "count": 0,
                "total": 0,
                "records": []
            }""",
            "records.item",
            SimpleRecord,
            SimpleMeta,
            [],
            SimpleMeta(count=0, total=0, next_page=None),
        ),
        (
            b"""{
                "count": 2,
                "total": 10,
                "next_page": "asdf",
                "records": [
                    {"id": 1, "value": "test1"},
                    {"id": 2, "value": "test2"}
                ]
            }""",
            "records.item",
            SimpleRecord,
            SimpleMeta,
            [
                SimpleRecord(id=1, value="test1"),
                SimpleRecord(id=2, value="test2"),
            ],
            SimpleMeta(count=2, total=10, next_page="asdf"),
        ),
        (
            b"""{
                "count": 2,
                "total": 10,
                "next_page": "asdf"
            }""",
            "records.item",
            SimpleRecord,
            SimpleMeta,
            [],
            SimpleMeta(count=2, total=10, next_page="asdf"),
        ),
        (
            # Repeated fields - not likely to encounter this in the wild, but it
            # would technically work. Notice how the records items across both
            # matching fields are returned, but in the remainder only the "last"
            # value for the repeated field `total` is present.
            b"""{
                "count": 2,
                "records": [
                    {"id": 1, "value": "test1"}
                ],
                "total": 10,
                "records": [
                    {"id": 2, "value": "test2"}
                ],
                "next_page": "asdf",
                "total": 20
            }""",
            "records.item",
            SimpleRecord,
            SimpleMeta,
            [
                SimpleRecord(id=1, value="test1"),
                SimpleRecord(id=2, value="test2"),
            ],
            SimpleMeta(count=2, total=20, next_page="asdf"),
        ),
        (
            b"""{
                "paging": {
                    "next": {
                        "after": "asdf"
                    }
                },
                "total": 10,
                "result": {
                    "records": [
                        {
                            "vid": 1,
                            "properties": {
                                "lastmodifieddate": {
                                    "value": "2021-01-01T00:00:00.000Z"
                                }
                            }
                        },
                        {
                            "vid": 2,
                            "properties": {
                                "lastmodifieddate": {
                                    "value": "2021-01-02T00:00:00.000Z"
                                }
                            }
                        }
                    ]
                }
            }""",
            "result.records.item",
            ComplexRecord,
            ComplexMeta,
            [
                ComplexRecord(
                    vid=1,
                    properties=ComplexRecord.Properties(
                        lastmodifieddate=ComplexRecord.Properties.Timestamp(
                            value="2021-01-01T00:00:00.000Z"
                        )
                    ),
                ),
                ComplexRecord(
                    vid=2,
                    properties=ComplexRecord.Properties(
                        lastmodifieddate=ComplexRecord.Properties.Timestamp(
                            value="2021-01-02T00:00:00.000Z"
                        )
                    ),
                ),
            ],
            ComplexMeta(
                paging=ComplexMeta.Paging(next=ComplexMeta.Cursor(after="asdf")),
                total=10,
            ),
        ),
        (
            # Works with more than just array items.
            b"""{
                "count": 2,
                "total": 10,
                "next_page": "asdf",
                "obj": {
                    "nested": {"id": 1, "value": "test1"}
                },
                "obj": {
                    "nested": {"id": 2, "value": "test2"}
                }
            }""",
            "obj.nested",
            SimpleRecord,
            SimpleMeta,
            [
                SimpleRecord(id=1, value="test1"),
                SimpleRecord(id=2, value="test2"),
            ],
            SimpleMeta(count=2, total=10, next_page="asdf"),
        ),
        # Arrays of objects.
        (
            b"""[
                {"id": 1, "value": "test1"},
                {"id": 2, "value": "test2"}
            ]""",
            "item",
            SimpleRecord,
            None,
            [
                SimpleRecord(id=1, value="test1"),
                SimpleRecord(id=2, value="test2"),
            ],
            None,
        ),
        (
            b"""[
                {"record": {"id": 1, "value": "test1"}},
                {"blah": {"something": "else"}},
                {"record": {"id": 2, "value": "test2"}}
            ]""",
            "item.record",
            SimpleRecord,
            None,
            [
                SimpleRecord(id=1, value="test1"),
                SimpleRecord(id=2, value="test2"),
            ],
            None,
        ),
    ],
)
async def test_incremental_json_processor(
    input_data, prefix, record_cls, remainder_cls, want_records, want_remainder
):
    if remainder_cls:
        processor = IncrementalJsonProcessor(
            bytes_gen(input_data), prefix, record_cls, remainder_cls
        )
    else:
        processor = IncrementalJsonProcessor(bytes_gen(input_data), prefix, record_cls)

    got = []
    async for record in processor:
        got.append(record)

    assert want_records == got
    if remainder_cls:
        assert want_remainder == processor.get_remainder()
