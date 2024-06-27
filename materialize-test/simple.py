import asyncio
import fileinput
import sys
from timeit import timeit
from typing import Any, AsyncGenerator, NotRequired, Optional, TypedDict
from pydantic import BaseModel, SkipValidation, TypeAdapter

import msgspec
import orjson




class Load(msgspec.Struct, forbid_unknown_fields=True):
    keyPacked: str
    binding: int | None = 0
    keyJson: tuple[Any, ...] = ()  # calculated in the connector, for now


class Flush(msgspec.Struct, forbid_unknown_fields=True):
    pass


class Store(msgspec.Struct, forbid_unknown_fields=True):
    binding: int | None = 0
    keyPacked: str | None = None
    valuesPacked: str | None = None
    doc: dict[Any, Any] | None = None  # TODO: Can we this as bytes instead?
    exists: bool | None = False
    delete: bool | None = False
    valuesJson: tuple[Any, ...] = ()  # calculated in the connector, for now
    keyJson: tuple[Any, ...] = ()  # calculated in the connector, for now


class StartCommit(msgspec.Struct, forbid_unknown_fields=True):
    runtimeCheckpoint: dict[Any, Any]


class Acknowledge(msgspec.Struct, forbid_unknown_fields=True):
    pass


class TransactorRequest(msgspec.Struct, forbid_unknown_fields=True):
    load: Load | None = None
    flush: Flush | None = None
    store: Store | None = None
    startCommit: StartCommit | None = None
    acknowledge: Acknowledge | None = None

class LoadPyd(BaseModel):
    keyPacked: str
    binding: int | None = 0
    keyJson: tuple[Any, ...] = ()  # calculated in the connector, for now


class FlushPyd(BaseModel):
    pass


class StorePyd(BaseModel):
    binding: SkipValidation[int | None] = 0
    keyPacked: SkipValidation[str | None] = None
    valuesPacked: SkipValidation[str | None] = None
    doc: SkipValidation[dict[Any, Any] | None] = None  # TODO: Can we this as bytes instead?
    exists: SkipValidation[bool | None] = False
    delete: SkipValidation[bool | None] = False
    valuesJson: SkipValidation[tuple[Any, ...]] = ()  # calculated in the connector, for now
    keyJson: SkipValidation[tuple[Any, ...]] = ()  # calculated in the connector, for now


class StartCommitPyd(BaseModel):
    runtimeCheckpoint: dict[Any, Any]


class AcknowledgePyd(BaseModel):
    pass

class StorePydDict(TypedDict):
    binding: int | None
    keyPacked: str | None
    valuesPacked: str | None
    doc: dict[Any, Any] | None # TODO: Can we this as bytes instead?
    exists: bool | None
    delete: bool | None
    valuesJson: tuple[Any, ...]   # calculated in the connector, for now
    keyJson: tuple[Any, ...]   # calculated in the connector, for now



class TransactorRequestPyd(BaseModel):
    load: SkipValidation[LoadPyd | None] = None
    flush: FlushPyd | None = None
    store: SkipValidation[StorePyd | None] = None
    startCommit: StartCommitPyd | None = None
    acknowledge: AcknowledgePyd | None = None


class LoadPydDict(TypedDict):
    keyPacked: str
    binding: int | None = 0
    keyJson: tuple[Any, ...] = ()  # calculated in the connector, for now


class FlushPydDict(TypedDict):
    pass


class StartCommitPydDict(TypedDict):
    runtimeCheckpoint: dict[Any, Any]


class AcknowledgePydDict(TypedDict):
    pass


class TransactorRequestPydDict(TypedDict):
    store: SkipValidation[StorePydDict | None]



async def stdin_jsonl() -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=1 << 27)  # 128 MiB.
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while line := await reader.readline():
        yield line

async def msgspec_like_normal():
    inp = stdin_jsonl()

    async def other_gen() -> AsyncGenerator[bytes, None]:
        async for line in inp:
            yield line

    other = other_gen()

    decoder = msgspec.json.Decoder(TransactorRequest)

    async for line in other:
        req = decoder.decode(line)

async def msgspec_std_in():
    decoder = msgspec.json.Decoder(TransactorRequest)

    for line in fileinput.input():
        req = decoder.decode(line)

async def orjson_like_normal():
    inp = stdin_jsonl()

    async def other_gen() -> AsyncGenerator[bytes, None]:
        async for line in inp:
            yield line

    other = other_gen()

    async for line in other:
        req = orjson.loads(line)

async def orjson_std_in():
    for line in fileinput.input():
        req = orjson.loads(line)

async def pydantic_like_normal():
    inp = stdin_jsonl()

    async def other_gen() -> AsyncGenerator[bytes, None]:
        async for line in inp:
            yield line

    other = other_gen()

    async for line in other:
        req = TransactorRequestPyd.model_validate_json(line)

async def pydantic_std_in():
    for line in fileinput.input():
        req = TransactorRequestPyd.model_validate_json(line)

async def pydantic_validate_python():
    inp = stdin_jsonl()

    async def other_gen() -> AsyncGenerator[bytes, None]:
        async for line in inp:
            yield line

    other = other_gen()

    # thing = TypeAdapter(TransactorRequestPydDict)

    async for line in other:
        # req = thing.validate_python(orjson.loads(line))
        # req = TransactorRequestPyd.model_construct(**orjson.loads(line))
        req = TransactorRequestPyd.model_validate(orjson.loads(line))

def runit():
    # asyncio.run(msgspec_like_normal()) # 265122 RPS
    # asyncio.run(orjson_like_normal()) # 258480 RPS
    # asyncio.run(pydantic_like_normal()) # 83913 RPS
    
    asyncio.run(pydantic_validate_python()) # 160423 RPS
    # with SkipValidation this was 174359 RPS
    # model construct was 173800 RPS
    # SkipValidation with model_construct was 210000 RPS

    # asyncio.run(pydantic_std_in()) # 84932 RPS
    # asyncio.run(orjson_std_in()) # 318809 RPS
    # asyncio.run(msgspec_std_in()) # 328987 RPS


elapsed = timeit(runit, number=1)
print(f"Elapsed time: {elapsed} seconds")
print(f"Rows per second: {2000004 / elapsed}")