
from logging import Logger
from typing import Awaitable, Callable
from estuary_cdk.materialize import response
from pydantic import BaseModel


class ExistingColumn(BaseModel):
    name: str
    nullable: bool
    type: str
    character_max_length: int | None = None
    has_default: bool = False


class InformationSchema(BaseModel):
    class Table(BaseModel):
        columns: dict[str, list[ExistingColumn]]

    namespaces: dict[str, Table]

FetchColumnsFn = Callable[
    [Logger],
    Awaitable[list[tuple[str | None, str, ExistingColumn]]],
]

async def Validate(
    log: Logger,
    fetch_columns: FetchColumnsFn,

) -> response.Validated:
    got = await fetch_columns(log)