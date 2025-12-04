from collections.abc import AsyncGenerator
from typing import Callable, TypeVar

from pydantic import AwareDatetime

from source_zoho.models import ZohoModule

T = TypeVar("T", bound=ZohoModule)
U = TypeVar("U")


async def areduce(
    fn: Callable[[U, T], U],
    agen: AsyncGenerator[T, None],
    initial: U,
) -> U:
    acc = initial

    async for item in agen:
        acc = fn(acc, item)

    return acc


async def achain(*agens: AsyncGenerator[T, None]) -> AsyncGenerator[T, None]:
    for agen in agens:
        async for item in agen:
            yield item


def dt_to_ts(dt: AwareDatetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")
