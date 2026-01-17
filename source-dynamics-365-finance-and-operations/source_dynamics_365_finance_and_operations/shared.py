import logging
from datetime import datetime
from logging import Logger
from typing import Any, Callable, Protocol, TypeVar, runtime_checkable

T_co = TypeVar('T_co', covariant=True)


class CacheInfoProtocol(Protocol):
    """Protocol for cache statistics returned by alru_cache.cache_info()."""
    @property
    def hits(self) -> int: ...
    @property
    def misses(self) -> int: ...
    @property
    def maxsize(self) -> int | None: ...
    @property
    def currsize(self) -> int: ...


@runtime_checkable
class CachedAsyncFunc(Protocol[T_co]):
    """Protocol for an alru_cache decorated async function."""
    __wrapped__: Callable[..., Any]
    def cache_info(self) -> CacheInfoProtocol: ...
    async def __call__(self, *args: Any, **kwargs: Any) -> T_co: ...


async def call_with_cache_logging(
    cached_func: CachedAsyncFunc[T_co],
    log: Logger,
    *args: Any,
    **kwargs: Any
) -> T_co:
    """Call a cached function and log on cache miss."""
    if not log.isEnabledFor(logging.DEBUG):
        return await cached_func(*args, **kwargs)

    info_before = cached_func.cache_info()
    result = await cached_func(*args, **kwargs)
    info_after = cached_func.cache_info()

    if info_after.misses > info_before.misses:
        log.debug(f"Cache miss: {cached_func.__wrapped__.__name__}", {
            "hits": info_after.hits,
            "misses": info_after.misses,
            "currsize": info_after.currsize,
            "maxsize": info_after.maxsize,
        })

    return result


def str_to_dt(string: str) -> datetime:
    normalized = string.replace('.', ':')
    return datetime.fromisoformat(normalized)


def is_datetime_format(s: str) -> bool:
    try:
        str_to_dt(s)
        return True
    except ValueError:
        return False
