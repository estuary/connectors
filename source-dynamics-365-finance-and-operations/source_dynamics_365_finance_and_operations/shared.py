from datetime import datetime
from logging import Logger
from typing import Any, Callable, Protocol, runtime_checkable


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
class CachedAsyncFunc(Protocol):
    """Protocol for an alru_cache decorated async function."""
    __wrapped__: Callable[..., Any]
    def cache_info(self) -> CacheInfoProtocol: ...
    async def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


async def call_with_cache_logging(
    cached_func: CachedAsyncFunc,
    log: Logger,
    *args: Any,
    **kwargs: Any
) -> Any:
    """Call a cached function and log on cache miss."""
    info_before = cached_func.cache_info()
    result = await cached_func(*args, **kwargs)
    info_after = cached_func.cache_info()

    func_name = cached_func.__wrapped__.__name__

    if info_after.misses > info_before.misses:
        log.debug(f"Cache miss: {func_name}", {
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
