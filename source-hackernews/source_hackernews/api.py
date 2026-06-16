import asyncio
import itertools
import json
from logging import Logger
from typing import Any, AsyncGenerator, Awaitable, Callable

from pydantic import ValidationError

from estuary_cdk.http import HTTPSession
from estuary_cdk.buffer_ordered import buffer_ordered

from .models import Item, User, UpdatedItems

API_URL = "https://hacker-news.firebaseio.com/v0"

# Number of item IDs fetched per backfill page.
BACKFILL_BATCH_SIZE = 100

# Ranking list endpoints, each returning story IDs in ranked display order.
STORY_LIST_CATEGORIES = [
    "topstories",
    "newstories",
    "beststories",
    "askstories",
    "showstories",
    "jobstories",
]


async def _get_json(http: HTTPSession, log: Logger, path: str) -> Any | None:
    """
    Fetch and decode a HackerNews API endpoint.

    The API responds with the literal `null` for deleted or non-existent
    items and users, which is returned here as None. Connection errors and 5xx
    responses are retried by the CDK's HTTPSession before propagating, so no
    additional retry handling is needed here.
    """
    body = await http.request(log, f"{API_URL}{path}")
    if not body or body.strip() == b"null":
        return None
    return json.loads(body)


async def fetch_item(http: HTTPSession, log: Logger, item_id: int) -> Item | None:
    data = await _get_json(http, log, f"/item/{item_id}.json")
    if data is None:
        return None
    try:
        return Item.model_validate(data)
    except ValidationError as e:
        log.warning(f"Failed to validate item {item_id}: {e}")
        return None


async def fetch_user(http: HTTPSession, log: Logger, user_id: str) -> User | None:
    data = await _get_json(http, log, f"/user/{user_id}.json")
    if data is None:
        return None
    try:
        return User.model_validate(data)
    except ValidationError as e:
        log.warning(f"Failed to validate user {user_id}: {e}")
        return None


async def fetch_updates(http: HTTPSession, log: Logger) -> UpdatedItems | None:
    data = await _get_json(http, log, "/updates.json")
    if data is None:
        return None
    try:
        return UpdatedItems.model_validate(data)
    except ValidationError as e:
        log.warning(f"Failed to validate updates: {e}")
        return None


async def fetch_max_item_id(http: HTTPSession, log: Logger) -> int:
    data = await _get_json(http, log, "/maxitem.json")
    if data is None:
        return 0
    try:
        return int(data)
    except (ValueError, TypeError):
        log.warning(f"Invalid max item ID response: {data}")
        return 0


async def fetch_story_ids(http: HTTPSession, log: Logger, category: str) -> list[int]:
    """Fetch the current ordered story IDs for a ranking list (e.g. topstories)."""
    data = await _get_json(http, log, f"/{category}.json")
    if not data:
        return []
    return [int(story_id) for story_id in data]


async def _fetch_concurrent[Id, T](
    log: Logger,
    ids: list[Id],
    fetch_one: Callable[[Id], Awaitable[T | None]],
    concurrency: int,
) -> AsyncGenerator[T, None]:
    """
    Fetch many entities concurrently, yielding them in the order of `ids`.

    A semaphore bounds the number of in-flight requests, while `buffer_ordered`
    preserves input ordering so backfill page cursors advance monotonically.
    Entities that are missing (None) or raise are skipped, since a single bad
    ID should not abort an ID-walking backfill.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def bounded(id_: Id) -> T | None:
        async with semaphore:
            return await fetch_one(id_)

    async def _batches() -> AsyncGenerator[Awaitable[list[T]], None]:
        batch_size = min(50, max(10, len(ids) // 20))
        for batch in itertools.batched(ids, batch_size):

            async def gather_batch(batch: tuple[Id, ...] = batch) -> list[T]:
                results = await asyncio.gather(
                    *(bounded(id_) for id_ in batch),
                    return_exceptions=True,
                )
                fetched: list[T] = []
                for result in results:
                    if isinstance(result, BaseException):
                        log.warning(f"Error fetching entity: {result}")
                    elif result is not None:
                        fetched.append(result)
                return fetched

            yield gather_batch()

    async for batch in buffer_ordered(_batches(), max(1, concurrency // 5)):
        for entity in batch:
            yield entity


def fetch_items_batch(
    http: HTTPSession, log: Logger, item_ids: list[int], concurrency: int = 10
) -> AsyncGenerator[Item, None]:
    """Fetch multiple items concurrently, in the order of `item_ids`."""
    return _fetch_concurrent(
        log, item_ids, lambda item_id: fetch_item(http, log, item_id), concurrency
    )


def fetch_users_batch(
    http: HTTPSession, log: Logger, user_ids: list[str], concurrency: int = 10
) -> AsyncGenerator[User, None]:
    """Fetch multiple users concurrently, in the order of `user_ids`."""
    return _fetch_concurrent(
        log, user_ids, lambda user_id: fetch_user(http, log, user_id), concurrency
    )
