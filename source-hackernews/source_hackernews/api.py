from datetime import UTC
import json
from logging import Logger
from typing import (
    Optional,
    AsyncGenerator,
    Any,
    List,
    TypeVar,
    Awaitable,
    Iterable,
)
import itertools
import asyncio

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[List[T]]:
    """Batch data from the iterable into lists of length n.

    This is a backport of itertools.batched from Python 3.12 for Python 3.11 compatibility.
    """
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := list(itertools.islice(it, n)):
        yield batch


from estuary_cdk.http import HTTPSession
from pydantic import ValidationError

from .models import Item, User, UpdatedItems
from .buffer_ordered import buffer_ordered

# API Constants
API_BASE_URL = "https://hacker-news.firebaseio.com"
API_VERSION = "v0"
API_URL = f"{API_BASE_URL}/{API_VERSION}"

# HackerNews API Endpoints
ENDPOINTS = {
    # Items
    "item": "/item/{item_id}.json",
    "max_item": "/maxitem.json",
    # Users
    "user": "/user/{user_id}.json",
    # Live Updates
    "updates": "/updates.json",
    # Story Categories
    "top_stories": "/topstories.json",
    "new_stories": "/newstories.json",
    "best_stories": "/beststories.json",
    "ask_stories": "/askstories.json",
    "show_stories": "/showstories.json",
    "job_stories": "/jobstories.json",
}


async def _make_request(
    http: HTTPSession, log: Logger, endpoint: str, **kwargs
) -> Optional[bytes]:
    """
    Make a request to the HackerNews API with retries and exponential backoff.

    Args:
        http: HTTP session
        log: Logger instance
        endpoint: API endpoint to call
        **kwargs: Format parameters for the endpoint URL

    Returns:
        Optional[bytes]: Response content if successful, None otherwise
    """
    url = f"{API_URL}{endpoint.format(**kwargs)}"
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = await http.request(log, url)
            return response
        except Exception as e:
            if attempt == max_retries - 1:
                log.error(
                    f"Failed to make request to {url} after {max_retries} attempts: {e}"
                )
                return None
            delay = base_delay * (2**attempt)  # Exponential backoff
            log.warning(
                f"Request to {url} failed (attempt {attempt + 1}/{max_retries}), retrying in {delay:.1f}s: {e}"
            )
            await asyncio.sleep(delay)

    return None


async def _parse_json_response(response: Optional[bytes]) -> Optional[Any]:
    """
    Parse JSON response from API, handling null responses.

    Args:
        response: Raw response bytes

    Returns:
        Optional[Any]: Parsed JSON data if successful, None for null/invalid responses
    """
    if not response:
        return None

    try:
        response_str = response.decode("utf-8")
        if (
            response_str == "null"
        ):  # HackerNews API returns "null" for deleted/missing items
            return None
        return json.loads(response_str)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return None


async def fetch_item(http: HTTPSession, log: Logger, item_id: int) -> Optional[Item]:
    """
    Fetch a single item from the HackerNews API.

    Args:
        http: HTTP session
        log: Logger instance
        item_id: Item ID to fetch

    Returns:
        Optional[Item]: Parsed item if successful, None if not found or error
    """
    response = await _make_request(http, log, ENDPOINTS["item"], item_id=item_id)
    data = await _parse_json_response(response)

    if not data:
        return None

    try:
        return Item.model_validate(data)
    except ValidationError as e:
        log.warning(f"Failed to validate item {item_id}: {e}")
        return None


async def fetch_user(http: HTTPSession, log: Logger, user_id: str) -> Optional[User]:
    """
    Fetch a single user from the HackerNews API.

    Args:
        http: HTTP session
        log: Logger instance
        user_id: User ID (username) to fetch

    Returns:
        Optional[User]: Parsed user if successful, None if not found or error
    """
    response = await _make_request(http, log, ENDPOINTS["user"], user_id=user_id)
    data = await _parse_json_response(response)

    if not data:
        return None

    try:
        return User.model_validate(data)
    except ValidationError as e:
        log.warning(f"Failed to validate user {user_id}: {e}")
        return None


async def fetch_updates(http: HTTPSession, log: Logger) -> Optional[UpdatedItems]:
    """
    Fetch the latest updates (changed items and profiles) from HackerNews.

    Args:
        http: HTTP session
        log: Logger instance

    Returns:
        Optional[UpdatedItems]: Updates if successful, None if error
    """
    response = await _make_request(http, log, ENDPOINTS["updates"])
    data = await _parse_json_response(response)

    if not data:
        return None

    try:
        return UpdatedItems.model_validate(data)
    except ValidationError as e:
        log.warning(f"Failed to validate updates: {e}")
        return None


async def fetch_max_item_id(http: HTTPSession, log: Logger) -> int:
    """
    Fetch the current maximum item ID with retries.

    Args:
        http: HTTP session
        log: Logger instance

    Returns:
        int: Maximum item ID, or 0 if request fails
    """
    response = await _make_request(http, log, ENDPOINTS["max_item"])
    data = await _parse_json_response(response)

    if data is not None:
        try:
            return int(data)
        except (ValueError, TypeError):
            log.warning(f"Invalid max item ID response: {data}")

    return 0


async def fetch_items_batch(
    http: HTTPSession, log: Logger, item_ids: List[int], concurrency: int = 10
) -> AsyncGenerator[Item, None]:
    """
    Fetch multiple items in parallel with controlled concurrency.

    Args:
        http: HTTP session
        log: Logger instance
        item_ids: List of item IDs to fetch
        concurrency: Maximum concurrent requests

    Yields:
        Item: Successfully fetched and validated items
    """
    # Use semaphore to control concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_single_item(item_id: int) -> Optional[Item]:
        async with semaphore:
            return await fetch_item(http, log, item_id)

    async def _batches_gen() -> AsyncGenerator[Awaitable[List[Item]], None]:
        batch_size = min(50, max(10, len(item_ids) // 20))  # Dynamic batch size
        for batch_ids in batched(item_ids, batch_size):
            tasks = [fetch_single_item(item_id) for item_id in batch_ids]

            async def gather_batch() -> List[Item]:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                items = []
                for result in results:
                    if isinstance(result, Item):
                        items.append(result)
                    elif isinstance(result, Exception):
                        log.warning(f"Error fetching item: {result}")
                return items

            yield gather_batch()

    # Use buffer_ordered for controlled parallel processing
    total_yielded = 0
    async for batch_result in buffer_ordered(_batches_gen(), max(1, concurrency // 5)):
        for item in batch_result:
            total_yielded += 1
            if total_yielded % 100 == 0:
                log.info(f"Fetched {total_yielded} items", {"progress": total_yielded})
            yield item
