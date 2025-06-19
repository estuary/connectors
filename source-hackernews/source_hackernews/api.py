import json
from logging import Logger
from typing import (
    Optional,
    AsyncGenerator,
    Any,
)
import asyncio

from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPMixin
from pydantic import TypeAdapter

from .models import Item

# API Constants
API_BASE_URL = "https://hacker-news.firebaseio.com"
API_VERSION = "v0"
API_URL = f"{API_BASE_URL}/{API_VERSION}"

# Endpoint paths
ENDPOINTS = {
    "item": "/item/{item_id}.json",
    "max_item": "/maxitem.json",
}


async def _parse_json_response(response: Optional[bytes]) -> Optional[Any]:
    """
    Parse JSON response from API.

    Args:
        response: Raw response bytes

    Returns:
        Optional[Any]: Parsed JSON data if successful, None otherwise
    """
    if not response:
        return None
    try:
        return json.loads(response.decode("utf-8"))
    except json.JSONDecodeError:
        return None


async def fetch_item(http: HTTPMixin, log: Logger, item_id: int) -> Optional[Item]:
    """Fetch a single item from the Hacker News API."""
    url = f"{API_URL}{ENDPOINTS['item'].format(item_id=item_id)}"
    try:
        response = await http.request(log, url)
        if response == "null":
            return None
        return TypeAdapter(Item).validate_json(response)
    except json.JSONDecodeError:
        log.warning(f"Failed to decode JSON for item {item_id}")
        return None


async def fetch_page(
    http: HTTPMixin, log: Logger, start_cursor: PageCursor, log_cutoff: LogCursor
) -> AsyncGenerator[Item | int, None]:
    """
    Fetch a single item from the API and yield it if it's newer than the cutoff.

    Args:
        http: HTTP session
        log: Logger instance
        start_cursor: Item ID to fetch
        log_cutoff: Time cutoff for backfill

    Yields:
        Item | int: The fetched item or next cursor
    """
    url = f"{API_URL}{ENDPOINTS['item'].format(item_id=start_cursor)}"
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = await http.request(log, url)
            data = await _parse_json_response(response)

            if not data:
                return

            item = Item.model_validate(data)

            # Stop the backfill when we catch up
            if item.time > log_cutoff:
                return

            yield item
            yield start_cursor + 1
            return
        except Exception as e:
            if attempt == max_retries - 1:
                log.error(
                    f"Failed to fetch item {start_cursor} after {max_retries} attempts: {e}"
                )
                return
            delay = base_delay * (2**attempt)
            log.warning(f"Request to {url} failed, retrying in {delay} seconds: {e}")
            await asyncio.sleep(delay)


async def fetch_max_item_id(http: HTTPMixin, log: Logger) -> int:
    """
    Fetch the current maximum item ID with retries.

    Args:
        http: HTTP session
        log: Logger instance

    Returns:
        int: Maximum item ID, or 0 if request fails
    """
    url = f"{API_URL}{ENDPOINTS['max_item']}"
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = await http.request(log, url)
            data = await _parse_json_response(response)
            return int(data) if data else 0
        except Exception as e:
            if attempt == max_retries - 1:
                log.error(
                    f"Failed to fetch max item ID after {max_retries} attempts: {e}"
                )
                return 0
            delay = base_delay * (2**attempt)
            log.warning(
                f"Failed to fetch max item ID, retrying in {delay} seconds: {e}"
            )
            await asyncio.sleep(delay)

    return 0
