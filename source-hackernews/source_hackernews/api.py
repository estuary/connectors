from datetime import datetime
import json
from logging import Logger
from typing import Optional, AsyncGenerator, Any, Dict, List, Type, TypeVar, Awaitable, Iterable
import itertools
import asyncio

from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPSession
from pydantic import TypeAdapter

from .models import Item
from .buffer_ordered import buffer_ordered

# API Constants
API_BASE_URL = "https://hacker-news.firebaseio.com"
API_VERSION = "v0"
API_URL = f"{API_BASE_URL}/{API_VERSION}"

# Endpoint paths
ENDPOINTS = {
    "item": "/item/{item_id}.json",
    "max_item": "/maxitem.json",
}

async def _make_request(http: HTTPSession, log: Logger, endpoint: str, **kwargs) -> Optional[bytes]:
    """
    Make a request to the Hacker News API with retries and rate limiting.
    
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
                log.error(f"Failed to make request to {url} after {max_retries} attempts: {e}")
                return None
            delay = base_delay * (2 ** attempt)  # Exponential backoff
            log.warning(f"Request to {url} failed, retrying in {delay} seconds: {e}")
            await asyncio.sleep(delay)
    
    return None

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
        return json.loads(response.decode('utf-8'))
    except json.JSONDecodeError:
        return None

async def fetch_item(http: HTTPSession, log, item_id: int) -> Optional[Item]:
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
    http: HTTPSession, 
    log: Logger, 
    start_cursor: PageCursor, 
    log_cutoff: LogCursor
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
    response = await _make_request(http, log, ENDPOINTS["item"], item_id=start_cursor)
    data = await _parse_json_response(response)

    if not data:
        return

    try:
        item = Item.model_validate(data)

        # Stop the backfill when we catch up
        if item.time > log_cutoff:
            return

        yield item
        yield start_cursor + 1
    except Exception as e:
        log.error(f"Error processing item {start_cursor}: {e}")
        return


async def fetch_max_item_id(http: HTTPSession, log: Logger) -> int:
    """
    Fetch the current maximum item ID with retries.
    
    Args:
        http: HTTP session
        log: Logger instance
        
    Returns:
        int: Maximum item ID, or 0 if request fails
    """
    max_retries = 3
    base_delay = 1.0

    for attempt in range(max_retries):
        try:
            response = await _make_request(http, log, ENDPOINTS["max_item"])
            data = await _parse_json_response(response)
            return int(data) if data else 0
        except Exception as e:
            if attempt == max_retries - 1:
                log.error(f"Failed to fetch max item ID after {max_retries} attempts: {e}")
                return 0
            delay = base_delay * (2 ** attempt)  # Exponential backoff
            log.warning(f"Failed to fetch max item ID, retrying in {delay} seconds: {e}")
            await asyncio.sleep(delay)
    
    return 0
