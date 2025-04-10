from datetime import datetime
import json
from logging import Logger
from typing import Optional, AsyncGenerator, Any, Dict, List, Type, TypeVar

from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPSession
from pydantic import TypeAdapter

from .models import Item, User, Story, Comment, Job, Poll, PollOption
from .buffer_ordered import buffer_ordered

# API Constants
API_BASE_URL = "https://hacker-news.firebaseio.com"
API_VERSION = "v0"
API_URL = f"{API_BASE_URL}/{API_VERSION}"

# Endpoint paths
ENDPOINTS = {
    "item": "/item/{item_id}.json",
    "user": "/user/{user_id}.json",
    "max_item": "/maxitem.json",
    "top_stories": "/topstories.json",
    "new_stories": "/newstories.json",
    "best_stories": "/beststories.json",
    "ask_stories": "/askstories.json",
    "show_stories": "/showstories.json",
    "job_stories": "/jobstories.json",
    "updates": "/updates.json"
}

MAX_CONCURRENT_REQUESTS = 5

async def _make_request(http: HTTPSession, log: Logger, endpoint: str, **kwargs) -> Optional[bytes]:
    """
    Make a request to the Hacker News API.
    
    Args:
        http: HTTP session
        log: Logger instance
        endpoint: API endpoint to call
        **kwargs: Format parameters for the endpoint URL
        
    Returns:
        Optional[bytes]: Response content if successful, None otherwise
    """
    url = f"{API_URL}{endpoint.format(**kwargs)}"
    return await http.request(log, url)

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

async def fetch_items_parallel(
    http: HTTPSession,
    log,
    item_ids: List[int],
    item_type: Optional[Type[Item]] = None,
) -> AsyncGenerator[Item, None]:
    """
    Fetch multiple items in parallel while preserving order.
    
    Args:
        http: HTTP session
        log: Logger
        item_ids: List of item IDs to fetch
        item_type: Optional type to filter items by
        
    Yields:
        Items in the same order as item_ids
    """
    async def gen():
        for item_id in item_ids:
            yield fetch_item(http, log, item_id)
    
    async for item in buffer_ordered(gen(), MAX_CONCURRENT_REQUESTS):
        if item is not None and (item_type is None or isinstance(item, item_type)):
            yield item

async def fetch_page(
    http: HTTPSession,
    log,
    page: Optional[int] = None,
    item_type: Optional[Type[Item]] = None,
) -> AsyncGenerator[Item | int, None]:
    """
    Fetch a page of items from the Hacker News API.
    
    Args:
        http: HTTP session
        log: Logger
        page: Optional page number
        item_type: Optional type to filter items by
        
    Yields:
        Items or next page number
    """
    url = f"{API_URL}{ENDPOINTS['new_stories']}"
    response = await http.request(log, url)
    item_ids = TypeAdapter(List[int]).validate_json(response)
    
    start_idx = page or 0
    end_idx = min(start_idx + 30, len(item_ids))
    
    async for item in fetch_items_parallel(http, log, item_ids[start_idx:end_idx], item_type):
        yield item
    
    if end_idx < len(item_ids):
        yield end_idx

async def fetch_changes(
    http: HTTPSession,
    log,
    since: datetime,
    until: Optional[datetime] = None,
) -> AsyncGenerator[Item, None]:
    """
    Fetch items that have changed since a given time.
    
    Args:
        http: HTTP session
        log: Logger
        since: Start time
        until: Optional end time
        
    Yields:
        Changed items
    """
    url = f"{API_URL}{ENDPOINTS['updates']}"
    response = await http.request(log, url)
    updates = TypeAdapter(dict).validate_json(response)
    
    item_ids = updates.get("items", [])
    async for item in fetch_items_parallel(http, log, item_ids):
        if item.time >= since and (until is None or item.time <= until):
            yield item

async def fetch_user(http: HTTPSession, log: Logger, user_id: str) -> Optional[User]:
    """
    Fetch a user's profile.
    
    Args:
        http: HTTP session
        log: Logger instance
        user_id: ID of the user to fetch
        
    Returns:
        Optional[User]: User object if found, None otherwise
    """
    response = await _make_request(http, log, ENDPOINTS["user"], user_id=user_id)
    data = await _parse_json_response(response)
    return User.model_validate(data) if data else None

async def fetch_max_item_id(http: HTTPSession, log: Logger) -> int:
    """
    Fetch the current maximum item ID.
    
    Args:
        http: HTTP session
        log: Logger instance
        
    Returns:
        int: Maximum item ID, or 0 if request fails
    """
    response = await _make_request(http, log, ENDPOINTS["max_item"])
    data = await _parse_json_response(response)
    return int(data) if data else 0

async def _fetch_story_list(http: HTTPSession, log: Logger, endpoint: str) -> List[int]:
    """
    Generic function to fetch a list of story IDs.
    
    Args:
        http: HTTP session
        log: Logger instance
        endpoint: Endpoint to fetch stories from
        
    Returns:
        List[int]: List of story IDs
    """
    response = await _make_request(http, log, endpoint)
    data = await _parse_json_response(response)
    return data if data else []

async def fetch_top_stories(http: HTTPSession, log: Logger) -> List[int]:
    """Fetch the IDs of the top stories."""
    return await _fetch_story_list(http, log, ENDPOINTS["top_stories"])

async def fetch_new_stories(http: HTTPSession, log: Logger) -> List[int]:
    """Fetch the IDs of the newest stories."""
    return await _fetch_story_list(http, log, ENDPOINTS["new_stories"])

async def fetch_best_stories(http: HTTPSession, log: Logger) -> List[int]:
    """Fetch the IDs of the best stories."""
    return await _fetch_story_list(http, log, ENDPOINTS["best_stories"])

async def fetch_ask_stories(http: HTTPSession, log: Logger) -> List[int]:
    """Fetch the IDs of the latest Ask HN stories."""
    return await _fetch_story_list(http, log, ENDPOINTS["ask_stories"])

async def fetch_show_stories(http: HTTPSession, log: Logger) -> List[int]:
    """Fetch the IDs of the latest Show HN stories."""
    return await _fetch_story_list(http, log, ENDPOINTS["show_stories"])

async def fetch_job_stories(http: HTTPSession, log: Logger) -> List[int]:
    """Fetch the IDs of the latest job stories."""
    return await _fetch_story_list(http, log, ENDPOINTS["job_stories"])

async def fetch_updates(http: HTTPSession, log: Logger) -> Dict[str, List[int]]:
    """
    Fetch the latest changed items and profiles.
    
    Args:
        http: HTTP session
        log: Logger instance
        
    Returns:
        Dict[str, List[int]]: Dictionary containing lists of changed items and profiles
    """
    response = await _make_request(http, log, ENDPOINTS["updates"])
    data = await _parse_json_response(response)
    return data if data else {"items": [], "profiles": []}