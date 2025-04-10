from datetime import datetime
import json
from logging import Logger
from typing import Optional, AsyncGenerator, Any, Dict, List

from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPSession

from .models import Item, User

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

async def fetch_changes(
    http: HTTPSession, 
    log: Logger, 
    start_cursor: PageCursor
) -> AsyncGenerator[Item | int, None]:
    """
    Fetch a single item for change detection.
    
    Args:
        http: HTTP session
        log: Logger instance
        start_cursor: Item ID to fetch
        
    Yields:
        Item | int: The fetched item or next cursor
    """
    response = await _make_request(http, log, ENDPOINTS["item"], item_id=start_cursor)
    data = await _parse_json_response(response)
    
    if not data:
        return
        
    try:
        item = Item.model_validate(data)
        yield item
        yield start_cursor + 1
    except Exception as e:
        log.debug(f"Failed to fetch item {start_cursor}: {e}")
        return


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