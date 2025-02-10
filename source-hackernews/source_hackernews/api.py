from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPSession
from logging import Logger

logger = Logger(name=__file__)

from .models import Item

API = "https://hacker-news.firebaseio.com"


async def fetch_page(
    http: HTTPSession, log: Logger, start_cursor: PageCursor, log_cutoff: LogCursor
):
    url = f"{API}/v0/item/{start_cursor}.json"

    req = await http.request(log, url)

    item = Item.model_validate_json(req)

    # stop the backfill when we catch up
    if item.time > log_cutoff:
        return

    yield item
    yield start_cursor + 1


async def fetch_changes(http: HTTPSession, log: Logger, start_cursor: PageCursor):
    url = f"{API}/v0/item/{start_cursor}.json"

    req = await http.request(log, url)

    try:
        item = Item.model_validate_json(req)
        yield item
        yield start_cursor + 1
    except Exception as e:
        # If the item doesn't exist yet, we've caught up to real-time
        log.debug(f"Failed to fetch item {start_cursor}: {e}")
        return
