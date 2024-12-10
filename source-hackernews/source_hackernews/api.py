from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPSession
from logging import Logger

logger = Logger(name=__file__)

from .models import (
    Item
)

API = "https://hacker-news.firebaseio.com"


async def fetch_page(http: HTTPSession, log: Logger, start_cursor: PageCursor, log_cutoff: LogCursor):
    url = f"{API}/v0/item/{start_cursor}.json"

    req = await http.request(log, url)

    item = Item.model_validate_json(req)

    # stop the backfill when we catch up
    if item.time > log_cutoff:
        return

    yield item
    yield start_cursor + 1
