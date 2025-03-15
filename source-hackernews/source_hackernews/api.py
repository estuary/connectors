import asyncio
from estuary_cdk.capture.common import PageCursor, LogCursor
from estuary_cdk.http import HTTPSession
from logging import Logger
from .models import Item

API = "https://hacker-news.firebaseio.com"

logger = Logger(name=__file__)


async def fetch_page(http: HTTPSession, log: Logger, start_cursor: PageCursor, log_cutoff: LogCursor, concurrency: int = 5):
    async def fetch_single_page(cursor):
        url = f"{API}/v0/item/{cursor}.json"
        try:
            req = await http.request(log, url)
            item = Item.model_validate_json(req)
            if item.time > log_cutoff:
                return None
            return item, cursor + 1
        except Exception as e:
            log.error(f"Error fetching page for cursor {cursor}: {e}")
            return None

    tasks = {asyncio.create_task(fetch_single_page(start_cursor + i)) for i in range(concurrency)}

    while tasks:
        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            result = task.result()
            if result is None:
                continue
            item, next_cursor = result
            yield item
            tasks.add(asyncio.create_task(fetch_single_page(next_cursor)))


async def fetch_changes(http: HTTPSession, log: Logger, start_cursor: PageCursor, concurrency: int = 5):
    async def fetch_single_change(cursor):
        url = f"{API}/v0/item/{cursor}.json"
        try:
            req = await http.request(log, url)
            item = Item.model_validate_json(req)
            return item, cursor + 1
        except Exception as e:
            log.debug(f"Failed to fetch item {cursor}: {e}")
            return None

    tasks = {asyncio.create_task(fetch_single_change(start_cursor + i)) for i in range(concurrency)}

    while tasks:
        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            result = task.result()
            if result is None:
                continue
            item, next_cursor = result
            print(f"Fetched change item: {item}")
            tasks.add(asyncio.create_task(fetch_single_change(next_cursor)))
