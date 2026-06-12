from collections.abc import AsyncGenerator
from datetime import datetime
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor

from .models import (
    ApiKey,
    Automation,
    Campaign,
    CollectionMeta,
    MailchimpEntity,
    MailchimpList,
    OAuthMetadata,
)

# Mailchimp's base URL is data-center-dependent. The `{dc}` placeholder (e.g. "us6")
# must be resolved from the configured credentials at runtime via `resolve_base_url`
# before any request is made.
API = "https://{dc}.api.mailchimp.com/3.0"
OAUTH_METADATA_URL = "https://login.mailchimp.com/oauth2/metadata"


async def resolve_base_url(log: Logger, http: HTTPSession, credentials: object) -> str:
    """Resolve the account's data-center-specific base URL.

    API keys carry their data center as a suffix (`<key>-us6`); OAuth tokens
    require asking Mailchimp's metadata endpoint, which reports the account's
    `api_endpoint` (e.g. "https://us6.api.mailchimp.com").
    """
    if isinstance(credentials, ApiKey):
        _, _, dc = credentials.password.rpartition("-")
        if not dc:
            raise ValueError(
                (
                    "Mailchimp API key is missing its data center suffix "
                    "(expected a key ending in e.g. -us21)."
                )
            )
        return API.format(dc=dc)

    metadata = OAuthMetadata.model_validate_json(
        await http.request(log, OAUTH_METADATA_URL)
    )
    return f"{metadata.api_endpoint}/3.0"


# Documented maximum `count` across Mailchimp collection endpoints
# (live-verified accepted on /lists and /campaigns).
MAX_PAGE_SIZE = 1000


async def _fetch_collection_page[T: MailchimpEntity](
    http: HTTPSession,
    base_url: str,
    model: type[T],
    params: dict[str, str | int],
    log: Logger,
) -> AsyncGenerator[T | CollectionMeta, None]:
    """Stream one page of `model`'s collection endpoint, yielding each item
    and then the envelope remainder (whose `total_items` drives pagination)."""
    _, body = await http.request_stream(log, f"{base_url}/{model.PATH}", params=params)
    processor = IncrementalJsonProcessor(
        body(),
        f"{model.ITEMS_KEY}.item",
        model,
        remainder_cls=CollectionMeta,
    )

    async for item in processor:
        yield item

    yield processor.get_remainder()


async def _snapshot_collection[T: MailchimpEntity](
    http: HTTPSession,
    base_url: str,
    model: type[T],
    log: Logger,
) -> AsyncGenerator[T, None]:
    """Page a top-level collection to exhaustion (snapshot streams)."""
    offset = 0

    while True:
        count = 0
        total_items = 0
        page = _fetch_collection_page(
            http,
            base_url,
            model,
            {"count": MAX_PAGE_SIZE, "offset": offset},
            log,
        )

        async for item in page:
            if isinstance(item, CollectionMeta):
                total_items = item.total_items
                continue
            yield item
            count += 1

        offset += count
        if count == 0 or offset >= total_items:
            return


async def snapshot_lists(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[MailchimpList, None]:
    async for doc in _snapshot_collection(http, base_url, MailchimpList, log):
        yield doc


async def snapshot_automations(
    http: HTTPSession,
    base_url: str,
    log: Logger,
) -> AsyncGenerator[Automation, None]:
    async for doc in _snapshot_collection(http, base_url, Automation, log):
        yield doc


def _campaign_params(offset: int, since: datetime) -> dict[str, str | int]:
    return {
        "count": MAX_PAGE_SIZE,
        "offset": offset,
        "since_create_time": since.isoformat(),
        "sort_field": "create_time",
        "sort_dir": "ASC",
    }


async def fetch_campaigns(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Campaign | LogCursor, None]:
    assert isinstance(log_cursor, datetime)

    last_seen = log_cursor
    offset = 0
    emitted = False

    while True:
        count = 0
        total_items = 0
        page = _fetch_collection_page(
            http,
            base_url,
            Campaign,
            _campaign_params(offset, log_cursor),
            log,
        )

        async for item in page:
            if isinstance(item, CollectionMeta):
                total_items = item.total_items
                continue

            count += 1
            # Suppressing docs at-or-before the cursor makes `since_*`
            # boundary inclusivity irrelevant.
            if item.create_time <= log_cursor:
                continue

            yield item
            emitted = True
            if item.create_time > last_seen:
                last_seen = item.create_time

        offset += count
        if count == 0 or offset >= total_items:
            break

    if emitted:
        yield last_seen


async def backfill_campaigns(
    http: HTTPSession,
    base_url: str,
    start_date: datetime,
    log: Logger,
    page: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Campaign | PageCursor, None]:
    assert page is None or isinstance(page, int)
    assert isinstance(cutoff, datetime)

    offset = page or 0
    # ASC sort under a frozen [start_date, cutoff) query keeps offsets stable
    # across restarts: new campaigns fall outside before_create_time.
    params = _campaign_params(offset, start_date)
    params["before_create_time"] = cutoff.isoformat()

    count = 0
    total_items = 0
    page_gen = _fetch_collection_page(http, base_url, Campaign, params, log)

    async for item in page_gen:
        if isinstance(item, CollectionMeta):
            total_items = item.total_items
            continue
        yield item
        count += 1

    if count > 0 and offset + count < total_items:
        yield offset + count
