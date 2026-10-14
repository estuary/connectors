import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture.common import LogCursor, PageCursor

from ..models import (
    TIKTOK_DATETIME_FORMAT,
    Ad,
    AdGroup,
    Campaign,
    TikTokEntity,
)
from .client import TikTokClient, paginate


@dataclass(frozen=True)
class BuyingTypeGroup:
    """Buying types TikTok accepts together in one `filtering.buying_types`."""

    buying_types: tuple[str, ...]
    page_size: int


# `RESERVATION_TOP_VIEW` cannot be combined with any other buying type, so full
# coverage takes one request per group.
BUYING_TYPE_GROUPS = (
    BuyingTypeGroup(("AUCTION", "RESERVATION_RF"), page_size=1000),
    # TikTok rejects a `page_size` above 100 when `buying_types` includes
    # `RESERVATION_TOP_VIEW`.
    BuyingTypeGroup(("RESERVATION_TOP_VIEW",), page_size=100),
)

# TikTok recommends creation-time filters span at most six months.
BACKFILL_WINDOW = timedelta(days=180)
# Entities are backfilled from here, whatever the configured start date, which
# limits report history only: report rows reference objects of any age, and no
# TikTok ad object predates 2016.
ENTITY_HISTORY_START = datetime(2016, 1, 1, tzinfo=UTC)
# `modified_after` is documented in UTC, but neither its time zone nor its
# inclusivity is verified, so it is queried a day early. The window filter drops
# what the margin re-lists.
MODIFIED_AFTER_MARGIN = timedelta(days=1)
ONE_SECOND = timedelta(seconds=1)


def _to_tiktok_datetime(value: datetime) -> str:
    return value.astimezone(UTC).strftime(TIKTOK_DATETIME_FORMAT)


# TikTok's listings may show a change late, by an unprobed amount, so polls stop
# this far behind the present and the backfill's cutoff lags the same.
EVENTUAL_CONSISTENCY_DELAY = timedelta(minutes=5)


def settled_second() -> datetime:
    """The latest second TikTok's listings are assumed to show in full."""
    return datetime.now(tz=UTC).replace(microsecond=0) - EVENTUAL_CONSISTENCY_DELAY


async def _list_entities[EntityT: TikTokEntity](
    client: TikTokClient,
    model: type[EntityT],
    advertiser_id: str,
    group: BuyingTypeGroup,
    extra_filters: dict[str, str],
    log: Logger,
) -> AsyncGenerator[EntityT, None]:
    """Every object of one advertiser and buying-type group matching `extra_filters`.

    TikTok lists newest id first with no other order, so pages shift whenever the
    filtered set changes mid-walk. An insertion only re-reads an object, which the
    collection key collapses; a removal would skip one. Callers therefore filter
    on sets that only grow while they are walked.
    """
    filters = {"buying_types": list(group.buying_types), **extra_filters}
    params = {"advertiser_id": advertiser_id, "filtering": json.dumps(filters)}
    async for doc in paginate(client, model.PATH, params, group.page_size, model, log):
        yield doc


async def _fetch_changed[EntityT: TikTokEntity](
    client: TikTokClient,
    model: type[EntityT],
    advertiser_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[EntityT | LogCursor, None]:
    """Emit every object of one advertiser modified since the cursor, as TikTok
    lists it. Deletions aren't captured yet: a deleted object is emitted with
    its final state, which looks like a paused one, and a parent's deletion
    leaves its children unchanged.

                cursor   cursor + 1s     horizon = now − 5 min
    ───────────────┼──────────┼────────────────┼──────▶ time (1s ticks)
                   │          │                │
    request ═══════╪══════════╪════════════════╪══════▶
    emitted ───────┼──────────[════════════════]
                   │          │                └─ later changes may not show
                   │          │                   in TikTok's listings yet;
                   │          │                   the next poll reads them
                   │          └─ first emitted second
                   └─ emitted by the previous poll

    The window applies to `modify_time`. Where the model has a server-side
    modified-after filter, it is queried `MODIFIED_AFTER_MARGIN` early; the
    window itself is always applied client-side.
    """
    assert isinstance(log_cursor, datetime)

    horizon = settled_second()
    if horizon <= log_cursor:
        return

    modified_filter: dict[str, str] = {}
    if model.MODIFIED_AFTER_FILTER:
        modified_filter[model.MODIFIED_AFTER_FILTER] = _to_tiktok_datetime(
            log_cursor - MODIFIED_AFTER_MARGIN
        )

    for group in BUYING_TYPE_GROUPS:
        # Every status, so a deletion mid-walk cannot shift a page.
        async for doc in _list_entities(
            client,
            model,
            advertiser_id,
            group,
            {"secondary_status": model.ALL_STATUS, **modified_filter},
            log,
        ):
            if log_cursor < doc.get_cursor() <= horizon:
                yield doc

    yield horizon


async def _backfill_created[EntityT: TikTokEntity](
    client: TikTokClient,
    model: type[EntityT],
    advertiser_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[EntityT | PageCursor, None]:
    """Emit one advertiser's objects created in one creation-time window per
    invocation that were unchanged at the cutoff, newest window first, so that
    current objects arrive before history.

               S − 1s    S               E − 1s    E
    ──────────────┼──────┼──────────────────┼──────┼──▶ create_time (1s ticks)
                  │      │                  │      │
    start_time ───(══════╪══════════════════╪══════╪═══▶
    end_time ═════╪══════╪══════════════════╪══════)
    emitted ──────┼──────[══════════════════]      │
                  │      │                  │      └─ the PageCursor, or the
                  │      │                  │         cutoff at first
                  │      │                  └─ last emitted second
                  │      └─ first emitted second; the next, older window
                  │         ends here
                  └─ exclusive start, queried 1s early

    S = max(E − 180d, start_date). Both creation bounds are exclusive. The
    PageCursor is S; creation time never changes, so a window re-walks
    identically after any resume gap. The last window starts at `start_date`.
    An object modified at or after `cutoff` is left to the incremental task,
    which emits its latest state.
    """
    assert isinstance(cutoff, datetime)

    if page_cursor is None:
        window_end = cutoff
    else:
        assert isinstance(page_cursor, str)
        window_end = datetime.fromisoformat(page_cursor)

    if window_end <= start_date:
        return

    window_start = max(window_end - BACKFILL_WINDOW, start_date)
    creation_filters = {
        "creation_filter_start_time": _to_tiktok_datetime(window_start - ONE_SECOND),
        "creation_filter_end_time": _to_tiktok_datetime(window_end),
    }

    for group in BUYING_TYPE_GROUPS:
        # Every status, so a deletion mid-walk cannot shift a page.
        async for doc in _list_entities(
            client,
            model,
            advertiser_id,
            group,
            {**creation_filters, "secondary_status": model.ALL_STATUS},
            log,
        ):
            if doc.get_cursor() < cutoff:
                yield doc

    if window_start > start_date:
        yield window_start.isoformat()


async def fetch_campaigns(
    client: TikTokClient,
    advertiser_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Campaign | LogCursor, None]:
    async for item in _fetch_changed(client, Campaign, advertiser_id, log, log_cursor):
        yield item


async def backfill_campaigns(
    client: TikTokClient,
    advertiser_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Campaign | PageCursor, None]:
    async for item in _backfill_created(
        client, Campaign, advertiser_id, start_date, log, page_cursor, cutoff
    ):
        yield item


async def fetch_ad_groups(
    client: TikTokClient,
    advertiser_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[AdGroup | LogCursor, None]:
    async for item in _fetch_changed(client, AdGroup, advertiser_id, log, log_cursor):
        yield item


async def backfill_ad_groups(
    client: TikTokClient,
    advertiser_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[AdGroup | PageCursor, None]:
    async for item in _backfill_created(
        client, AdGroup, advertiser_id, start_date, log, page_cursor, cutoff
    ):
        yield item


async def fetch_ads(
    client: TikTokClient,
    advertiser_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[Ad | LogCursor, None]:
    async for item in _fetch_changed(client, Ad, advertiser_id, log, log_cursor):
        yield item


async def backfill_ads(
    client: TikTokClient,
    advertiser_id: str,
    start_date: datetime,
    log: Logger,
    page_cursor: PageCursor,
    cutoff: LogCursor,
) -> AsyncGenerator[Ad | PageCursor, None]:
    async for item in _backfill_created(
        client, Ad, advertiser_id, start_date, log, page_cursor, cutoff
    ):
        yield item


async def list_campaign_ids(
    client: TikTokClient,
    advertiser_id: str,
    log: Logger,
) -> list[str]:
    """Every campaign ID of one advertiser, deleted campaigns included."""
    ids: dict[str, None] = {}
    for group in BUYING_TYPE_GROUPS:
        async for campaign in _list_entities(
            client,
            Campaign,
            advertiser_id,
            group,
            {"secondary_status": Campaign.ALL_STATUS},
            log,
        ):
            ids[campaign.get_id()] = None
    return list(ids)
