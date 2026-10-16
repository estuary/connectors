import asyncio
import json
import time
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Any
from zoneinfo import ZoneInfo

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import Headers, HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel

from .models import (
    TIKTOK_DATETIME_FORMAT,
    Ad,
    AdGroup,
    Advertiser,
    AdvertiserBatch,
    AdvertiserList,
    Campaign,
    Cascade,
    TikTokEntity,
    TikTokEnvelope,
    TikTokPageRemainder,
    TikTokResponse,
)

API = "https://business-api.tiktok.com/open_api/v1.3"
SANDBOX_API = "https://sandbox-ads.tiktok.com/open_api/v1.3"


# TikTok's rate limits belong to the developer app, per endpoint, so every
# capture using Estuary's OAuth app shares them. It answers an excess with HTTP
# 200 and a throttle code, which the CDK's rate limiter never sees. Apps start
# at 10 requests per second; the sandbox allows 1.
SANDBOX_REQUESTS_PER_SECOND = 1.0
REQUESTS_PER_SECOND = 10.0
# A throttle doubles the spacing between request starts, up to this many
# seconds, and each request TikTok accepts eases it back by `PACE_RECOVERY`,
# as the CDK's rate limiter does for HTTP 429. Captures sharing an app then
# back off together instead of keeping it throttled.
MAX_PACE_SECONDS = 10.0
PACE_RECOVERY = 0.98


class TikTokClient:
    """One capture's access to TikTok: its HTTP session and API host, and one
    pace shared by every request, so that the subtasks of many advertisers stay
    under TikTok's rate limit together. The pace slows when TikTok throttles and
    recovers as it accepts requests."""

    def __init__(self, http: HTTPSession, base_url: str, requests_per_second: float):
        self.http = http
        self.base_url = base_url
        self._start_interval = 1 / requests_per_second
        self._interval = self._start_interval
        self._next_start = 0.0
        self._pacing = asyncio.Lock()

    def throttled(self) -> None:
        self._interval = min(self._interval * 2, MAX_PACE_SECONDS)

    def accepted(self) -> None:
        self._interval = max(self._start_interval, self._interval * PACE_RECOVERY)

    async def _wait_for_turn(self) -> None:
        """Space request starts by the current pace."""
        async with self._pacing:
            now = time.monotonic()
            if self._next_start > now:
                await asyncio.sleep(self._next_start - now)
            self._next_start = max(now, self._next_start) + self._interval

    async def request(self, log: Logger, path: str, params: dict[str, Any]) -> bytes:
        await self._wait_for_turn()
        return await self.http.request(log, f"{self.base_url}/{path}", params=params)

    async def request_stream(
        self, log: Logger, path: str, params: dict[str, Any]
    ) -> tuple[Headers, Callable[[], AsyncGenerator[bytes, None]]]:
        await self._wait_for_turn()
        return await self.http.request_stream(
            log, f"{self.base_url}/{path}", params=params
        )


def tiktok_client(http: HTTPSession, is_sandbox: bool) -> TikTokClient:
    if is_sandbox:
        return TikTokClient(http, SANDBOX_API, SANDBOX_REQUESTS_PER_SECOND)
    return TikTokClient(http, API, REQUESTS_PER_SECOND)


class TikTokAPIError(Exception):
    """TikTok reports most failures with HTTP 200 and a non-zero `code` in the
    body, so HTTP-level error handling alone would treat them as success."""

    def __init__(self, code: int, message: str):
        super().__init__(f"TikTok API error {code}: {message}")
        self.code = code
        self.message = message


# TikTok signals throttling, its own server errors and maintenance in the body
# of an HTTP 200, so the CDK's HTTP retries never see them; these codes are
# retried here instead. Only throttling slows the shared pace.
THROTTLE_CODES = frozenset({40016, 40100, 40132, 40133})
SERVER_ERROR_CODES = frozenset({50000, 50002, 51305, 60001})
# Seven retries, sleeping 5, 10, 20, 40, 80, 160 and 300 seconds: about ten
# minutes before a lasting failure fails the capture.
RETRY_ATTEMPTS = 8
MAX_RETRY_SLEEP_SECONDS = 300


async def _retry_if_transient(
    client: TikTokClient, log: Logger, code: int, message: str, path: str, attempt: int
) -> None:
    """Return once the caller should retry a request TikTok throttled or failed
    on its side; raise for every other non-zero code, and once the attempts are
    spent."""
    is_throttled = code in THROTTLE_CODES
    if (is_throttled or code in SERVER_ERROR_CODES) and attempt < RETRY_ATTEMPTS:
        if is_throttled:
            client.throttled()
        delay = min(5 * 2 ** (attempt - 1), MAX_RETRY_SLEEP_SECONDS)
        log.warning(
            "TikTok throttled the request, retrying"
            if is_throttled
            else "TikTok reported a server error, retrying",
            {"path": path, "code": code, "attempt": attempt, "sleep_seconds": delay},
        )
        await asyncio.sleep(delay)
        return
    raise TikTokAPIError(code, message)


async def _get[DataT: BaseModel](
    client: TikTokClient,
    path: str,
    params: dict[str, Any],
    data_model: type[DataT],
    log: Logger,
) -> DataT:
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        body = await client.request(log, path, params)

        envelope = TikTokEnvelope.model_validate_json(body)
        if envelope.code == 0:
            client.accepted()
            return TikTokResponse[data_model].model_validate_json(body).data

        await _retry_if_transient(
            client, log, envelope.code, envelope.message, path, attempt
        )

    raise AssertionError("unreachable: the last attempt either returns or raises")


async def list_authorized_advertiser_ids(
    client: TikTokClient,
    app_id: str,
    secret: str,
    log: Logger,
) -> list[str]:
    """Return the IDs of every advertiser the access token is authorized for."""
    data = await _get(
        client,
        "oauth2/advertiser/get/",
        {"app_id": app_id, "secret": secret},
        AdvertiserList,
        log,
    )
    return [advertiser.advertiser_id for advertiser in data.advertisers]


class NoAdvertisersError(RuntimeError):
    pass


async def resolve_advertiser_ids(
    client: TikTokClient,
    configured_ids: list[str],
    app_id: str | None,
    secret: str | None,
    log: Logger,
) -> list[str]:
    """The advertisers a capture reads: the configured IDs, or else every
    advertiser the token is authorized for, which takes the app ID and secret.
    Raises NoAdvertisersError for none, since capturing no advertisers would
    empty the Advertisers collection."""
    if configured_ids:
        return configured_ids
    # Validation requires configured IDs when there is no app ID.
    assert app_id is not None and secret is not None
    ids = await list_authorized_advertiser_ids(client, app_id, secret, log)
    if not ids:
        raise NoAdvertisersError(
            "The authorized TikTok account has no advertisers. Authorize an account with access to at least one advertiser."
        )
    return ids


class AdvertiserAccounts:
    """Every advertiser's account details, read for all of them at once on first
    use and then fixed for the life of the connector process: each one's report
    time zone, and, at start-up, proof that TikTok can still read it."""

    def __init__(self, client: TikTokClient, advertiser_ids: list[str]):
        self._client = client
        self._advertiser_ids = advertiser_ids
        self._zones: dict[str, ZoneInfo] | None = None
        self._lock = asyncio.Lock()

    async def _all_zones(self, log: Logger) -> dict[str, ZoneInfo]:
        async with self._lock:
            if self._zones is None:
                accounts = await get_advertisers(
                    self._client, self._advertiser_ids, log
                )
                self._zones = {a.advertiser_id: a.zone() for a in accounts}
            return self._zones

    async def check(self, log: Logger) -> None:
        """Raise InaccessibleAdvertiserError, with advice, for an advertiser
        TikTok can't read."""
        await self._all_zones(log)

    async def zone(self, advertiser_id: str, log: Logger) -> ZoneInfo:
        zones = await self._all_zones(log)
        if advertiser_id not in zones:
            raise RuntimeError(
                f"TikTok returned no account details for advertiser {advertiser_id}. "
                "Remove it from Advertiser IDs or, if that field is blank, enter the IDs of the advertisers to capture without it."
            )
        return zones[advertiser_id]


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


async def paginate[ItemT: BaseModel](
    client: TikTokClient,
    path: str,
    params: dict[str, Any],
    page_size: int,
    model: type[ItemT],
    log: Logger,
    check_headers: Callable[[Headers], None] | None = None,
) -> AsyncGenerator[ItemT, None]:
    """Every item of a paged `data.list` response, retrying throttled pages.
    `check_headers` sees each page's headers before any of its items is emitted."""
    page = 1
    attempt = 1
    while True:
        headers, body = await client.request_stream(
            log, path, {**params, "page": page, "page_size": page_size}
        )
        if check_headers is not None:
            check_headers(headers)

        processor = IncrementalJsonProcessor(
            body(),
            "data.list.item",
            model,
            TikTokPageRemainder,
        )
        async for item in processor:
            yield item

        remainder = processor.get_remainder()
        if remainder.code != 0:
            # A failed response carries no items, so retrying the page re-emits nothing.
            await _retry_if_transient(
                client, log, remainder.code, remainder.message, path, attempt
            )
            attempt += 1
            continue

        client.accepted()
        if page >= remainder.page_info().total_page:
            return
        page += 1
        attempt = 1


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


async def _ids_with_status(
    client: TikTokClient,
    model: type[TikTokEntity],
    advertiser_id: str,
    group: BuyingTypeGroup,
    status: str,
    extra_filters: dict[str, str],
    log: Logger,
) -> set[str]:
    """IDs of the objects with a deletion `status` matching `extra_filters`.
    Deleted sets only grow, so reading one is safe from page shifts."""
    return {
        doc.get_id()
        async for doc in _list_entities(
            client,
            model,
            advertiser_id,
            group,
            {**extra_filters, "secondary_status": status},
            log,
        )
    }


async def _deleted_ids(
    client: TikTokClient,
    model: type[TikTokEntity],
    advertiser_id: str,
    group: BuyingTypeGroup,
    extra_filters: dict[str, str],
    log: Logger,
) -> tuple[set[str], dict[Cascade, set[str]]]:
    """The directly deleted IDs, and per cascade the IDs of objects whose parent
    was deleted."""
    direct = await _ids_with_status(
        client, model, advertiser_id, group, model.DELETED_STATUS, extra_filters, log
    )
    cascaded = {
        cascade: await _ids_with_status(
            client, model, advertiser_id, group, cascade.status, extra_filters, log
        )
        for cascade in model.CASCADES
    }
    return direct, cascaded


async def _fetch_changed[EntityT: TikTokEntity](
    client: TikTokClient,
    model: type[EntityT],
    advertiser_id: str,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[EntityT | LogCursor, None]:
    """Emit every object of one advertiser modified since the cursor, and every
    object whose parent was deleted since the cursor; deleted ones as deletions.

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

    The window applies to an object's `modify_time`, or, for an object whose
    parent was deleted, to the parent's `modify_time`: a cascade leaves the
    child's own `modify_time` unchanged. Where the model has a server-side
    modified-after filter, it is queried `MODIFIED_AFTER_MARGIN` early; the
    window itself is always applied client-side.
    """
    assert isinstance(log_cursor, datetime)

    horizon = settled_second()
    if horizon <= log_cursor:
        return

    def in_window(entity: TikTokEntity) -> bool:
        return log_cursor < entity.get_cursor() <= horizon

    modified_filter: dict[str, str] = {}
    if model.MODIFIED_AFTER_FILTER:
        modified_filter[model.MODIFIED_AFTER_FILTER] = _to_tiktok_datetime(
            log_cursor - MODIFIED_AFTER_MARGIN
        )

    for group in BUYING_TYPE_GROUPS:
        # A deletion stamps `modify_time`, so only recent deletions matter.
        deleted_ids = await _ids_with_status(
            client,
            model,
            advertiser_id,
            group,
            model.DELETED_STATUS,
            modified_filter,
            log,
        )

        # Cascades come from their own listings: a cascade leaves the child's
        # `modify_time` unchanged, so a modified-after listing never holds
        # it. Deletion stamps the parent's `modify_time`, so each cascade is
        # emitted once, and a poll with no parent deleted lists no children.
        deleted_parents: dict[Cascade, set[str]] = {}
        for cascade in model.CASCADES:
            parents = [
                parent
                async for parent in _list_entities(
                    client,
                    cascade.parent,
                    advertiser_id,
                    group,
                    {"secondary_status": cascade.parent.DELETED_STATUS},
                    log,
                )
            ]
            deleted_parents[cascade] = {p.get_id() for p in parents}
            parents_deleted_now = {p.get_id() for p in parents if in_window(p)}
            if not parents_deleted_now:
                continue
            async for doc in _list_entities(
                client,
                model,
                advertiser_id,
                group,
                {"secondary_status": cascade.status},
                log,
            ):
                if getattr(doc, cascade.parent_id_field) in parents_deleted_now:
                    doc.meta_ = model.Meta(op="d")
                    yield doc

        # Every status, so a deletion mid-walk cannot shift a page. Deleted
        # objects, and children of deleted parents, look like paused ones and
        # are emitted as deletions.
        async for doc in _list_entities(
            client,
            model,
            advertiser_id,
            group,
            {"secondary_status": model.ALL_STATUS, **modified_filter},
            log,
        ):
            if in_window(doc):
                if doc.get_id() in deleted_ids or any(
                    getattr(doc, c.parent_id_field) in deleted_parents[c]
                    for c in model.CASCADES
                ):
                    doc.meta_ = model.Meta(op="d")
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
    invocation that were live and unchanged at the cutoff, newest window first,
    so that current objects arrive before history.

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
    which emits its latest state, including a deletion made during the walk. A
    parent's deletion during the walk leaves the child's `modify_time` alone,
    so a child emitted live and cascaded meanwhile is emitted again as a
    deletion.
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
        direct_ids, cascaded_ids = await _deleted_ids(
            client, model, advertiser_id, group, creation_filters, log
        )
        deleted_ids = direct_ids.union(*cascaded_ids.values())
        emitted: set[str] = set()

        # Every status, so a deletion mid-walk cannot shift a page.
        async for doc in _list_entities(
            client,
            model,
            advertiser_id,
            group,
            {**creation_filters, "secondary_status": model.ALL_STATUS},
            log,
        ):
            if doc.get_id() not in deleted_ids and doc.get_cursor() < cutoff:
                emitted.add(doc.get_id())
                yield doc

        for cascade in model.CASCADES:
            async for doc in _list_entities(
                client,
                model,
                advertiser_id,
                group,
                {**creation_filters, "secondary_status": cascade.status},
                log,
            ):
                if doc.get_id() in emitted:
                    doc.meta_ = model.Meta(op="d")
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


# `advertiser/info` rejects requests naming more than 100 advertisers. For one it
# can't read it fails the whole request, naming the advertiser, with 40001:
# TikTok's code for any missing permission.
ADVERTISER_INFO_BATCH_SIZE = 100
NO_PERMISSION = 40001


class InaccessibleAdvertiserError(TikTokAPIError):
    """TikTok can't read one of the requested advertisers' details."""


async def get_advertisers(
    client: TikTokClient,
    advertiser_ids: list[str],
    log: Logger,
) -> list[Advertiser]:
    """Account details for `advertiser_ids`, in advertiser id order."""
    ids = sorted(advertiser_ids)
    advertisers: list[Advertiser] = []
    for start in range(0, len(ids), ADVERTISER_INFO_BATCH_SIZE):
        batch = ids[start : start + ADVERTISER_INFO_BATCH_SIZE]
        try:
            data = await _get(
                client,
                "advertiser/info/",
                {"advertiser_ids": json.dumps(batch)},
                AdvertiserBatch,
                log,
            )
        except TikTokAPIError as err:
            if err.code != NO_PERMISSION or not any(i in err.message for i in batch):
                raise
            raise InaccessibleAdvertiserError(
                err.code,
                f"{err.message} Remove it from Advertiser IDs or, if that field is blank, enter the IDs of the advertisers to capture without it.",
            ) from err
        advertisers.extend(data.advertisers)
    return sorted(advertisers, key=lambda a: a.advertiser_id)


async def snapshot_advertisers(
    client: TikTokClient,
    advertiser_ids: list[str],
    log: Logger,
) -> AsyncGenerator[Advertiser, None]:
    """Every advertiser in scope, in advertiser id order so that an unchanged set
    of accounts produces an identical snapshot."""
    for advertiser in await get_advertisers(client, advertiser_ids, log):
        yield advertiser
