from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
import json
from logging import Logger
from typing import Any, AsyncGenerator, Mapping

from estuary_cdk.capture.common import LogCursor, PageCursor
from estuary_cdk.http import HTTPSession

from pydantic import BaseModel, Field

from .models import (
    Ad,
    AdGroup,
    Campaign,
    ENTITY_DATETIME_FORMAT,
    Image,
    Location,
    TikTokResource,
    TimestampedResource,
    Video,
)


PRODUCTION_API = "https://business-api.tiktok.com/open_api/v1.3"
# Sandbox accounts live on their own host, and a sandbox token is not valid against
# production (nor the reverse).
SANDBOX_API = "https://sandbox-ads.tiktok.com/open_api/v1.3"


def resolve_base_url(is_sandbox: bool) -> str:
    return SANDBOX_API if is_sandbox else PRODUCTION_API

# TikTok reports failures inside an HTTP 200 body: "These return codes take precedence over
# HTTP status codes in determining whether a call is successful." Every response must be
# dispatched on `code`, because the transport layer sees a success either way.
SUCCESS_CODE = 0

# The token is rejected or no longer authorizes the account being read.
AUTH_ERROR_CODES = frozenset(
    {
        40101,  # app id and secret do not match, or the auth code is invalid
        40102,  # access token has expired
        40104,  # access token is empty
        40105,  # access token is invalid or incorrect
        40106,  # core user is invalid: the token does not cover this advertiser
        40107,  # invalid refresh token (Marketing API tokens cannot be refreshed)
        40110,  # auth code has been cancelled, used, or expired
        40113,  # the developer app is blocked or does not exist
    }
)

# The credentials are valid but lack the permission the request needs.
PERMISSION_ERROR_CODES = frozenset(
    {
        40001,  # no permission for the operation
        40118,  # app or advertiser is not allowlisted for the feature
        40119,  # developer and advertiser are not in the same company
        40124,  # developer profile is not approved
        40125,  # developer lacks permission, or the fields are unsupported
    }
)

# The advertiser is gone; it should be dropped from the sync rather than retried.
ADVERTISER_GONE_CODES = frozenset(
    {
        40300,  # advertiser does not exist or has been deleted
        40301,  # advertiser cannot be matched
    }
)

# Throttling. `40133` is per-advertiser and per-path, and TikTok publishes no number for it.
THROTTLE_CODES = frozenset({40016, 40100, 40132, 40133})

# Set when a report covers more than 20,000 ads. TikTok then silently returns only the most
# recently created 20,000 and still answers with code 0, so this header is the only signal
# that the response is incomplete.
ADS_THROTTLE_HEADER = "X-Tt-Ads-Throttle"


class TikTokApiError(RuntimeError):
    """A non-zero `code` in an otherwise successful HTTP response."""

    def __init__(self, code: int, message: str, request_id: str, url: str):
        self.code = code
        self.message = message
        self.request_id = request_id
        self.url = url
        super().__init__(
            f"TikTok API returned code {code} for {url}: {message} "
            f"(request_id: {request_id})"
        )

    @property
    def is_auth_error(self) -> bool:
        return self.code in AUTH_ERROR_CODES

    @property
    def is_permission_error(self) -> bool:
        return self.code in PERMISSION_ERROR_CODES

    @property
    def is_advertiser_gone(self) -> bool:
        return self.code in ADVERTISER_GONE_CODES

    @property
    def is_throttled(self) -> bool:
        return self.code in THROTTLE_CODES


class Envelope(BaseModel, extra="allow"):
    """The wrapper TikTok returns around every response body."""

    code: int
    message: str
    # Diagnostic only — nothing branches on it, and it is the id TikTok support asks for.
    request_id: str = ""


class PageInfo(BaseModel, extra="allow"):
    page: int
    page_size: int
    total_number: int
    total_page: int


class AuthorizedAdvertiser(BaseModel, extra="allow"):
    advertiser_id: str
    advertiser_name: str


class AuthorizedAdvertisersResponse(Envelope):
    class Data(BaseModel, extra="allow"):
        # Aliased because TikTok names this "list", which would shadow the builtin.
        advertisers: list[AuthorizedAdvertiser] = Field(
            default_factory=list, alias="list"
        )

    data: Data | None = None


class Advertiser(BaseModel, extra="allow"):
    advertiser_id: str
    name: str
    # `timezone` carries a POSIX-style offset such as `Etc/GMT+8`, whose sign is inverted
    # relative to the usual reading (`Etc/GMT+8` is UTC-8). `display_timezone` is a real
    # IANA zone name, so report date arithmetic uses that instead.
    timezone: str
    display_timezone: str
    currency: str


class AdvertiserInfoResponse(Envelope):
    class Data(BaseModel, extra="allow"):
        advertisers: list[Advertiser] = Field(default_factory=list, alias="list")

    data: Data | None = None


def raise_for_code(response: Envelope, url: str) -> None:
    """Raise when TikTok signalled a failure inside a successful HTTP response."""
    if response.code != SUCCESS_CODE:
        raise TikTokApiError(response.code, response.message, response.request_id, url)


def warn_if_ads_truncated(
    headers: Mapping[str, Any],
    log: Logger,
    url: str,
    request_id: str,
) -> None:
    """Surface the silent 20,000-ad truncation that report responses apply.

    A report spanning more than 20,000 ads returns only the most recently created 20,000
    with an unchanged `code`, and which ads are dropped drifts as new ones are created.
    Without this the gap reads as a complete result.
    """
    truncation = headers.get(ADS_THROTTLE_HEADER)
    if not truncation:
        return

    log.warning(
        "TikTok truncated this response to the most recent 20,000 ads; older ads are missing",
        {
            "url": url,
            "detail": truncation,
            "request_id": request_id,
        },
    )


def json_param(value: Any) -> str:
    """Render a list or object parameter the way TikTok expects it in a query string."""
    return json.dumps(value, separators=(",", ":"))


async def request_envelope[EnvelopeT: Envelope](
    http: HTTPSession,
    log: Logger,
    url: str,
    model: type[EnvelopeT],
    params: dict[str, Any] | None = None,
) -> EnvelopeT:
    """Perform a request and parse TikTok's envelope, raising on an unsuccessful `code`."""
    response = model.model_validate_json(
        await http.request(log, url, params=params)
    )
    raise_for_code(response, url)

    return response


async def fetch_authorized_advertiser_ids(
    http: HTTPSession,
    base_url: str,
    app_id: str,
    secret: str,
    log: Logger,
) -> list[str]:
    """List every advertiser the credentials can read.

    The `advertiser_ids` returned when the access token was minted is only a snapshot:
    TikTok grants the app access to newly shared accounts without re-authorization, so the
    authoritative list has to be re-read rather than cached from the token exchange.
    """
    url = f"{base_url}/oauth2/advertiser/get/"
    response = await request_envelope(
        http,
        log,
        url,
        AuthorizedAdvertisersResponse,
        params={"app_id": app_id, "secret": secret},
    )

    if response.data is None:
        return []

    return [advertiser.advertiser_id for advertiser in response.data.advertisers]


async def fetch_advertisers(
    http: HTTPSession,
    base_url: str,
    advertiser_ids: list[str],
    log: Logger,
) -> list[Advertiser]:
    """Fetch account details, including the timezone report dates are expressed in."""
    url = f"{base_url}/advertiser/info/"
    response = await request_envelope(
        http,
        log,
        url,
        AdvertiserInfoResponse,
        params={"advertiser_ids": json_param(advertiser_ids)},
    )

    if response.data is None:
        return []

    return response.data.advertisers


class AdvertiserRegistry:
    """The advertiser accounts a capture covers.

    Resolution is deferred until a stream first reads, so building the resource list stays
    free of network access and discovery works without reaching the API.
    """

    def __init__(
        self,
        http: HTTPSession,
        base_url: str,
        configured_ids: list[str],
        app_id: str | None,
        secret: str | None,
    ):
        self._http = http
        self._base_url = base_url
        self._configured_ids = configured_ids
        self._app_id = app_id
        self._secret = secret
        self._ids: list[str] | None = None
        self._accounts: list[Advertiser] | None = None

    async def ids(self, log: Logger) -> list[str]:
        if self._ids is not None:
            return self._ids

        if self._configured_ids:
            self._ids = self._configured_ids
        elif self._app_id and self._secret:
            self._ids = await fetch_authorized_advertiser_ids(
                self._http, self._base_url, self._app_id, self._secret, log
            )
        else:
            self._ids = []

        return self._ids

    async def accounts(self, log: Logger) -> list[Advertiser]:
        if self._accounts is not None:
            return self._accounts

        ids = await self.ids(log)
        accounts: list[Advertiser] = []

        for batch_start in range(0, len(ids), ADVERTISER_INFO_BATCH_SIZE):
            batch = ids[batch_start : batch_start + ADVERTISER_INFO_BATCH_SIZE]
            accounts.extend(
                await fetch_advertisers(self._http, self._base_url, batch, log)
            )

        self._accounts = accounts
        return self._accounts


# `/advertiser/info/` documents no maximum, but TikTok rejects oversized id lists with code
# 40011 and every other id filter caps at 100.
ADVERTISER_INFO_BATCH_SIZE = 100


# --- Entity streams -------------------------------------------------------------------

@dataclass(frozen=True)
class BuyingTypeGroup:
    """Buying types that may be requested together, and any page ceiling that imposes."""

    types: list[str]
    page_size_cap: int | None = None


# Without a buying-type filter TikTok omits reservation entities from the ad-object endpoints,
# so they would be missing with no error to indicate it. RESERVATION_TOP_VIEW cannot be
# combined with any other value — TikTok answers `40002 "The value 'RESERVATION_TOP_VIEW'
# cannot be combined with other enum values for this parameter"` — so each group is swept as
# its own request and the results are unioned.
BUYING_TYPE_GROUPS = (
    BuyingTypeGroup(["AUCTION", "RESERVATION_RF"]),
    # TikTok documents a lower page ceiling for this buying type. An empty account cannot
    # distinguish a honoured page size from a silently clamped one, so the documented ceiling
    # is applied rather than assumed away.
    BuyingTypeGroup(["RESERVATION_TOP_VIEW"], page_size_cap=100),
)

# `/adgroup/get/` and `/ad/get/` default to `STATUS_NOT_DELETE`, which hides deleted objects.
# TikTok also removes old ads over time, so without this a long-lived account silently
# accumulates gaps.
ALL_STATUSES = "STATUS_ALL"

# TikTok suggests keeping creation-time filters within a six-month range.
BACKFILL_WINDOW = timedelta(days=180)


class PagedResponse(Envelope):
    class Data(BaseModel, extra="allow"):
        page_info: PageInfo | None = None

    data: Data | None = None


def to_api_datetime(value: datetime) -> str:
    """Render a datetime the way TikTok's entity time filters expect it (UTC)."""
    return value.astimezone(UTC).strftime(ENTITY_DATETIME_FORMAT)


def last_elapsed_second() -> datetime:
    """The most recent second that had fully elapsed, and so can no longer gain updates."""
    return datetime.now(tz=UTC).replace(microsecond=0) - timedelta(seconds=1)


async def _paginate(
    http: HTTPSession,
    base_url: str,
    log: Logger,
    model: type[TikTokResource],
    advertiser_id: str,
    request_params: dict[str, Any],
    page_size: int,
) -> AsyncGenerator[TikTokResource, None]:
    """Walk every page of one endpoint for one advertiser within a single invocation.

    The offset never crosses a checkpoint. TikTok sorts these endpoints by id descending and
    offers no ordering override, so an object created mid-walk shifts every later page; a
    resumed offset would skip whatever slid across the boundary.
    """
    url = f"{base_url}/{model.PATH}"
    page = 1

    while True:
        response = await request_envelope(
            http,
            log,
            url,
            PagedResponse,
            params={
                **request_params,
                "advertiser_id": advertiser_id,
                "page": page,
                "page_size": page_size,
            },
        )

        if response.data is None:
            return

        items = (response.data.model_extra or {}).get(model.ITEMS_KEY) or []
        for item in items:
            yield model.model_validate(
                item, context={"advertiser_id": advertiser_id}
            )

        page_info = response.data.page_info
        if page_info is None or page >= page_info.total_page:
            return

        page += 1


def _page_size(model: type[TikTokResource], group: BuyingTypeGroup) -> int:
    if group.page_size_cap is None:
        return model.MAX_PAGE_SIZE

    return min(model.MAX_PAGE_SIZE, group.page_size_cap)


def _entity_filters(
    group: BuyingTypeGroup,
    modified_after: datetime | None = None,
    created_between: tuple[datetime, datetime] | None = None,
) -> dict[str, Any]:
    filters: dict[str, Any] = {
        "buying_types": group.types,
        "primary_status": ALL_STATUSES,
    }

    if modified_after is not None:
        filters["modified_after"] = to_api_datetime(modified_after)

    if created_between is not None:
        start, end = created_between
        filters["creation_filter_start_time"] = to_api_datetime(start)
        filters["creation_filter_end_time"] = to_api_datetime(end)

    return {"filtering": json_param(filters)}


async def _fetch_modified(
    http: HTTPSession,
    base_url: str,
    model: type[TimestampedResource],
    registry: AdvertiserRegistry,
    supports_modified_after: bool,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    """Emit entities modified since the cursor.

                 log_cursor            horizon = last elapsed second
    ─────────────────┼────────────────────────┼─────▶ time (1s ticks)
                     │                        │
    modified_after ══[════════════════════════╪═════▶
    emitted ─────────(════════════════════════]
                     │                        └─ the present second is still
                     │                           in progress; its changes wait
                     │                           for the next poll's window
                     └─ excluded: already emitted by the previous poll

    `modified_after` is treated as inclusive because TikTok does not document its
    inclusivity; a re-read is collapsed by the collection key, whereas assuming exclusivity
    and being wrong would drop a document permanently. TikTok offers no upper time bound, so
    the horizon is applied client-side.
    """
    assert isinstance(log_cursor, datetime)

    horizon = last_elapsed_second()
    if horizon <= log_cursor:
        return

    for advertiser_id in await registry.ids(log):
        for group in BUYING_TYPE_GROUPS:
            request_params = _entity_filters(
                group,
                modified_after=log_cursor if supports_modified_after else None,
            )

            async for doc in _paginate(
                http,
                base_url,
                log,
                model,
                advertiser_id,
                request_params,
                _page_size(model, group),
            ):
                assert isinstance(doc, TimestampedResource)
                cursor = doc.get_cursor()

                if cursor <= log_cursor or cursor > horizon:
                    continue

                yield doc

    yield horizon


async def _backfill_created(
    http: HTTPSession,
    base_url: str,
    model: type[TimestampedResource],
    registry: AdvertiserRegistry,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    """Sweep historical entities one creation-time window at a time.

    Resume is keyed on creation time rather than a page offset. Creation time is immutable,
    so a closed past window holds a fixed set of objects: deleting one removes an entry
    without renumbering the rest, and the window can be re-walked identically after any gap.
    """
    assert isinstance(cutoff, datetime)

    if page is None:
        start = start_date
    else:
        assert isinstance(page, str)
        start = datetime.fromisoformat(page)

    if start >= cutoff:
        return

    end = min(start + BACKFILL_WINDOW, cutoff)

    for advertiser_id in await registry.ids(log):
        for group in BUYING_TYPE_GROUPS:
            request_params = _entity_filters(group, created_between=(start, end))

            async for doc in _paginate(
                http,
                base_url,
                log,
                model,
                advertiser_id,
                request_params,
                _page_size(model, group),
            ):
                assert isinstance(doc, TimestampedResource)
                yield doc

    if end >= cutoff:
        return

    yield end.isoformat()


async def fetch_campaigns(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    """Campaigns modified since the cursor.

    `/campaign/get/` exposes no modify-time filter — only a creation-time one, which cannot
    surface an edit to an older campaign — so the full list is re-read and narrowed here.
    """
    async for item in _fetch_modified(
        http, base_url, Campaign, registry, False, log, log_cursor
    ):
        yield item


async def backfill_campaigns(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    async for item in _backfill_created(
        http, base_url, Campaign, registry, start_date, log, page, cutoff
    ):
        yield item


async def fetch_ad_groups(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    """Ad groups modified since the cursor.

    Like campaigns, `/adgroup/get/` offers only a creation-time filter, so modification
    narrowing happens here.
    """
    async for item in _fetch_modified(
        http, base_url, AdGroup, registry, False, log, log_cursor
    ):
        yield item


async def backfill_ad_groups(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    async for item in _backfill_created(
        http, base_url, AdGroup, registry, start_date, log, page, cutoff
    ):
        yield item


async def fetch_ads(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TimestampedResource | LogCursor, None]:
    """Ads modified since the cursor, narrowed server-side.

    `/ad/get/` is the only ad-object endpoint carrying a `modified_after` filter, which
    matters most here because ads are the highest-cardinality of the three.
    """
    async for item in _fetch_modified(
        http, base_url, Ad, registry, True, log, log_cursor
    ):
        yield item


async def backfill_ads(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    start_date: datetime,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[TimestampedResource | PageCursor, None]:
    async for item in _backfill_created(
        http, base_url, Ad, registry, start_date, log, page, cutoff
    ):
        yield item


# --- Creative assets ------------------------------------------------------------------

# TikTok returns at most this many creative assets per advertiser, ordered by modify time,
# and offers no way to reach past it. Assets beyond the limit are unreachable.
CREATIVE_ASSET_LIMIT = 10_000


async def _fetch_creative_assets(
    http: HTTPSession,
    base_url: str,
    model: type[TikTokResource],
    registry: AdvertiserRegistry,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TikTokResource | LogCursor, None]:
    """Emit creative assets modified since the cursor, newest first.

    These endpoints carry no time filter, so the walk stops as soon as it reaches an asset at
    or behind the cursor — which the modify-time ordering makes sufficient. TikTok serves
    only the 10,000 most recently modified assets per advertiser, so an account holding more
    than that has older assets that no request can reach.
    """
    assert isinstance(log_cursor, datetime)

    horizon = last_elapsed_second()
    if horizon <= log_cursor:
        return

    for advertiser_id in await registry.ids(log):
        seen = 0

        async for doc in _paginate(
            http,
            base_url,
            log,
            model,
            advertiser_id,
            {"sort_field": "MODIFY_TIME", "sort_type": "DESC"},
            model.MAX_PAGE_SIZE,
        ):
            seen += 1
            cursor = doc.get_cursor()

            if cursor <= log_cursor:
                break

            if cursor <= horizon:
                yield doc

        if seen >= CREATIVE_ASSET_LIMIT:
            log.warning(
                "advertiser holds at least as many creative assets as TikTok will return; "
                "older assets cannot be captured",
                {
                    "advertiser_id": advertiser_id,
                    "stream": model.NAME,
                    "limit": CREATIVE_ASSET_LIMIT,
                },
            )

    yield horizon


async def fetch_videos(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TikTokResource | LogCursor, None]:
    """Videos modified since the cursor.

    There is no separate backfill: the first poll starts from the configured start date and
    walks back through every reachable asset, so history arrives on that sweep.
    """
    async for item in _fetch_creative_assets(http, base_url, Video, registry, log, log_cursor):
        yield item


async def fetch_images(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
    log_cursor: LogCursor,
) -> AsyncGenerator[TikTokResource | LogCursor, None]:
    """Images modified since the cursor. Backfill is subsumed, as for videos."""
    async for item in _fetch_creative_assets(http, base_url, Image, registry, log, log_cursor):
        yield item


# --- Snapshots ------------------------------------------------------------------------


async def snapshot_advertisers(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
) -> AsyncGenerator[dict[str, Any], None]:
    """Every advertiser account in scope. TikTok exposes no modification time for these."""
    for advertiser in await registry.accounts(log):
        yield advertiser.model_dump(mode="json")


async def snapshot_locations(
    http: HTTPSession,
    base_url: str,
    registry: AdvertiserRegistry,
    log: Logger,
) -> AsyncGenerator[dict[str, Any], None]:
    """Ad delivery regions available to each advertiser."""
    url = f"{base_url}/{Location.PATH}"

    for advertiser_id in await registry.ids(log):
        response = await request_envelope(
            http,
            log,
            url,
            PagedResponse,
            params={"advertiser_id": advertiser_id},
        )

        if response.data is None:
            continue

        for region in (response.data.model_extra or {}).get(Location.ITEMS_KEY) or []:
            yield {**region, "advertiser_id": advertiser_id}
