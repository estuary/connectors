import asyncio
import time
from dataclasses import dataclass, field
from enum import StrEnum
from logging import Logger
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

from estuary_cdk.http import BodyGeneratorFunction, Headers, HTTPError, HTTPSession


class Operation(StrEnum):
    SEARCH_CATALOG_ITEMS = "searchCatalogItems"
    SEARCH_ORDERS = "searchOrders"
    GET_MARKETPLACE_PARTICIPATIONS = "getMarketplaceParticipations"
    LIST_TRANSACTIONS = "listTransactions"
    GET_REPORTS = "getReports"
    CREATE_REPORT = "createReport"
    GET_REPORT = "getReport"
    GET_REPORT_DOCUMENT = "getReportDocument"


class Priority(StrEnum):
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


@dataclass(frozen=True)
class OperationLimit:
    rate: float  # requests per second
    burst: int


# Amazon's default usage plans. A selling account can be granted more, which
# Amazon reports per response in the `x-amzn-RateLimit-Limit` header.
DEFAULT_LIMITS: dict[Operation, OperationLimit] = {
    Operation.SEARCH_CATALOG_ITEMS: OperationLimit(rate=2, burst=2),
    Operation.SEARCH_ORDERS: OperationLimit(rate=0.0056, burst=20),
    Operation.GET_MARKETPLACE_PARTICIPATIONS: OperationLimit(rate=0.016, burst=15),
    Operation.LIST_TRANSACTIONS: OperationLimit(rate=0.5, burst=10),
    Operation.GET_REPORTS: OperationLimit(rate=0.0222, burst=10),
    Operation.CREATE_REPORT: OperationLimit(rate=0.0167, burst=15),
    Operation.GET_REPORT: OperationLimit(rate=2, burst=15),
    Operation.GET_REPORT_DOCUMENT: OperationLimit(rate=0.0167, burst=15),
}

# Report types whose createReport requests Amazon limits further, per
# seller, on top of createReport's usage plan.
REPORT_TYPE_LIMITS: dict[str, OperationLimit] = {
    # Three per five minutes. One every 100 seconds keeps any five minutes
    # at three; a burst of three would allow five after an idle spell.
    "GET_SALES_AND_TRAFFIC_REPORT": OperationLimit(rate=3 / 300, burst=1),
}

RATE_LIMIT_HEADER = "x-amzn-RateLimit-Limit"

# Tokens a backfill leaves in an operation's bucket, so incremental requests
# for the same operation don't wait behind a long backfill.
BACKFILL_RESERVE = 2


@dataclass
class TokenBucket:
    """
    A token bucket mirroring Amazon's per-operation usage plan: tokens refill
    at `rate` per second up to `burst`, and each request spends one.

    A backfill request is admitted only while more than the reserve would
    remain, so incremental requests are never starved by a backfill.

    The bucket starts with a single token rather than a full burst, because
    a restarted connector can't know how much of Amazon's bucket its previous
    run spent. A backfill's first request therefore waits for the reserve to
    refill, about six minutes at searchOrders' default rate.

    Only the first attempt of a request is paced here. Retries of a 429
    happen inside the CDK's HTTP session, which paces them with its own
    session-wide backoff.
    """

    limit: OperationLimit
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    rate: float = field(init=False)
    tokens: float = field(init=False)
    updated_at: float = field(init=False)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        self.rate = self.limit.rate
        self.tokens = 1.0
        self.updated_at = self.clock()

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(
            float(self.limit.burst), self.tokens + (now - self.updated_at) * self.rate
        )
        self.updated_at = now

    def reserve_for(self, priority: Priority) -> int:
        if priority == Priority.BACKFILL:
            return min(BACKFILL_RESERVE, self.limit.burst - 1)
        return 0

    async def acquire(self, priority: Priority) -> None:
        needed = 1.0 + self.reserve_for(priority)
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= needed:
                    self.tokens -= 1.0
                    return
                wait = (needed - self.tokens) / self.rate

            await self.sleep(wait)

    def set_rate(self, rate: float) -> bool:
        """Adopt the rate Amazon reports for this account. Returns whether it changed."""
        if rate <= 0 or rate == self.rate:
            return False
        self._refill()
        self.rate = rate
        return True


@dataclass
class SPAPIClient:
    """
    Issues SP-API requests, holding each one until its operation's token
    bucket admits it. Buckets are shared by every stream of the capture.
    """

    http: HTTPSession
    base_url: str
    limits: dict[Operation, OperationLimit] = field(
        default_factory=lambda: dict(DEFAULT_LIMITS)
    )
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    buckets: dict[Operation, TokenBucket] = field(init=False, default_factory=dict)
    report_type_buckets: dict[str, TokenBucket] = field(init=False, default_factory=dict)

    def bucket(self, operation: Operation) -> TokenBucket:
        if operation not in self.buckets:
            self.buckets[operation] = TokenBucket(
                self.limits[operation], clock=self.clock, sleep=self.sleep
            )
        return self.buckets[operation]

    def report_type_bucket(self, report_type: str) -> TokenBucket | None:
        """
        The bucket that paces createReport for `report_type` in addition to
        createReport's own, if Amazon limits that type further.
        """
        if report_type not in REPORT_TYPE_LIMITS:
            return None
        if report_type not in self.report_type_buckets:
            self.report_type_buckets[report_type] = TokenBucket(
                REPORT_TYPE_LIMITS[report_type], clock=self.clock, sleep=self.sleep
            )
        return self.report_type_buckets[report_type]

    async def get(
        self,
        log: Logger,
        operation: Operation,
        path: str,
        params: dict[str, Any],
        priority: Priority,
    ) -> tuple[Headers, BodyGeneratorFunction]:
        return await self._request(log, operation, path, priority, params=params)

    async def post(
        self,
        log: Logger,
        operation: Operation,
        path: str,
        json: dict[str, Any],
        priority: Priority,
        pace: TokenBucket | None = None,
    ) -> tuple[Headers, BodyGeneratorFunction]:
        return await self._request(
            log, operation, path, priority, pace, method="POST", json=json
        )

    async def _request(
        self,
        log: Logger,
        operation: Operation,
        path: str,
        priority: Priority,
        pace: TokenBucket | None = None,
        **kwargs: Any,
    ) -> tuple[Headers, BodyGeneratorFunction]:
        """
        Sends a request once its operation's bucket admits it, and then
        `pace`, if given, so requests leave in the order `pace` admits them.
        """
        await self.bucket(operation).acquire(priority)
        if pace is not None:
            await pace.acquire(priority)
        headers, body = await self.http.request_stream(
            log, f"{self.base_url}{path}", **kwargs
        )
        self._adopt_reported_rate(log, operation, headers)
        return headers, body

    def _adopt_reported_rate(
        self, log: Logger, operation: Operation, headers: Headers
    ) -> None:
        bucket = self.bucket(operation)
        reported = headers.get(RATE_LIMIT_HEADER)
        if reported is not None:
            try:
                rate = float(reported)
            except ValueError:
                log.warning(
                    "unparseable rate limit header",
                    {"operation": operation, "value": reported},
                )
            else:
                if bucket.set_rate(rate):
                    log.info(
                        "adopted the rate limit Amazon reports for this operation",
                        {"operation": operation, "rate": rate},
                    )

    async def download(
        self, log: Logger, url: str, name: str
    ) -> tuple[Headers, BodyGeneratorFunction]:
        """
        Downloads a pre-signed URL, such as a report document's, which `name`
        identifies in errors. The URL carries its own authorization, so the
        access token is not sent, and no SP-API usage plan applies.

        A failed download's error leaves out the URL's query and the
        response body, which can carry its signature and credentials.
        """
        # The CDK's connection retries log the full URL before any error
        # reaches here, and can't be told not to. Only the URL's expiry,
        # five minutes after it is issued, limits that exposure.
        try:
            return await self.http.request_stream(log, url, with_token=False)
        except HTTPError as err:
            parts = urlsplit(url)
            raise HTTPError(
                f"Downloading {name} from {parts.scheme}://{parts.netloc}{parts.path} "
                f"failed with HTTP status {err.code}. The pre-signed URL and the "
                "response are left out of this message.",
                err.code,
            ) from None
