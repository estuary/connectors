import json
import logging
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Any, AsyncIterator

import pytest
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError
from pydantic import ValidationError as PydanticValidationError

from source_amazon_selling_partner import api, resources
from source_amazon_selling_partner.api import (
    PaginationTokenRejected,
    backfill_orders,
    fetch_orders,
)
from source_amazon_selling_partner.client import SPAPIClient
from source_amazon_selling_partner.models import EndpointConfig, Order

from .fake_amazon import FakeAmazon, order

LOG = logging.getLogger("test")
NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
MARKETPLACES = ["ATVPDKIKX0DER"]


class FakeTime:
    def __init__(self) -> None:
        self.t = 0.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += max(seconds, 1e-3)


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api, "now", lambda: NOW)


def client_for(fake: FakeAmazon) -> SPAPIClient:
    time = FakeTime()
    return SPAPIClient(http=fake, base_url="https://fake", clock=time.clock, sleep=time.sleep)  # type: ignore[arg-type]


async def collect(gen: AsyncIterator[Any]):
    return [item async for item in gen]


@pytest.mark.asyncio
async def test_fetch_orders_pages_through_window_and_advances_cursor():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[
            order("a", NOW - timedelta(days=3), cursor + timedelta(seconds=1)),
            order("b", NOW - timedelta(days=3), cursor + timedelta(minutes=5)),
            order("c", NOW - timedelta(days=3), cursor + timedelta(minutes=30)),
            # Inside the overlap: re-emitted, harmless for a keyed collection.
            order("d", NOW - timedelta(days=3), cursor - timedelta(minutes=5)),
            # Before the overlap.
            order("e", NOW - timedelta(days=3), cursor - timedelta(minutes=20)),
        ],
    )

    out = await collect(
        fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(minutes=10), LOG, cursor)
    )

    docs = [o.orderId for o in out if isinstance(o, Order)]
    assert sorted(docs) == ["a", "b", "c", "d"]
    horizon = NOW - api.SEARCH_LAG
    assert out[-1] == horizon
    assert len(fake.requests) == 2  # page size 2, four orders

    first = fake.requests[0]
    assert first["lastUpdatedAfter"] == "2026-03-01T10:50:01Z"
    assert first["lastUpdatedBefore"] == "2026-03-01T11:57:30Z"
    assert first["marketplaceIds"] == "ATVPDKIKX0DER"
    assert "BUYER" not in first["includedData"]
    assert "RECIPIENT" not in first["includedData"]
    assert fake.requests[1]["paginationToken"].startswith("token-")


@pytest.mark.asyncio
async def test_fetch_orders_window_is_capped_by_window_size():
    cursor = NOW - timedelta(days=3)
    fake = FakeAmazon(now=NOW, orders=[])
    out = await collect(
        fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor)
    )
    assert out == [cursor + timedelta(days=1)]


@pytest.mark.asyncio
async def test_fetch_orders_waits_when_caught_up():
    fake = FakeAmazon(now=NOW, orders=[])
    cursor = NOW - api.SEARCH_LAG - api.MIN_INCREMENTAL_WINDOW + timedelta(seconds=1)
    out = await collect(
        fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor)
    )
    assert out == []
    assert fake.requests == []


@pytest.mark.parametrize("overlap", [timedelta(), timedelta(minutes=10)])
@pytest.mark.asyncio
async def test_seam_between_backfill_and_incremental_misses_nothing(monkeypatch: pytest.MonkeyPatch, overlap: timedelta):
    # Build the resource's real initial state at a known time.
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff + timedelta(microseconds=250_000)))
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(cutoff - timedelta(minutes=1)))
    fake = FakeAmazon(
        now=NOW,
        orders=[
            order("before", cutoff - timedelta(seconds=1), cutoff - timedelta(seconds=1)),
            order("at", cutoff, cutoff),
            order("after", cutoff + timedelta(seconds=1), cutoff + timedelta(seconds=1)),
        ],
        page_size=10,
    )
    client = client_for(fake)
    state = resources.orders(LOG, client, config).initial_state
    assert state.backfill.cutoff == cutoff

    backfilled = await collect(
        backfill_orders(client, MARKETPLACES, cutoff - timedelta(minutes=1), timedelta(days=1), LOG, state.backfill.next_page, state.backfill.cutoff)
    )
    incremental = await collect(
        fetch_orders(client, MARKETPLACES, timedelta(days=1), overlap, LOG, state.inc[resources.REALTIME].cursor)
    )

    seen = {o.orderId for o in backfilled + incremental if isinstance(o, Order)}
    assert seen == {"before", "at", "after"}
    # The backfill covers creation up to cutoff−1s, incremental updates from cutoff.
    assert [o.orderId for o in backfilled if isinstance(o, Order)] == ["before"]


@pytest.mark.asyncio
async def test_backfill_resumes_from_the_saved_start_when_a_blank_start_date_has_moved():
    # A blank start_date is 30 days before each start, so a restart the next
    # day passes a later one; the saved page keeps the first.
    config = make_config(["ATVPDKIKX0DER"])
    saved = resources.orders(LOG, client_for(FakeAmazon(now=NOW, orders=[])), config).initial_state.backfill
    assert saved.next_page == api.ceil_to_tick(config.start_date).isoformat()

    first_start = datetime(2026, 1, 1, tzinfo=UTC)
    fake = FakeAmazon(now=NOW, orders=[order("first-day", first_start, first_start)], page_size=10)
    out = await collect(
        backfill_orders(client_for(fake), MARKETPLACES, first_start + timedelta(days=1), timedelta(days=1), LOG, first_start.isoformat(), NOW - timedelta(hours=1))
    )
    assert [o.orderId for o in out if isinstance(o, Order)] == ["first-day"]
    assert fake.requests[0]["createdAfter"] == "2026-01-01T00:00:00Z"


class FrozenDatetime:
    """Stands in for the `datetime` class in resources, freezing now()."""

    def __init__(self, at: datetime) -> None:
        self.at = at

    def now(self, tz: tzinfo | None = None):
        return self.at


def make_config(marketplaces: list[str], **extra: Any) -> EndpointConfig:
    return EndpointConfig.model_validate(
        {
            "credentials": {"credentials_title": "Login with Amazon", "client_id": "x", "client_secret": "y", "refresh_token": "z"},
            "region": "North America",
            "marketplace_ids": marketplaces,
            **extra,
        }
    )


@pytest.mark.asyncio
async def test_backfill_walks_windows_with_inclusive_bounds():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    cutoff = datetime(2026, 1, 3, 12, tzinfo=UTC)
    fake = FakeAmazon(
        now=NOW,
        orders=[
            order("d1-start", start, start),
            order("d1-end", start + timedelta(days=1) - timedelta(seconds=1), NOW - timedelta(hours=3)),
            order("d2-start", start + timedelta(days=1), start + timedelta(days=1)),
            order("past-cutoff", cutoff, cutoff),
        ],
        page_size=10,
    )
    client = client_for(fake)

    page = None
    emitted: list[str] = []
    pages: list[str] = []
    while True:
        out = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, page, cutoff))
        emitted += [o.orderId for o in out if isinstance(o, Order)]
        cursors = [o for o in out if not isinstance(o, Order)]
        if not cursors:
            break
        page = cursors[-1]
        pages.append(page)

    assert emitted == ["d1-start", "d1-end", "d2-start"]
    assert pages == ["2026-01-02T00:00:00Z|172800.0|86400.0"]
    assert fake.requests[0]["createdAfter"] == "2026-01-01T00:00:00Z"
    assert fake.requests[0]["createdBefore"] == "2026-01-01T23:59:59Z"
    assert fake.requests[-1]["createdBefore"] == "2026-01-03T11:59:59Z"


@pytest.mark.asyncio
async def test_backfill_waits_out_a_recent_cutoff(monkeypatch: pytest.MonkeyPatch):
    cutoff = NOW - timedelta(seconds=30)
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        monkeypatch.setattr(api, "now", lambda: NOW + timedelta(seconds=seconds))

    monkeypatch.setattr(api.asyncio, "sleep", fake_sleep)
    fake = FakeAmazon(now=NOW + timedelta(minutes=5), orders=[])

    await collect(backfill_orders(client_for(fake), MARKETPLACES, cutoff - timedelta(hours=1), timedelta(days=1), LOG, None, cutoff))
    assert slept and slept[0] == pytest.approx((cutoff - timedelta(seconds=1) + api.SEARCH_LAG - NOW).total_seconds())


@pytest.mark.parametrize("span", [timedelta(days=2), timedelta(minutes=90)])
@pytest.mark.parametrize("backfill", [True, False])
@pytest.mark.asyncio
async def test_rejected_token_retries_halved_orders_window(backfill: bool, span: timedelta, caplog: pytest.LogCaptureFixture):
    start = NOW - timedelta(days=3)
    window_size = span + api.TICK if backfill and span == timedelta(minutes=90) else span
    end = start + window_size - (api.TICK if backfill else timedelta())
    smaller_end = start + (
        timedelta(hours=1) if span == timedelta(minutes=90)
        else timedelta(days=1) - (api.TICK if backfill else timedelta())
    )
    fake = FakeAmazon(
        now=NOW,
        orders=[order(str(i), start + timedelta(minutes=i), start + timedelta(minutes=i)) for i in range(1, 4)],
        expire_token_once=True,
    )

    with caplog.at_level(logging.WARNING, logger=LOG.name):
        if backfill:
            out = await collect(backfill_orders(client_for(fake), MARKETPLACES, start, window_size, LOG, None, NOW - timedelta(hours=1)))
        else:
            out = await collect(fetch_orders(client_for(fake), MARKETPLACES, span, timedelta(minutes=10), LOG, start))

    assert [o.orderId for o in out if isinstance(o, Order)] == ["1", "2", "1", "2", "3"]
    assert out[-1] == (f"{api.to_iso(smaller_end + api.TICK)}|{(smaller_end + api.TICK - start).total_seconds()}|{window_size.total_seconds()}" if backfill else smaller_end)
    assert len(fake.requests) == 4
    lower, upper = ("createdAfter", "createdBefore") if backfill else ("lastUpdatedAfter", "lastUpdatedBefore")
    assert fake.requests[0][upper] == api.to_iso(end)
    assert "paginationToken" in fake.requests[1]
    assert "paginationToken" not in fake.requests[2]
    assert fake.requests[2][lower] == fake.requests[0][lower]
    assert fake.requests[2][upper] == api.to_iso(smaller_end)
    assert "paginationToken" in fake.requests[3]
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert caplog.records[0].args == {"window_start": start, "window_end": end, "new_window_end": smaller_end}


@pytest.mark.parametrize("backfill", [True, False])
@pytest.mark.parametrize("span", [timedelta(hours=8, seconds=7), timedelta(minutes=90), timedelta(hours=1), timedelta(minutes=30)])
@pytest.mark.asyncio
async def test_rejected_tokens_shrink_orders_window_until_floor(backfill: bool, span: timedelta, caplog: pytest.LogCaptureFixture):
    start = NOW - timedelta(days=3)
    fake = FakeAmazon(
        now=NOW,
        orders=[order(str(i), start + timedelta(minutes=i), start + timedelta(minutes=i)) for i in range(1, 4)],
        expired_tokens=True,
    )
    expected_ends = [start + span]
    if span == timedelta(minutes=90):
        expected_ends.append(start + timedelta(hours=1))
    elif span > timedelta(hours=1):
        expected_ends += [
            start + timedelta(hours=4, seconds=3),
            start + timedelta(hours=2, seconds=1),
            start + timedelta(hours=1),
        ]

    with caplog.at_level(logging.WARNING, logger=LOG.name):
        with pytest.raises(PaginationTokenRejected, match="retries with smaller windows down to one hour"):
            if backfill:
                await collect(backfill_orders(client_for(fake), MARKETPLACES, start, span + api.TICK, LOG, None, NOW - timedelta(hours=1)))
            else:
                await collect(fetch_orders(client_for(fake), MARKETPLACES, span, timedelta(), LOG, start))

    upper = "createdBefore" if backfill else "lastUpdatedBefore"
    assert [r[upper] for r in fake.requests if "paginationToken" not in r] == [api.to_iso(end) for end in expected_ends]
    assert len(fake.requests) == 2 * len(expected_ends)
    assert len(caplog.records) == len(expected_ends) - 1


@pytest.mark.asyncio
async def test_backfill_rejected_token_in_final_window_resumes_to_cutoff():
    cutoff = NOW - timedelta(hours=1)
    start = cutoff - timedelta(hours=4)
    smaller_end = start + timedelta(hours=2) - api.TICK
    fake = FakeAmazon(
        now=NOW,
        orders=[
            order("1", start + timedelta(minutes=1), start),
            order("2", start + timedelta(minutes=2), start),
            order("3", smaller_end, start),
            order("rest-start", smaller_end + api.TICK, start),
            order("rest-end", cutoff - api.TICK, start),
            order("at-cutoff", cutoff, cutoff),
        ],
        expire_token_once=True,
    )
    client = client_for(fake)
    window_size = timedelta(days=1)

    out = await collect(backfill_orders(client, MARKETPLACES, start, window_size, LOG, None, cutoff))

    assert [o.orderId for o in out if isinstance(o, Order)] == ["1", "2", "1", "2", "3"]
    assert [o for o in out if not isinstance(o, Order)] == [f"{api.to_iso(smaller_end + api.TICK)}|7200.0|86400.0"]
    assert len(fake.requests) == 4
    assert fake.requests[0]["createdBefore"] == api.to_iso(cutoff - api.TICK)
    assert "paginationToken" in fake.requests[1]
    assert "paginationToken" not in fake.requests[2]
    assert fake.requests[2]["createdAfter"] == api.to_iso(start)
    assert fake.requests[2]["createdBefore"] == api.to_iso(smaller_end)

    rest = await collect(backfill_orders(client, MARKETPLACES, start, window_size, LOG, out[-1], cutoff))

    assert [o.orderId for o in rest if isinstance(o, Order)] == ["rest-start", "rest-end"]
    assert [o for o in rest if not isinstance(o, Order)] == []
    assert len(fake.requests) == 5
    assert fake.requests[4]["createdAfter"] == api.to_iso(smaller_end + api.TICK)
    assert fake.requests[4]["createdBefore"] == api.to_iso(cutoff - api.TICK)


@pytest.mark.asyncio
async def test_expired_pagination_token_raises_clear_error():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[order(str(i), cursor, cursor + timedelta(minutes=i)) for i in range(1, 4)],
        expired_tokens=True,
    )
    with pytest.raises(PaginationTokenRejected, match="Window Size"):
        await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))


@pytest.mark.asyncio
async def test_token_refresh_failure_mid_pagination_is_not_blamed_on_page_token():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(
        now=NOW,
        orders=[order(str(i), cursor, cursor + timedelta(minutes=i)) for i in range(1, 4)],
        refresh_fails_on_page=2,
    )
    with pytest.raises(HTTPError) as exc:
        await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))
    assert not isinstance(exc.value, PaginationTokenRejected)


@pytest.mark.asyncio
async def test_response_without_orders_array_fails_instead_of_advancing():
    cursor = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[], orders_key="results")
    with pytest.raises(PydanticValidationError):
        await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))


@pytest.mark.asyncio
async def test_a_search_amazon_cut_short_ends_the_window_where_amazon_stopped():
    updated = NOW - timedelta(minutes=30)
    fake = FakeAmazon(
        now=NOW,
        orders=[order("a", updated - timedelta(days=1), updated)],
        echoed_bound_shortfall=timedelta(minutes=5),
    )

    items = await collect(
        fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, NOW - timedelta(hours=1))
    )
    requested = datetime.fromisoformat(fake.requests[-1]["lastUpdatedBefore"])
    assert [i.orderId for i in items if isinstance(i, Order)] == ["a"]
    assert items[-1] == requested - timedelta(minutes=5)

    start = NOW - timedelta(days=2)
    items = await collect(
        backfill_orders(client_for(fake), MARKETPLACES, start, timedelta(days=1), LOG, None, NOW - timedelta(hours=1))
    )
    requested = datetime.fromisoformat(fake.requests[-1]["createdBefore"])
    assert requested == start + timedelta(days=1) - api.TICK
    assert items[-1].split("|")[0] == api.to_iso(requested - timedelta(minutes=5) + api.TICK)


@pytest.mark.asyncio
async def test_a_search_amazon_ends_before_the_window_start_fails_instead_of_advancing():
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=timedelta(days=3))

    with pytest.raises(RuntimeError, match="updated only up to"):
        await collect(
            fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, NOW - timedelta(hours=1))
        )
    with pytest.raises(RuntimeError, match="created only up to"):
        await collect(
            backfill_orders(
                client_for(fake), MARKETPLACES, NOW - timedelta(days=2), timedelta(days=1), LOG, None, NOW - timedelta(hours=1)
            )
        )


@pytest.mark.asyncio
async def test_an_echo_at_the_window_edge_raises_or_advances_one_tick():
    # Incremental: the cursor is already covered, so an echo equal to it is no progress.
    cursor = NOW - timedelta(hours=1)
    horizon = api.floor_to_tick(NOW - api.SEARCH_LAG)
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=horizon - cursor)
    with pytest.raises(RuntimeError, match="not past the window start"):
        await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=horizon - cursor - api.TICK)
    items = await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))
    assert items[-1] == cursor + api.TICK

    # Backfill: window_start is inclusive, so an echo equal to it covers one tick.
    start = NOW - timedelta(days=2)
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=timedelta(days=1))
    with pytest.raises(RuntimeError, match="before the window start"):
        await collect(backfill_orders(client_for(fake), MARKETPLACES, start, timedelta(days=1), LOG, None, NOW - timedelta(hours=1)))
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=timedelta(days=1) - api.TICK)
    items = await collect(backfill_orders(client_for(fake), MARKETPLACES, start, timedelta(days=1), LOG, None, NOW - timedelta(hours=1)))
    assert items[-1].split("|")[0] == api.to_iso(start + api.TICK)


@pytest.mark.asyncio
async def test_a_short_echo_on_an_early_page_ends_the_window():
    updated = NOW - timedelta(minutes=30)
    fake = FakeAmazon(
        now=NOW,
        orders=[order(f"o{i}", updated - timedelta(days=1), updated) for i in range(3)],
        page_size=1,
        echoed_bound_shortfall=timedelta(minutes=5),
    )
    real_respond = fake._respond

    def first_page_only(body):
        if "pagination" not in body:
            body.pop("lastUpdatedBefore", None)
        return real_respond(body)

    fake._respond = first_page_only
    cursor = NOW - timedelta(hours=1)
    items = await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))
    assert items[-1] == api.floor_to_tick(NOW - api.SEARCH_LAG) - timedelta(minutes=5)


@pytest.mark.asyncio
async def test_a_fixed_echo_shortfall_stops_the_backfill_at_its_last_window():
    # Known limit: if Amazon always echoes a bound short of the request, the last
    # window can never be confirmed. The first-contact probe checks the echo first.
    start = NOW - timedelta(days=2)
    cutoff = NOW - timedelta(hours=1)
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=timedelta(minutes=5))
    page = None
    with pytest.raises(RuntimeError, match="before the window start"):
        for _ in range(10):
            items = await collect(backfill_orders(client_for(fake), MARKETPLACES, start, timedelta(days=3), LOG, page, cutoff))
            page = items[-1]
    assert isinstance(page, str)
    assert page.split("|")[0] == api.to_iso(cutoff - api.TICK - timedelta(minutes=5) + api.TICK)


@pytest.mark.asyncio
async def test_a_fractional_echo_rounds_down_so_the_next_window_reads_again():
    cursor = NOW - timedelta(hours=1)
    horizon = api.floor_to_tick(NOW - api.SEARCH_LAG)
    fake = FakeAmazon(now=NOW, orders=[], echoed_bound_shortfall=timedelta(minutes=5, milliseconds=300))
    items = await collect(fetch_orders(client_for(fake), MARKETPLACES, timedelta(days=1), timedelta(), LOG, cursor))
    assert items[-1] == horizon - timedelta(minutes=5, seconds=1)


@pytest.mark.asyncio
async def test_backfill_rounds_fractional_start_up():
    start = datetime(2026, 1, 1, 12, 0, 0, 500_000, tzinfo=UTC)
    fake = FakeAmazon(now=NOW, orders=[order("too-early", start.replace(microsecond=0), start)], page_size=10)
    out = await collect(
        backfill_orders(client_for(fake), MARKETPLACES, start, timedelta(days=1), LOG, None, datetime(2026, 1, 2, tzinfo=UTC))
    )
    assert [o for o in out if isinstance(o, Order)] == []
    assert fake.requests[0]["createdAfter"] == "2026-01-01T12:00:01Z"


@pytest.mark.asyncio
async def test_validate_credentials_checks_marketplaces():
    class Http:
        token_source = None

    fake = FakeAmazon(now=NOW, orders=[])

    def config(marketplaces: list[str]) -> EndpointConfig:
        return EndpointConfig.model_validate(
            {
                "credentials": {"credentials_title": "Login with Amazon", "client_id": "x", "client_secret": "y", "refresh_token": "z"},
                "region": "North America",
                "marketplace_ids": marketplaces,
            }
        )

    http = Http()
    http.request_stream = fake.request_stream  # type: ignore[attr-defined]
    await resources.validate_credentials(LOG, http, config(["ATVPDKIKX0DER"]))  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="A1F83G8C2ARO7P"):
        await resources.validate_credentials(LOG, http, config(["A1F83G8C2ARO7P"]))  # type: ignore[arg-type]


# Login with Amazon answers a wrong or revoked refresh token with a 400.
@pytest.mark.parametrize(
    "code,expected", [(400, ValidationError), (401, ValidationError), (403, ValidationError), (503, HTTPError)]
)
@pytest.mark.asyncio
async def test_validate_credentials_only_reports_auth_failures_as_config_errors(code: int, expected: type[Exception]):
    class Http:
        token_source = None

        async def request_stream(self, log: logging.Logger, url: str, **kwargs: Any):
            raise HTTPError(f"status {code}", code)

    with pytest.raises(expected):
        await resources.validate_credentials(LOG, Http(), make_config(["ATVPDKIKX0DER"]))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_lwa_refresh_sends_refresh_grant_and_raw_token_header():
    calls = []

    class Session:
        async def request(self, log: logging.Logger, url: str, method: str = "GET", headers: dict[str, str] | None = None, form: dict[str, str] | None = None, with_token: bool = True, **kwargs: Any):
            calls.append({"url": url, "method": method, "form": form, "with_token": with_token})
            return b'{"access_token": "Atza|abc", "token_type": "bearer", "expires_in": 3600}'

    http: Any = type("Http", (), {"token_source": None})()
    resources._client(http, make_config(["ATVPDKIKX0DER"]))  # type: ignore[arg-type]
    token_source = http.token_source

    token_type, token = await token_source.fetch_token(LOG, Session())  # type: ignore[arg-type]

    assert token == "Atza|abc"
    assert token_source.authorization_header == "x-amz-access-token"
    assert calls == [
        {
            "url": "https://api.amazon.com/auth/o2/token",
            "method": "POST",
            "form": {"grant_type": "refresh_token", "client_id": "x", "client_secret": "y", "refresh_token": "z"},
            "with_token": False,
        }
    ]


def test_config_rejects_repeated_marketplaces():
    with pytest.raises(ValueError, match="must not repeat"):
        EndpointConfig.model_validate(
            {
                "credentials": {"credentials_title": "Login with Amazon", "client_id": "x", "client_secret": "y", "refresh_token": "z"},
                "region": "Europe",
                "marketplace_ids": ["A1F83G8C2ARO7P", "A1F83G8C2ARO7P"],
            }
        )


def emitted_last_updated_time(value: str) -> str:
    return json.loads(Order.model_validate({**order("o1", NOW, NOW), "lastUpdatedTime": value}).model_dump_json())[
        "lastUpdatedTime"
    ]


@pytest.mark.parametrize(
    "older, newer",
    [
        ("2026-03-01T10:00:00Z", "2026-03-01T10:00:01Z"),
        # An offset that sorts above a later UTC time as written.
        ("2026-03-01T11:00:00+02:00", "2026-03-01T10:00:00Z"),
        # A fraction that sorts below the whole second as written.
        ("2026-03-01T10:00:00Z", "2026-03-01T10:00:00.5Z"),
    ],
)
def test_orders_collection_keeps_the_latest_copy_whatever_the_commit_order(older: str, newer: str):
    # The backfill can commit an older copy of an order after the incremental
    # task committed a newer one. The non-delete reduction keeps the copy with
    # the greater lastUpdatedTime, which Flow compares as a string.
    schema = Order.model_json_schema(mode="serialization")
    assert schema["else"] == {"reduce": {"strategy": "maximize", "key": ["/lastUpdatedTime"]}}
    assert schema["properties"]["lastUpdatedTime"] == {"format": "date-time", "title": "Lastupdatedtime", "type": "string"}

    assert emitted_last_updated_time(older) < emitted_last_updated_time(newer)
    assert emitted_last_updated_time("2026-03-01T11:00:00.5+01:00") == "2026-03-01T10:00:00.500000Z"


@pytest.mark.asyncio
@pytest.mark.parametrize("configured,expected_days", [(1, [1, 2, 4, 8, 16, 30, 30]), (40, [40, 40])])
async def test_backfill_grows_single_page_windows_and_caps(configured: int, expected_days: list[int]):
    start = NOW - timedelta(days=400)
    cutoff = NOW - timedelta(days=1)
    fake = FakeAmazon(now=NOW, orders=[])
    client = client_for(fake)
    page = None
    for days in expected_days:
        out = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=configured), LOG, page, cutoff))
        request = fake.requests[-1]
        lo = datetime.fromisoformat(request["createdAfter"])
        hi = datetime.fromisoformat(request["createdBefore"])
        assert hi + api.TICK - lo == timedelta(days=days)
        assert isinstance(out[-1], str)
        page = out[-1]


@pytest.mark.asyncio
async def test_backfill_legacy_cursor_uses_configured_size():
    start = NOW - timedelta(days=20)
    fake = FakeAmazon(now=NOW, orders=[])
    out = await collect(backfill_orders(client_for(fake), MARKETPLACES, start + timedelta(days=1), timedelta(days=3), LOG, api.to_iso(start), NOW - timedelta(days=1)))
    assert fake.requests[0]["createdAfter"] == api.to_iso(start)
    assert fake.requests[0]["createdBefore"] == api.to_iso(start + timedelta(days=3) - api.TICK)
    assert out == [f"{api.to_iso(start + timedelta(days=3))}|518400.0|259200.0"]


@pytest.mark.asyncio
async def test_backfill_dense_window_holds_size_then_sparse_window_grows():
    start = NOW - timedelta(days=20)
    fake = FakeAmazon(now=NOW, orders=[order(str(i), start, start) for i in range(Order.PAGE_SIZE // 2 + 1)], page_size=2)
    client = client_for(fake)
    out = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, f"{api.to_iso(start)}|345600.0", NOW - timedelta(days=1)))
    assert len(fake.requests) == 26
    assert out[-1] == f"{api.to_iso(start + timedelta(days=4))}|345600.0|86400.0"
    out = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, out[-1], NOW - timedelta(days=1)))
    assert fake.requests[-1]["createdBefore"] == api.to_iso(start + timedelta(days=8) - api.TICK)
    assert out[-1] == f"{api.to_iso(start + timedelta(days=8))}|691200.0|86400.0"


@pytest.mark.asyncio
@pytest.mark.parametrize("successful_pages", [1, 2])
async def test_backfill_carries_shrunk_size_before_growing_again(successful_pages: int):
    start = NOW - timedelta(days=20)
    fake = FakeAmazon(
        now=NOW,
        orders=[order("a", start, start), order("b", start, start), order("c", start + timedelta(days=3), start)],
        expire_token_once=True,
        page_size=2 if successful_pages == 1 else 1,
    )
    client = client_for(fake)
    cutoff = NOW - timedelta(days=1)
    out = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=4), LOG, None, cutoff))
    assert out[-1] == f"{api.to_iso(start + timedelta(days=2))}|172800.0|345600.0"
    before = len(fake.requests)
    out = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=4), LOG, out[-1], cutoff))
    assert fake.requests[before]["createdAfter"] == api.to_iso(start + timedelta(days=2))
    assert fake.requests[before]["createdBefore"] == api.to_iso(start + timedelta(days=4) - api.TICK)
    assert out[-1] == f"{api.to_iso(start + timedelta(days=4))}|345600.0|345600.0"


@pytest.mark.asyncio
async def test_backfill_rejection_after_growth_shrinks_carried_size():
    start = NOW - timedelta(days=20)
    cutoff = NOW - timedelta(days=1)
    busy = start + timedelta(days=1, hours=1)
    fake = FakeAmazon(now=NOW, orders=[order(str(i), busy, busy) for i in range(3)])
    client = client_for(fake)
    first = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, None, cutoff))
    assert first[-1] == f"{api.to_iso(start + timedelta(days=1))}|172800.0|86400.0"
    fake.expire_token_once = True
    second = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, first[-1], cutoff))
    assert second[-1] == f"{api.to_iso(start + timedelta(days=2))}|86400.0|86400.0"
    before = len(fake.requests)
    await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, second[-1], cutoff))
    request = fake.requests[before]
    assert datetime.fromisoformat(request["createdBefore"]) + api.TICK - datetime.fromisoformat(request["createdAfter"]) == timedelta(days=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_size", [timedelta(hours=1), timedelta(days=3)])
async def test_backfill_changed_window_size_resets_carried_size(changed_size: timedelta):
    start = NOW - timedelta(days=20)
    cutoff = NOW - timedelta(days=1)
    fake = FakeAmazon(now=NOW, orders=[])
    client = client_for(fake)
    first = await collect(backfill_orders(client, MARKETPLACES, start, timedelta(days=1), LOG, None, cutoff))
    before = len(fake.requests)
    out = await collect(backfill_orders(client, MARKETPLACES, start, changed_size, LOG, first[-1], cutoff))
    lo = start + timedelta(days=1)
    assert fake.requests[before]["createdAfter"] == api.to_iso(lo)
    assert fake.requests[before]["createdBefore"] == api.to_iso(lo + changed_size - api.TICK)
    assert out[-1].split("|")[2] == str(changed_size.total_seconds())


@pytest.mark.asyncio
@pytest.mark.parametrize("count, next_days", [(Order.PAGE_SIZE // 2, 2), (Order.PAGE_SIZE // 2 + 1, 1)])
async def test_backfill_single_page_growth_requires_at_most_half_a_page(count: int, next_days: int):
    start = NOW - timedelta(days=20)
    fake = FakeAmazon(now=NOW, orders=[order(str(i), start, start) for i in range(count)], page_size=Order.PAGE_SIZE)
    out = await collect(backfill_orders(client_for(fake), MARKETPLACES, start, timedelta(days=1), LOG, None, NOW - timedelta(days=1)))
    assert len(fake.requests) == 1
    assert out[-1].split("|")[1] == str(timedelta(days=next_days).total_seconds())
