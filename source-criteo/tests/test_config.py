"""Offline tests for endpoint configuration validation."""

import asyncio
import json
import json as json_module
from datetime import UTC, datetime, timedelta
from logging import getLogger
from typing import Any

import pytest
from estuary_cdk.flow import ValidationError as ValidationError_
from estuary_cdk.http import HTTPError
from pydantic import ValidationError
from source_criteo.api import (
    _fetch_advertiser_ids,
    resolve_advertiser_ids,
    snapshot_ad_sets,
    snapshot_audiences,
)
from source_criteo.resources import (
    validate_advertiser_ids,
    validate_credentials,
    validate_reports,
)
from source_criteo.models import (
    EndpointConfig,
    ReportConfig,
    ReportGrain,
    report_document_model,
    report_key,
    report_stream_name,
)

LOG = getLogger(__name__)


def _report(**overrides) -> ReportConfig:
    fields: dict[str, Any] = {
        "name": "MyReport",
        "dimensions": ["CampaignId"],
        "metrics": ["Clicks"],
    }
    fields.update(overrides)
    return ReportConfig(**fields)


def test_dimensions_are_deduplicated_in_order():
    report = _report(dimensions=["CampaignId", "OS", "CampaignId"])
    assert report.dimensions == ["CampaignId", "OS"]
    assert report_key(report) == ["/Day", "/CampaignId", "/OS"]


def test_the_grain_leads_the_key_and_defaults_to_day():
    assert _report().grain is ReportGrain.DAY
    assert report_key(_report()) == ["/Day", "/CampaignId"]
    assert report_key(_report(grain="Hour")) == ["/Hour", "/CampaignId"]


def test_the_grain_alone_is_a_valid_grouping():
    report = _report(dimensions=[])
    assert report.all_dimensions == ["Day"]
    assert report_key(report) == ["/Day"]


def test_the_grain_is_the_only_way_to_set_time_granularity():
    for time_dimension in ("Hour", "Day", "Week", "Month", "Year"):
        with pytest.raises(ValidationError, match="time granularity is set by"):
            _report(dimensions=[time_dimension, "CampaignId"])


def test_an_unknown_grain_is_rejected():
    with pytest.raises(ValidationError):
        _report(grain="Week")


def test_report_bindings_are_namespaced_away_from_entity_streams():
    # The prefix is what makes a report named after a built-in stream harmless,
    # so no name needs reserving.
    assert report_stream_name(_report(name="MyReport")) == "custom_report_MyReport"

    for entity_stream in ("advertisers", "audiences", "ad_sets", "campaigns"):
        report = _report(name=entity_stream)
        assert report_stream_name(report) != entity_stream


def test_report_names_must_be_name_safe():
    with pytest.raises(ValidationError, match="Invalid report name"):
        _report(name="my/report")


def test_unknown_timezones_are_rejected():
    # Checked against the real tz database, so a misspelling of a real zone is
    # caught too — something a shape pattern could not do.
    for unknown in ["", "not a timezone", "America/New York", "America/New_Yrok"]:
        with pytest.raises(ValidationError, match="Invalid timezone"):
            _report(timezone=unknown)


def test_a_field_cannot_be_both_dimension_and_metric():
    with pytest.raises(ValidationError, match="both a dimension and a metric"):
        _report(dimensions=["Clicks"], metrics=["Clicks"])


def test_dimension_names_must_be_identifiers():
    # A name with a slash would silently corrupt the collection key's JSON pointer.
    with pytest.raises(ValidationError, match="Invalid dimension name"):
        _report(dimensions=["Campaign/Id"])


def test_currency_must_be_an_iso_4217_code():
    with pytest.raises(ValidationError, match="Invalid currency"):
        _report(currency="pounds")


def test_report_names_must_be_unique():
    with pytest.raises(ValidationError, match="Duplicate report name"):
        EndpointConfig(
            credentials={"client_id": "id", "client_secret": "secret"},  # type: ignore[arg-type]
            reports=[_report(), _report()],
        )


def test_advertiser_ids_are_deduplicated():
    config = EndpointConfig(
        credentials={"client_id": "id", "client_secret": "secret"},  # type: ignore[arg-type]
        advertiser_ids=["1", "2", "1"],
    )
    assert config.advertiser_ids == ["1", "2"]


def test_report_document_requires_the_grain_and_every_dimension():
    model = report_document_model(_report())

    model.model_validate({"Day": "2026-01-01", "CampaignId": "1", "Clicks": 3})

    with pytest.raises(ValidationError):
        model.model_validate({"Day": "2026-01-01", "Clicks": 3})

    with pytest.raises(ValidationError):
        model.model_validate({"CampaignId": "1", "Clicks": 3})


class PagedAudienceSession:
    """Serves `total` audiences in pages of `page_size`."""

    def __init__(self, total: int, page_size: int) -> None:
        self.total = total
        self.page_size = page_size
        self.offsets: list[int] = []

    async def request(
        self,
        log,
        url: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> bytes:
        assert params is not None
        offset = params["offset"]
        self.offsets.append(offset)
        page = [
            {"id": str(index), "type": "Audience"}
            for index in range(offset, min(offset + self.page_size, self.total))
        ]

        import json as json_module

        return json_module.dumps(
            {
                "data": page,
                "meta": {
                    "limit": self.page_size,
                    "offset": offset,
                    "totalItems": self.total,
                },
            }
        ).encode()


@pytest.mark.asyncio
async def test_audience_pagination_walks_every_page_once():
    session = PagedAudienceSession(total=250, page_size=100)

    audiences = [
        audience
        async for audience in snapshot_audiences(session, ["123"], LOG)  # type: ignore[arg-type]
    ]

    assert [audience.id for audience in audiences] == [str(i) for i in range(250)]
    assert session.offsets == [0, 100, 200]


@pytest.mark.asyncio
async def test_a_missing_pagination_meta_fails_loudly():
    class NoMetaSession:
        async def request(self, *args, **kwargs) -> bytes:
            return json.dumps({"data": [{"id": "1", "type": "Audience"}]}).encode()

    with pytest.raises(RuntimeError, match="pagination `meta` object"):
        async for _ in snapshot_audiences(NoMetaSession(), ["123"], LOG):  # type: ignore[arg-type]
            pass


@pytest.mark.asyncio
async def test_api_errors_are_propagated():
    class ErroringSession:
        async def request(self, *args, **kwargs) -> bytes:
            return json.dumps(
                {
                    "data": [],
                    "errors": [
                        {
                            "type": "authorization",
                            "code": "invalid",
                            "title": "Forbidden",
                            "detail": "no access to advertiser 123",
                        }
                    ],
                }
            ).encode()

    with pytest.raises(RuntimeError, match="no access to advertiser 123"):
        async for _ in snapshot_audiences(ErroringSession(), ["123"], LOG):  # type: ignore[arg-type]
            pass


@pytest.mark.asyncio
async def test_a_truncated_search_result_fails_loudly():
    # ad-sets/campaigns search take no pagination parameters, so a `meta`
    # reporting more items than were served is unreachable data.
    class TruncatedSession:
        async def request(self, *args, **kwargs) -> bytes:
            return json.dumps(
                {
                    "data": [{"id": "1", "type": "AdSet"}],
                    "meta": {"limit": 1, "offset": 0, "totalItems": 5},
                }
            ).encode()

    with pytest.raises(RuntimeError, match="takes no pagination"):
        async for _ in snapshot_ad_sets(TruncatedSession(), ["123"], LOG):  # type: ignore[arg-type]
            pass


def test_a_name_dimension_pulls_in_its_paired_id():
    # Criteo returns CampaignId whether or not it was asked for, so rows are at
    # ID granularity; keying on the name alone would collapse same-named campaigns.
    report = _report(dimensions=["Campaign"])
    assert report.dimensions == ["Campaign", "CampaignId"]
    assert report_key(report) == ["/Day", "/Campaign", "/CampaignId"]


def test_an_explicit_id_dimension_is_not_duplicated():
    report = _report(dimensions=["Campaign", "CampaignId"])
    assert report.dimensions == ["Campaign", "CampaignId"]


def test_an_id_only_dimension_is_left_alone():
    # The name counterpart also comes back, but the ID alone is already a
    # sufficient key, so nothing needs adding.
    report = _report(dimensions=["CampaignId"])
    assert report.dimensions == ["CampaignId"]


def test_tz_database_names_are_accepted():
    for timezone in [
        "UTC", "Africa/Cairo", "America/New_York", "Asia/Tokyo",
        "Asia/Kolkata", "Europe/London", "Pacific/Auckland",
    ]:
        assert _report(timezone=timezone).timezone == timezone


def test_fixed_offset_and_code_timezones_are_rejected():
    # Criteo accepts all of these; the connector deliberately does not. A fixed
    # offset cannot follow daylight saving, and the bare codes are ambiguous
    # across countries (IST is India, Israel and Ireland).
    for timezone in ["PST", "JST", "EDT", "IST", "UTC-5", "UTC+14", "UTC+5:30"]:
        with pytest.raises(ValidationError, match="Invalid timezone"):
            _report(timezone=timezone)


def test_legacy_fixed_offset_tz_names_are_rejected():
    # These are real tz database entries, so a membership check alone would let
    # them through — and they are exactly the trap being avoided: `EST` is
    # permanently UTC-5 and never observes DST, unlike `America/New_York`.
    for timezone in ["EST", "MST", "GMT", "EST5EDT"]:
        with pytest.raises(ValidationError, match="Area/Location"):
            _report(timezone=timezone)


def test_etc_gmt_timezones_are_rejected_by_name():
    # Etc/GMT+5 means UTC-5, so it gets its own message rather than the generic one.
    with pytest.raises(ValidationError, match="inverted sign"):
        _report(timezone="Etc/GMT+5")


@pytest.mark.asyncio
async def test_pagination_meta_without_limit_or_offset_still_walks():
    class SparseMetaSession:
        async def request(self, *args, **kwargs) -> bytes:
            return json.dumps(
                {
                    "data": [{"id": "1", "type": "Audience"}],
                    "meta": {"totalItems": 1},
                }
            ).encode()

    audiences = [
        a async for a in snapshot_audiences(SparseMetaSession(), ["1"], LOG)  # type: ignore[arg-type]
    ]
    assert [a.id for a in audiences] == ["1"]


def _config(**overrides) -> EndpointConfig:
    fields: dict[str, Any] = {
        "credentials": {"client_id": "id", "client_secret": "secret"},
    }
    fields.update(overrides)
    return EndpointConfig(**fields)


class PortfolioSession:
    """Answers /advertisers/me with `ids`, and accepts a token_source assignment."""

    def __init__(self, ids: list[str]) -> None:
        self.ids = ids
        self.token_source = None

    async def request(self, *args, **kwargs) -> bytes:
        return json.dumps(
            {"data": [{"id": i, "type": "advertiser"} for i in self.ids]}
        ).encode()


@pytest.mark.asyncio
async def test_validate_rejects_a_200_carrying_errors():
    class UngrantedSession(PortfolioSession):
        async def request(self, *args, **kwargs) -> bytes:
            return json.dumps(
                {"data": [], "errors": [{"detail": "no marketing solutions grant"}]}
            ).encode()

    with pytest.raises(ValidationError_, match="no marketing solutions grant"):
        await validate_credentials(LOG, UngrantedSession([]), _config())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_validate_rejects_advertiser_ids_outside_the_portfolio():
    session = PortfolioSession(["1", "2", "3"])

    errors = await validate_advertiser_ids(
        LOG, session, _config(advertiser_ids=["2", "999"])  # type: ignore[arg-type]
    )

    assert len(errors) == 1
    assert "999" in errors[0], "the unknown id is named"
    assert "1, 2, 3" in errors[0], "the accessible ids are suggested"


@pytest.mark.asyncio
async def test_validate_accepts_advertiser_ids_in_the_portfolio():
    session = PortfolioSession(["1", "2", "3"])

    assert await validate_advertiser_ids(  # type: ignore[arg-type]
        LOG, session, _config(advertiser_ids=["3", "1"])
    ) == []


@pytest.mark.asyncio
async def test_validate_truncates_a_long_suggestion_list():
    session = PortfolioSession([str(n) for n in range(100, 160)])

    errors = await validate_advertiser_ids(  # type: ignore[arg-type]
        LOG, session, _config(advertiser_ids=["nope"])
    )

    assert "and 40 more" in errors[0]


@pytest.mark.asyncio
async def test_validate_rejects_reports_with_an_empty_portfolio():
    session = PortfolioSession([])

    errors = await validate_reports(LOG, session, _config(reports=[_report()]))  # type: ignore[arg-type]

    assert len(errors) == 1 and "portfolio is empty" in errors[0]


@pytest.mark.asyncio
async def test_an_empty_portfolio_is_fine_without_reports():
    await validate_credentials(LOG, PortfolioSession([]), _config())  # type: ignore[arg-type]
    assert await validate_reports(LOG, PortfolioSession([]), _config()) == []  # type: ignore[arg-type]


class ProbeSession(PortfolioSession):
    """Serves the portfolio, and a configurable statistics report response."""

    def __init__(self, ids: list[str], payload: dict[str, Any] | None = None) -> None:
        super().__init__(ids)
        self.payload = payload if payload is not None else {"Rows": []}
        self.probes: list[dict[str, Any]] = []

    async def request_stream(self, log, url, method="GET", params=None, json=None, **kwargs):
        assert json is not None
        self.probes.append(json)
        encoded = json_module.dumps(self.payload).encode()

        async def body():
            yield encoded

        return {}, body


@pytest.mark.asyncio
async def test_validate_probes_each_report_for_a_single_past_day():
    session = ProbeSession(["1"])
    reports = [_report(name="One"), _report(name="Two", grain="Hour")]

    assert await validate_reports(LOG, session, _config(reports=reports)) == []  # type: ignore[arg-type]

    assert len(session.probes) == 2, "every report is probed"

    yesterday = (
        datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=1)
    )
    for probe in session.probes:
        assert probe["startDate"] == probe["endDate"] == yesterday.isoformat()

    assert session.probes[0]["dimensions"] == ["Day", "CampaignId"]
    assert session.probes[1]["dimensions"] == ["Hour", "CampaignId"]


@pytest.mark.asyncio
async def test_validate_reports_surfaces_a_rejection_from_criteo():
    class RejectingSession(ProbeSession):
        async def request_stream(self, *args, **kwargs):
            raise HTTPError("Unknown metric 'Clickz'", 400)

    errors = await validate_reports(  # type: ignore[arg-type]
        LOG, RejectingSession(["1"]), _config(reports=[_report(name="Bad")])
    )

    assert len(errors) == 1
    assert "Bad" in errors[0] and "Clickz" in errors[0]


@pytest.mark.asyncio
async def test_validate_reports_surfaces_envelope_errors():
    session = ProbeSession(
        ["1"], payload={"Rows": [], "errors": [{"detail": "currency not supported"}]}
    )

    errors = await validate_reports(LOG, session, _config(reports=[_report()]))  # type: ignore[arg-type]

    assert len(errors) == 1 and "currency not supported" in errors[0]


@pytest.mark.asyncio
async def test_validate_reports_rejects_a_response_missing_a_key_column():
    # Criteo answering without a dimension the collection is keyed on would break
    # every document, so it is caught here rather than at capture time.
    session = ProbeSession(["1"], payload={"Rows": [{"Day": "2026-01-01"}]})

    errors = await validate_reports(LOG, session, _config(reports=[_report()]))  # type: ignore[arg-type]

    assert len(errors) == 1 and "does not match what the collection is keyed on" in errors[0]


@pytest.mark.asyncio
async def test_validate_reports_is_a_no_op_without_reports():
    session = ProbeSession(["1"])

    assert await validate_reports(LOG, session, _config()) == []  # type: ignore[arg-type]

    assert session.probes == []


class CountingPortfolioSession:
    """Counts /advertisers/me requests so cache hits are observable."""

    def __init__(self) -> None:
        self.requests = 0

    async def request(self, *args, **kwargs) -> bytes:
        self.requests += 1
        return json.dumps(
            {"data": [{"id": "1", "type": "advertiser"}, {"id": "2", "type": "advertiser"}]}
        ).encode()


@pytest.fixture(autouse=True)
def _clear_advertiser_cache():
    # The cache is module-level, so it outlives a single test.
    _fetch_advertiser_ids.cache_clear()
    yield
    _fetch_advertiser_ids.cache_clear()


@pytest.mark.asyncio
async def test_the_portfolio_is_fetched_once_per_cache_key():
    session = CountingPortfolioSession()

    for _ in range(5):
        assert await resolve_advertiser_ids(session, [], LOG) == ["1", "2"]  # type: ignore[arg-type]

    assert session.requests == 1


@pytest.mark.asyncio
async def test_configured_advertiser_ids_never_hit_the_api():
    session = CountingPortfolioSession()

    assert await resolve_advertiser_ids(session, ["7"], LOG) == ["7"]  # type: ignore[arg-type]
    assert session.requests == 0


@pytest.mark.asyncio
async def test_concurrent_misses_share_one_request():
    session = CountingPortfolioSession()

    results = await asyncio.gather(
        *(resolve_advertiser_ids(session, [], LOG) for _ in range(8))  # type: ignore[arg-type]
    )

    assert all(r == ["1", "2"] for r in results)
    assert session.requests == 1


@pytest.mark.asyncio
async def test_a_caller_cannot_corrupt_the_cached_portfolio():
    session = CountingPortfolioSession()

    first = await resolve_advertiser_ids(session, [], LOG)  # type: ignore[arg-type]
    first.append("mutated")

    assert await resolve_advertiser_ids(session, [], LOG) == ["1", "2"]  # type: ignore[arg-type]
    assert session.requests == 1


@pytest.mark.asyncio
async def test_every_broken_report_is_reported_not_just_the_first():
    class AllRejectingSession(ProbeSession):
        async def request_stream(self, log, url, method="GET", params=None, json=None, **kwargs):
            self.probes.append(json)
            raise HTTPError(f"Unknown metric in {json['metrics']}", 400)

    session = AllRejectingSession(["1"])
    reports = [_report(name="One"), _report(name="Two"), _report(name="Three")]

    errors = await validate_reports(LOG, session, _config(reports=reports))  # type: ignore[arg-type]

    assert len(session.probes) == 3, "probing continues past the first failure"
    assert len(errors) == 3
    assert {"One", "Two", "Three"} == {
        name for name in ("One", "Two", "Three") if any(name in e for e in errors)
    }


@pytest.mark.asyncio
async def test_advertiser_and_report_problems_are_reported_together():
    # The whole point of collecting: one publish attempt surfaces everything.
    class RejectingSession(ProbeSession):
        async def request_stream(self, log, url, method="GET", params=None, json=None, **kwargs):
            self.probes.append(json)
            raise HTTPError("Unknown metric 'Clickz'", 400)

    session = RejectingSession(["1", "2"])
    config = _config(advertiser_ids=["1", "bogus"], reports=[_report(name="Bad")])

    errors = [
        *await validate_advertiser_ids(LOG, session, config),  # type: ignore[arg-type]
        *await validate_reports(LOG, session, config),  # type: ignore[arg-type]
    ]

    assert len(errors) == 2
    assert any("bogus" in e for e in errors)
    assert any("Clickz" in e for e in errors)


@pytest.mark.asyncio
async def test_reports_are_probed_with_advertisers_that_exist():
    # A bad advertiser_ids entry is reported once, by validate_advertiser_ids —
    # it must not also break every report probe and bury their own problems.
    session = ProbeSession(["1", "2"])
    config = _config(advertiser_ids=["2", "bogus"], reports=[_report()])

    assert await validate_reports(LOG, session, config) == []  # type: ignore[arg-type]
    assert session.probes[0]["advertiserIds"] == "2", "only the valid ID is probed"


@pytest.mark.asyncio
async def test_reports_fall_back_to_the_portfolio_when_no_configured_id_is_valid():
    session = ProbeSession(["1", "2"])
    config = _config(advertiser_ids=["bogus"], reports=[_report()])

    assert await validate_reports(LOG, session, config) == []  # type: ignore[arg-type]
    assert session.probes[0]["advertiserIds"] == "1,2"
