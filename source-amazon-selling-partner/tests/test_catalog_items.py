import copy
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from estuary_cdk.incremental_csv_processor import CSVProcessingError

from source_amazon_selling_partner import api, reports
from source_amazon_selling_partner.api import fetch_catalog_items
from source_amazon_selling_partner.client import Priority
from source_amazon_selling_partner.models import CatalogItem, Report, SettlementReportRow
from source_amazon_selling_partner.reports import ReportEncodingError, ReportFailed, ReportRoleMissing

from .fake_amazon import FakeAmazon, FakeDocument, report
from .test_orders import NOW, client_for, collect

LOG = logging.getLogger("test")
DAY = timedelta(days=1)
MARKETS = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2"]
EXAMPLE = json.loads((Path(__file__).parent / "sandbox_examples/get_catalog_item.json").read_text())


@pytest.fixture(autouse=True)
def freeze(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api, "now", lambda: NOW)
    monkeypatch.setattr(reports, "now", lambda: NOW)


def fixture(asins: list[str], markets: list[str] = MARKETS[:1]):
    fake = FakeAmazon(now=NOW, orders=[])
    for market in markets:
        fake.listings[market] = "asin1\tseller-sku\tstatus\n" + "".join(f"{a}\tSKU-{a}\tActive\n" for a in asins)
        fake.catalog_items[market] = []
        for asin in dict.fromkeys(asins):
            item = {"asin": asin, "summaries": copy.deepcopy(EXAMPLE["summaries"])}
            item["summaries"][0]["marketplaceId"] = market
            fake.catalog_items[market].append(item)
    return fake


async def refresh(fake: FakeAmazon, markets: list[str] = MARKETS[:1], cursor: datetime = NOW - DAY, interval: timedelta = DAY):
    return await collect(fetch_catalog_items(client_for(fake), markets, interval, LOG, cursor))


@pytest.mark.asyncio
async def test_refresh_summary_skus_status_and_cursor():
    asin = EXAMPLE["asin"]
    fake = fixture([asin])
    fake.listings[MARKETS[0]] += f"{asin}\tAAA\tInactive\n{asin}\tAAA\tActive\n"
    fake.catalog_items[MARKETS[0]][0]["summaries"].insert(0, {"marketplaceId": MARKETS[1], "brand": "wrong"})
    fake.catalog_items[MARKETS[0]][0]["summaries"][1]["futureField"] = {"value": 1}
    doc, cursor = await refresh(fake)
    assert isinstance(doc, CatalogItem)
    assert doc.asin == asin and doc.marketplaceId == MARKETS[0]
    for key, value in EXAMPLE["summaries"][0].items():
        assert doc.model_dump()[key] == value
    assert cast(dict[str, Any], doc.model_extra)["futureField"] == {"value": 1}
    assert doc.sellerSkus == ["AAA", f"SKU-{asin}"]
    assert doc.listingStatus == "Active"
    assert doc.refreshedAt == cursor == NOW
    assert fake.create_requests[0]["reportOptions"] == {"preferredReportDocumentLocale": "en_US"}


@pytest.mark.asyncio
async def test_batches_dedupes_and_pages():
    asins = [f"B{i:09}" for i in range(45)]
    fake = fixture(asins + asins)
    fake.catalog_page_size = 7
    out = await refresh(fake)
    assert sorted(d.asin for d in out[:-1]) == asins
    first_pages = [r for r in fake.catalog_requests if "pageToken" not in r]
    assert [len(r["identifiers"].split(",")) for r in first_pages] == [20, 20, 5]
    assert out[-1] == NOW


@pytest.mark.asyncio
async def test_blank_and_missing_asins_are_skipped(caplog: pytest.LogCaptureFixture):
    fake = fixture(["present", "missing"])
    fake.listings[MARKETS[0]] += "\tblank-sku\tActive\n"
    fake.catalog_items[MARKETS[0]].pop()
    with caplog.at_level(logging.DEBUG):
        out = await refresh(fake)
    assert [d.asin for d in out[:-1]] == ["present"]
    assert all(
        identifier.strip()
        for request in fake.catalog_requests
        for identifier in request["identifiers"].split(",")
    )
    assert "catalog ASIN not returned" in caplog.text
    assert sum(r.message == "refreshed catalog items" for r in caplog.records) == 1


@pytest.mark.asyncio
async def test_marketplaces_have_separate_reports_and_lookups():
    fake = fixture([EXAMPLE["asin"]], MARKETS)
    out = await refresh(fake, MARKETS)
    assert [(d.asin, d.marketplaceId) for d in out[:-1]] == [(EXAMPLE["asin"], m) for m in MARKETS]
    assert [r["marketplaceIds"] for r in fake.create_requests] == [[m] for m in MARKETS]
    assert [r["marketplaceIds"] for r in fake.catalog_requests] == MARKETS


@pytest.mark.asyncio
async def test_interval_gates_all_work():
    fake = fixture(["a"])
    assert await refresh(fake, cursor=NOW - timedelta(hours=23)) == []
    assert fake.create_requests == fake.catalog_requests == fake.report_requests == []
    assert await refresh(fake, cursor=NOW - timedelta(hours=2), interval=timedelta(hours=1))


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ["artikel\tstatus\na\tActive\n", "artikel\tstatus\n", ""])
async def test_missing_asin_column_names_report_and_columns(text: str):
    fake = fixture([])
    fake.listings[MARKETS[0]] = text
    with pytest.raises(RuntimeError, match="GET_MERCHANT_LISTINGS_ALL_DATA.*asin1.*columns seen"):
        await refresh(fake)


@pytest.mark.asyncio
@pytest.mark.parametrize("report_forbidden", [False, True])
async def test_403_names_product_listing_role(report_forbidden: bool):
    fake = fixture(["a"])
    if report_forbidden:
        fake.forbidden_report_types.add(CatalogItem.REPORT_TYPE)
    else:
        fake.catalog_forbidden = True
    with pytest.raises(ReportRoleMissing, match="Product Listing role") as exc:
        await refresh(fake)
    if not report_forbidden:
        assert "likely cause" in str(exc.value)
        assert "403: Access denied" in str(exc.value)


@pytest.mark.asyncio
async def test_wrong_marketplace_summary_is_skipped():
    fake = fixture(["a"])
    fake.catalog_items[MARKETS[0]][0]["summaries"][0]["marketplaceId"] = MARKETS[1]
    assert await refresh(fake) == [NOW]


@pytest.mark.asyncio
@pytest.mark.parametrize("interval", [DAY, timedelta(hours=7)])
async def test_restart_reuses_requested_report(monkeypatch: pytest.MonkeyPatch, interval: timedelta):
    fake = fixture(["a"])
    with monkeypatch.context() as interrupted:
        def fail_poll(*args: object):
            raise RuntimeError("refresh interrupted after createReport")

        interrupted.setattr(fake, "_get_report", fail_poll)
        with pytest.raises(RuntimeError, match="refresh interrupted"):
            await refresh(fake, interval=interval)
    assert len(fake.create_requests) == 1
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    bucket = epoch + ((NOW - epoch) // interval) * interval
    request = fake.create_requests[0]
    assert datetime.fromisoformat(request["dataStartTime"]) == bucket
    assert request["dataStartTime"] == request["dataEndTime"]

    restarted = NOW + timedelta(minutes=3, microseconds=123456)
    fake.now = restarted
    monkeypatch.setattr(api, "now", lambda: restarted)
    monkeypatch.setattr(reports, "now", lambda: restarted)
    doc, cursor = await refresh(fake, interval=interval)
    assert doc.asin == "a"
    assert doc.refreshedAt == cursor == restarted
    assert [r["marketplaceIds"] for r in fake.create_requests] == [[MARKETS[0]]]


@pytest.mark.asyncio
@pytest.mark.parametrize("column", ["seller-sku", "status"])
async def test_missing_listing_column_fails(column: str):
    fake = fixture([])
    columns = [c for c in ("asin1", "seller-sku", "status") if c != column]
    fake.listings[MARKETS[0]] = "\t".join(columns) + "\n"
    with pytest.raises(
        RuntimeError,
        match=f"GET_MERCHANT_LISTINGS_ALL_DATA.*{column}.*columns seen.*localized headers",
    ):
        await refresh(fake)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statuses, expected",
    [(["Inactive", "Active"], "Active"), (["", "Inactive", "Incomplete"], "Inactive")],
)
async def test_status_prefers_active_otherwise_first_nonblank(statuses: list[str], expected: str):
    fake = fixture(["a"])
    fake.listings[MARKETS[0]] = "asin1\tseller-sku\tstatus\n" + "".join(
        f"a\tSKU-{i}\t{status}\n" for i, status in enumerate(statuses)
    )
    doc, cursor = await refresh(fake)
    assert doc.listingStatus == expected
    assert cursor == NOW


@pytest.mark.asyncio
async def test_blank_seller_sku_is_not_emitted():
    fake = fixture(["a"])
    fake.listings[MARKETS[0]] += "a\t\tActive\n"
    doc, _ = await refresh(fake)
    assert doc.sellerSkus == ["SKU-a"]


@pytest.mark.asyncio
async def test_listing_ragged_rows_are_skipped_once_per_report(caplog: pytest.LogCaptureFixture):
    fake = fixture(["a", "b"])
    fake.listings[MARKETS[0]] = (
        "asin1\tseller-sku\tstatus\n"
        "a\tSKU-a\tActive\n"
        "private-short\tprivate-sku\n"
        "private-long\tprivate-sku\tActive\tprivate-extra\n"
        "b\tSKU-b\tActive\n"
    )
    with caplog.at_level(logging.WARNING):
        out = await refresh(fake)
    assert [doc.asin for doc in out[:-1]] == ["a", "b"]
    assert out[-1] == NOW
    assert len(caplog.records) == 1
    assert cast(dict[str, Any], caplog.records[0].args)["rows_skipped"] == 2
    assert cast(dict[str, Any], caplog.records[0].args)["encoding"] == "utf-8-sig"
    assert cast(dict[str, Any], caplog.records[0].args)["report_id"] == "requested-1"
    assert "private" not in caplog.text


async def read_document(body: bytes, content_type: str | None, report_type: str = CatalogItem.REPORT_TYPE, required_columns: tuple[str, ...] = ()):
    fake = FakeAmazon(now=NOW, orders=[])
    metadata = Report.model_validate(report("r1", NOW, report_type=report_type))
    fake.documents["doc-r1"] = FakeDocument(body, content_type=content_type)
    fake.download_chunk_size = 3
    return await collect(reports.read_report(
        client_for(fake), metadata, Priority.BACKFILL, LOG, required_columns=required_columns,
    ))


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", [None, "text/plain"])
@pytest.mark.parametrize(
    "suffix, expected, encoding",
    [
        (b"price \x80 and l\x92article", "price € and l’article", "cp1252"),
        (b"undefined \x81", "undefined \x81", "latin-1"),
        ("商品".encode(), "商品", "utf-8-sig"),
        (b"truncated \xe2\x82", "truncated â‚", "cp1252"),
    ],
)
async def test_listing_without_charset_selects_encoding(suffix: bytes, expected: str, encoding: str, content_type: str | None, caplog: pytest.LogCaptureFixture):
    body = b"asin1\tseller-sku\tstatus\titem-name\na\tSKU-a\tActive\tplain\n"
    body += b"b\tSKU-b\tActive\t" + suffix
    with caplog.at_level(logging.WARNING):
        out = await read_document(body, content_type)
    assert [row["item-name"] for _, row in out] == ["plain", expected]
    if encoding == "utf-8-sig":
        # A clean UTF-8 document is routine and logs no warning.
        assert caplog.records == []
    else:
        assert len(caplog.records) == 1
        assert caplog.records[0].args == {
            "report_id": "r1", "report_document_id": "doc-r1",
            "report_type": CatalogItem.REPORT_TYPE, "content_type": content_type,
            "rows_skipped": 0, "encoding": encoding,
        }


@pytest.mark.asyncio
async def test_listing_with_every_row_ragged_fails():
    with pytest.raises(RuntimeError, match="no row matching its header's width; 2 row"):
        await read_document(b"asin1\tseller-sku\tstatus\nshort\nlong\trow\twith\textra\n", None)


@pytest.mark.asyncio
@pytest.mark.parametrize("good, ragged, fails", [
    (990, 10, False), (989, 11, True), (1989, 11, False), (89, 11, True),
    (1980, 20, False), (1979, 21, True),
])
async def test_listing_ragged_row_limit(good: int, ragged: int, fails: bool):
    body = b"asin1\tseller-sku\tstatus\n" + b"a\tSKU\tActive\n" * good + b"short\n" * ragged
    if fails:
        with pytest.raises(RuntimeError, match=f"has {ragged} of {good + ragged} row"):
            await read_document(body, None)
    else:
        assert len(await read_document(body, None)) == good


@pytest.mark.asyncio
async def test_listing_encoding_and_ragged_rows_share_one_warning(caplog: pytest.LogCaptureFixture):
    body = b"asin1\tseller-sku\tstatus\na\tSKU-\x80\tActive\nshort\nlong\trow\twith\textra\n"
    with caplog.at_level(logging.WARNING):
        out = await read_document(body, None)
    assert [row["seller-sku"] for _, row in out] == ["SKU-€"]
    assert len(caplog.records) == 1
    assert caplog.records[0].args == {
        "report_id": "r1", "report_document_id": "doc-r1",
        "report_type": CatalogItem.REPORT_TYPE, "content_type": None,
        "rows_skipped": 2, "encoding": "cp1252",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content_type, value",
    [("text/plain; charset=UTF-8", b"\x80"), ("text/plain; charset=cp1252", b"\x81")],
)
async def test_listing_declared_charset_stays_strict(content_type: str | None, value: bytes):
    with pytest.raises(ReportEncodingError):
        await read_document(b"asin1\tseller-sku\tstatus\na\t" + value + b"\tActive\n", content_type)


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["utf-16", "shift_jis"])
async def test_listing_declared_charset_is_honored_without_warning(caplog: pytest.LogCaptureFixture, encoding: str):
    with caplog.at_level(logging.INFO):
        out = await read_document(
            "asin1\tseller-sku\tstatus\na\tSKU-商品\tActive\n".encode(encoding),
            f"text/plain; charset={encoding}",
        )
    assert out[0][1]["seller-sku"] == "SKU-商品"
    assert all(record.levelno < logging.WARNING for record in caplog.records)
    assert caplog.records[-1].message == "read report document"


@pytest.mark.asyncio
async def test_other_report_without_charset_stays_strict_on_bytes():
    with pytest.raises(ReportEncodingError):
        await read_document(b"one\ttwo\na\t\x80\n", None, SettlementReportRow.REPORT_TYPE)


@pytest.mark.asyncio
@pytest.mark.parametrize("row", [b"short\n", b"long\trow\textra\n"])
@pytest.mark.parametrize("required_columns", [(), ("one",)])
async def test_other_report_stays_strict_on_ragged_rows(row: bytes, required_columns: tuple[str, ...]):
    with pytest.raises((CSVProcessingError, RuntimeError), match="columns"):
        await read_document(
            b"one\ttwo\n" + row, "text/plain; charset=UTF-8",
            SettlementReportRow.REPORT_TYPE, required_columns,
        )


@pytest.mark.asyncio
async def test_item_without_summaries_is_missing(caplog: pytest.LogCaptureFixture):
    fake = fixture(["a", "b"])
    fake.catalog_items[MARKETS[0]][0] = {"asin": "a"}
    with caplog.at_level(logging.INFO):
        out = await refresh(fake)
    assert [doc.asin for doc in out[:-1]] == ["b"]
    assert out[-1] == NOW
    summary = next(r for r in caplog.records if r.message == "refreshed catalog items")
    assert cast(dict[str, int], summary.args)["missing_asins"] == 1


@pytest.mark.asyncio
async def test_response_filters_unrequested_and_repeated_items(monkeypatch: pytest.MonkeyPatch):
    fake = fixture(["a", "b"])
    fake.catalog_page_size = 1
    respond = fake._respond  # pyright: ignore[reportPrivateUsage]
    repeated = copy.deepcopy(fake.catalog_items[MARKETS[0]][0])
    extra = {**repeated, "asin": "unrequested"}

    def respond_with_extra_items(body: dict[str, Any]):
        if "items" in body:
            body["items"].append(extra)
            if len(fake.catalog_requests) > 1:
                body["items"].append(repeated)
        return respond(body)

    monkeypatch.setattr(fake, "_respond", respond_with_extra_items)
    out = await refresh(fake)
    assert [doc.asin for doc in out[:-1]] == ["a", "b"]
    assert len(fake.catalog_requests) == 2
    assert out[-1] == NOW


@pytest.mark.asyncio
async def test_response_without_items_is_empty(monkeypatch: pytest.MonkeyPatch):
    fake = fixture(["a"])
    respond = fake._respond  # pyright: ignore[reportPrivateUsage]

    def respond_without_items(body: dict[str, Any]):
        if "items" in body:
            body.pop("items")
        return respond(body)

    monkeypatch.setattr(fake, "_respond", respond_without_items)
    assert await refresh(fake) == [NOW]


@pytest.mark.asyncio
async def test_twice_cancelled_report_completes_empty_refresh():
    fake = fixture([])
    fake.default_lifecycle = ["CANCELLED"]
    assert await refresh(fake) == [NOW]
    assert len(fake.create_requests) == 2
    assert fake.catalog_requests == []
    assert await refresh(fake, cursor=NOW) == []
    assert len(fake.create_requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("interval", [timedelta(), timedelta(seconds=-1)])
async def test_nonpositive_interval_fails_clearly(interval: timedelta):
    fake = fixture(["a"])
    with pytest.raises(ValueError, match="catalog_items.*at least.*REPORT_TIMEOUT.*same or the next request bucket"):
        await refresh(fake, interval=interval)
    assert fake.create_requests == fake.report_requests == []


@pytest.mark.parametrize("interval", [None, timedelta(), timedelta(hours=2, minutes=59)])
def test_validation_rejects_short_catalog_interval(interval: timedelta | None):
    from types import SimpleNamespace
    from estuary_cdk.capture.common import Resource
    from estuary_cdk.flow import ValidationError
    from source_amazon_selling_partner.models import OrdersResourceConfig
    from source_amazon_selling_partner.resources import validate_bindings
    from .test_orders import make_config

    kwargs: dict[str, Any] = {} if interval is None else {"interval": interval}
    binding = SimpleNamespace(resourceConfig=OrdersResourceConfig(name="catalog_items", **kwargs))
    with pytest.raises(ValidationError, match="catalog_items.*at least.*REPORT_TIMEOUT.*same or the next request bucket"):
        validate_bindings(make_config(MARKETS), [(binding, cast(Resource, SimpleNamespace(name="catalog_items")))])
    binding.resourceConfig.interval = api.CATALOG_MIN_INTERVAL
    validate_bindings(make_config(MARKETS), [(binding, cast(Resource, SimpleNamespace(name="catalog_items")))])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval, first, delay",
    [
        (DAY, datetime(2026, 3, 1, 23, 30, tzinfo=UTC), timedelta(minutes=35)),
        (2 * DAY, datetime(2026, 3, 2, 1, tzinfo=UTC), timedelta(hours=25)),
    ],
)
async def test_restart_across_edge_or_after_24h_reuses_report(monkeypatch: pytest.MonkeyPatch, interval: timedelta, first: datetime, delay: timedelta):
    fake = fixture(["a"])
    cursor = first - 2 * interval
    fake.now = first
    monkeypatch.setattr(api, "now", lambda: fake.now)
    monkeypatch.setattr(reports, "now", lambda: fake.now)
    with monkeypatch.context() as interrupted:
        def fail_poll(*args: object):
            raise RuntimeError("interrupted")
        interrupted.setattr(fake, "_get_report", fail_poll)
        with pytest.raises(RuntimeError, match="interrupted"):
            await refresh(fake, cursor=cursor, interval=interval)
    assert len(fake.create_requests) == 1
    fake.now = first + delay
    doc, completed = await refresh(fake, cursor=cursor, interval=interval)
    assert doc.asin == "a"
    assert doc.refreshedAt == completed == fake.now
    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
async def test_bucket_after_completed_refresh_creates_new_report(monkeypatch: pytest.MonkeyPatch):
    fake = fixture(["a"])
    first = datetime(2026, 3, 1, 0, 5, tzinfo=UTC)
    fake.now = first
    monkeypatch.setattr(api, "now", lambda: fake.now)
    monkeypatch.setattr(reports, "now", lambda: fake.now)
    _, cursor = await refresh(fake, cursor=first - DAY)
    fake.now = first + DAY
    _, completed = await refresh(fake, cursor=cursor)
    assert completed == fake.now
    assert len(fake.create_requests) == 2
    assert fake.create_requests[0]["dataStartTime"] != fake.create_requests[1]["dataStartTime"]


@pytest.mark.asyncio
async def test_catalog_lookback_is_bounded_by_retention(monkeypatch: pytest.MonkeyPatch):
    fake = fixture(["a"])
    interval = timedelta(days=200)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    bucket = epoch + ((NOW - epoch) // interval) * interval
    assert bucket - reports.RETENTION_MARGIN < NOW - reports.REPORT_RETENTION
    # FakeAmazon rejects createdSince outside retention, so this also checks
    # that a catalog interval longer than retention still runs.
    assert await refresh(fake, cursor=NOW - interval, interval=interval)
    assert fake.report_requests
    assert all(
        datetime.fromisoformat(request["createdSince"])
        >= NOW - reports.REPORT_RETENTION + reports.RETENTION_MARGIN
        for request in fake.report_requests
        if "nextToken" not in request
    )
    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_bucket", [False, True])
async def test_reused_report_fatal_keeps_diagnostic(monkeypatch: pytest.MonkeyPatch, previous_bucket: bool):
    fake = fixture(["a"])
    fake.default_lifecycle = ["IN_PROGRESS", "FATAL"]
    fake.fatal_document = FakeDocument(b"Amazon diagnostic: listing export failed")
    cursor = NOW - 3 * DAY
    with monkeypatch.context() as interrupted:
        def fail_poll(*args: object):
            raise RuntimeError("interrupted")
        interrupted.setattr(fake, "_get_report", fail_poll)
        with pytest.raises(RuntimeError, match="interrupted"):
            await refresh(fake, cursor=cursor)
    request_time = fake.create_requests[0]["dataStartTime"]
    if previous_bucket:
        fake.now += DAY
        monkeypatch.setattr(api, "now", lambda: fake.now)
        monkeypatch.setattr(reports, "now", lambda: fake.now)
    with pytest.raises(ReportFailed, match="Amazon diagnostic: listing export failed") as exc:
        await refresh(fake, cursor=cursor)
    assert datetime.fromisoformat(request_time).date().isoformat() in str(exc.value)
    assert len(fake.create_requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_bucket", [False, True])
async def test_reuses_report_created_before_bucket_start(monkeypatch: pytest.MonkeyPatch, previous_bucket: bool):
    fake = fixture(["a"])
    # The same-bucket retry must rely on the clock-skew margin, not an older lookback.
    cursor = NOW - (3 * DAY if previous_bucket else DAY)
    with monkeypatch.context() as interrupted:
        def fail_poll(*args: object):
            raise RuntimeError("interrupted")
        interrupted.setattr(fake, "_get_report", fail_poll)
        with pytest.raises(RuntimeError, match="interrupted"):
            await refresh(fake, cursor=cursor)
    bucket = datetime.fromisoformat(fake.create_requests[0]["dataStartTime"])
    fake.reports[0]["createdTime"] = (bucket - timedelta(seconds=5)).isoformat()
    if previous_bucket:
        fake.now += DAY
        monkeypatch.setattr(api, "now", lambda: fake.now)
        monkeypatch.setattr(reports, "now", lambda: fake.now)
    doc, completed = await refresh(fake, cursor=cursor)
    assert doc.asin == "a"
    assert completed == fake.now
    assert len(fake.create_requests) == 1
