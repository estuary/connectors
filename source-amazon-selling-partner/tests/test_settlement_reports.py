import gzip
import logging
from datetime import datetime, timedelta
from typing import Any

import pytest
from estuary_cdk.http import HTTPError

from source_amazon_selling_partner import api, reports, resources
from source_amazon_selling_partner.api import (
    backfill_settlement_reports,
    fetch_settlement_reports,
)
from source_amazon_selling_partner.client import (
    DEFAULT_LIMITS,
    Operation,
    OperationLimit,
    Priority,
    SPAPIClient,
)
from source_amazon_selling_partner.models import Report, SettlementReportRow
from source_amazon_selling_partner.reports import (
    RecentReports,
    ReportDocumentMissing,
    ReportDocumentTruncated,
    ReportEncodingError,
    charset_encoding,
    list_reports,
    read_report,
)

from .fake_amazon import FakeAmazon, FakeDocument, report, settlement_tsv
from .test_financial_transactions import record_operations
from .test_orders import NOW, make_config, FrozenDatetime, client_for, collect
from .test_resources import SteppingTime

LOG = logging.getLogger("test")
DAY = timedelta(days=1)
START = NOW - timedelta(days=60)
JAPAN = "A1VC38T7YXB528"


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api, "now", lambda: NOW)


def rows(out: list[Any]) -> list[SettlementReportRow]:
    return [r for r in out if isinstance(r, SettlementReportRow)]


def keys(out: list[Any]) -> list[tuple[str, int]]:
    return [(r.reportId, r.rowNumber) for r in rows(out)]


def cursors(out: list[Any]) -> list:
    return [r for r in out if not isinstance(r, SettlementReportRow)]


def fake_with(*settlements: tuple[str, datetime, int]) -> FakeAmazon:
    fake = FakeAmazon(now=NOW, orders=[])
    for report_id, created, items in settlements:
        fake.reports.append(report(report_id, created))
        fake.documents[f"doc-{report_id}"] = FakeDocument(
            settlement_tsv(f"s-{report_id}", [("Order", f"order-{i}", "Principal", "1.00") for i in range(items)]).encode()
        )
    return fake


async def backfill(fake: FakeAmazon, page: str | None = None, cutoff: datetime = NOW, start: datetime = START, recent: RecentReports | None = None):
    return await collect(
        backfill_settlement_reports(client_for(fake), recent or RecentReports(), start, LOG, page, cutoff)
    )


# Report engine


@pytest.mark.asyncio
async def test_list_reports_follows_next_token_as_the_only_parameter(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(Report, "PAGE_SIZE", 2)
    fake = fake_with(*[(f"r{i}", NOW - timedelta(days=i + 1), 1) for i in range(5)])
    request_params = {"reportTypes": SettlementReportRow.REPORT_TYPE, "processingStatuses": "DONE"}

    listed = await list_reports(client_for(fake), request_params, Priority.INCREMENTAL, LOG)

    assert sorted(r.reportId for r in listed) == ["r0", "r1", "r2", "r3", "r4"]
    assert fake.report_requests[0] == {**request_params, "pageSize": 2}
    assert len(fake.report_requests) == 3
    for req in fake.report_requests[1:]:
        assert list(req) == ["nextToken"]


@pytest.mark.asyncio
async def test_listing_asks_for_done_settlement_reports_in_the_window():
    fake = fake_with(("r1", NOW - DAY, 1))
    fake.reports.append(report("queued", NOW - DAY, status="IN_QUEUE", document_id=None))
    fake.reports.append(report("other-type", NOW - DAY, report_type="GET_FLAT_FILE_OPEN_LISTINGS_DATA"))

    out = await backfill(fake)

    assert {r.reportId for r in rows(out)} == {"r1"}
    assert fake.report_requests[0] == {
        "reportTypes": "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
        "processingStatuses": "DONE",
        "createdSince": api.to_iso(START),
        "createdUntil": api.to_iso(NOW - api.TICK),
        "pageSize": 100,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [False, True])
async def test_reads_plain_and_gzip_documents_without_the_access_token(compressed: bool):
    fake = fake_with(("r1", NOW - DAY, 3))
    fake.documents["doc-r1"].compressed = compressed
    client = client_for(fake)
    operations = record_operations(client)

    out = await collect(backfill_settlement_reports(client, RecentReports(), START, LOG, None, NOW))

    assert keys(out) == [("r1", n) for n in range(1, 5)]
    # The fake document host refuses requests that carry the access token.
    assert len(fake.downloads) == 1
    assert operations == [
        (Operation.GET_REPORTS, Priority.BACKFILL),
        (Operation.GET_REPORT_DOCUMENT, Priority.BACKFILL),
    ]


@pytest.mark.asyncio
async def test_rows_carry_report_metadata_and_values_exactly_as_in_the_file():
    fake = fake_with()
    created = NOW - DAY
    fake.reports.append(report("r1", created, marketplaces=["A2EUQ1WTGCTBG2"]))
    fake.documents["doc-r1"] = FakeDocument(
        settlement_tsv(
            "s-1",
            [
                ("Order", "111-222", 'Item "A"', "1.234,56"),
                ("Order", "111-223", '"Gift" wrap', "1"),
                ("Order", "111-224", '"', "2"),
                ("Order", "111-225", 'x"', "3"),
            ],
            total="1.234,56",
        ).encode()
    )

    summary, item, *quoted = rows(await backfill(fake))

    assert summary.summaryRow and not item.summaryRow
    assert summary.rowNumber == 1 and item.rowNumber == 2
    extra = item.model_extra or {}
    assert extra["amount"] == "1.234,56"
    assert extra["amount-description"] == 'Item "A"'
    assert (summary.model_extra or {})["total-amount"] == "1.234,56"
    # The summary row's blank transaction columns stay blank strings.
    assert (summary.model_extra or {})["order-id"] == ""
    assert item.reportId == "r1"
    assert item.reportDocumentId == "doc-r1"
    assert item.reportCreatedTime == created
    assert item.dataStartTime == created - timedelta(days=14)
    assert item.dataEndTime == created - timedelta(hours=1)
    assert item.marketplaceIds == ["A2EUQ1WTGCTBG2"]
    # Quotes are ordinary characters, even leading or unbalanced ones.
    assert [(r.model_extra or {})["amount-description"] for r in quoted] == ['"Gift" wrap', '"', 'x"']
    assert [(r.model_extra or {})["amount"] for r in quoted] == ["1", "2", "3"]


@pytest.mark.asyncio
async def test_identical_rows_are_kept_with_distinct_keys():
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    fake.documents["doc-r1"] = FakeDocument(
        settlement_tsv("s-1", [("Order", "111", "Principal", "5.00")] * 3).encode()
    )

    out = rows(await backfill(fake))

    assert [r.rowNumber for r in out] == [1, 2, 3, 4]
    assert len({r.model_dump_json(exclude={"rowNumber", "summaryRow"}) for r in out[1:]}) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["text/plain;charset=Shift_JIS", "text/plain; charset=windows-31j"])
async def test_japanese_documents_decode_as_cp932(content_type: str):
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY, marketplaces=[JAPAN]))
    # ① is in cp932 but not in strict Shift_JIS.
    text = settlement_tsv("s-1", [("注文", "250-1", "商品代金①", "１，０００")])
    fake.documents["doc-r1"] = FakeDocument(text.encode("cp932"), content_type=content_type)
    fake.download_chunk_size = 3  # splits multi-byte characters

    item = rows(await backfill(fake))[1]
    extra = item.model_extra or {}
    assert extra["transaction-type"] == "注文"
    assert extra["amount-description"] == "商品代金①"
    assert extra["amount"] == "１，０００"


@pytest.mark.asyncio
async def test_charset_from_content_type_is_used():
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    text = settlement_tsv("s-1", [("Order", "1", "Frais d'expédition", "3,50")])
    fake.documents["doc-r1"] = FakeDocument(text.encode("cp1252"), content_type="text/plain; charset=Cp1252")

    assert (rows(await backfill(fake))[1].model_extra or {})["amount-description"] == "Frais d'expédition"


@pytest.mark.asyncio
async def test_utf8_byte_order_mark_is_dropped():
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    fake.documents["doc-r1"] = FakeDocument(
        b"\xef\xbb\xbf" + settlement_tsv("s-1", []).encode(), content_type=None
    )
    assert "settlement-id" in (rows(await backfill(fake))[0].model_extra or {})


def test_charset_encoding_reads_supersets_and_rejects_unknown_charsets():
    assert charset_encoding(None) is None
    assert charset_encoding("text/plain") is None
    assert charset_encoding("text/plain;charset=UTF-8") == "utf-8-sig"
    assert charset_encoding("text/plain;charset=Shift_JIS") == "cp932"
    # ISO-8859-1 reads as Windows-1252, its superset.
    assert charset_encoding("text/plain;charset=iso-8859-1") == "cp1252"
    assert charset_encoding("text/plain;charset=latin1") == "cp1252"
    assert charset_encoding("text/plain;charset=windows-1252") == "cp1252"
    with pytest.raises(LookupError, match="no-such-charset"):
        charset_encoding("text/plain;charset=no-such-charset")


@pytest.mark.asyncio
@pytest.mark.parametrize("charset", ["ISO-8859-1", "latin1"])
async def test_iso_8859_1_document_reads_as_windows_1252(charset: str):
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    header, summary = settlement_tsv("s-1", []).encode().splitlines(keepends=True)
    # 0x80, 0x96 and 0x92 are a euro sign, an en dash and a curly
    # apostrophe in Windows-1252, but control characters in strict Latin-1.
    item = b"s-1\t\t\t\t\tEUR\tOrder\t1\tFrais \x80 \x96 l\x92envoi\t3,50\n"
    fake.documents["doc-r1"] = FakeDocument(
        header + summary + item, content_type=f"text/plain; charset={charset}"
    )
    fake.download_chunk_size = 3

    extra = rows(await backfill(fake))[1].model_extra or {}
    assert extra["amount-description"] == "Frais € – l’envoi"


@pytest.mark.asyncio
@pytest.mark.parametrize("content_type", ["text/plain", None])
async def test_document_without_charset_reads_as_utf8_and_warns(caplog: pytest.LogCaptureFixture, content_type: str | None):
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    text = settlement_tsv("s-1", [("注文", "250-1", "商品代金", "１，０００")])
    fake.documents["doc-r1"] = FakeDocument(text.encode(), content_type=content_type)
    fake.download_chunk_size = 3  # splits multi-byte characters

    with caplog.at_level(logging.WARNING, logger="test"):
        out = rows(await backfill(fake))

    assert (out[1].model_extra or {})["amount-description"] == "商品代金"
    warnings = [r for r in caplog.records if r.message.startswith("report document names no charset")]
    assert [r.args["content_type"] for r in warnings] == [content_type]  # type: ignore[index,call-overload]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item, content_type, encoding",
    [
        # Shift_JIS text, and no charset to say so.
        ("s-1\t\t\t\t\tJPY\tOrder\t1\t商品代金\t1\n".encode("cp932"), None, "utf-8-sig"),
        # 0x81 is undefined in Windows-1252.
        (b"s-1\t\t\t\t\tEUR\tOrder\t1\tFrais \x81\t1\n", "text/plain; charset=Cp1252", "cp1252"),
        # The document ends partway through a character.
        ("s-1\t\t\t\t\tJPY\tOrder\t1\tx\t商品".encode()[:-1], "text/plain;charset=UTF-8", "utf-8-sig"),
    ],
)
async def test_document_that_is_not_text_in_its_encoding_fails_naming_the_report(item: bytes, content_type: str | None, encoding: str):
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    header, summary = settlement_tsv("s-1", []).encode().splitlines(keepends=True)
    fake.documents["doc-r1"] = FakeDocument(header + summary + item, content_type=content_type)

    with pytest.raises(ReportEncodingError, match=f"doc-r1 of report r1 .* isn't {encoding} text") as err:
        await backfill(fake)
    assert repr(content_type) in str(err.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [b"", settlement_tsv("s-1", []).split("\n")[0].encode() + b"\n"])
async def test_document_without_data_rows_warns_with_its_id(caplog: pytest.LogCaptureFixture, content: bytes):
    fake = fake_with()
    fake.reports.append(report("empty", NOW - DAY))
    fake.documents["doc-empty"] = FakeDocument(content)

    with caplog.at_level(logging.WARNING, logger="test"):
        out = await backfill(fake)

    assert rows(out) == []
    warnings = [r for r in caplog.records if r.message == "report document has no data rows"]
    assert len(warnings) == 1
    assert warnings[0].args == {
        "report_id": "empty",
        "report_document_id": "doc-empty",
        "report_type": SettlementReportRow.REPORT_TYPE,
        "content_type": "text/plain;charset=UTF-8",
    }


@pytest.mark.asyncio
async def test_done_report_without_a_document_fails():
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY, document_id=None))
    with pytest.raises(ReportDocumentMissing, match="r1"):
        await backfill(fake)


@pytest.mark.asyncio
async def test_failed_document_fetch_fails_instead_of_reading_empty():
    fake = fake_with(("r1", NOW - DAY, 1))
    fake.failing_documents.add("doc-r1")
    with pytest.raises(HTTPError):
        await backfill(fake)


@pytest.mark.asyncio
async def test_document_url_is_fetched_fresh_for_every_download():
    fake = fake_with(("r1", NOW - DAY, 1), ("r2", NOW - 2 * DAY, 1))
    client = client_for(fake)
    events: list[str] = []
    get, download = client.get, client.download

    async def recording_get(log: logging.Logger, operation: Operation, *args: Any):
        events.append(operation)
        # Each request takes the fake a while, longer than a URL lasts in all.
        fake.now += timedelta(minutes=3)
        return await get(log, operation, *args)

    async def recording_download(log: logging.Logger, url: str, name: str):
        events.append("download")
        return await download(log, url, name)

    client.get = recording_get  # type: ignore[method-assign]
    client.download = recording_download  # type: ignore[method-assign]

    # Read the same reports twice, as a restart would.
    for _ in range(2):
        out = await collect(backfill_settlement_reports(client, RecentReports(), START, LOG, None, NOW))
        assert len(rows(out)) == 4

    one_pass = [Operation.GET_REPORTS] + [Operation.GET_REPORT_DOCUMENT, "download"] * 2
    assert events == one_pass * 2
    assert len(set(fake.downloads)) == 4


@pytest.mark.asyncio
async def test_an_expired_url_fails():
    fake = fake_with(("r1", NOW - DAY, 1))
    client = client_for(fake)
    doc = await reports.fetch_report_document(client, "doc-r1", Priority.INCREMENTAL, LOG)
    fake.now += timedelta(minutes=6)
    with pytest.raises(HTTPError) as raised:
        await client.download(LOG, doc.url, "report document doc-r1")
    assert raised.value.code == 403


@pytest.mark.asyncio
async def test_a_failed_download_does_not_reveal_the_signed_url():
    fake = fake_with(("r1", NOW - DAY, 1))
    client = client_for(fake)
    get = client.get

    async def slow_get(*args: Any):
        # The document URL expires before it is downloaded.
        response = await get(*args)
        fake.now += timedelta(minutes=6)
        return response

    client.get = slow_get  # type: ignore[method-assign]

    with pytest.raises(HTTPError) as raised:
        await collect(backfill_settlement_reports(client, RecentReports(), START, LOG, None, NOW))

    message = str(raised.value)
    assert raised.value.code == 403
    assert "X-Amz-" not in message and "Signature" not in message and "?" not in message
    assert "doc-r1" in message
    assert "https://fake-documents.example/doc-r1" in message
    assert raised.value.__suppress_context__


@pytest.mark.asyncio
async def test_column_named_like_a_connector_field_fails():
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    fake.documents["doc-r1"] = FakeDocument(b"rowNumber\tamount\n1\t2\n")
    with pytest.raises(RuntimeError, match="rowNumber"):
        await backfill(fake)


@pytest.mark.asyncio
@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("content_type", ["text/plain;charset=UTF-8", None])
@pytest.mark.parametrize("first_description", ["Principal", "Gebühr"])
async def test_document_is_streamed_not_read_whole(compressed: bool, content_type: str | None, first_description: str):
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    items = [("Order", f"{i:03d}-{i * 7919 % 10007:05d}", f"Principal {i}", f"{i}.{i % 100:02d}") for i in range(5000)]
    items[0] = ("Order", "000-00000", first_description, "0.00")
    content = settlement_tsv("s-1", items).encode()
    fake.documents["doc-r1"] = FakeDocument(content, compressed=compressed, content_type=content_type)
    fake.download_chunk_size = 64
    listed = await list_reports(
        client_for(fake), {"reportTypes": SettlementReportRow.REPORT_TYPE}, Priority.BACKFILL, LOG
    )

    gen = read_report(client_for(fake), listed[0], Priority.BACKFILL, LOG)
    await anext(gen)  # the summary row
    first = await anext(gen)
    served_for_first_rows = fake.chunks_served
    rest = [row async for row in gen]

    assert first[1]["amount-description"] == first_description
    assert len(rest) == 4999
    assert served_for_first_rows < fake.chunks_served / 10


@pytest.mark.asyncio
async def test_gzip_document_of_several_members_is_read_in_full():
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    text = settlement_tsv("s-1", [("Order", str(i), "Principal", "1") for i in range(4)]).encode()
    lines = text.splitlines(keepends=True)
    head, tail = b"".join(lines[:3]), b"".join(lines[3:])
    fake.documents["doc-r1"] = FakeDocument(text, compressed=True, served=gzip.compress(head) + gzip.compress(tail))

    out = rows(await backfill(fake))

    assert [r.rowNumber for r in out] == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
@pytest.mark.parametrize("cut", [8, 20])
async def test_cut_short_gzip_document_fails(cut: int):
    fake = fake_with()
    fake.reports.append(report("r1", NOW - DAY))
    text = settlement_tsv("s-1", [("Order", str(i), "Principal", "1") for i in range(4)]).encode()
    # 8 drops only the trailer, which follows a complete last line.
    fake.documents["doc-r1"] = FakeDocument(text, compressed=True, served=gzip.compress(text)[:-cut])

    with pytest.raises(ReportDocumentTruncated):
        await backfill(fake)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["IN_PROGRESS", "FATAL"])
async def test_reading_a_report_that_is_not_done_fails(status: str):
    fake = fake_with()
    unfinished = Report.model_validate(report("r1", NOW - DAY, status=status))

    with pytest.raises(ValueError, match="not DONE"):
        await collect(read_report(client_for(fake), unfinished, Priority.INCREMENTAL, LOG))
    assert fake.document_requests == []


# Backfill


@pytest.mark.asyncio
async def test_backfill_reads_oldest_first_and_checkpoints_between_created_times():
    t1, t2 = NOW - 20 * DAY, NOW - 10 * DAY
    fake = fake_with(("a", t1, 1), ("c", t2, 1), ("b", t2, 1), ("d", NOW - DAY, 1))

    out = await backfill(fake)

    assert [k for k in keys(out) if k[1] == 1] == [("a", 1), ("b", 1), ("c", 1), ("d", 1)]
    # No cursor between b and c, which share a createdTime, and none after
    # the last report, which ends the backfill.
    assert cursors(out) == [t1.isoformat(), t2.isoformat()]
    assert out.index(t1.isoformat()) == 2
    assert out.index(t2.isoformat()) == 7


@pytest.mark.asyncio
async def test_backfill_restart_mid_report_rereads_it_and_skips_nothing():
    t1, t2, t3 = NOW - 20 * DAY, NOW - 10 * DAY, NOW - 5 * DAY
    fake = fake_with(("a", t1, 2), ("b", t2, 2), ("c", t3, 2))

    # The first run stops partway through report b, after a's checkpoint.
    first: list = []
    gen = backfill_settlement_reports(client_for(fake), RecentReports(), START, LOG, None, NOW)
    async for item in gen:
        first.append(item)
        if keys(first)[-1:] == [("b", 2)]:
            break
    await gen.aclose()
    saved = cursors(first)[-1]
    assert saved == t1.isoformat()

    # A new process resumes from the saved cursor.
    second = await backfill(fake, page=saved)
    assert keys(second)[0] == ("b", 1)
    assert {k[0] for k in keys(second)} == {"b", "c"}
    assert set(keys(first)) | set(keys(second)) == {(r, n) for r in "abc" for n in (1, 2, 3)}
    assert fake.report_requests[-1]["createdSince"] == api.to_iso(t1)


@pytest.mark.asyncio
async def test_backfill_marks_a_report_read_only_after_its_last_row():
    fake = fake_with(("a", NOW - 20 * DAY, 2))
    recent = RecentReports()

    gen = backfill_settlement_reports(client_for(fake), recent, START, LOG, None, NOW)
    assert isinstance(await anext(gen), SettlementReportRow)
    await gen.aclose()
    assert "a" not in recent.created

    # The same process reads it again, in full.
    out = await backfill(fake, recent=recent)
    assert keys(out) == [("a", 1), ("a", 2), ("a", 3)]
    assert "a" in recent.created


@pytest.mark.asyncio
async def test_backfill_resume_keeps_reports_sharing_a_fractional_watermark_second():
    t1 = NOW - 20 * DAY + timedelta(milliseconds=500)
    fake = fake_with(("a", t1, 1))
    fake.reports.append(report("b", t1 + timedelta(milliseconds=200)))
    fake.reports[-1]["createdTime"] = (t1 + timedelta(milliseconds=200)).isoformat()
    fake.reports[0]["createdTime"] = t1.isoformat()
    fake.documents["doc-b"] = fake.documents["doc-a"]

    out = await backfill(fake, page=t1.isoformat())

    assert {k[0] for k in keys(out)} == {"b"}


@pytest.mark.asyncio
async def test_backfill_starts_no_earlier_than_amazons_retention():
    fake = fake_with(("old", NOW - 89 * DAY, 1))

    out = await backfill(fake, start=NOW - 400 * DAY)

    assert {k[0] for k in keys(out)} == {"old"}
    since = datetime.fromisoformat(fake.report_requests[0]["createdSince"])
    assert NOW - reports.REPORT_RETENTION < since <= NOW - reports.REPORT_RETENTION + reports.RETENTION_MARGIN


@pytest.mark.asyncio
async def test_backfill_with_start_date_at_or_after_cutoff_requests_nothing():
    fake = fake_with()
    assert await backfill(fake, start=NOW) == []
    assert fake.report_requests == []


@pytest.mark.asyncio
async def test_backfill_skips_reports_already_read_by_this_process():
    fake = fake_with(("a", NOW - 2 * DAY, 1))
    recent = RecentReports()
    await backfill(fake, recent=recent)
    out = await backfill(fake, recent=recent)
    assert rows(out) == []
    assert len(fake.downloads) == 1


# Incremental


@pytest.mark.asyncio
async def test_incremental_lists_the_overlap_and_advances_to_the_last_elapsed_second():
    cursor = NOW - timedelta(hours=1)
    fake = fake_with(
        ("in-overlap", cursor - DAY, 1),
        ("new", cursor + timedelta(minutes=30), 1),
        ("before-overlap", cursor - api.REPORT_LISTING_OVERLAP, 1),
    )
    client = client_for(fake)
    operations = record_operations(client)

    out = await collect(fetch_settlement_reports(client, RecentReports(), START, LOG, cursor))

    assert {k[0] for k in keys(out)} == {"in-overlap", "new"}
    assert out[-1] == NOW - api.TICK
    assert cursors(out) == [NOW - api.TICK]
    assert fake.report_requests[0]["createdSince"] == api.to_iso(cursor + api.TICK - api.REPORT_LISTING_OVERLAP)
    assert fake.report_requests[0]["createdUntil"] == api.to_iso(NOW - api.TICK)
    assert {op for op, _ in operations} == {Operation.GET_REPORTS, Operation.GET_REPORT_DOCUMENT}
    assert {p for _, p in operations} == {Priority.INCREMENTAL}


@pytest.mark.asyncio
async def test_incremental_reads_each_report_once_per_process_and_catches_late_listings(monkeypatch: pytest.MonkeyPatch):
    cursor = NOW - timedelta(hours=1)
    fake = fake_with(("first", cursor - DAY, 1))
    client = client_for(fake)
    recent = RecentReports()

    out = await collect(fetch_settlement_reports(client, recent, START, LOG, cursor))
    assert {k[0] for k in keys(out)} == {"first"}

    # An hour later Amazon lists a report created before the previous cursor.
    later = NOW + timedelta(hours=1)
    monkeypatch.setattr(api, "now", lambda: later)
    fake.now = later
    fake.reports.append(report("late", cursor - timedelta(hours=2)))
    fake.documents["doc-late"] = fake.documents["doc-first"]

    out = await collect(fetch_settlement_reports(client, recent, START, LOG, NOW - api.TICK))
    assert {k[0] for k in keys(out)} == {"late"}
    assert fake.document_requests == ["doc-first", "doc-late"]

    # After a restart an hour later, the overlap is read again.
    restarted = later + timedelta(hours=1)
    monkeypatch.setattr(api, "now", lambda: restarted)
    fake.now = restarted
    out = await collect(fetch_settlement_reports(client, RecentReports(), START, LOG, later - api.TICK))
    assert {k[0] for k in keys(out)} == {"first", "late"}


@pytest.mark.asyncio
async def test_incremental_marks_a_report_read_only_after_its_last_row():
    cursor = NOW - timedelta(hours=1)
    fake = fake_with(("a", cursor - DAY, 2))
    client = client_for(fake)
    recent = RecentReports()

    gen = fetch_settlement_reports(client, recent, START, LOG, cursor)
    assert isinstance(await anext(gen), SettlementReportRow)
    await gen.aclose()
    assert "a" not in recent.created

    # The next poll in the same process reads it again, in full.
    out = await collect(fetch_settlement_reports(client, recent, START, LOG, cursor))
    assert keys(out) == [("a", 1), ("a", 2), ("a", 3)]
    assert fake.document_requests == ["doc-a", "doc-a"]


@pytest.mark.asyncio
async def test_incremental_waits_when_caught_up():
    fake = fake_with()
    assert await collect(fetch_settlement_reports(client_for(fake), RecentReports(), START, LOG, NOW - api.TICK)) == []
    assert fake.report_requests == []


@pytest.mark.asyncio
async def test_incremental_never_lists_past_amazons_retention(caplog: pytest.LogCaptureFixture):
    fake = fake_with(("kept", NOW - 80 * DAY, 1))
    with caplog.at_level(logging.WARNING, logger="test"):
        out = await collect(fetch_settlement_reports(client_for(fake), RecentReports(), NOW - 400 * DAY, LOG, NOW - 120 * DAY))
    assert {k[0] for k in keys(out)} == {"kept"}
    assert any("90-day retention" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_reports_amazon_returns_outside_the_window_are_not_read():
    cursor = NOW - timedelta(hours=1)
    since = cursor + api.TICK - api.REPORT_LISTING_OVERLAP
    fake = fake_with(
        ("first", since, 1),
        ("last", NOW - api.TICK, 1),
        ("too-early", since - api.TICK, 1),
        ("too-late", NOW, 1),
    )
    fake.ignore_report_bounds = True

    out = await collect(fetch_settlement_reports(client_for(fake), RecentReports(), START, LOG, cursor))

    assert {k[0] for k in keys(out)} == {"first", "last"}
    assert sorted(fake.document_requests) == ["doc-first", "doc-last"]


@pytest.mark.asyncio
async def test_incremental_lists_nothing_created_before_start_date():
    cursor = NOW - timedelta(hours=1)
    start = cursor - DAY + timedelta(milliseconds=300)
    fake = fake_with(("before-start", start - timedelta(hours=1), 1), ("after-start", start + DAY / 2, 1))

    out = await collect(fetch_settlement_reports(client_for(fake), RecentReports(), start, LOG, cursor))

    assert {k[0] for k in keys(out)} == {"after-start"}
    assert fake.report_requests[0]["createdSince"] == api.to_iso(api.floor_to_tick(start))
    assert cursors(out) == [NOW - api.TICK]


@pytest.mark.asyncio
@pytest.mark.parametrize("start", [NOW - 30 * DAY, NOW + timedelta(hours=2)])
async def test_a_later_start_date_never_skips_reports_created_after_the_cursor(start: datetime):
    # A blank start_date is 30 days before each restart, so after a pause
    # it can be later than the cursor.
    cursor = NOW - 45 * DAY
    fake = fake_with(
        ("before-cursor", cursor - DAY, 1),
        ("after-cursor", cursor + DAY, 1),
        ("recent", NOW - DAY, 1),
    )

    out = await collect(fetch_settlement_reports(client_for(fake), RecentReports(), start, LOG, cursor))

    assert {k[0] for k in keys(out)} == {"after-cursor", "recent"}
    # The overlap, all before start_date, is not listed again.
    assert fake.report_requests[0]["createdSince"] == api.to_iso(cursor + api.TICK)
    assert cursors(out) == [NOW - api.TICK]


@pytest.mark.asyncio
async def test_report_finished_after_the_overlap_is_missed_until_the_binding_is_backfilled(monkeypatch: pytest.MonkeyPatch):
    cursor = NOW - timedelta(hours=1)
    created = cursor - api.REPORT_LISTING_OVERLAP - DAY
    fake = fake_with(("slow", created, 1))
    # Amazon finishes the report only after the cursor has passed the
    # overlap beyond its createdTime.
    fake.reports[0]["processingStatus"] = "IN_PROGRESS"
    time = SteppingTime()
    client = SPAPIClient(http=fake, base_url="https://fake", clock=time.clock, sleep=time.sleep)  # type: ignore[arg-type]
    out = await collect(fetch_settlement_reports(client, RecentReports(), START, LOG, cursor - 10 * DAY))
    assert keys(out) == []
    fake.reports[0]["processingStatus"] = "DONE"

    out = await collect(fetch_settlement_reports(client, RecentReports(), START, LOG, cursor))
    assert keys(out) == []

    # The binding sets no backfill schedule: a scheduled backfill restarts
    # the capture and reads every report Amazon still keeps again.
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    resource = resources.settlement_reports(LOG, client, make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(START)))
    resource.open(None, 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]
    assert resource.initial_config.schedule == ""

    # Backfilling the binding reads it.
    backfilled = await collect(recorded["fetch_page"](LOG, None, cursor))
    assert {k[0] for k in keys(backfilled)} == {"slow"}


# Resource wiring and the seam


@pytest.mark.asyncio
async def test_seam_between_backfill_and_incremental(monkeypatch: pytest.MonkeyPatch):
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff + timedelta(microseconds=250_000)))
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    fake = fake_with(
        ("before", cutoff - api.TICK, 1),
        ("at", cutoff, 1),
        ("after", cutoff + api.TICK, 1),
    )
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(NOW - 30 * DAY))
    resource = resources.settlement_reports(LOG, client_for(fake), config)
    resource.open(None, 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]

    state = resource.initial_state
    assert resource.key == ["/reportId", "/rowNumber"]
    assert state.backfill.cutoff == cutoff
    assert state.inc.cursor == cutoff - api.TICK

    backfilled = await collect(recorded["fetch_page"](LOG, None, state.backfill.cutoff))
    incremental = await collect(recorded["fetch_changes"](LOG, state.inc.cursor))

    assert {k[0] for k in keys(backfilled)} == {"before"}
    # The overlap lists "before" again, but this process already read it.
    assert {k[0] for k in keys(incremental)} == {"at", "after"}
    assert fake.document_requests.count("doc-before") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("start_after_cutoff", [timedelta(days=-1), timedelta(hours=2)])
async def test_wired_backfill_honours_a_recent_or_future_start_date(monkeypatch: pytest.MonkeyPatch, start_after_cutoff: timedelta):
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff))
    recorded: dict = {}
    monkeypatch.setattr(resources, "open_binding", lambda *args, **kwargs: recorded.update(kwargs))
    start = cutoff + start_after_cutoff
    fake = fake_with(("before-start", cutoff - 2 * DAY, 1), ("after-cutoff", cutoff + timedelta(minutes=30), 1))
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(start))
    time = SteppingTime()
    client = SPAPIClient(http=fake, base_url="https://fake", clock=time.clock, sleep=time.sleep)  # type: ignore[arg-type]
    resource = resources.settlement_reports(LOG, client, config)
    resource.open(None, 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]

    state = resource.initial_state
    backfilled = await collect(recorded["fetch_page"](LOG, None, state.backfill.cutoff))
    incremental = await collect(recorded["fetch_changes"](LOG, state.inc.cursor))

    assert keys(backfilled) == []
    # As with orders, reports created after the cutoff and before a later
    # start_date are captured.
    assert {k[0] for k in keys(incremental)} == {"after-cutoff"}
    assert "doc-before-start" not in fake.document_requests


def test_settlement_reports_poll_hourly(monkeypatch: pytest.MonkeyPatch):
    resource = resources.settlement_reports(LOG, client_for(fake_with()), make_config(["ATVPDKIKX0DER"]))
    assert resource.initial_config.interval == timedelta(hours=1)


def test_report_operations_use_amazons_default_limits():
    assert DEFAULT_LIMITS[Operation.GET_REPORTS] == OperationLimit(rate=0.0222, burst=10)
    assert DEFAULT_LIMITS[Operation.GET_REPORT] == OperationLimit(rate=2, burst=15)
    assert DEFAULT_LIMITS[Operation.GET_REPORT_DOCUMENT] == OperationLimit(rate=0.0167, burst=15)


@pytest.mark.asyncio
async def test_backfill_from_the_initial_state_skips_reports_just_before_start_date(monkeypatch: pytest.MonkeyPatch):
    start = NOW - 10 * DAY
    fake = fake_with(
        ("just-before", start - timedelta(milliseconds=500), 1),
        ("at-start", start, 1),
    )
    # Amazon's createdTime has milliseconds; report() writes whole seconds.
    fake.reports[0]["createdTime"] = (start - timedelta(milliseconds=500)).isoformat()
    client = client_for(fake)
    state = resources.settlement_reports(LOG, client, make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(start))).initial_state
    out = await collect(
        backfill_settlement_reports(client, RecentReports(), start, LOG, state.backfill.next_page, state.backfill.cutoff)
    )
    assert {k[0] for k in keys(out)} == {"at-start"}
