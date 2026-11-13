import gzip
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import Logger
from typing import Any

from estuary_cdk.http import HTTPError

from source_amazon_selling_partner.models import FinancialTransaction, Order, Report

SEARCH_LAG_MIN = timedelta(minutes=2)
MAX_POSTED_SPAN = timedelta(days=180)
REPORT_RETENTION = timedelta(days=90)
DOCUMENT_URL_LIFETIME = timedelta(minutes=5)
DOCUMENT_HOST = "https://fake-documents.example/"
DOCUMENTS_PATH = "/reports/2021-06-30/documents/"
REPORT_PATH = "/reports/2021-06-30/reports/"
# The longest data range of an order report, and createReport's limit on
# marketplaces.
MAX_ORDER_REPORT_RANGE = timedelta(days=30)
MAX_REPORT_MARKETPLACES = 25

# Amazon cancels a Sales and Traffic report that starts more than two years
# ago.
SALES_AND_TRAFFIC_HISTORY = timedelta(days=730)
SALES_AND_TRAFFIC_OPTIONS = {
    "dateGranularity": {"DAY", "WEEK", "MONTH"},
    "asinGranularity": {"PARENT", "CHILD", "SKU"},
}


@dataclass
class FakeDocument:
    """A report document's bytes as stored, before any compression."""

    content: bytes
    compressed: bool = False
    content_type: str | None = "text/plain;charset=UTF-8"
    # Bytes served in place of the (compressed) content, such as a
    # cut-short or multi-member gzip file.
    served: bytes | None = None


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(value: str) -> datetime:
    return datetime.fromisoformat(value)


@dataclass
class FakeAmazon:
    """
    Stands in for HTTPSession.request_stream, enforcing the searchOrders rules
    the connector relies on: exactly one lower bound, inclusive bounds, an
    upper bound at least two minutes before now, and token pagination.

    listTransactions follows its model file: postedAfter is required and
    inclusive, postedBefore is exclusive and defaults to two minutes before
    now, both must be more than two minutes before now, windows longer than
    180 days come back empty, and a nextToken page must repeat the arguments
    of the request that produced it. It can also return empty pages.

    getReports and getReportDocument follow their model file (see
    _get_reports), and documents are served from pre-signed URLs that expire
    five minutes after they are issued and refuse the access token.

    createReport and getReport make order reports by last update and Sales
    and Traffic reports (see _create_report), whose statuses can be scripted
    per report.
    """

    now: datetime
    orders: list[dict[str, Any]]
    transactions: list[dict[str, Any]] = field(default_factory=list)
    # Put an empty page, with a nextToken, before every page of transactions.
    empty_transaction_pages: bool = False
    # Leave the transactions array out of empty pages, which the model allows.
    omit_empty_transactions: bool = False
    # Send the transactions array of empty pages as null instead.
    null_empty_transactions: bool = False
    # Leave postedDate out of returned transactions; filtering still uses it.
    omit_posted_date: bool = False
    payload_key: str = "payload"
    transactions_key: str = "transactions"
    page_size: int = 2
    rate_header: str | None = None
    expired_tokens: bool = False
    # Fail the access-token refresh that precedes this page, as Login with
    # Amazon does when the seller revokes the application.
    refresh_fails_on_page: int | None = None
    orders_key: str = "orders"
    # Echo the search's upper bound this much earlier than requested, as if
    # Amazon had searched a shorter window.
    echoed_bound_shortfall: timedelta | None = None
    reports: list[dict[str, Any]] = field(default_factory=list)
    documents: dict[str, FakeDocument] = field(default_factory=dict)
    # Report document ids whose getReportDocument fails.
    failing_documents: set[str] = field(default_factory=set)
    # Bytes per streamed chunk of a document download.
    download_chunk_size: int = 5
    # Chunks of document downloads handed out so far.
    chunks_served: int = 0
    # Ignore getReports' createdSince and createdUntil, as Amazon might if
    # their inclusivity differs from what the connector assumes.
    ignore_report_bounds: bool = False
    # Lines of the order reports createReport makes, as column -> value. A
    # report holds the lines whose last-updated-date is in its range.
    order_lines: list[dict[str, str]] = field(default_factory=list)
    # Exclude both bounds of a requested report's range, as Amazon might.
    exclusive_data_bounds: bool = False
    # How long after its last-updated-date an order line is recorded. A
    # report holds only lines recorded by its createdTime. Amazon's order
    # report page gives up to 30 minutes.
    record_delay: timedelta = timedelta(0)
    # The statuses each requested report goes through, in creation order:
    # the first on creation, then one more per getReport. A report stays at
    # its last status. Reports past the list follow default_lifecycle.
    lifecycles: list[list[str]] = field(default_factory=list)
    default_lifecycle: list[str] = field(
        default_factory=lambda: ["IN_QUEUE", "IN_PROGRESS", "DONE"]
    )
    # List requested reports with dataStartTime or dataEndTime this much
    # later than sent.
    echoed_start_shift: timedelta | None = None
    echoed_end_shift: timedelta | None = None
    # createReport calls whose response is lost after the report is made, so
    # the HTTP client sends the same request again.
    lost_create_responses: int = 0
    # The diagnostic document a report that ends FATAL gets, if any.
    fatal_document: FakeDocument | None = None
    # The salesAndTrafficByDate entries of each marketplace, which Sales and
    # Traffic reports are made from.
    sales_and_traffic: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # The tab-separated listings report for each marketplace.
    listings: dict[str, str] = field(default_factory=dict)
    # The catalog items returned for each marketplace.
    catalog_items: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    catalog_requests: list[dict[str, Any]] = field(default_factory=list)
    catalog_forbidden: bool = False
    catalog_page_size: int = 20
    # Whether the application and seller have the Brand Analytics role, which
    # createReport for Sales and Traffic needs.
    brand_analytics: bool = True
    # Report types createReport refuses with a 403.
    forbidden_report_types: set[str] = field(default_factory=set)
    requests: list[dict[str, Any]] = field(default_factory=list)
    report_requests: list[dict[str, Any]] = field(default_factory=list)
    create_requests: list[dict[str, Any]] = field(default_factory=list)
    get_report_requests: list[str] = field(default_factory=list)
    _statuses_left: dict[str, list[str]] = field(default_factory=dict)
    _report_options: dict[str, dict[str, str]] = field(default_factory=dict)
    document_requests: list[str] = field(default_factory=list)
    downloads: list[str] = field(default_factory=list)
    _report_pages: dict[str, tuple[int, list[dict[str, Any]]]] = field(default_factory=dict)
    _issued_urls: dict[str, tuple[str, datetime]] = field(default_factory=dict)
    _pages: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    _transaction_pages: dict[str, tuple[dict[str, Any], list[list[dict[str, Any]]]]] = field(
        default_factory=dict
    )

    async def request_stream(self, log: Logger, url: str, method: str = "GET", params: dict[str, Any] | None = None, **kwargs: Any):
        params = dict(params or {})

        # Report traffic is recorded apart from requests, which existing
        # tests count.
        if url.startswith(DOCUMENT_HOST):
            return self._download(url, params, kwargs)
        if method == "POST":
            assert url.endswith(Report.LIST_PATH), url
            return self._create_report(dict(kwargs["json"]))
        if REPORT_PATH in url:
            return self._get_report(url.rsplit("/", 1)[1], params)
        if url.endswith(Report.LIST_PATH):
            self.report_requests.append(params)
            return self._get_reports(params)
        if DOCUMENTS_PATH in url:
            return self._get_report_document(url.rsplit("/", 1)[1], params)

        if url.endswith("/catalog/2022-04-01/items"):
            self.catalog_requests.append(params)
            if self.catalog_forbidden:
                raise HTTPError("403: Access denied", 403)
            assert params["identifiersType"] == "ASIN"
            assert params["includedData"] == "summaries"
            assert params["pageSize"] == 20
            marketplace = params["marketplaceIds"]
            assert "," not in marketplace
            identifiers = params["identifiers"].split(",")
            assert len(identifiers) <= 20
            assert all(identifier.strip() for identifier in identifiers)
            items = [i for i in self.catalog_items.get(marketplace, []) if i["asin"] in identifiers]
            offset = int(params.get("pageToken", 0))
            page = items[offset:offset + self.catalog_page_size]
            pagination = {}
            if offset + len(page) < len(items):
                pagination["nextToken"] = str(offset + len(page))
            return self._respond({"numberOfResults": len(items), "items": page, "pagination": pagination})

        self.requests.append(params)

        if url.endswith("/sellers/v1/marketplaceParticipations"):
            return self._respond({"payload": [{"marketplace": {"id": "ATVPDKIKX0DER"}}]})

        if url.endswith(FinancialTransaction.PATH):
            return self._list_transactions(dict(params))

        assert url.endswith(Order.PATH), url

        if self.refresh_fails_on_page == len(self.requests):
            raise HTTPError(
                "Encountered HTTP error status 400 which cannot be retried.\n"
                "URL: https://api.amazon.com/auth/o2/token\nResponse:\ninvalid_grant",
                400,
            )

        if "paginationToken" in params:
            if self.expired_tokens:
                raise HTTPError("400 Bad Request: InvalidInput", 400)
            matching = self._pages.pop(params["paginationToken"])
        else:
            matching = self._search(params)

        page, rest = matching[: self.page_size], matching[self.page_size :]
        body: dict[str, Any] = {self.orders_key: page}
        if rest:
            token = f"token-{len(self.requests)}"
            self._pages[token] = rest
            body["pagination"] = {"nextToken": token}
        for bound in ("lastUpdatedBefore", "createdBefore"):
            if bound in params:
                body[bound] = params[bound]
                if self.echoed_bound_shortfall:
                    body[bound] = iso(parse(params[bound]) - self.echoed_bound_shortfall)
        return self._respond(body)

    def _search(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        has_updated = "lastUpdatedAfter" in params
        has_created = "createdAfter" in params
        if has_updated == has_created:
            raise HTTPError("400: exactly one of createdAfter, lastUpdatedAfter", 400)

        field_name, after, before = (
            ("lastUpdatedTime", "lastUpdatedAfter", "lastUpdatedBefore")
            if has_updated
            else ("createdTime", "createdAfter", "createdBefore")
        )
        lo = parse(params[after])
        hi = parse(params[before]) if before in params else self.now - SEARCH_LAG_MIN
        if hi > self.now - SEARCH_LAG_MIN:
            raise HTTPError(f"400: {before} must be at least 2 minutes before now", 400)

        return [o for o in self.orders if lo <= parse(o[field_name]) <= hi]

    def _list_transactions(self, params: dict[str, Any]):
        if "nextToken" in params:
            token = params.pop("nextToken")
            if token not in self._transaction_pages:
                raise HTTPError("400: invalid nextToken", 400)
            args, pages = self._transaction_pages.pop(token)
            if params != args:
                raise HTTPError("400: nextToken must be sent with the same arguments", 400)
        else:
            args = params
            matching = self._search_transactions(params)
            pages = [matching[i : i + self.page_size] for i in range(0, len(matching), self.page_size)] or [[]]
            if self.empty_transaction_pages:
                pages = [page for chunk in pages for page in ([], chunk)]

        page, rest = pages[0], pages[1:]
        if self.omit_posted_date:
            page = [{k: v for k, v in t.items() if k != "postedDate"} for t in page]
        payload: dict[str, Any] = {}
        if page or not self.omit_empty_transactions:
            payload[self.transactions_key] = page or (None if self.null_empty_transactions else [])
        if rest:
            token = f"txn-token-{len(self.requests)}"
            self._transaction_pages[token] = (args, rest)
            payload["nextToken"] = token
        return self._respond({self.payload_key: payload})

    def _search_transactions(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        latest = self.now - SEARCH_LAG_MIN
        if "postedAfter" not in params:
            raise HTTPError("400: postedAfter is required without a related identifier", 400)
        lo = parse(params["postedAfter"])
        if lo >= latest:
            raise HTTPError("400: postedAfter must be more than 2 minutes before now", 400)
        hi = latest
        if "postedBefore" in params:
            hi = parse(params["postedBefore"])
            if hi <= lo or hi >= latest:
                raise HTTPError(
                    "400: postedBefore must be later than postedAfter and more than 2 minutes before now",
                    400,
                )
        if hi - lo > MAX_POSTED_SPAN:
            return []

        marketplace = params.get("marketplaceId")
        return [
            t
            for t in self.transactions
            if lo <= parse(t["postedDate"]) < hi
            and (marketplace is None or t["marketplaceDetails"]["marketplaceId"] == marketplace)
        ]

    def _get_reports(self, params: dict[str, Any]):
        """
        getReports per its model file: a nextToken request carries no other
        parameter, other requests need reportTypes, pageSize is 1 to 100 and
        defaults to 10, createdSince defaults to 90 days ago and createdUntil
        to now, and only reports from the last 90 days exist. Bounds are
        inclusive. Reports come newest first. That an older createdSince is
        rejected is this fake's assumption.
        """
        if "nextToken" in params:
            if len(params) != 1:
                raise HTTPError("400: nextToken must be the only parameter", 400)
            if params["nextToken"] not in self._report_pages:
                raise HTTPError("400: invalid nextToken", 400)
            page_size, matching = self._report_pages.pop(params["nextToken"])
        else:
            if "reportTypes" not in params:
                raise HTTPError("400: reportTypes or nextToken is required", 400)
            types = params["reportTypes"].split(",")
            if not 1 <= len(types) <= 10:
                raise HTTPError("400: reportTypes takes 1 to 10 values", 400)
            page_size = int(params.get("pageSize", 10))
            if not 1 <= page_size <= 100:
                raise HTTPError("400: pageSize must be 1 to 100", 400)

            oldest = self.now - REPORT_RETENTION
            since = parse(params["createdSince"]) if "createdSince" in params else oldest
            until = parse(params["createdUntil"]) if "createdUntil" in params else self.now
            if since < oldest:
                raise HTTPError("400: createdSince is past the 90-day retention", 400)
            statuses = params["processingStatuses"].split(",") if "processingStatuses" in params else None
            matching = sorted(
                (
                    r
                    for r in self.reports
                    if r["reportType"] in types
                    and (statuses is None or r["processingStatus"] in statuses)
                    and oldest <= parse(r["createdTime"])
                    and (self.ignore_report_bounds or since <= parse(r["createdTime"]) <= until)
                ),
                key=lambda r: r["createdTime"],
                reverse=True,
            )

        page, rest = matching[:page_size], matching[page_size:]
        body: dict[str, Any] = {"reports": page}
        if rest:
            token = f"report-token-{len(self.report_requests)}"
            self._report_pages[token] = (page_size, rest)
            body["nextToken"] = token
        return self._respond(body)

    def script(self, report_id: str, statuses: list[str]) -> None:
        """Sets the statuses a requested report goes through from its next getReport."""
        self._statuses_left[report_id] = list(statuses)

    def _create_report(self, body: dict[str, Any]):
        """
        createReport per its model file: reportType and 1 to 25 marketplaceIds
        are required, and the range's bounds default to now and must not be in
        the future. Rejecting an end before the start, or a range longer than
        an order report's 30 days, is this fake's assumption. Reports are
        listed with their marketplaceIds and range as sent.

        A Sales and Traffic report follows its schema: one marketplace,
        known option values, and cancelled if it starts more than two years
        ago. Without the Brand Analytics role, its createReport fails with
        403. Its document echoes the options and range in its
        reportSpecification and holds the marketplace's by-date entries in
        that range.
        """
        self.create_requests.append(body)
        if "reportType" not in body:
            raise HTTPError("400: reportType is required", 400)
        if not 1 <= len(body.get("marketplaceIds") or []) <= MAX_REPORT_MARKETPLACES:
            raise HTTPError("400: marketplaceIds takes 1 to 25 values", 400)
        body.setdefault("dataStartTime", iso(self.now))
        body.setdefault("dataEndTime", iso(self.now))
        start, end = parse(body["dataStartTime"]), parse(body["dataEndTime"])
        if end > self.now:
            raise HTTPError("400: dataEndTime must not be in the future", 400)
        if end < start:
            raise HTTPError("400: dataEndTime is before dataStartTime", 400)
        if end - start > MAX_ORDER_REPORT_RANGE:
            raise HTTPError("400: the date range is limited to 30 days", 400)

        options = body.get("reportOptions", {})
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in options.items()):
            raise HTTPError("400: reportOptions values must be strings", 400)
        number = len(self.create_requests)
        lifecycle = (
            self.lifecycles[number - 1]
            if number <= len(self.lifecycles)
            else self.default_lifecycle
        )
        if body["reportType"] in self.forbidden_report_types:
            raise HTTPError("403: Access to requested resource is denied.", 403)
        if body["reportType"] == SALES_AND_TRAFFIC_REPORT_TYPE:
            if not self.brand_analytics:
                raise HTTPError("403: Access to requested resource is denied.", 403)
            for name, value in options.items():
                if value not in SALES_AND_TRAFFIC_OPTIONS.get(name, ()):
                    raise HTTPError(f"400: invalid report option {name}={value}", 400)
            if len(body["marketplaceIds"]) != 1:
                raise HTTPError("400: this report type takes exactly one marketplace", 400)
            if start < self.now - SALES_AND_TRAFFIC_HISTORY:
                lifecycle = ["CANCELLED"]
        report_id = f"requested-{number}"
        created = {
            "reportId": report_id,
            "reportType": body["reportType"],
            "processingStatus": lifecycle[0],
            "createdTime": self.now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "dataStartTime": iso(start + self.echoed_start_shift) if self.echoed_start_shift else body["dataStartTime"],
            "dataEndTime": iso(end + self.echoed_end_shift) if self.echoed_end_shift else body["dataEndTime"],
            "marketplaceIds": body["marketplaceIds"],
        }
        self.reports.append(created)
        self._report_options[report_id] = options
        self._finish(created)
        self._statuses_left[report_id] = list(lifecycle[1:])
        if self.lost_create_responses:
            self.lost_create_responses -= 1
            return self._create_report(dict(body))
        return self._respond({"reportId": report_id})

    def _get_report(self, report_id: str, params: dict[str, Any]):
        self.get_report_requests.append(report_id)
        if params:
            raise HTTPError(f"400: unexpected parameters {params}", 400)
        report = next((r for r in self.reports if r["reportId"] == report_id), None)
        if report is None:
            raise HTTPError(f"404: report {report_id} not found", 404)
        if self._statuses_left.get(report_id):
            report["processingStatus"] = self._statuses_left[report_id].pop(0)
            self._finish(report)
        return self._respond(report)

    def _finish(self, report: dict[str, Any]) -> None:
        """Gives a DONE report its document, and a FATAL one the diagnostic document."""
        document_id = f"doc-{report['reportId']}"
        if report["processingStatus"] == "DONE" and report["reportType"] == SALES_AND_TRAFFIC_REPORT_TYPE:
            first, last = parse(report["dataStartTime"]).date(), parse(report["dataEndTime"]).date()
            (marketplace,) = report["marketplaceIds"]
            spec = {
                "reportType": SALES_AND_TRAFFIC_REPORT_TYPE,
                "reportOptions": self._report_options.get(report["reportId"], {}),
                "dataStartTime": first.isoformat(),
                "dataEndTime": last.isoformat(),
                "marketplaceIds": [marketplace],
            }
            days = [
                d
                for d in self.sales_and_traffic.get(marketplace, [])
                if first.isoformat() <= d["date"] <= last.isoformat()
            ]
            self.documents[document_id] = FakeDocument(
                sales_and_traffic_json(spec, days).encode(),
                compressed=True,
                content_type="application/json",
            )
            report["reportDocumentId"] = document_id
        elif report["processingStatus"] == "DONE" and report["reportType"] == "GET_MERCHANT_LISTINGS_ALL_DATA":
            (marketplace,) = report["marketplaceIds"]
            text = self.listings.get(marketplace, "asin1\tseller-sku\tstatus\n")
            self.documents[document_id] = FakeDocument(("\ufeff" + text).encode())
            report["reportDocumentId"] = document_id
        elif report["processingStatus"] == "DONE":
            lo, hi = parse(report["dataStartTime"]), parse(report["dataEndTime"])
            created = parse(report["createdTime"])
            lines = [
                line
                for line in self.order_lines
                if (
                    lo < parse(line["last-updated-date"]) < hi
                    if self.exclusive_data_bounds
                    else lo <= parse(line["last-updated-date"]) <= hi
                )
                and parse(line["last-updated-date"]) + self.record_delay <= created
            ]
            self.documents[document_id] = FakeDocument(all_orders_tsv(lines).encode())
            report["reportDocumentId"] = document_id
        elif report["processingStatus"] == "FATAL" and self.fatal_document is not None:
            self.documents[document_id] = self.fatal_document
            report["reportDocumentId"] = document_id

    def _get_report_document(self, document_id: str, params: dict[str, Any]):
        self.document_requests.append(document_id)
        if params:
            raise HTTPError(f"400: unexpected parameters {params}", 400)
        if document_id in self.failing_documents or document_id not in self.documents:
            raise HTTPError(f"404: report document {document_id} not found", 404)

        url = (
            f"{DOCUMENT_HOST}{document_id}?X-Amz-Algorithm=AWS4-HMAC-SHA256"
            f"&X-Amz-Credential=FAKEKEY%2F20260301%2Fus-east-1%2Fs3%2Faws4_request"
            f"&X-Amz-Security-Token=fake-session-token&X-Amz-Expires=300"
            f"&X-Amz-Signature=sig-{len(self._issued_urls)}"
        )
        self._issued_urls[url] = (document_id, self.now + DOCUMENT_URL_LIFETIME)
        body: dict[str, Any] = {"reportDocumentId": document_id, "url": url}
        if self.documents[document_id].compressed:
            body["compressionAlgorithm"] = "GZIP"
        return self._respond(body)

    def _download(self, url: str, params: dict[str, Any], kwargs: dict[str, Any]):
        """
        Serves a pre-signed URL. Like S3, it refuses a request that also
        carries the access token, an expired URL, or added parameters. Its
        errors name the full URL, as the CDK's do.
        """
        self.downloads.append(url)

        def error(status: int, reason: str) -> HTTPError:
            return HTTPError(
                f"Encountered HTTP error status {status} which cannot be retried.\nURL: {url}\nResponse:\n{reason}",
                status,
            )

        if kwargs.get("with_token", True):
            raise error(400, "x-amz-access-token sent to a pre-signed URL")
        if params:
            raise error(403, f"unexpected parameters {params}")
        if url not in self._issued_urls:
            raise error(403, "unknown URL")
        document_id, expires_at = self._issued_urls[url]
        if self.now > expires_at:
            raise error(403, "Request has expired")

        document = self.documents[document_id]
        if document.served is not None:
            data = document.served
        else:
            data = gzip.compress(document.content) if document.compressed else document.content
        headers = {} if document.content_type is None else {"Content-Type": document.content_type}
        size = self.download_chunk_size

        async def gen():
            for i in range(0, len(data), size):
                self.chunks_served += 1
                yield data[i : i + size]

        return headers, gen

    def _respond(self, body: dict[str, Any]):
        headers = {} if self.rate_header is None else {"x-amzn-RateLimit-Limit": self.rate_header}

        async def gen():
            yield json.dumps(body).encode()

        return headers, gen


def order(order_id: str, created: datetime, updated: datetime) -> dict[str, Any]:
    return {
        "orderId": order_id,
        "createdTime": iso(created),
        "lastUpdatedTime": iso(updated),
        "salesChannel": {"marketplaceId": "ATVPDKIKX0DER"},
        "orderItems": [],
    }


def transaction(
    transaction_id: str, posted: datetime, status: str = "RELEASED", marketplace: str = "ATVPDKIKX0DER"
) -> dict[str, Any]:
    return {
        "transactionId": transaction_id,
        "transactionStatus": status,
        "transactionType": "Shipment",
        # Amazon reports postedDate with millisecond precision.
        "postedDate": posted.strftime("%Y-%m-%dT%H:%M:%S.") + f"{posted.microsecond // 1000:03d}Z",
        "totalAmount": {"currencyAmount": 10, "currencyCode": "USD"},
        "marketplaceDetails": {"marketplaceId": marketplace, "marketplaceName": "Amazon.com"},
    }


SETTLEMENT_REPORT_TYPE = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"


def report(
    report_id: str,
    created: datetime,
    document_id: str | None = "default",
    status: str = "DONE",
    report_type: str = SETTLEMENT_REPORT_TYPE,
    marketplaces: list[str] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "reportId": report_id,
        "reportType": report_type,
        "processingStatus": status,
        "createdTime": created.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "dataStartTime": iso(created - timedelta(days=14)),
        "dataEndTime": iso(created - timedelta(hours=1)),
        "marketplaceIds": marketplaces or ["ATVPDKIKX0DER"],
    }
    if document_id is not None:
        body["reportDocumentId"] = f"doc-{report_id}" if document_id == "default" else document_id
    return body


SETTLEMENT_HEADER = [
    "settlement-id",
    "settlement-start-date",
    "settlement-end-date",
    "deposit-date",
    "total-amount",
    "currency",
    "transaction-type",
    "order-id",
    "amount-description",
    "amount",
]


def settlement_tsv(settlement_id: str, items: list[tuple[str, str, str, str]], total: str = "12.34") -> str:
    """
    A V2 settlement flat file: a header, a summary row with the totals and
    blank transaction columns, then one row per (type, order, description,
    amount) item.
    """
    lines = ["\t".join(SETTLEMENT_HEADER)]
    lines.append(
        "\t".join([settlement_id, "2026-02-01 00:00:00 UTC", "2026-02-15 00:00:00 UTC", "2026-02-17 00:00:00 UTC", total, "USD", "", "", "", ""])
    )
    for kind, order_id, description, amount in items:
        lines.append("\t".join([settlement_id, "", "", "", "", "USD", kind, order_id, description, amount]))
    return "\n".join(lines) + "\n"


ALL_ORDERS_REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"

# Some of the columns Amazon lists for the report.
ALL_ORDERS_HEADER = [
    "amazon-order-id",
    "purchase-date",
    "last-updated-date",
    "order-status",
    "sku",
    "quantity",
    "currency",
    "item-price",
]


def order_line(order_id: str, updated: datetime, sku: str = "SKU-1", price: str = "10.00") -> dict[str, str]:
    return {
        "amazon-order-id": order_id,
        "purchase-date": (updated - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "last-updated-date": updated.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "order-status": "Shipped",
        "sku": sku,
        "quantity": "1",
        "currency": "USD",
        "item-price": price,
    }


def all_orders_tsv(lines: list[dict[str, str]]) -> str:
    rows = [ALL_ORDERS_HEADER] + [[line[c] for c in ALL_ORDERS_HEADER] for line in lines]
    return "\n".join("\t".join(row) for row in rows) + "\n"


SALES_AND_TRAFFIC_REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"


def sales_and_traffic_day(day: str, sales: float = 100.0, sessions: int = 10) -> dict[str, Any]:
    """A salesAndTrafficByDate entry with some of the metrics Amazon's schema lists."""
    return {
        "date": day,
        "salesByDate": {
            "orderedProductSales": {"amount": sales, "currencyCode": "USD"},
            "unitsOrdered": 3,
            "totalOrderItems": 2,
        },
        "trafficByDate": {"sessions": sessions, "pageViews": 2 * sessions, "unitSessionPercentage": 12.5},
    }


def sales_and_traffic_json(spec: dict[str, Any], days: list[dict[str, Any]]) -> str:
    """A Sales and Traffic report document, with one by-ASIN entry this stream ignores."""
    return json.dumps(
        {
            "reportSpecification": spec,
            "salesAndTrafficByDate": days,
            "salesAndTrafficByAsin": [
                {
                    "parentAsin": "B000000001",
                    "childAsin": "B000000002",
                    "salesByAsin": {"unitsOrdered": 3},
                    "trafficByAsin": {"sessions": 10},
                }
            ],
        },
        ensure_ascii=False,
    )
