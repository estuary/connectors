# Amazon's published sandbox responses, parsed the way the capture parses live
# ones. Source and license: sandbox_examples/NOTICE.
from pathlib import Path

import pytest
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import ValidationError

from source_amazon_selling_partner.models import (
    FinancialTransaction,
    GetReportsResponse,
    ListTransactionsResponse,
    Order,
    ProcessingStatus,
    Report,
    ReportDocument,
    SearchOrdersResponse,
)

EXAMPLES = Path(__file__).parent / "sandbox_examples"


def example(name: str) -> bytes:
    return (EXAMPLES / name).read_bytes()


async def parse(name: str, prefix: str, item_cls: type, remainder_cls: type):
    async def body():
        yield example(name)

    processor = IncrementalJsonProcessor(body(), prefix, item_cls, remainder_cls)
    items = [item async for item in processor]
    return items, processor.get_remainder()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name, order_id, next_token",
    [
        ("search_orders_1.json", "250-1234567-8901234", "2YgYW55IGNhcm5hbCBwbGVhc3VyZS4"),
        ("search_orders_2.json", "202-7654321-1098765", None),
        ("search_orders_3.json", "171-2345678-9012345", None),
    ],
)
async def test_search_orders_examples(name: str, order_id: str, next_token: str | None):
    orders, remainder = await parse(name, "orders.item", Order, SearchOrdersResponse)

    assert [o.orderId for o in orders] == [order_id]
    assert orders[0].lastUpdatedTime >= orders[0].createdTime
    assert remainder.orders == []
    assert (remainder.pagination.nextToken if remainder.pagination else None) == next_token
    assert remainder.createdBefore is not None


@pytest.mark.asyncio
async def test_list_transactions_example():
    # Amazon's only example transaction has no transactionId, which the
    # capture requires as the collection key. The page itself still parses.
    async def body():
        yield example("list_transactions.json")

    processor = IncrementalJsonProcessor(
        body(), FinancialTransaction.ITEMS_PREFIX, FinancialTransaction, ListTransactionsResponse
    )
    with pytest.raises(ValidationError, match="transactionId"):
        _ = [t async for t in processor]

    page = ListTransactionsResponse.model_validate_json(example("list_transactions.json"))
    assert page.payload.nextToken == "Next token value"
    assert [t["transactionType"] for t in page.payload.transactions] == ["Shipment"]


def test_get_reports_example():
    page = GetReportsResponse.model_validate_json(example("get_reports.json"))

    assert [r.reportId for r in page.reports] == ["ReportId1"]
    assert page.reports[0].processingStatus == ProcessingStatus.IN_PROGRESS
    assert page.nextToken


def test_get_report_example():
    report = Report.model_validate_json(example("get_report.json"))

    assert report.reportId == "ReportId1"
    assert report.reportType == "FEE_DISCOUNTS_REPORT"
    assert report.processingStatus == ProcessingStatus.IN_PROGRESS


def test_get_report_document_example():
    document = ReportDocument.model_validate_json(example("get_report_document.json"))

    assert document.reportDocumentId == "0356cf79-b8b0-4226-b4b9-0ee058ea5760"
    assert document.url.startswith("https://")
    assert document.compressionAlgorithm is None
