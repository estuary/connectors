import functools
import inspect
import logging
from datetime import timedelta

import pytest
from estuary_cdk.capture.common import Resource

from source_amazon_selling_partner import api, reports, resources

from .fake_amazon import (
    FakeAmazon,
    FakeDocument,
    order,
    order_line,
    report,
    sales_and_traffic_day,
    settlement_tsv,
    transaction,
)
from .test_orders import NOW, FakeTime, make_config, FrozenDatetime, collect

LOG = logging.getLogger("test")


class SteppingTime(FakeTime):
    """
    FakeTime whose sleeps always move the clock: after many buckets' worth
    of sleeping, a sleep of a rounding error's length would not.
    """

    async def sleep(self, seconds: float) -> None:
        await super().sleep(seconds + 1e-3)


@pytest.mark.asyncio
async def test_every_resource_opens_and_its_fetch_functions_run(monkeypatch: pytest.MonkeyPatch):
    """
    Opens each resource as the CDK does and calls every fetch function it
    binds with the arguments the CDK passes, so a wiring mistake fails here.
    """
    cutoff = NOW - timedelta(hours=1)
    monkeypatch.setattr(resources, "datetime", FrozenDatetime(cutoff))
    monkeypatch.setattr(api, "now", lambda: NOW)
    monkeypatch.setattr(reports, "now", lambda: NOW)

    async def no_sleep(_):
        pass

    monkeypatch.setattr(api.asyncio, "sleep", no_sleep)
    time = SteppingTime()
    monkeypatch.setattr(
        resources, "SPAPIClient", functools.partial(resources.SPAPIClient, clock=time.clock, sleep=time.sleep)
    )

    fake = FakeAmazon(
        now=NOW,
        orders=[order("o1", cutoff - timedelta(days=1), cutoff + timedelta(minutes=10))],
        transactions=[transaction("t1", cutoff - timedelta(days=1))],
    )
    fake.reports.append(report("r1", cutoff - timedelta(days=1)))
    fake.documents["doc-r1"] = FakeDocument(settlement_tsv("s-1", [("Order", "o1", "Principal", "1")]).encode())
    fake.order_lines.append(order_line("o1", cutoff - timedelta(days=1)))
    fake.sales_and_traffic["ATVPDKIKX0DER"] = [sales_and_traffic_day((cutoff - timedelta(days=2)).date().isoformat())]
    config = make_config(["ATVPDKIKX0DER"], start_date=api.to_iso(cutoff - timedelta(days=10)))

    all_resources: list[Resource] = await resources.all_resources(LOG, fake, config)  # type: ignore[arg-type]
    assert [r.name for r in all_resources] == [
        "orders",
        "financial_transactions",
        "settlement_reports",
        "all_orders",
        "sales_and_traffic_by_date",
        "catalog_items",
    ]

    for resource in all_resources:
        recorded: dict = {}
        monkeypatch.setattr(resources, "open_binding", lambda *args, _recorded=recorded, **kwargs: _recorded.update(kwargs))
        opened = resource.open(type("Binding", (), {"resourceConfig": resource.initial_config})(), 0, resource.initial_state, None, [])  # type: ignore[arg-type,misc]
        if inspect.isawaitable(opened):
            await opened

        state = resource.initial_state
        if "fetch_page" in recorded:
            backfilled = await collect(recorded["fetch_page"](LOG, None, state.backfill.cutoff))
            assert backfilled, resource.name
        else:
            assert resource.name == "catalog_items"
            assert state.backfill is None

        fetch_changes = recorded["fetch_changes"]
        if isinstance(fetch_changes, dict):
            assert isinstance(state.inc, dict)
            for name, fetch in fetch_changes.items():
                await collect(fetch(LOG, state.inc[name].cursor))
        else:
            await collect(fetch_changes(LOG, state.inc.cursor))
