import logging

import pytest

from source_amazon_selling_partner.client import (
    Operation,
    OperationLimit,
    Priority,
    SPAPIClient,
    TokenBucket,
)

from .fake_amazon import FakeAmazon
from .test_orders import NOW, FakeTime

LOG = logging.getLogger("test")


@pytest.mark.asyncio
async def test_bucket_starts_with_one_token_then_paces_at_rate():
    time = FakeTime()
    bucket = TokenBucket(OperationLimit(rate=0.5, burst=10), clock=time.clock, sleep=time.sleep)

    await bucket.acquire(Priority.INCREMENTAL)
    assert time.slept == []

    await bucket.acquire(Priority.INCREMENTAL)
    await bucket.acquire(Priority.INCREMENTAL)
    assert time.t == pytest.approx(4.0)


@pytest.mark.asyncio
async def test_bucket_refills_up_to_burst_only():
    time = FakeTime()
    bucket = TokenBucket(OperationLimit(rate=1.0, burst=3), clock=time.clock, sleep=time.sleep)
    time.t = 1000.0

    for _ in range(3):
        await bucket.acquire(Priority.INCREMENTAL)
    assert time.slept == []
    await bucket.acquire(Priority.INCREMENTAL)
    assert time.slept == [pytest.approx(1.0)]


@pytest.mark.asyncio
async def test_backfill_leaves_reserve_for_incremental():
    time = FakeTime()
    bucket = TokenBucket(OperationLimit(rate=1.0, burst=5), clock=time.clock, sleep=time.sleep)
    time.t = 1000.0  # full bucket: 5 tokens

    for _ in range(3):
        await bucket.acquire(Priority.BACKFILL)
    assert time.slept == []
    assert bucket.tokens == pytest.approx(2.0)

    # A fourth backfill request would dip into the reserve, so it waits...
    await bucket.acquire(Priority.BACKFILL)
    assert time.slept == [pytest.approx(1.0)]
    # ...while incremental requests may spend it.
    await bucket.acquire(Priority.INCREMENTAL)
    await bucket.acquire(Priority.INCREMENTAL)
    assert len(time.slept) == 1


@pytest.mark.asyncio
async def test_client_adopts_reported_rate():
    fake = FakeAmazon(now=NOW, orders=[], rate_header="0.0167")
    time = FakeTime()
    client = SPAPIClient(http=fake, base_url="https://fake", clock=time.clock, sleep=time.sleep)  # type: ignore[arg-type]

    await client.get(LOG, Operation.GET_MARKETPLACE_PARTICIPATIONS, "/sellers/v1/marketplaceParticipations", {}, Priority.INCREMENTAL)
    assert client.bucket(Operation.GET_MARKETPLACE_PARTICIPATIONS).rate == pytest.approx(0.0167)


@pytest.mark.asyncio
async def test_client_ignores_garbled_rate_header():
    fake = FakeAmazon(now=NOW, orders=[], rate_header="fast")
    time = FakeTime()
    client = SPAPIClient(http=fake, base_url="https://fake", clock=time.clock, sleep=time.sleep)  # type: ignore[arg-type]

    await client.get(LOG, Operation.GET_MARKETPLACE_PARTICIPATIONS, "/sellers/v1/marketplaceParticipations", {}, Priority.INCREMENTAL)
    assert client.bucket(Operation.GET_MARKETPLACE_PARTICIPATIONS).rate == pytest.approx(0.016)
