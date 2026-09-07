import asyncio
from unittest.mock import MagicMock

import pytest

from estuary_cdk.utils import surface_slow


@pytest.mark.asyncio
async def test_fast_completion_returns_value_without_warnings():
    log = MagicMock()

    async def fast() -> int:
        return 42

    result = await surface_slow(log, "fast op", fast(), warn_after=0.5)

    assert result == 42
    log.warning.assert_not_called()


@pytest.mark.asyncio
async def test_slow_completion_warns_and_returns_value():
    log = MagicMock()

    async def slow() -> str:
        await asyncio.sleep(0.25)
        return "done"

    result = await surface_slow(
        log, "slow op", slow(), warn_after=0.05, warn_interval=0.05
    )

    assert result == "done"
    assert log.warning.call_count >= 2

    message, fields = log.warning.call_args_list[0].args
    assert message == "slow op is still running"
    assert fields["deadline_seconds"] is None
    assert isinstance(fields["elapsed_seconds"], int)


@pytest.mark.asyncio
async def test_deadline_cancels_and_raises():
    log = MagicMock()
    cancelled = asyncio.Event()

    async def hangs() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(TimeoutError, match="hung op did not complete within"):
        await surface_slow(
            log,
            "hung op",
            hangs(),
            warn_after=0.02,
            warn_interval=0.02,
            deadline=0.1,
        )

    # The shielded inner task must actually be cancelled, not leaked.
    _ = await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    assert log.warning.call_count >= 1


@pytest.mark.asyncio
async def test_no_deadline_never_cancels():
    log = MagicMock()

    async def eventually() -> str:
        await asyncio.sleep(0.25)
        return "finished"

    result = await surface_slow(
        log, "patient op", eventually(), warn_after=0.05, warn_interval=0.05
    )

    assert result == "finished"
    # Several warning intervals elapsed, yet the operation ran to completion.
    assert log.warning.call_count >= 3


@pytest.mark.asyncio
async def test_inner_exception_propagates():
    log = MagicMock()

    async def fails() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await surface_slow(log, "failing op", fails(), warn_after=0.5)


@pytest.mark.asyncio
async def test_inner_timeout_error_propagates_promptly():
    # Regression: an awaitable that itself raises TimeoutError must not be
    # mistaken for our polling timeout, which would loop warning forever.
    log = MagicMock()

    async def times_out() -> None:
        await asyncio.sleep(0.06)
        raise TimeoutError("inner timeout")

    with pytest.raises(TimeoutError, match="inner timeout"):
        await asyncio.wait_for(
            surface_slow(
                log,
                "inner-timeout op",
                times_out(),
                warn_after=0.02,
                warn_interval=0.02,
            ),
            timeout=1.0,
        )


@pytest.mark.asyncio
async def test_caller_cancellation_cancels_inner_task():
    log = MagicMock()
    cancelled = asyncio.Event()

    async def hangs() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    outer = asyncio.ensure_future(
        surface_slow(log, "cancelled op", hangs(), warn_after=10.0)
    )
    await asyncio.sleep(0.05)
    _ = outer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await outer

    await asyncio.wait_for(cancelled.wait(), timeout=1.0)
