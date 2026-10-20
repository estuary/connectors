import io
from datetime import date
import logging
from unittest.mock import MagicMock

import pytest

from estuary_cdk.capture.backfill_signals import (
    BackfillCoverage,
    backfill_signal_flag_defaults,
)
from estuary_cdk.capture.common import ConnectorState, ResourceConfig, SnapshotResource
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.transactor import Transactor
from estuary_cdk.feature_flags import FeatureFlagsConfig


class _Doc(BaseDocument, extra="allow"):
    id: str


def test_signals_default_on_only_for_tasks_created_on_or_after_the_date():
    class Advanced(FeatureFlagsConfig):
        FEATURE_FLAG_DEFAULTS = backfill_signal_flag_defaults(
            enabled_for_tasks_created_after=date(2026, 11, 4)
        )

    off = {"emit_backfill_signals": False, "ignore_config_for_backfill_signals": False}
    assert Advanced().resolve_feature_flags(date(2026, 11, 3)) == off
    assert Advanced().resolve_feature_flags(date(2026, 11, 4)) == {**off, "emit_backfill_signals": True}


def test_snapshot_resource_never_signals():
    with pytest.raises(AssertionError, match="never sends backfill signals"):
        _ = SnapshotResource(
            name="things",
            open=lambda *args: None,
            initial_config=ResourceConfig(name="things"),
            backfill_coverage=BackfillCoverage.FULL,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("send_signal", ["_backfill_begin", "_backfill_complete"])
async def test_signal_refuses_to_share_a_checkpoint(send_signal: str):
    output = io.BytesIO()
    task = Task(
        log=logging.getLogger("test-backfill-signals"),
        connector_status=MagicMock(),
        name="capture",
        output=output,
        stopping=Task.Stopping(),
        tg=MagicMock(),
        transactor=Transactor(output),
        catalog_task_name="acmeCo/capture",
    )
    task.captured(0, _Doc(id="a"))
    with pytest.raises(RuntimeError, match="stand alone"):
        await getattr(task, send_signal)(0, ConnectorState())
