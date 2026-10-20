import io
import logging
from unittest.mock import MagicMock

import pytest

from estuary_cdk.capture.common import ConnectorState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.capture.task import Task
from estuary_cdk.capture.transactor import Transactor


class _Doc(BaseDocument, extra="allow"):
    id: str


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
