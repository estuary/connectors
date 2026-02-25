from datetime import UTC, datetime
from logging import Logger
from typing import (
    AsyncGenerator,
)

from estuary_cdk.capture.common import (
    LogCursor,
    PageCursor,
)
from estuary_cdk.http import HTTPSession

from ..models import (
    Workflow,
    WorkflowsResponse,
)
from .shared import (
    HUB,
    EPOCH_PLUS_ONE_SECOND,
)

async def _fetch_workflows_updated_between(
    log: Logger,
    http: HTTPSession,
    start: datetime,
    end: datetime | None = None,
) -> AsyncGenerator[tuple[datetime, str, Workflow], None]:
    if not end:
        end = datetime.now(tz=UTC)

    assert start < end

    url = f"{HUB}/automation/v3/workflows"

    response = WorkflowsResponse.model_validate_json(
        await http.request(log, url),
    )

    for workflow in response.workflows:
        if start <= workflow.updatedAt <= end:
            yield (workflow.updatedAt, str(workflow.id), workflow)


async def fetch_workflows_page(
    http: HTTPSession,
    log: Logger,
    page: PageCursor | None,
    cutoff: LogCursor,
) -> AsyncGenerator[Workflow, None]:
    assert isinstance(cutoff, datetime)
    # All workflows are returned in a single response, and filtering is
    # performed client side to only yield workflows updated before the
    # cutoff. This means `page` is never used and should always be `None` here.
    assert page is None

    start = EPOCH_PLUS_ONE_SECOND

    async for _, _, workflow in _fetch_workflows_updated_between(
        log, http, start=start, end=cutoff,
    ):
        yield workflow


def fetch_recent_workflows(
    log: Logger,
    http: HTTPSession,
    _: bool,
    since: datetime,
    until: datetime | None,
) -> AsyncGenerator[tuple[datetime, str, Workflow], None]:
    return _fetch_workflows_updated_between(log, http, since, until)


def fetch_delayed_workflows(
    log: Logger,
    http: HTTPSession,
    _: bool,
    since: datetime,
    until: datetime,
) -> AsyncGenerator[tuple[datetime, str, Workflow], None]:
    return _fetch_workflows_updated_between(log, http, since, until)
