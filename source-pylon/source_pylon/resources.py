import functools
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceConfig, ResourceState
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    API_URLS,
    LAG,
    TICK,
    PylonClient,
    backfill_issues,
    fetch_issues,
    snapshot_issue_messages,
    snapshot_resources,
)
from .models import (
    SNAPSHOT_STREAMS,
    EndpointConfig,
    Issue,
    IssueMessage,
)

PylonResource = common.Resource[BaseDocument, ResourceConfig, ResourceState]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    try:
        _ = await http.request(log, f"{API_URLS[config.region]}/me")
    except HTTPError as err:
        if err.code in (401, 403):
            raise ValidationError(
                [
                    "Unable to authenticate with Pylon. Check your API key, its permissions, and the selected region."
                ]
            ) from err
        raise


async def all_resources(
    _log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[PylonResource]:
    """Discover the fixed stream set without making provider requests."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    client = PylonClient(http, API_URLS[config.region])
    # Windows step from the start date, so it must sit on a whole second.
    start_date = config.start_date.replace(microsecond=0)
    cutoff = max(datetime.now(tz=UTC).replace(microsecond=0) - LAG, start_date)

    def open_issues(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: object,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_issues, client),
            fetch_page=functools.partial(backfill_issues, client, start_date),
        )

    resources: list[PylonResource] = [
        PylonResource(
            name=Issue.name,
            key=["/id"],
            model=Issue,
            open=open_issues,
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff - TICK),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
            ),
            initial_config=ResourceConfig(
                name=Issue.name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
    ]

    def open_snapshot(
        fetch_snapshot: Callable[[Logger], AsyncGenerator[BaseDocument, None]],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: object,
    ):
        common.open_binding(
            binding, binding_index, state, task, fetch_snapshot=fetch_snapshot
        )

    def snapshot_resource(
        name: str,
        fetch_snapshot: Callable[[Logger], AsyncGenerator[BaseDocument, None]],
        interval: timedelta,
    ) -> PylonResource:
        return common.SnapshotResource(
            name=name,
            open=functools.partial(open_snapshot, fetch_snapshot),
            initial_config=ResourceConfig(name=name, interval=interval),
        )

    resources.append(
        snapshot_resource(
            IssueMessage.name,
            functools.partial(snapshot_issue_messages, client),
            timedelta(days=1),
        )
    )
    resources.extend(
        snapshot_resource(
            model.name,
            functools.partial(snapshot_resources, client, model),
            timedelta(hours=1),
        )
        for model in SNAPSHOT_STREAMS
    )
    return resources
