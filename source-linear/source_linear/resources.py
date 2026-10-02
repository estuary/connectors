import functools
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import CaptureBinding, ResourceConfig, ResourceState
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    API,
    TICK,
    floor_to_tick,
    backfill_initiatives,
    backfill_issues,
    backfill_projects,
    fetch_initiatives,
    fetch_issues,
    fetch_projects,
    snapshot_labels,
)
from .models import (
    INCREMENTAL_RESOURCES,
    EndpointConfig,
    Initiative,
    Issue,
    IssueLabel,
    LinearResource,
    Project,
)

# Linear expects the personal API key as a bare `Authorization: <key>` value, with no
# `Bearer` prefix. Both arguments below are load-bearing:
#   - `authorization_token_type=""` drops the prefix.
#   - the lowercase header name (equivalent on the wire — RFC 9110 header names are
#     case-insensitive) differs from the CDK's `DEFAULT_AUTHORIZATION_HEADER`, which
#     routes us to the branch that emits a bare token without logging a
#     "no token type prefix" warning on every single request.
# Keep both: the empty token type alone still authenticates correctly but floods the
# logs, and the header name alone would silently regain the `Bearer` prefix if the
# CDK ever compared header names case-insensitively.
AUTHORIZATION_HEADER = "authorization"


def _token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
        authorization_token_type="",
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured API key authenticates against Linear."""
    http.token_source = _token_source(config)

    try:
        await http.request(log, API, method="POST", json={"query": "{ viewer { id } }"})
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API key. Please confirm the provided Linear API key is correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"

        raise ValidationError([msg])


@dataclass(frozen=True)
class _Fetchers:
    fetch_changes: Callable
    fetch_page: Callable


# Every incremental stream is incremental + backfill on the same `updatedAt` cursor, so one
# builder covers all three; only the fetch pair differs.
_FETCHERS = {
    Issue: _Fetchers(fetch_issues, backfill_issues),
    Project: _Fetchers(fetch_projects, backfill_projects),
    Initiative: _Fetchers(fetch_initiatives, backfill_initiatives),
}


def _resource(
    entity: type[LinearResource],
    http: HTTPMixin,
    config: EndpointConfig,
) -> common.Resource:
    fetchers = _FETCHERS[entity]

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetchers.fetch_changes, http),
            fetch_page=functools.partial(fetchers.fetch_page, http, config.start_date),
        )

    # Seed the incremental cursor one tick below the cutoff. The incremental window's lower
    # bound is exclusive, so this makes its first emitted tick exactly `cutoff` — the tick
    # where the backfill's inclusive upper bound (`cutoff - 1 tick`) leaves off.
    cutoff = floor_to_tick(datetime.now(tz=UTC))

    return common.Resource(
        name=entity.name,
        key=["/id"],
        model=entity,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff - TICK),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(name=entity.name, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def _labels(http: HTTPMixin) -> common.Resource:
    """Labels are re-read in full each interval; see `IssueLabel` for why.

    A label absent from a snapshot is emitted as a deletion, so labels deleted in Linear
    are removed downstream. `start_date` does not apply.
    """

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(snapshot_labels, http),
        )

    return common.SnapshotResource(
        name=IssueLabel.name,
        open=open,
        initial_config=ResourceConfig(name=IssueLabel.name, interval=timedelta(minutes=5)),
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = _token_source(config)

    return [
        *(_resource(entity, http, config) for entity in INCREMENTAL_RESOURCES),
        _labels(http),
    ]
