import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.capture.common import (
    FetchSnapshotFn,
    Resource,
    ResourceConfig,
    ResourceState,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    COMMIT_SETTLE,
    GraphQLError,
    backfill_catalog_stats,
    backfill_publication_history,
    fetch_authorized_prefixes,
    fetch_catalog_stats,
    fetch_publication_history,
    snapshot_live_spec_refs,
    snapshot_stream,
)
from .models import (
    CATALOG_STATS_GRAINS,
    LIVE_SPEC_SNAPSHOT_STREAMS,
    SNAPSHOT_STREAMS,
    EndpointConfig,
    CatalogStats,
    Grain,
    PublicationHistoryItem,
)


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the API key authenticates and can read every configured prefix."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    # The API exchanges only dot-free refresh tokens and API keys per request.
    # A JWT access token is verified as-is and expires after an hour.
    if "." in config.credentials.access_token:
        raise ValidationError([
            "The provided credential is a short-lived access token. Please provide an Estuary refresh token or service account API key instead."
        ])

    try:
        authorized = [a.prefix for a in await fetch_authorized_prefixes(http, log)]
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API key. Please confirm the provided API key is correct and has not expired.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"
        raise ValidationError([msg])
    except GraphQLError as err:
        raise ValidationError([f"Encountered error validating API key.\n\n{err}"])

    if not authorized:
        raise ValidationError(["The provided API key cannot read any catalog prefixes."])

    unreadable = [
        p for p in config.prefixes
        if not any(p.startswith(a) for a in authorized)
    ]
    if unreadable:
        raise ValidationError([
            f"The provided API key cannot read these prefixes: {unreadable}. Readable prefixes: {authorized}"
        ])


def _create_snapshot_resource(
    name: str, interval: timedelta, fetch_snapshot: FetchSnapshotFn
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(binding, binding_index, state, task, fetch_snapshot=fetch_snapshot)

    return SnapshotResource(
        name=name,
        open=open,
        initial_config=ResourceConfig(name=name, interval=interval),
        schema_inference=True,
    )


def _create_publication_history_resource(
    http: HTTPMixin, config: EndpointConfig
) -> common.Resource:
    # The incremental cursor starts at the cutoff itself: the API's `after`
    # filter is exclusive and backfill includes the cutoff.
    cutoff = (datetime.now(tz=UTC) - COMMIT_SETTLE).replace(microsecond=0)

    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_publication_history, http, config),
            fetch_page=functools.partial(backfill_publication_history, http, config),
        )

    return Resource(
        name=PublicationHistoryItem.name,
        key=["/catalogName", "/publicationId"],
        model=PublicationHistoryItem,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(name=PublicationHistoryItem.name, interval=timedelta(minutes=5)),
        schema_inference=True,
    )


def _create_catalog_stats_resource(
    grain: Grain, http: HTTPMixin, config: EndpointConfig, cutoff: datetime
) -> common.Resource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(fetch_catalog_stats, http, config, grain),
            fetch_page=functools.partial(backfill_catalog_stats, http, config, grain),
        )

    return Resource(
        name=grain.stream_name,
        key=["/catalogName", "/grain", "/timestamp"],
        model=CatalogStats,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfig(name=grain.stream_name, interval=grain.interval),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)
    return [
        *(_create_catalog_stats_resource(g, http, config, cutoff) for g in CATALOG_STATS_GRAINS),
        *(
            _create_snapshot_resource(
                cls.name,
                timedelta(minutes=5),
                functools.partial(snapshot_live_spec_refs, cls, http, config),
            )
            for cls in LIVE_SPEC_SNAPSHOT_STREAMS
        ),
        _create_publication_history_resource(http, config),
        *(
            _create_snapshot_resource(
                cls.name, cls.interval, functools.partial(snapshot_stream, cls, http, config)
            )
            for cls in SNAPSHOT_STREAMS
        ),
    ]
