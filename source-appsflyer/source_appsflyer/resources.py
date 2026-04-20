import asyncio
import functools
from datetime import UTC, datetime, timedelta
from logging import Logger
from typing import Annotated, ClassVar, Literal

from estuary_cdk.capture import common
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.webhook.match import BodyDiscriminator
from estuary_cdk.capture.webhook.resources import (
    WebhookResourceConfig,
    open_webhook_binding,
)
from estuary_cdk.flow import CaptureBinding
from estuary_cdk.http import HTTPMixin, TokenSource
from pydantic import Field, TypeAdapter

from .api import (
    AGG_REPORT_TYPES,
    RAW_REPORT_TYPES,
    backfill_report,
    fetch_event_types,
    fetch_report,
    is_endpoint_supported,
)
from .models import (
    AppsFlyerWebhookDocument,
    ConnectorState,
    EndpointConfig,
    PullApiDocument,
)


class PullApiResourceConfig(ResourceConfig):
    type: Literal["pull"]


class AppsFlyerWebhookResourceConfig(WebhookResourceConfig):
    PATH_POINTERS: ClassVar[list[str]] = ["/name"]

    type: Literal["webhook"]


AnyResourceConfig = Annotated[
    PullApiResourceConfig | AppsFlyerWebhookResourceConfig,
    Field(discriminator="type"),
]

AnyResourceConfigAdapter: TypeAdapter[AnyResourceConfig] = TypeAdapter(
    AnyResourceConfig
)


async def _reconcile_connector_state(
    app_ids: list[str],
    binding: CaptureBinding[PullApiResourceConfig],
    state: ResourceState,
    task: common.Task,
):
    """Initialize subtask state for app IDs added to the config after initial discover."""
    if not (isinstance(state.inc, dict) and isinstance(state.backfill, dict)):
        return

    cutoff = datetime.now(tz=UTC)
    added = [app_id for app_id in app_ids if app_id not in state.inc]

    if not added:
        return

    for app_id in added:
        task.log.info(
            "Initializing new subtask state for app_id",
            {"app_id": app_id, "binding": binding.stateKey},
        )
        state.inc[app_id] = ResourceState.Incremental(cursor=cutoff)
        state.backfill[app_id] = ResourceState.Backfill(cutoff=cutoff, next_page=None)

    await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


async def open_report_binding(
    doc_type: type[PullApiDocument],
    app_ids: list[str],
    start_date: datetime,
    window_size: timedelta,
    http: HTTPMixin,
    binding: CaptureBinding[PullApiResourceConfig],
    binding_index: int,
    state: ResourceState,
    task: common.Task,
    all_bindings,
):
    await _reconcile_connector_state(app_ids, binding, state, task)

    common.open_binding(  # pyright: ignore[reportUnknownMemberType]
        binding,
        binding_index,
        state,
        task,
        fetch_changes={
            app_id: functools.partial(fetch_report, doc_type, app_id, window_size, http)
            for app_id in app_ids
        },
        fetch_page={
            app_id: functools.partial(
                backfill_report,
                doc_type,
                app_id,
                start_date,
                window_size,
                http,
            )
            for app_id in app_ids
        },
    )


async def all_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials)

    cutoff = datetime.now(tz=UTC)
    app_ids = config.parsed_app_ids()

    event_types = await fetch_event_types(log, http)

    all_doc_types: list[type[PullApiDocument]] = [*RAW_REPORT_TYPES, *AGG_REPORT_TYPES]

    async def probe_pair(
        doc_type: type[PullApiDocument],
        app_id: str,
    ) -> tuple[type[PullApiDocument], str, bool]:
        ok = await is_endpoint_supported(doc_type, app_id, log, http)
        return doc_type, app_id, ok

    # Probe every (doc_type, app_id) pair concurrently. AppsFlyer's
    # "not subscribed" / "Protect360 unavailable" errors are per-app, so an
    # endpoint can be valid for one configured app and not another. Each
    # probe returns a (doc_type, app_id, supported) triple.
    results = await asyncio.gather(
        *[probe_pair(dt, app_id) for dt in all_doc_types for app_id in app_ids]
    )

    # Bucket the results into {doc_type: [app_ids that can query it]}. Doc
    # types with no supported apps are absent from the dict and will not
    # become resources; apps that failed the probe for a given doc type are
    # absent from that doc type's subtask list.
    supported: dict[type[PullApiDocument], list[str]] = {}
    for doc_type, app_id, ok in results:
        if ok:
            supported.setdefault(doc_type, []).append(app_id)

    discriminator = BodyDiscriminator(key="event_name", known_values=event_types)
    webhook_resources = [
        Resource(
            name=f"{rule.display_name}",
            key=["/_meta/estuary_id"],
            model=AppsFlyerWebhookDocument,
            open=open_webhook_binding,  # pyright: ignore[reportArgumentType]
            initial_state=ResourceState(),
            initial_config=AppsFlyerWebhookResourceConfig(
                name=f"{rule.display_name}",
                type="webhook",
                match_rule=rule,
            ),
            schema_inference=True,
        )
        for rule in discriminator.create_match_rules()
    ]

    def make_resource(
        doc_type: type[PullApiDocument],
        supported_app_ids: list[str],
    ) -> Resource[PullApiDocument, PullApiResourceConfig, ResourceState]:
        return Resource(
            name=doc_type.resource_name,
            key=["/_meta/app_id", *[f"/{f}" for f in doc_type.key_fields]],
            model=doc_type,
            open=functools.partial(
                open_report_binding,
                doc_type,
                supported_app_ids,
                config.start_date,
                config.advanced.window_size,
                http,
            ),
            initial_state=ResourceState(
                inc={
                    app_id: ResourceState.Incremental(cursor=cutoff)
                    for app_id in supported_app_ids
                },
                backfill={
                    app_id: ResourceState.Backfill(cutoff=cutoff, next_page=None)
                    for app_id in supported_app_ids
                },
            ),
            initial_config=PullApiResourceConfig(
                name=doc_type.resource_name,
                type="pull",
                interval=timedelta(hours=4),
            ),
            schema_inference=True,
        )

    pull_resources = [
        make_resource(doc_type, ids) for doc_type, ids in supported.items()
    ]

    return [*webhook_resources, *pull_resources]
