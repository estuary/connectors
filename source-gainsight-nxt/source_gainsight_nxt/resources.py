from datetime import timedelta, datetime, UTC
from logging import Logger
import functools

from estuary_cdk.capture import common, Task
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from source_gainsight_nxt.models import (
    GainsightResource,
    GainsightResourceWithModifiedDate,
    EndpointConfig,
    Company,
    User,
    SuccessPlan,
    CsTask,
    ActivityTimeline,
    CallToAction,
    DaPicklist,
)
from source_gainsight_nxt.api import (
    fetch_object_fields,
    snapshot_resource,
    fetch_resource_page,
    fetch_resource_changes,
)


AUTHORIZATION_HEADER = "AccessKey"

FULL_REFRESH_RESOURCES: list[tuple[str, type[GainsightResource]]] = [
    ("da_picklist", DaPicklist),
]

INCREMENTAL_RESOURCES: list[tuple[str, type[GainsightResourceWithModifiedDate]]] = [
    ("companies", Company),
    ("users", User),
    ("success_plans", SuccessPlan),
    ("cs_tasks", CsTask),
    ("activity_timelines", ActivityTimeline),
    ("call_to_actions", CallToAction),
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )
    url = f"{str(config.domain).rstrip('/')}/v1/data/objects/query/Company"
    request_body = {
        "select": ["Gsid"],
        "limit": 1,
        "offset": 0,
    }

    try:
        await http.request(log, url, method="POST", json=request_body)
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


async def full_refresh_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open(
        fields: list[str],
        resource: type[GainsightResource],
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_resource,
                http,
                fields,
                str(config.domain),
                resource,
                config.start_date,
            ),
            tombstone=GainsightResource(_meta=GainsightResource.Meta(op="d")),
        )

    resources = []
    for name, resource in FULL_REFRESH_RESOURCES:
        fields = await fetch_object_fields(http, log, str(config.domain), resource)

        resources.append(
            common.Resource(
                name=name,
                key=["/_meta/row_id"],
                model=resource,
                open=functools.partial(open, fields, resource),
                initial_state=common.ResourceState(),
                initial_config=common.ResourceConfig(
                    name=name, interval=timedelta(minutes=5)
                ),
                schema_inference=True,
            )
        )

    return resources


async def incremental_resources(
    log: Logger,
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open(
        fields: list[str],
        resource: type[GainsightResourceWithModifiedDate],
        binding: CaptureBinding[common.ResourceConfig],
        binding_index: int,
        state: common.ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_page=functools.partial(
                fetch_resource_page,
                http,
                fields,
                str(config.domain),
                resource,
                config.start_date,
            ),
            fetch_changes=functools.partial(
                fetch_resource_changes,
                http,
                fields,
                str(config.domain),
                resource,
            ),
        )

    cutoff = datetime.now(tz=UTC)
    resources = []
    for name, resource in INCREMENTAL_RESOURCES:
        fields = await fetch_object_fields(http, log, str(config.domain), resource)

        resources.append(
            common.Resource(
                name=name,
                key=["/Gsid"],
                model=resource,
                open=functools.partial(open, fields, resource),
                initial_state=common.ResourceState(
                    inc=common.ResourceState.Incremental(cursor=cutoff),
                    backfill=common.ResourceState.Backfill(
                        cutoff=cutoff, next_page=None
                    ),
                ),
                initial_config=common.ResourceConfig(
                    name=name, interval=timedelta(minutes=5)
                ),
                schema_inference=True,
            )
        )

    return resources


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )

    return [
        *await full_refresh_resources(log, http, config),
        *await incremental_resources(log, http, config),
    ]
