import functools
from datetime import timedelta
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource
from estuary_cdk.capture.common import ResourceConfig, ResourceState

from .models import (
    EndpointConfig,
    QualtricsResource,
    IncrementalResource,
    Survey,
    SurveyQuestion,
    SurveyResponse,
    FullRefreshResourceFetchFn,
    IncrementalResourceFetchChangesFn,
)

from .api import (
    snapshot_surveys,
    snapshot_survey_questions,
    fetch_survey_responses_incremental,
)

AUTHORIZATION_HEADER = "X-API-TOKEN"


FULL_REFRESH_RESOURCES: list[
    tuple[type[QualtricsResource], FullRefreshResourceFetchFn]
] = [
    (
        Survey,
        snapshot_surveys,
    ),
    (
        SurveyQuestion,
        snapshot_survey_questions,
    ),
]

INCREMENTAL_RESOURCES: list[
    tuple[type[IncrementalResource], IncrementalResourceFetchChangesFn]
] = [
    (
        SurveyResponse,
        fetch_survey_responses_incremental,
    ),
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    headers = {
        AUTHORIZATION_HEADER: config.credentials.access_token,
        "Content-Type": "application/json",
    }
    url = f"https://{config.data_center}.qualtrics.com/API/v3/surveys"

    try:
        await http.request(log, url, headers=headers, params={"limit": 1})
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [
                    "Invalid API token. Please check your API token in Account Settings > Qualtrics IDs."
                ]
            )
        elif err.code == 404:
            raise ValidationError(
                [
                    f"Invalid data center '{config.data_center}'. Please check your data center ID in Account Settings > Qualtrics IDs."
                ]
            )
        else:
            raise ValidationError([f"Failed to connect to Qualtrics API: {err}"])


def full_refresh_resources(
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open(
        fetch_snapshot_fn: FullRefreshResourceFetchFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                fetch_snapshot_fn,
                http,
                config.data_center,
            ),
            tombstone=QualtricsResource(_meta=QualtricsResource.Meta(op="d")),
        )

    return [
        common.Resource(
            name=resource.RESOURCE_NAME,
            key=["/_meta/row_id"],
            model=resource,
            open=functools.partial(
                open,
                fetch_snapshot_fn,
            ),
            initial_state=common.ResourceState(),
            initial_config=common.ResourceConfig(
                name=resource.RESOURCE_NAME, interval=timedelta(minutes=15)
            ),
            schema_inference=True,
        )
        for resource, fetch_snapshot_fn in FULL_REFRESH_RESOURCES
    ]


def incremental_resources(
    http: HTTPMixin,
    config: EndpointConfig,
) -> list[common.Resource]:
    def open(
        fetch_changes_fn: IncrementalResourceFetchChangesFn,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_changes_fn,
                http,
                config.data_center,
                config.advanced.window_size,
            ),
        )

    return [
        common.Resource(
            name=resource.RESOURCE_NAME,
            key=resource.get_resource_key_json_path(),
            model=resource,
            open=functools.partial(
                open,
                fetch_changes_fn,
            ),
            initial_state=common.ResourceState(
                inc=common.ResourceState.Incremental(cursor=config.start_date),
            ),
            initial_config=common.ResourceConfig(
                name=resource.RESOURCE_NAME, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for resource, fetch_changes_fn in INCREMENTAL_RESOURCES
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )

    return [
        *full_refresh_resources(http, config),
        *incremental_resources(http, config),
    ]
