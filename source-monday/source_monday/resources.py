from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource
from estuary_cdk.capture.common import ResourceConfig

from .models import EndpointConfig, OAUTH2_SPEC, ResourceState, Board
from .api import snapshot_boards

COMMON_API = "https://api.monday.com/v2"


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """
    Checks if the provided client credentials belong to a valid OAuth app.
    """
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        await http.request(
            log, COMMON_API, method="POST", json={"query": "query {me {id}}"}
        )
    except HTTPError as err:
        msg = "Unknown error occurred."
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def boards(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
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
            fetch_snapshot=functools.partial(snapshot_boards, http),
            tombstone=Board(_meta=Board.Meta(op="d")),
        )

    return common.Resource(
        name="boards",
        key=["/_meta/row_id"],
        model=Board,
        open=open,
        initial_state=ResourceState(),
        initial_config=ResourceConfig(name="boards", interval=timedelta(seconds=10)),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    return [
        boards(log, http, config),
    ]
