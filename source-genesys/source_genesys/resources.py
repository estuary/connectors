from datetime import timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, HTTPError


from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    FullRefreshResource,
    GenesysStream,
    Conversation,
    OAUTH2_SPEC,
    Users,
    FULL_REFRESH_STREAMS
)
from .api import (
    base_url,
    snapshot_resources,
    fetch_conversations,
)


def update_oauth2spec_with_domain(config: EndpointConfig):
    """
    Updates the OAuth2Spec with the config's domain.
    """
    login_url = OAUTH2_SPEC.accessTokenUrlTemplate.replace("REGION_DEPENDENT_DOMAIN", config.genesys_cloud_domain)
    OAUTH2_SPEC.accessTokenUrlTemplate = login_url


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    """
    Checks if the provided client credentials belong to a valid OAuth app.
    """
    update_oauth2spec_with_domain(config)

    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    # The /users endpoint is used since no permissions are required to access it.
    url = f"{base_url(config.genesys_cloud_domain)}/{Users.path}"
    params = {
        "pageSize": 1,
        "pageNumber": 1,
        "sortOrder": "ASC",
    }

    try:
        await http.request(log, url, params=params)
    except HTTPError as err:
        msg = f"Encountered issue while validating credentials. Please confirm provided credentials and selected Genesys Cloud Domain are correct."
        raise ValidationError([msg, err.message])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            stream: type[GenesysStream],
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
                snapshot_resources,
                http,
                config.genesys_cloud_domain,
                stream,
            ),
            tombstone=FullRefreshResource(_meta=FullRefreshResource.Meta(op="d"))
        )


    resources: list[common.Resource] = []

    for stream in FULL_REFRESH_STREAMS:
        resources.append(
            common.Resource(
                name=stream.name,
                key=["/_meta/row_id"],
                model=FullRefreshResource,
                open=functools.partial(open, stream),
                initial_state=ResourceState(),
                initial_config=ResourceConfig(
                    name=stream.name, interval=timedelta(minutes=30)
                ),
                schema_inference=True,
            )
        )

    return resources


def conversations(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:

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
            fetch_changes=functools.partial(
                fetch_conversations,
                http,
                config.genesys_cloud_domain,
            ),
        )

    return common.Resource(
        name='conversations',
        key=["/conversationId"],
        model=Conversation,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='conversations', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    update_oauth2spec_with_domain(config)

    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)

    return [
        conversations(log, http, config),
        *full_refresh_resources(log, http, config),
    ]