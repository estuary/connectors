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
    User,
    Conversation,
    Team,
    OAUTH2_SPEC,
)
from .api import (
    snapshot_users,
    fetch_conversations,
    fetch_teams,
    COMMON_API,
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
    url = f"{COMMON_API}.{config.genesys_cloud_domain}/api/v2/users"
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


def users(
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
            fetch_snapshot=functools.partial(
                snapshot_users,
                http,
                config.genesys_cloud_domain,
            ),
            tombstone=User(_meta=User.Meta(op="d"))
        )

    return common.Resource(
            name='users',
            key=["/_meta/row_id"],
            model=User,
            open=open,
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name='users', interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
    
def teams(log: Logger, http: HTTPMixin, config: EndpointConfig) -> common.Resource:
    def open(
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_teams,
                http,
                config.genesys_cloud_domain
            ),
        )
    return common.Resource(
        name='teams',
        key=["/id"],
        model=Team,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=config.start_date),
        ),
        initial_config=ResourceConfig(
            name='teams', interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )

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
        users(log, http, config),
        teams(log, http, config),
    ]