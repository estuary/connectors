from datetime import timedelta, datetime, UTC
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
    OAUTH2_SPEC,
)
from .api import (
    snapshot_users,
    fetch_conversations,
    backfill_conversations,
    _dt_to_str,
    COMMON_API,
)

# The backing data set for Genesys' asynchronous analytics jobs is not updated in real-time.
# It can take hours to a day for the data to be available. We shift the cutoff between backfills
# & incremental replication two days to ensure we're only backfilling over a data range where
# data is available.
ASYNC_JOB_DATA_AVAILABILITY_DELAY = 2


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
            fetch_page=functools.partial(
                backfill_conversations,
                http,
                config.genesys_cloud_domain,
            )
        )

    backfill_start = _dt_to_str(config.start_date)
    cutoff = datetime.now(tz=UTC) - timedelta(days=ASYNC_JOB_DATA_AVAILABILITY_DELAY)

    return common.Resource(
        name='conversations',
        key=["/conversationId"],
        model=Conversation,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(next_page=backfill_start, cutoff=cutoff)
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
    ]