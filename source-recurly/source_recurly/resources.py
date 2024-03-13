from datetime import UTC, datetime, timedelta
import functools
from logging import Logger

from estuary_cdk.flow import CaptureBinding
from estuary_cdk.capture import common, Task
from estuary_cdk.http import HTTPMixin, TokenSource, AccessToken

from .models import (
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    Account,
)
from .api import (
    fetch_accounts,
)


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=AccessToken(
            credentials_title= "Private App Credentials",
            access_token=config.apiToken,
        ),
    )
    
    def open(
        entity: str,
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes=functools.partial(
                fetch_accounts,
                http,
                config.organization,
                entity,
            ),
        )
    
    started_at = datetime.now(tz=UTC)

    return [common.Resource(
        name="Account",
        key=["/id"],
        model=Account,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=started_at),
            backfill=ResourceState.Backfill(next_page=None, cutoff=started_at),
        ),
        initial_config=ResourceConfig(name="Account"),
        schema_inference=True,
    )]
    
