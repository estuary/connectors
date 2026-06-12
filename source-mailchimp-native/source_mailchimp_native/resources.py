import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfigWithSchedule,
    ResourceState,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_campaigns,
    fetch_campaigns,
    resolve_base_url,
    snapshot_automations,
    snapshot_lists,
)
from .models import (
    OAUTH2_SPEC,
    SCHEDULED_BACKFILL_STREAMS,
    Automation,
    Campaign,
    EndpointConfig,
    MailchimpEntity,
    MailchimpList,
)

DEFAULT_SCHEDULE = "0 0 * * *"

MailchimpResource = Resource[BaseDocument, ResourceConfigWithSchedule, ResourceState]

# Top-level collections captured as snapshots: small, mutable, and without any
# updated_at-style cursor field, so periodic full re-lists with tombstoned
# deletions beat a created-time cursor that can never observe updates.
SNAPSHOT_RESOURCES: list[type[MailchimpEntity]] = [
    MailchimpList,
    Automation,
]


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )

    try:
        base_url = await resolve_base_url(log, http, config.credentials)
        _ = await http.request(log, f"{base_url}/ping")
    except ValueError as err:
        raise ValidationError([str(err)])
    except HTTPError as err:
        if err.code == 401:
            msg = (
                "Invalid credentials. Please confirm the provided credentials "
                f"are correct.\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def snapshot_resources(http: HTTPMixin, base_url: str) -> list[MailchimpResource]:
    """Return Resource objects for all snapshot (full-refresh) streams."""

    snapshot_fetchers = {
        MailchimpList.NAME: functools.partial(snapshot_lists, http, base_url),
        Automation.NAME: functools.partial(snapshot_automations, http, base_url),
    }

    def open(
        resource_name: str,
        binding: CaptureBinding[ResourceConfigWithSchedule],
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
            fetch_snapshot=snapshot_fetchers[resource_name],
        )

    return [
        SnapshotResource(
            name=model.NAME,
            open=functools.partial(open, model.NAME),
            initial_config=ResourceConfigWithSchedule(
                name=model.NAME, interval=timedelta(hours=1)
            ),
        )
        for model in SNAPSHOT_RESOURCES
    ]


def campaigns(
    http: HTTPMixin, base_url: str, config: EndpointConfig
) -> MailchimpResource:
    """Return Resource for incremental + backfill campaigns capture."""
    cutoff = datetime.now(tz=UTC)

    def open(
        binding: CaptureBinding[ResourceConfigWithSchedule],
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
            fetch_changes=functools.partial(fetch_campaigns, http, base_url),
            fetch_page=functools.partial(
                backfill_campaigns, http, base_url, config.start_date
            ),
        )

    return MailchimpResource(
        name=Campaign.NAME,
        key=["/id"],
        model=Campaign,
        open=open,
        initial_state=ResourceState(
            inc=ResourceState.Incremental(cursor=cutoff),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=None),
        ),
        initial_config=ResourceConfigWithSchedule(
            name=Campaign.NAME,
            interval=timedelta(minutes=5),
            schedule=(
                DEFAULT_SCHEDULE if Campaign.NAME in SCHEDULED_BACKFILL_STREAMS else ""
            ),
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[MailchimpResource]:
    """Return all resources for the Mailchimp connector."""
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC, credentials=config.credentials
    )
    base_url = await resolve_base_url(log, http, config.credentials)

    return [
        *snapshot_resources(http, base_url),
        campaigns(http, base_url, config),
    ]
