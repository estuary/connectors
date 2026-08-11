import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task
from estuary_cdk.capture.common import (
    Resource,
    ResourceConfig,
    ResourceState,
    SnapshotResource,
    open_binding,
)
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    API,
    backfill_contacts,
    fetch_contacts_changes,
    snapshot_resource,
    snapshot_webhooks,
)
from .models import (
    SNAPSHOT_RESOURCES,
    BrevoResource,
    Contact,
    EndpointConfig,
    Webhook,
)


AUTHORIZATION_HEADER = "api-key"


BrevoStream = Resource[BrevoResource, ResourceConfig, ResourceState]


def _token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against Brevo."""
    http.token_source = _token_source(config)

    # /account is unpaginated, always present, and readable by every API key.
    try:
        await http.request(log, f"{API}/account")
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided API key is correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def snapshot_resources(http: HTTPMixin) -> list[BrevoStream]:
    def open(
        model: type[BrevoResource],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings,
    ):
        # Webhooks need the three-way type fan-out. Everything else is a plain
        # drain of a single collection.
        fetch_snapshot = (
            functools.partial(snapshot_webhooks, http)
            if model is Webhook
            else functools.partial(snapshot_resource, http, model=model)
        )

        open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=fetch_snapshot,
        )

    return [
        SnapshotResource(
            name=model.NAME,
            open=functools.partial(open, model),
            initial_config=ResourceConfig(
                name=model.NAME,
                interval=timedelta(hours=1),
            ),
        )
        for model in SNAPSHOT_RESOURCES
    ]


def contacts(http: HTTPMixin) -> BrevoStream:
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)

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
            fetch_changes=functools.partial(fetch_contacts_changes, http),
            fetch_page=functools.partial(backfill_contacts, http),
        )

    return BrevoStream(
        name=Contact.NAME,
        key=Contact.KEY,
        model=Contact,
        open=open,
        initial_state=ResourceState(
            # Starting the cursor 1s behind `cutoff` overlaps the first
            # incremental poll with the tail of the backfill, so nothing modified
            # in the instant between the two can slip through the seam. The
            # overlap only ever duplicates, since documents are keyed by id.
            inc=ResourceState.Incremental(cursor=cutoff - timedelta(seconds=1)),
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=0),
        ),
        initial_config=ResourceConfig(
            name=Contact.NAME,
            interval=timedelta(minutes=5),
        ),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[BrevoStream]:
    http.token_source = _token_source(config)

    return [
        *snapshot_resources(http),
        contacts(http),
    ]
