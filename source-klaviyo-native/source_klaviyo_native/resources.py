import functools
from datetime import UTC, datetime, timedelta
from logging import Logger

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    backfill_events,
    backfill_incremental_resources,
    fetch_incremental_resources,
    snapshot_coupon_codes,
    snapshot_resources,
)
from .models import (
    Accounts,
    BaseStream,
    Campaigns,
    CouponCodes,
    EmailCampaigns,
    EmailCampaignsArchived,
    EndpointConfig,
    Events,
    FULL_REFRESH_STREAMS,
    Flows,
    FlowsArchived,
    INCREMENTAL_STREAMS,
    IncrementalStream,
    MobilePushCampaigns,
    MobilePushCampaignsArchived,
    ResourceConfig,
    ResourceState,
    SmsCampaigns,
    SmsCampaignsArchived,
)
from .shared import dt_to_str

# Klaviyo doesn't use the standard "Bearer" token type in the Authorization
# header. Instead, it uses "Klaviyo-API-Key" as the token type.
AUTHORIZATION_TOKEN_TYPE = "Klaviyo-API-Key"
EVENTS_EVENTUAL_CONSISTENCY_HORIZON = timedelta(days=4)


async def validate_credentials(
        log: Logger, http: HTTPMixin, config: EndpointConfig
):
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_token_type=AUTHORIZATION_TOKEN_TYPE)

    try:
        async for _ in snapshot_resources(http, Accounts, log):
            break
    except HTTPError as err:
        msg = 'Unknown error occurred.'
        if err.code == 401:
            msg = f"Invalid credentials. Please confirm the provided credentials are correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating credentials.\n\n{err.message}"

        raise ValidationError([msg])


def full_refresh_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
            model: type[BaseStream],
            binding: CaptureBinding[ResourceConfig],
            binding_index: int,
            state: ResourceState,
            task: Task,
            all_bindings
    ):
        if issubclass(model, CouponCodes):
            fetch_snapshot = functools.partial(
                snapshot_coupon_codes,
                http,
            )
        else:
            fetch_snapshot = functools.partial(
                snapshot_resources,
                http,
                model,
            )

        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=fetch_snapshot,
            tombstone=common.BaseDocument(_meta=common.BaseDocument.Meta(op="d"))
        )

    resources = [
        common.Resource(
            name=model.name,
            key=["/_meta/row_id"],
            model=common.BaseDocument,
            open=functools.partial(open, model),
            initial_state=ResourceState(),
            initial_config=ResourceConfig(
                name=model.name, interval=timedelta(minutes=60)
            ),
            schema_inference=True,
        )
        for model in FULL_REFRESH_STREAMS
    ]

    return resources


def incremental_resources(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:

    def open(
        model: type[IncrementalStream],
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
                fetch_incremental_resources,
                http,
                model,
                None,
            ),
            fetch_page=functools.partial(
                backfill_incremental_resources,
                http,
                model,
            )
        )

    cutoff = datetime.now(tz=UTC)
    start = dt_to_str(config.start_date - timedelta(seconds=1))

    resources = [
            common.Resource(
            name=model.name,
            key=["/id"],
            model=IncrementalStream,
            open=functools.partial(open, model),
            initial_state=ResourceState(
                inc=ResourceState.Incremental(cursor=cutoff),
                backfill=ResourceState.Backfill(cutoff=cutoff, next_page=start)
            ),
            initial_config=ResourceConfig(
                name=model.name, interval=timedelta(minutes=5)
            ),
            schema_inference=True,
        )
        for model in INCREMENTAL_STREAMS
    ]

    return resources


def campaigns(
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
            fetch_changes={
                # Klaviyo has various different campaign types, and the API only allows
                # querying for a single campaign type at a time. Additionally, archived
                # and non-archived campaigns must also be queried in separate API calls.
                # Using subtasks to query different campaign types as well as the archived
                # variations of each helps get around these restrictions.
                "email": functools.partial(
                    fetch_incremental_resources, http, EmailCampaigns, None,
                ),
                "archived_email": functools.partial(
                    fetch_incremental_resources, http, EmailCampaignsArchived, None,
                ),
                "sms": functools.partial(
                    fetch_incremental_resources, http, SmsCampaigns, None,
                ),
                "archived_sms": functools.partial(
                    fetch_incremental_resources, http, SmsCampaignsArchived, None,
                ),
                "mobile_push": functools.partial(
                    fetch_incremental_resources, http, MobilePushCampaigns, None,
                ),
                "archived_mobile_push": functools.partial(
                    fetch_incremental_resources, http, MobilePushCampaignsArchived, None,
                )
            },
            fetch_page={
                "email": functools.partial(
                    backfill_incremental_resources, http, EmailCampaigns,
                ),
                "archived_email": functools.partial(
                    backfill_incremental_resources, http, EmailCampaignsArchived,
                ),
                "sms": functools.partial(
                    backfill_incremental_resources, http, SmsCampaigns,
                ),
                "archived_sms": functools.partial(
                    backfill_incremental_resources, http, SmsCampaignsArchived,
                ),
                "mobile_push": functools.partial(
                    backfill_incremental_resources, http, MobilePushCampaigns,
                ),
                "archived_mobile_push": functools.partial(
                    backfill_incremental_resources, http, MobilePushCampaignsArchived,
                ),
            }
        )

    cutoff = datetime.now(tz=UTC)
    start = dt_to_str(config.start_date - timedelta(seconds=1))

    return common.Resource(
        name=Campaigns.name,
        key=["/id"],
        model=IncrementalStream,
        open=open,
        initial_state=ResourceState(
            inc={
                "email": ResourceState.Incremental(cursor=cutoff),
                "archived_email": ResourceState.Incremental(cursor=cutoff),
                "sms": ResourceState.Incremental(cursor=cutoff),
                "archived_sms": ResourceState.Incremental(cursor=cutoff),
                "mobile_push": ResourceState.Incremental(cursor=cutoff),
                "archived_mobile_push": ResourceState.Incremental(cursor=cutoff),
            },
            backfill={
                "email": ResourceState.Backfill(cutoff=cutoff, next_page=start),
                "archived_email": ResourceState.Backfill(cutoff=cutoff, next_page=start),
                "sms": ResourceState.Backfill(cutoff=cutoff, next_page=start),
                "archived_sms": ResourceState.Backfill(cutoff=cutoff, next_page=start),
                "mobile_push": ResourceState.Backfill(cutoff=cutoff, next_page=start),
                "archived_mobile_push": ResourceState.Backfill(cutoff=cutoff, next_page=start),
            }
        ),
        initial_config=ResourceConfig(
            name=Campaigns.name, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def flows(
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
            # Klaviyo's API does not support querying archived and non-archived flows
            # in a single API request. Having separate subtasks that query archived / non-archived
            # flows for the same binding helps address the API's limitation.
            fetch_changes={
                "active": functools.partial(
                    fetch_incremental_resources, http, Flows, None,
                ),
                "archived": functools.partial(
                    fetch_incremental_resources, http, FlowsArchived, None,
                ),
            },
            fetch_page={
                "active": functools.partial(
                    backfill_incremental_resources, http, Flows,
                ),
                "archived": functools.partial(
                    backfill_incremental_resources, http, FlowsArchived,
                ),
            }
        )

    cutoff = datetime.now(tz=UTC)
    start = dt_to_str(config.start_date - timedelta(seconds=1))

    return common.Resource(
        name=Flows.name,
        key=["/id"],
        model=IncrementalStream,
        open=open,
        initial_state=ResourceState(
            inc={
                "active": ResourceState.Incremental(cursor=cutoff),
                "archived": ResourceState.Incremental(cursor=cutoff),
            },
            backfill={
                "active": ResourceState.Backfill(cutoff=cutoff, next_page=start),
                "archived": ResourceState.Backfill(cutoff=cutoff, next_page=start),
            }
        ),
        initial_config=ResourceConfig(
            name=Flows.name, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def _build_events_resource(
    http: HTTPMixin,
    config: EndpointConfig,
    stream_name: str,
    model: type[Events],
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
            fetch_changes={
                # The Klaviyo Events API is eventually consistent, so it is likely
                # the "realtime" subtask will miss change events. The "lookback"
                # subtask will follow behind to capture those missed changes.
                "realtime": functools.partial(
                    fetch_incremental_resources, http, model, None,
                ),
                "lookback": functools.partial(
                    fetch_incremental_resources, http, model, EVENTS_EVENTUAL_CONSISTENCY_HORIZON,
                ),
            },
            fetch_page=functools.partial(backfill_events, http, config.advanced.window_size, model)
        )

    cutoff = datetime.now(tz=UTC)
    start = dt_to_str(config.start_date - timedelta(seconds=1))

    return common.Resource(
        name=stream_name,
        key=["/id"],
        model=IncrementalStream,
        open=open,
        initial_state=ResourceState(
            inc={
                "realtime": ResourceState.Incremental(cursor=cutoff),
                "lookback": ResourceState.Incremental(cursor=cutoff - EVENTS_EVENTUAL_CONSISTENCY_HORIZON),
            },
            backfill=ResourceState.Backfill(cutoff=cutoff, next_page=start)
        ),
        initial_config=ResourceConfig(
            name=stream_name, interval=timedelta(minutes=5)
        ),
        schema_inference=True,
    )


def events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> common.Resource:
    return _build_events_resource(http, config, Events.name, Events)


def custom_events(
        log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    return [
        _build_events_resource(
            http,
            config,
            f"custom_{custom_stream.name}",
            custom_stream.build_model(),
        )
        for custom_stream in config.advanced.custom_event_streams
    ]


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    http.token_source = TokenSource(oauth_spec=None, credentials=config.credentials, authorization_token_type=AUTHORIZATION_TOKEN_TYPE)

    resources = [
        *full_refresh_resources(log, http, config),
        *incremental_resources(log, http, config),
        campaigns(log, http, config),
        flows(log, http, config),
        events(log, http, config),
        *custom_events(log, http, config),
    ]

    return resources
