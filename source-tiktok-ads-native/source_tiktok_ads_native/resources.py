import functools
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from logging import Logger
from collections.abc import AsyncGenerator, Callable
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.flow import AccessToken, CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import (
    ENTITY_HISTORY_START,
    AdvertiserAccounts,
    InaccessibleAdvertiserError,
    NoAdvertisersError,
    TikTokClient,
    backfill_ads,
    fetch_ads,
    snapshot_advertisers,
    tiktok_client,
    TikTokAPIError,
    backfill_ad_groups,
    backfill_campaigns,
    get_advertisers,
    fetch_ad_groups,
    resolve_advertiser_ids,
    settled_second,
    fetch_campaigns,
)
from .reports import backfill_report, check_report, fetch_report
from .models import (
    OAUTH2_SPEC,
    Granularity,
    ReportConfig,
    report_document_model,
    Ad,
    AdGroup,
    Campaign,
    ConnectorState,
    EndpointConfig,
    ResourceConfig,
    ResourceState,
    TikTokEntity,
)

# TikTok expects the raw token in an `Access-Token` header, not `Authorization: Bearer`.
AUTHORIZATION_HEADER = "Access-Token"

# Each poll lists every object of every advertiser, so entity streams poll less
# often than the 5-minute default.
ENTITY_INTERVAL = timedelta(minutes=30)
# Snapshots re-read everything, and advertiser accounts rarely change.
SNAPSHOT_INTERVAL = timedelta(hours=12)
# Each report poll re-reads the whole lookback window for every advertiser.
REPORT_INTERVAL = timedelta(hours=12)

type TikTokResource = common.Resource[Any, ResourceConfig, ResourceState]


def _set_token_source(http: HTTPMixin, config: EndpointConfig) -> None:
    http.token_source = TokenSource(
        oauth_spec=OAUTH2_SPEC,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
    )


def _app_credentials(config: EndpointConfig) -> tuple[str | None, str | None]:
    """The app ID and secret that listing advertisers takes, which only OAuth
    credentials carry."""
    if isinstance(config.credentials, AccessToken):
        return None, None
    return config.credentials.client_id, config.credentials.client_secret


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the credentials authenticate and can see every configured advertiser."""
    _set_token_source(http, config)
    client = tiktok_client(http, config.advanced.is_sandbox)

    # A failed OAuth sign-in answers HTTP 200, which reaches the config as an
    # empty token rather than an error.
    if not config.credentials.access_token.strip():
        raise ValidationError(
            [
                "TikTok didn't return an access token. Please re-authorize the connector, or check the access token."
            ]
        )

    # The sandbox has no `oauth2/advertiser/get`, so it can't discover advertisers.
    if config.advanced.is_sandbox and not config.advertiser_ids:
        raise ValidationError(
            [
                "Sandbox ad accounts require Advertiser IDs. Enter the advertiser ID shown on your app's Sandbox Ad Account page."
            ]
        )

    app_id, secret = _app_credentials(config)
    if not config.advertiser_ids and app_id is None:
        raise ValidationError(
            [
                "Advertiser IDs are required when authenticating with an access token. Enter the IDs of the advertisers to capture, or authenticate with OAuth."
            ]
        )

    try:
        advertiser_ids = await resolve_advertiser_ids(
            client, config.advertiser_ids, app_id, secret, log
        )
        # The capture reads every advertiser's details, so an advertiser TikTok
        # can't read fails here rather than when the capture starts.
        accessible_ids = [
            advertiser.advertiser_id
            for advertiser in await get_advertisers(client, advertiser_ids, log)
        ]
    except NoAdvertisersError as err:
        raise ValidationError([str(err)])
    except InaccessibleAdvertiserError as err:
        raise ValidationError([err.message])
    except (HTTPError, TikTokAPIError) as err:
        raise ValidationError(
            [
                f"Could not authenticate with TikTok. Please re-authorize the connector, or check the access token.\n\n{err}"
            ]
        )

    missing = set(advertiser_ids) - set(accessible_ids)
    if missing:
        raise ValidationError(
            [
                f"The authorized TikTok account cannot access these advertiser IDs: {', '.join(sorted(missing))}"
            ]
        )

    # TikTok rejects an unknown metric or dimension combination by name, so one
    # small request per report surfaces every configuration mistake at once.
    report_errors: list[str] = []
    for report in config.reports():
        try:
            await check_report(client, report, accessible_ids[0], log)
        except TikTokAPIError as err:
            report_errors.append(f"Report {report.name}: {err.message}")
    if report_errors:
        raise ValidationError(report_errors)


def _per_advertiser_state(
    advertiser_ids: list[str], inc_cursor: datetime, cutoff: datetime | None
) -> ResourceState:
    """One incremental state per advertiser, plus one backfill state when there
    is a `cutoff`, so that each advertiser is captured as its own subtask."""
    return ResourceState(
        inc={
            advertiser_id: ResourceState.Incremental(cursor=inc_cursor)
            for advertiser_id in advertiser_ids
        },
        backfill=None
        if cutoff is None
        else {
            advertiser_id: ResourceState.Backfill(cutoff=cutoff, next_page=None)
            for advertiser_id in advertiser_ids
        },
    )


async def _add_new_advertisers(
    binding: CaptureBinding[ResourceConfig],
    state: ResourceState,
    initial_state: ResourceState,
    task: Task,
) -> None:
    """Give each advertiser without state its initial state, so that one added
    after the capture started is backfilled from `start_date` rather than joining
    at the others' cursors. An advertiser is known by its incremental state,
    since a finished backfill removes its own."""
    assert isinstance(state.inc, dict) and isinstance(initial_state.inc, dict)
    new_ids = [i for i in initial_state.inc if i not in state.inc]
    if not new_ids:
        return

    for advertiser_id in new_ids:
        state.inc[advertiser_id] = deepcopy(initial_state.inc[advertiser_id])
        if isinstance(initial_state.backfill, dict):
            if not isinstance(state.backfill, dict):
                state.backfill = {}
            state.backfill[advertiser_id] = deepcopy(
                initial_state.backfill[advertiser_id]
            )

    task.log.info(
        "Adding state for new advertisers",
        {"binding": binding.stateKey, "advertiser_ids": new_ids},
    )
    await task.checkpoint(ConnectorState(bindingStateV1={binding.stateKey: state}))


def _entity_resource(
    name: str,
    model: type[TikTokEntity],
    fetch_changes: Callable[..., AsyncGenerator[Any, None]],
    fetch_page: Callable[..., AsyncGenerator[Any, None]],
    client: TikTokClient,
    accounts: AdvertiserAccounts,
    advertiser_ids: list[str],
) -> TikTokResource:
    # The seam sits on a whole second that lags the present like every poll's
    # horizon. The backfill covers creation times up to `cutoff - 1s`, so the
    # first incremental window starts at `cutoff`.
    cutoff = settled_second()
    initial_state = _per_advertiser_state(
        advertiser_ids, cutoff - timedelta(seconds=1), cutoff
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: Any,
    ):
        await _add_new_advertisers(binding, state, initial_state, task)
        # An advertiser TikTok can no longer read fails here, with advice.
        await accounts.check(task.log)
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                advertiser_id: functools.partial(fetch_changes, client, advertiser_id)
                for advertiser_id in advertiser_ids
            },
            fetch_page={
                advertiser_id: functools.partial(
                    fetch_page, client, advertiser_id, ENTITY_HISTORY_START
                )
                for advertiser_id in advertiser_ids
            },
        )

    return common.Resource(
        name=name,
        key=[f"/{model.ID_FIELD}"],
        model=model,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(name=name, interval=ENTITY_INTERVAL),
        schema_inference=True,
    )


def _advertisers(client: TikTokClient, advertiser_ids: list[str]) -> TikTokResource:
    def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: Any,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=functools.partial(
                snapshot_advertisers, client, advertiser_ids
            ),
        )

    return common.SnapshotResource(
        name="Advertisers",
        open=open,
        initial_config=ResourceConfig(name="Advertisers", interval=SNAPSHOT_INTERVAL),
    )


def _report_resource(
    report: ReportConfig,
    client: TikTokClient,
    accounts: AdvertiserAccounts,
    advertiser_ids: list[str],
    start_date: datetime,
    lookback_days: int,
) -> TikTokResource:
    model = report_document_model(report)
    is_lifetime = report.granularity == Granularity.LIFETIME
    # Report dates are whole days: the backfill owns every day before the cutoff's
    # date, and the incremental re-reads from that date onward. Lifetime rows have
    # no dates, so there is no history to walk.
    cutoff = datetime.now(tz=UTC).replace(microsecond=0)
    initial_state = _per_advertiser_state(
        advertiser_ids, cutoff, None if is_lifetime else cutoff
    )

    async def open(
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        _all_bindings: Any,
    ):
        await _add_new_advertisers(binding, state, initial_state, task)
        zone_of = {i: await accounts.zone(i, task.log) for i in advertiser_ids}
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_changes={
                advertiser_id: functools.partial(
                    fetch_report,
                    client,
                    advertiser_id,
                    zone_of[advertiser_id],
                    report,
                    model,
                    start_date,
                    lookback_days,
                )
                for advertiser_id in advertiser_ids
            },
            fetch_page=None
            if is_lifetime
            else {
                advertiser_id: functools.partial(
                    backfill_report,
                    client,
                    advertiser_id,
                    zone_of[advertiser_id],
                    report,
                    model,
                    start_date,
                )
                for advertiser_id in advertiser_ids
            },
        )

    return common.Resource(
        name=report.name,
        key=report.key(),
        model=model,
        open=open,
        initial_state=initial_state,
        initial_config=ResourceConfig(name=report.name, interval=REPORT_INTERVAL),
        schema_inference=True,
    )


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[TikTokResource]:
    """Enumerate every stream the connector exposes."""
    _set_token_source(http, config)
    client = tiktok_client(http, config.advanced.is_sandbox)
    app_id, secret = _app_credentials(config)
    # Each advertiser is captured as its own subtask, so they're resolved here.
    advertiser_ids = await resolve_advertiser_ids(
        client, config.advertiser_ids, app_id, secret, log
    )
    accounts = AdvertiserAccounts(client, advertiser_ids)

    return [
        _advertisers(client, advertiser_ids),
        _entity_resource(
            "Campaigns",
            Campaign,
            fetch_campaigns,
            backfill_campaigns,
            client,
            accounts,
            advertiser_ids,
        ),
        _entity_resource(
            "AdGroups",
            AdGroup,
            fetch_ad_groups,
            backfill_ad_groups,
            client,
            accounts,
            advertiser_ids,
        ),
        _entity_resource(
            "Ads",
            Ad,
            fetch_ads,
            backfill_ads,
            client,
            accounts,
            advertiser_ids,
        ),
        *(
            _report_resource(
                report,
                client,
                accounts,
                advertiser_ids,
                config.start_date,
                config.advanced.lookback_window_days,
            )
            for report in config.reports()
        ),
    ]
