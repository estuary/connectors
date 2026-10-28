import functools
from datetime import timedelta
from logging import Logger
from typing import Any

from estuary_cdk.capture import Task, common
from estuary_cdk.capture.common import ResourceConfig, ResourceState
from estuary_cdk.flow import CaptureBinding, ValidationError
from estuary_cdk.http import HTTPError, TokenSource

from .api import (
    AccountAccessError,
    AccountDirectory,
    query_user,
    snapshot_accounts,
    snapshot_ad_groups,
    snapshot_ads,
    snapshot_campaigns,
)
from .errors import INVALID_CREDENTIALS, fault_of
from .http import BingAdsHTTPMixin
from .models import EndpointConfig, OAUTH2_SPEC

ENTITY_INTERVAL = timedelta(hours=1)


def _configure_http(http: BingAdsHTTPMixin, config: EndpointConfig) -> None:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    http.developer_token = config.developer_token


async def validate_credentials(log: Logger, http: BingAdsHTTPMixin, config: EndpointConfig):
    """Confirm the credentials authenticate and every account the capture
    will read can be read with the headers the capture uses."""
    _configure_http(http, config)

    try:
        await query_user(http, log)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [f"Invalid credentials. Please re-authenticate with Microsoft.\n\n{err.message}"]
            )
        fault = fault_of(err)
        if fault is not None and INVALID_CREDENTIALS in fault.codes():
            raise ValidationError(
                [
                    "Microsoft rejected the credentials or developer token. Confirm the "
                    f"developer token is a production token.\n\n{fault.describe()}"
                ]
            )
        raise

    try:
        await AccountDirectory(http, config.account_ids).accounts(log)
    except AccountAccessError as err:
        raise ValidationError(err.errors)


def entity_resources(directory: AccountDirectory, http: BingAdsHTTPMixin) -> list[common.Resource]:
    def open(
        fetch_snapshot: common.FetchSnapshotFn[Any],
        binding: CaptureBinding[ResourceConfig],
        binding_index: int,
        state: ResourceState,
        task: Task,
        all_bindings: Any,
    ):
        common.open_binding(
            binding,
            binding_index,
            state,
            task,
            fetch_snapshot=fetch_snapshot,
        )

    snapshots = {
        "accounts": functools.partial(snapshot_accounts, directory),
        "campaigns": functools.partial(snapshot_campaigns, http, directory),
        "ad_groups": functools.partial(snapshot_ad_groups, http, directory),
        "ads": functools.partial(snapshot_ads, http, directory),
    }

    return [
        common.SnapshotResource(
            name=name,
            open=functools.partial(open, fetch_snapshot),
            initial_config=ResourceConfig(name=name, interval=ENTITY_INTERVAL),
        )
        for name, fetch_snapshot in snapshots.items()
    ]


async def all_resources(
    log: Logger, http: BingAdsHTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes. Makes no API calls:
    accounts are discovered once, lazily, when a stream first needs them."""
    _configure_http(http, config)
    directory = AccountDirectory(http, config.account_ids)

    return [
        *entity_resources(directory, http),
    ]
