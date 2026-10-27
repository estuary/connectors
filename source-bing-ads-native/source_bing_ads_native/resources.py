from logging import Logger

from estuary_cdk.capture import common
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, TokenSource

from .api import query_user
from .http import BingAdsHTTPMixin
from .models import EndpointConfig, OAUTH2_SPEC


def _configure_http(http: BingAdsHTTPMixin, config: EndpointConfig) -> None:
    http.token_source = TokenSource(oauth_spec=OAUTH2_SPEC, credentials=config.credentials)
    http.developer_token = config.developer_token


async def validate_credentials(log: Logger, http: BingAdsHTTPMixin, config: EndpointConfig):
    """Confirm the OAuth credentials and developer token authenticate.

    `User/Query` is cheap, read-only and needs no account headers.
    Account-level checks are added with account discovery.
    """
    _configure_http(http, config)

    try:
        await query_user(http, log)
    except HTTPError as err:
        if err.code == 401:
            raise ValidationError(
                [f"Invalid credentials. Please re-authenticate with Microsoft.\n\n{err.message}"]
            )
        raise


async def all_resources(
    log: Logger, http: BingAdsHTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    _configure_http(http, config)

    # `add-stream` appends resources here. Empty until the first stream is added.
    return []
