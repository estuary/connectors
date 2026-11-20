from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import base_url
from .models import EndpointConfig

AUTHORIZATION_TOKEN_TYPE = "Token"


def _set_token_source(http: HTTPMixin, config: EndpointConfig) -> None:
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_token_type=AUTHORIZATION_TOKEN_TYPE,
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured credentials authenticate against the provider."""
    _set_token_source(http, config)
    # billing_descriptions is a tiny reference table every account has.
    url = f"{base_url(config.subdomain, config.account_name)}/billing_descriptions/"

    try:
        await http.request(log, url)
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API token. Please confirm the provided API token is correct.\n\n{err.message}"
        elif err.code == 404:
            msg = f"SaaSOptics account not found. Please confirm the subdomain and account name match your SaaSOptics login URL.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API token.\n\n{err.message}"

        raise ValidationError([msg])


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    _set_token_source(http, config)
    # `add-stream` appends resources here. Empty until the first stream is added.
    return []
