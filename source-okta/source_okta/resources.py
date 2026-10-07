from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import base_url
from .models import EndpointConfig

# Okta API tokens use their own authorization scheme in place of `Bearer`.
AUTHORIZATION_TOKEN_TYPE = "SSWS"


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured API token authenticates against the Okta org."""
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_token_type=AUTHORIZATION_TOKEN_TYPE,
    )
    url = f"{base_url(config.domain)}/users"

    try:
        await http.request(log, url, params={"limit": 1})
    except HTTPError as err:
        if err.code == 401:
            msg = (
                "Invalid API token. Confirm the token is correct, was created in the "
                f"Okta org at {config.domain}, and has not been revoked after 30 days "
                f"without use.\n\n{err.message}"
            )
        elif err.code == 403:
            msg = (
                "The API token is valid but its admin role cannot list users. Create "
                f"the token from an account with a read-only admin role or higher.\n\n{err.message}"
            )
        else:
            msg = f"Encountered error validating API token.\n\n{err.message}"

        raise ValidationError([msg])


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    http.token_source = TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_token_type=AUTHORIZATION_TOKEN_TYPE,
    )
    # `add-stream` appends resources here. Empty until the first stream is added.
    return []
