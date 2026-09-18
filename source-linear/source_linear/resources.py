from logging import Logger

from estuary_cdk.capture import common, Task
from estuary_cdk.flow import ValidationError
from estuary_cdk.http import HTTPError, HTTPMixin, TokenSource

from .api import API
from .models import EndpointConfig

# Linear expects the personal API key as a bare `Authorization: <key>` value, with no
# `Bearer` prefix. Both arguments below are load-bearing:
#   - `authorization_token_type=""` drops the prefix.
#   - the lowercase header name (equivalent on the wire — RFC 9110 header names are
#     case-insensitive) differs from the CDK's `DEFAULT_AUTHORIZATION_HEADER`, which
#     routes us to the branch that emits a bare token without logging a
#     "no token type prefix" warning on every single request.
# Keep both: the empty token type alone still authenticates correctly but floods the
# logs, and the header name alone would silently regain the `Bearer` prefix if the
# CDK ever compared header names case-insensitively.
AUTHORIZATION_HEADER = "authorization"


def _token_source(config: EndpointConfig) -> TokenSource:
    return TokenSource(
        oauth_spec=None,
        credentials=config.credentials,
        authorization_header=AUTHORIZATION_HEADER,
        authorization_token_type="",
    )


async def validate_credentials(log: Logger, http: HTTPMixin, config: EndpointConfig):
    """Confirm the configured API key authenticates against Linear."""
    http.token_source = _token_source(config)

    try:
        await http.request(log, API, method="POST", json={"query": "{ viewer { id } }"})
    except HTTPError as err:
        if err.code == 401:
            msg = f"Invalid API key. Please confirm the provided Linear API key is correct.\n\n{err.message}"
        else:
            msg = f"Encountered error validating API key.\n\n{err.message}"

        raise ValidationError([msg])


async def all_resources(
    log: Logger, http: HTTPMixin, config: EndpointConfig
) -> list[common.Resource]:
    """Enumerate every stream the connector exposes."""
    # `add-stream` appends resources here. Empty until the first stream is added.
    return []
