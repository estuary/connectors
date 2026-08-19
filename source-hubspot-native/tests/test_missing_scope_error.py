"""Unit tests for the missing-scope 403 test.

`is_missing_scope_error` gates every permission-dependent path in the connector:
discovery drops a binding, the `properties` snapshot skips an object type, and
the association probe omits an entity, all on the strength of it returning True.
A 403 it doesn't recognize is treated as fatal, so a wording it fails to match
takes down whatever binding hit it.

HubSpot words these differently per endpoint *and* per authentication method, and
MISSING_SCOPE_REGEX has been extended multiple times as new wordings surfaced in
production. These cases pin each one against the response body that prompted it.
"""

import json
from logging import getLogger
from typing import Any

import pytest

from estuary_cdk.http import HTTPError
from source_hubspot_native.api.shared import is_missing_scope_error

log = getLogger("test_missing_scope_error")


def _http_error(body: dict[str, Any] | str, status: int = 403) -> HTTPError:
    """An HTTPError shaped the way HTTPMixin builds one for a 4xx: the response
    body is embedded in `message`, which is all the predicate gets to inspect."""
    rendered = body if isinstance(body, str) else json.dumps(body, indent=2)
    return HTTPError(
        f"Encountered HTTP error status {status} which cannot be retried.\n"
        "URL: https://api.hubapi.com/crm/v3/properties/leads\n"
        f"Response:\n{rendered}",
        status,
    )


# Observed reading /crm/v3/properties/leads with an OAuth token whose install
# didn't grant crm.objects.leads.read. Note the absence of a `category`.
OAUTH_VIEW_SCHEMA_REFUSAL = {
    "status": "error",
    "message": (
        "You do not have permissions to view_schema object type "
        "ObjectTypeId{legacyObjectType=LEAD} in portal 1234567 "
        "(requires one of [leads-read])"
    ),
    "correlationId": "00000000-0000-0000-0000-000000000000",
}

# The same call, same portal, same missing scope, using a private legacy app's
# token. HubSpot returns a structurally different body, carrying a category.
PRIVATE_APP_MISSING_SCOPES_REFUSAL = {
    "status": "error",
    "message": (
        "This app hasn't been granted all required scopes to make this call. "
        "Read more about required scopes here: https://developers.hubspot.com/scopes."
    ),
    "correlationId": "00000000-0000-0000-0000-000000000000",
    "errors": [
        {
            "message": "One or more of the following scopes are required.",
            "context": {
                "requiredGranularScopes": [
                    "crm.objects.leads.read",
                    "crm.objects.leads.write",
                    "e-commerce",
                ]
            },
        }
    ],
    "links": {"scopes": "https://developers.hubspot.com/scopes"},
    "category": "MISSING_SCOPES",
}


@pytest.mark.parametrize(
    "body",
    [
        OAUTH_VIEW_SCHEMA_REFUSAL,
        PRIVATE_APP_MISSING_SCOPES_REFUSAL,
        # Reading /crm/v3/objects/email_events without the scope.
        "This app hasn't been granted all required scopes to make this call.",
        # The legacy workflows endpoint. The "EXTERNAL " prefix is why the
        # pattern isn't anchored.
        "EXTERNAL auth request is missing required 'workflows-access-public-api' scope.",
        "This oauth-token (some token details) does not have proper permissions!",
    ],
    ids=[
        "oauth_view_schema",
        "private_app_missing_scopes",
        "app_not_granted_scopes",
        "external_auth_request",
        "oauth_token_improper_permissions",
    ],
)
def test_recognizes_every_observed_refusal(body: dict[str, Any] | str):
    assert is_missing_scope_error(log, _http_error(body)) is True


def test_private_app_refusal_is_caught_by_its_category_alone():
    """The category is matched independently of the prose, so a rewording of
    that sentence doesn't silently stop the private-app path from degrading."""
    reworded = dict(PRIVATE_APP_MISSING_SCOPES_REFUSAL, message="Reworded by HubSpot.")

    assert is_missing_scope_error(log, _http_error(reworded)) is True


def test_oauth_refusal_has_no_category_to_fall_back_on():
    """Guards against anyone replacing the prose patterns with the category
    check: this body carries no category at all."""
    assert "category" not in OAUTH_VIEW_SCHEMA_REFUSAL


@pytest.mark.parametrize(
    "body",
    [
        {"status": "error", "message": "This resource is private to its owner."},
    ],
    ids=["unrelated_forbidden"],
)
def test_other_403s_stay_fatal(body: dict[str, Any]):
    assert is_missing_scope_error(log, _http_error(body)) is False


@pytest.mark.parametrize("status", [400, 401, 404, 500], ids=str)
def test_only_403_is_considered(status: int):
    """A missing scope is always a 403, so nothing else should be mistaken for
    one -- including a 401, which means the token itself is bad."""
    assert is_missing_scope_error(
        log, _http_error(PRIVATE_APP_MISSING_SCOPES_REFUSAL, status)
    ) is False
