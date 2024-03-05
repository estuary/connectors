import estuary_cdk.pydantic_polyfill  # Must be first.

import asyncio
import urllib
from estuary_cdk import shim_airbyte_cdk, flow
from source_hubspot import SourceHubspot

scopes = [
    "oauth",
    "forms",
    "files",
    "tickets",
    "e-commerce",
    "sales-email-read",
    "forms-uploaded-files",
    "crm.lists.read",
    "crm.objects.contacts.read",
    "files.ui_hidden.read",
    "crm.schemas.contacts.read",
    "crm.objects.companies.read",
    "crm.objects.deals.read",
    "crm.schemas.companies.read",
    "crm.schemas.deals.read",
    "crm.objects.owners.read",
]

optional_scopes = [
    "content",
    "automation",
    "crm.objects.feedback_submissions.read",
]

asyncio.run(
    shim_airbyte_cdk.CaptureShim(
        delegate=SourceHubspot(),
        oauth2=flow.OAuth2Spec(
            provider="hubspot",
            authUrlTemplate=(
                "https://app.hubspot.com/oauth/authorize?"
                r"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&scope="
                + urllib.parse.quote(" ".join(scopes))
                + r"&optional_scope="
                + urllib.parse.quote(" ".join(optional_scopes))
                + r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&response_type=code&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
            ),
            accessTokenUrlTemplate="https://api.hubapi.com/oauth/v1/token",
            accessTokenHeaders={"content-type": "application/x-www-form-urlencoded"},
            accessTokenBody=(
                "grant_type=authorization_code"
                r"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}"
                r"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}"
                r"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}"
                r"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"
            ),
            accessTokenResponseMap={
                "refresh_token": "/refresh_token",
            },
        ),
        schema_inference=False,
    ).serve()
)
