from datetime import datetime, UTC, timedelta
from enum import StrEnum
import json
from typing import TYPE_CHECKING
from urllib import parse

from estuary_cdk.capture.common import (
    BaseDocument,
    RotatingOAuth2Credentials,
    OAuth2RotatingTokenSpec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)

from pydantic import AwareDatetime, BaseModel, Field


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def urlencode_field(field: str):
    return "{{#urlencode}}{{{ " + field + " }}}{{/urlencode}}"


scopes = [
    "accounts.read",
    "auditLogs.read",
    "batches.read",
    "batchItems.read",
    "calls.read",
    "callDispositions.read",
    "callPurposes.read",
    "complianceRequests.read",
    "contentCategories.read",
    "contentCategoryMemberships.read",
    "contentCategoryOwnerships.read",
    "currencyTypes.read",
    "customObjectRecords.read",
    "datedConversionRates.read",
    "duties.read",
    "emailAddresses.read",
    "events.read",
    "favorites.read",
    "imports.read",
    "jobRoles.read",
    "mailboxes.read",
    "mailings.read",
    "mailAliases.read",
    "opportunities.read",
    "opportunityProspectRoles.read",
    "opportunityStages.read",
    "orgSettings.read",
    "personas.read",
    "phoneNumbers.read",
    "products.read",
    "profiles.read",
    "prospects.read",
    "purchases.read",
    "recipients.read",
    "recordActorAssignments.read",
    "roles.read",
    "rulesets.read",
    "sequences.read",
    "sequenceStates.read",
    "sequenceSteps.read",
    "sequenceTemplates.read",
    "snippets.read",
    "stages.read",
    "tasks.read",
    "taskDispositions.read",
    "taskPriorities.read",
    "taskPurposes.read",
    "teams.read",
    "templates.read",
    "users.read",
    "webhooks.read",
]


accessTokenBody = {
    "grant_type": "authorization_code",
    "code": "{{{ code }}}",
    "client_id": "{{{ client_id }}}",
    "client_secret": "{{{ client_secret }}}",
    "redirect_uri": "{{{ redirect_uri }}}",
}


OAUTH2_SPEC = OAuth2RotatingTokenSpec(
    provider="outreach",
    accessTokenBody=json.dumps(accessTokenBody),
    authUrlTemplate=(
        "https://api.outreach.io/oauth/authorize?"
        f"response_type=code&"
        f"scope=" + parse.quote(' '.join(scopes)) + "&"
        f"state={urlencode_field('state')}&"
        f"client_id={urlencode_field('client_id')}&"
        f"redirect_uri={urlencode_field('redirect_uri')}"
    ),
    accessTokenUrlTemplate="https://api.outreach.io/oauth/token",
    accessTokenResponseMap={
        "refresh_token": "/refresh_token",
        "access_token": "/access_token",
        "access_token_expires_at": r"{{#now_plus}}{{ expires_in }}{{/now_plus}}",
    },
    accessTokenHeaders={
        "Content-Type": "application/json",
    },
    additionalTokenExchangeBody=None,
)


if TYPE_CHECKING:
    OAuth2Credentials = RotatingOAuth2Credentials
else:
    OAuth2Credentials = RotatingOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class EndpointConfig(BaseModel):
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )
    credentials: OAuth2Credentials = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class OutreachResource(BaseDocument, extra="allow"):
    id: int


class OutreachResponse(BaseModel, extra="allow"):
    class Links(BaseModel, extra="allow"):
        first: str
        next: str | None = None

    data: list[OutreachResource]
    # links is not present if there are no results returned.
    links: Links | None = None


class CursorField(StrEnum):
    CREATED_AT = "createdAt"
    UPDATED_AT = "updatedAt"


# Resources and their name, path component, additional query params (if any), and cursor field.
INCREMENTAL_RESOURCES: list[tuple[str, str, dict[str, str | int | bool] | None, CursorField]] = [
    ("accounts", "accounts", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("calls", "calls", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("call_dispositions", "callDispositions", None, CursorField.UPDATED_AT),
    ("call_purposes", "callPurposes", None, CursorField.UPDATED_AT),
    ("email_addresses", "emailAddresses", None, CursorField.UPDATED_AT),
    ("events", "events", {"provideDataConnections": "true"}, CursorField.CREATED_AT),
    ("mailboxes", "mailboxes", None, CursorField.UPDATED_AT),
    ("mailings", "mailings", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("opportunities", "opportunities", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("opportunity_stages", "opportunityStages", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("prospects", "prospects", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("stages", "stages", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("tasks", "tasks", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
    ("teams", "teams", None, CursorField.UPDATED_AT),
    ("templates", "templates", None, CursorField.UPDATED_AT),
    ("users", "users", {"provideDataConnections": "true"}, CursorField.UPDATED_AT),
]
