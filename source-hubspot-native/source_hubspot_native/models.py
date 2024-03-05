from datetime import datetime
from enum import StrEnum, auto
from pydantic import BaseModel, Field, AwareDatetime, model_validator
from typing import Literal, Generic, TypeVar, Annotated, ClassVar, TYPE_CHECKING
import urllib.parse

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    AccessToken,
    BaseDocument,
    BaseOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)

scopes = [
    "crm.lists.read",
    "crm.objects.companies.read",
    "crm.objects.contacts.read",
    "crm.objects.deals.read",
    "crm.objects.owners.read",
    "crm.schemas.companies.read",
    "crm.schemas.contacts.read",
    "crm.schemas.deals.read",
    "e-commerce",
    "files",
    "files.ui_hidden.read",
    "forms",
    "forms-uploaded-files",
    "sales-email-read",
    "tickets",
]

optional_scopes = [
    "automation",
    "content",
    "crm.objects.custom.read",
    "crm.objects.feedback_submissions.read",
    "crm.objects.goals.read",
    "crm.schemas.custom.read",
]

# TODO(johnny): Lift this string building into higher-order helpers.
OAUTH2_SPEC = OAuth2Spec(
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
)

if TYPE_CHECKING:
    OAuth2Credentials = BaseOAuth2Credentials
else:
    OAuth2Credentials = BaseOAuth2Credentials.for_provider(OAUTH2_SPEC.provider)


class EndpointConfig(BaseModel):
    credentials: OAuth2Credentials | AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]


# Names of things within the HubSpot API domain.
class Names(StrEnum):
    companies = auto()
    contacts = auto()
    deals = auto()
    engagements = auto()
    line_items = auto()
    properties = auto()
    tickets = auto()


# A Property is a HubSpot or HubSpot-user defined attribute that's
# attached to a HubSpot CRM object.
class Property(BaseDocument, extra="allow"):

    name: str = ""
    calculated: bool = False
    hubspotObject: str = "unknown"  # Added by us.


class Properties(BaseDocument, extra="forbid"):
    results: list[Property]


# Base Struct for all CRM Objects within HubSpot.
class BaseCRMObject(BaseDocument, extra="forbid"):
    # Class-scoped metadata attached to concrete subclasses.
    NAME: ClassVar[str]
    ASSOCIATED_ENTITIES: ClassVar[list[str]]
    CACHED_PROPERTIES: ClassVar[Properties]

    class History(BaseDocument, extra="forbid"):
        timestamp: datetime
        value: str
        sourceType: str
        sourceId: str | None = None
        updatedByUserId: int | None = None

    id: int
    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    archived: bool

    properties: dict[str, str | None]
    propertiesWithHistory: dict[str, list[History]] = {}

    class InlineAssociations(BaseModel):
        class Entry(BaseModel):
            id: int
            type: str  # E.x. "child_to_parent_company", "company_to_company"

        results: list[Entry]

    # Inlined associations are present on PageResult (backfill) responses,
    # but not within the BatchResult responses used for incremental streaming.
    associations: Annotated[
        dict[str, InlineAssociations],
        # Schematize as always-empty.
        Field(json_schema_extra={"additionalProperties": False}),
    ] = {}

    @model_validator(mode="after")
    def _post_init(self) -> "BaseCRMObject":
        # Clear properties and history which don't have current values.
        self.properties = {k: v for k, v in self.properties.items() if v is not None}
        self.propertiesWithHistory = {
            k: v for k, v in self.propertiesWithHistory.items() if len(v)
        }

        # If the model has attached inline associations,
        # hoist them to corresponding arrays. Then clear associations.
        for ae in self.ASSOCIATED_ENTITIES:
            if inline := self.associations.get(ae, None):
                setattr(self, ae, [int(k.id) for k in inline.results])

        delattr(self, "associations")
        return self


# CRMObject is a generic, concrete subclass of BaseCRMObject.
CRMObject = TypeVar("CRMObject", bound=BaseCRMObject)


class Company(BaseCRMObject):
    NAME = Names.companies
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals]

    contacts: list[int] = []
    deals: list[int] = []


class Contact(BaseCRMObject):
    NAME = Names.contacts
    ASSOCIATED_ENTITIES = [Names.companies]

    companies: list[int] = []


class Deal(BaseCRMObject):
    NAME = Names.deals
    ASSOCIATED_ENTITIES = [Names.contacts, Names.engagements, Names.line_items]

    contacts: list[int] = []
    engagements: list[int] = []
    line_items: list[int] = []


class Engagement(BaseCRMObject):
    NAME = Names.engagements
    ASSOCIATED_ENTITIES = [Names.deals]

    deals: list[int] = []


class Ticket(BaseCRMObject):
    NAME = Names.tickets
    ASSOCIATED_ENTITIES = [Names.contacts, Names.engagements, Names.line_items]

    contacts: list[int] = []
    engagements: list[int] = []
    line_items: list[int] = []


# An Association, as returned by the v4 associations API.
class Association(BaseModel, extra="forbid"):

    class Type(BaseModel, extra="forbid"):
        category: Literal["HUBSPOT_DEFINED", "USER_DEFINED"]
        # Type IDs are defined here: https://developers.hubspot.com/docs/api/crm/associations
        typeId: int
        # Ex "Child Company", "Parent Company".
        # These seem potentially useful, but they're not available when doing
        # paged reads of CRM entities, so they're omitted in output so that
        # paged vs batched reads of CRM records can have a consistent shape.
        label: str | None

    class From(BaseModel, extra="forbid"):
        id: int

    class To(BaseModel, extra="forbid"):
        toObjectId: int
        associationTypes: list["Association.Type"]

    from_: From = Field(alias="from")
    to: list[To]

    # TODO(johnny): This can potentially have a `pager`, but I'm leaving it out
    # to see if it's ever encountered. If it is, I think we handle by ignoring
    # it and having a position of "we'll get (only) the first page".


# An unconstrained Item type.
Item = TypeVar("Item")


# Common shape of a v3 API paged listing.
class PageResult(BaseModel, Generic[Item], extra="forbid"):

    class Cursor(BaseModel, extra="forbid"):
        after: str
        link: str

    class Paging(BaseModel, extra="forbid"):
        next: "PageResult.Cursor"

    results: list[Item]
    paging: Paging | None = None


# Common shape of a v3 API batch read.
class BatchResult(BaseModel, Generic[Item], extra="forbid"):

    class Error(BaseModel, extra="forbid"):
        status: Literal["error"]
        category: Literal["OBJECT_NOT_FOUND"]
        message: str
        context: dict
        subCategory: str | None = None

    status: Literal["COMPLETE"]
    results: list[Item]
    startedAt: datetime
    completedAt: datetime
    errors: list[Error] = []
    numErrors: int = 0


# The following are models for HubSpot's "legacy" APIs for fetching
# recently-changed entities. We model them because these APIs are
# fast and HubSpot doesn't offer an equivalent, more-recent API.
# We model _just_ enough of these APIs in order to retrieve IDs
# and modification times.


class OldRecentCompanies(BaseModel):

    class Item(BaseModel):
        class Properties(BaseModel):
            class Timestamp(BaseModel):
                timestamp: int

            hs_lastmodifieddate: Timestamp

        companyId: int
        properties: Properties

    results: list[Item]
    hasMore: bool
    offset: int
    total: int


class OldRecentContacts(BaseModel):

    class Item(BaseModel):
        class Properties(BaseModel):
            class Timestamp(BaseModel):
                value: str

            lastmodifieddate: Timestamp

        vid: int
        properties: Properties

    contacts: list[Item]
    has_more: bool = Field(alias="has-more")
    time_offset: int = Field(alias="time-offset")
    vid_offset: int = Field(alias="vid-offset")


class OldRecentDeals(BaseModel):

    class Item(BaseModel):
        class Properties(BaseModel):
            class Timestamp(BaseModel):
                timestamp: int

            hs_lastmodifieddate: Timestamp

        dealId: int
        properties: Properties

    results: list[Item]
    hasMore: bool
    offset: int
    total: int


class OldRecentEngagements(BaseModel):

    class Item(BaseModel):
        class Engagement(BaseModel):
            id: int
            lastUpdated: int

        engagement: Engagement

    results: list[Item]
    hasMore: bool
    offset: int
    total: int


class OldRecentTicket(BaseModel):
    timestamp: int
    objectId: int
