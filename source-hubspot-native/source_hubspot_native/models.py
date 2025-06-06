import urllib.parse
from datetime import datetime
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Annotated, ClassVar, Generic, Literal, Self, TypeVar
from pydantic.json_schema import SkipJsonSchema

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    BaseOAuth2Credentials,
    OAuth2Spec,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field, model_validator

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
    capturePropertyHistory: bool = Field(
        title="Capture Property History",
        description="Include historical data for changes to properties of HubSpot objects in captured documents.",
        default=False,
    )

    # In order to publish this connector in the HubSpot marketplace, HubSpot requires
    # that "setup documentation [and] in platform experience should not include an app that
    # requires API keys in order to be installed". Essentially, users should only be presented
    # the option to authenticate with OAuth in the UI.
    # 
    # There are already existing captures authenticating with access tokens, so we can't
    # simply strip out support for access token authentication. Patching model_json_schema to remove
    # AccessToken makes OAuth2 the only authentication option in the UI, and effectively
    # "hides" the capability to use access tokens. This should satify HubSpot's requirement
    # while retaining support for access token authentication for already created captures.
    @classmethod
    def model_json_schema(cls, *args, **kwargs) -> dict:
        schema = super().model_json_schema(*args, **kwargs)

        creds = schema.get("properties", {}).get("credentials", {})
        one_of = creds.get("oneOf", [])
        filtered_one_of = []

        for option in one_of:
            if "$ref" in option:
                ref = option["$ref"]
                def_name = ref.split("/")[-1]
                # Do not include AccessToken as an authentication option.
                if "AccessToken" in def_name:
                    continue

            filtered_one_of.append(option)

        creds["oneOf"] = filtered_one_of
        return schema


# We use ResourceState directly, without extending it.
ConnectorState = GenericConnectorState[ResourceState]


# Names of things within the HubSpot API domain.
class Names(StrEnum):
    commerce_payments = auto()
    companies = auto()
    contacts = auto()
    deal_pipelines = auto()
    deals = auto()
    email_events = auto()
    engagements = auto()
    invoices = auto()
    line_items = auto()
    owners = auto()
    products = auto()
    properties = auto()
    quotes = auto()
    subscriptions = auto()
    tickets = auto()
    emails = auto()
    meetings = auto()
    notes = auto()
    tasks = auto()
    content = auto()
    orders = auto()
    carts = auto()
    partner_clients = auto()
    marketing_event = auto()


# A Property is a HubSpot or HubSpot-user defined attribute that's
# attached to a HubSpot CRM object.
class Property(BaseDocument, extra="allow"):
    name: str = ""
    calculated: bool = False
    hubspotObject: str = "unknown"  # Added by us.


class Properties(BaseDocument, extra="forbid"):
    results: list[Property]


class DealPipeline(BaseDocument, extra="allow"):
    createdAt: AwareDatetime | None
    updatedAt: AwareDatetime | None


class DealPipelines(BaseDocument, extra="forbid"):
    results: list[DealPipeline]


class Owner(BaseDocument, extra="allow"):
    createdAt: AwareDatetime | None
    updatedAt: AwareDatetime | None


# Base Struct for all CRM Objects within HubSpot.
class BaseCRMObject(BaseDocument, extra="forbid"):
    ASSOCIATED_ENTITIES: ClassVar[list[str]]

    class History(BaseModel, extra="forbid"):
        timestamp: datetime
        value: str
        sourceType: str
        sourceId: str | None = None
        sourceLabel: str | None = None
        updatedByUserId: int | None = None

    id: int
    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    archived: bool

    properties: dict[str, str | None]
    propertiesWithHistory: dict[str, list[History]] | None = None

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
    def _post_init(self) -> Self:
        # Clear properties and history which don't have current values.
        self.properties = {k: v for k, v in self.properties.items() if v}
        if self.propertiesWithHistory:
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
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals]

    contacts: list[int] = []
    deals: list[int] = []


class Contact(BaseCRMObject):
    ASSOCIATED_ENTITIES = [Names.companies]

    companies: list[int] = []


class Deal(BaseCRMObject):
    ASSOCIATED_ENTITIES = [Names.contacts, Names.engagements, Names.line_items]

    contacts: list[int] = []
    engagements: list[int] = []
    line_items: list[int] = []


class Engagement(BaseCRMObject):
    ASSOCIATED_ENTITIES = [
        Names.contacts,
        Names.companies,
        Names.deals,
        Names.tickets,
        Names.quotes,
        Names.orders,
        Names.emails,
        Names.meetings,
        Names.notes,
        Names.tasks,
        Names.content,
        Names.orders,
        Names.carts,
        Names.partner_clients,
        Names.marketing_event,
    ]

    contacts: list[int] = []
    companies: list[int] = []
    deals: list[int] = []
    tickets: list[int] = []
    content: list[int] = []
    quotes: list[int] = []
    orders: list[int] = []
    emails: list[int] = []
    meetings: list[int] = []
    notes: list[int] = []
    tasks: list[int] = []
    content: list[int] = []
    orders: list[int] = []
    carts: list[int] = []
    partner_clients: list[int] = []
    marketing_event: list[int] = []


class Ticket(BaseCRMObject):
    ASSOCIATED_ENTITIES = [Names.contacts, Names.engagements, Names.line_items]

    contacts: list[int] = []
    engagements: list[int] = []
    line_items: list[int] = []


class Product(BaseCRMObject):
    # Products are not associated with other objects in HubSpot.
    ASSOCIATED_ENTITIES = []


class LineItem(BaseCRMObject):
    ASSOCIATED_ENTITIES = [
        Names.commerce_payments,
        Names.products,
        Names.deals,
        Names.invoices,
        Names.quotes,
        Names.subscriptions,
    ]

    commerce_payments: list[int] = []
    products: list[int] = []
    deals: list[int] = []
    invoices: list[int] = []
    quotes: list[int] = []
    subscriptions: list[int] = []


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
    # TODO(whb): We did encounter this, and for now have decided to ignore any
    # extra pages.
    class Cursor(BaseModel, extra="forbid"):
        after: str
        link: str

    class Paging(BaseModel, extra="forbid"):
        next: "Association.Cursor"

    paging: Paging | None = None


# An unconstrained Item type.
Item = TypeVar("Item")


# Common shape of a v3 API paged listing for a GET request to the objects endpoint for a particular
# object.
class PageResult(BaseModel, Generic[Item], extra="forbid"):
    class Cursor(BaseModel, extra="forbid"):
        after: str
        link: str

    class Paging(BaseModel, extra="forbid"):
        next: "PageResult.Cursor"

    results: list[Item]
    paging: Paging | None = None


# Common shape of a v3 search API listing, which is the same as PageResult but includes a field for
# the total number of records returned, and doesn't have a "link" in the paging.next object.
class SearchPageResult(BaseModel, Generic[Item], extra="forbid"):
    class Cursor(BaseModel, extra="forbid"):
        after: str

    class Paging(BaseModel, extra="forbid"):
        next: "SearchPageResult.Cursor"

    results: list[Item]
    paging: Paging | None = None
    total: int


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
    has_more: bool = Field(alias="has-more")  # type: ignore
    time_offset: int = Field(alias="time-offset")  # type: ignore
    vid_offset: int = Field(alias="vid-offset")  # type: ignore


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


# EmailEvent and EmailEventsResponse represent an email event and the shape of the email events API
# response, respectively.


class EmailEvent(BaseDocument, extra="allow"):
    id: str
    created: AwareDatetime
    type: Literal[
        "SENT",
        "DROPPED",
        "PROCESSED",
        "DELIVERED",
        "DEFERRED",
        "BOUNCE",
        "OPEN",
        "CLICK",
        "PRINT",
        "FORWARD",
        "STATUSCHANGE",
        "SPAMREPORT",
        "SUPPRESSED",
        "SUPPRESSION",  # "SUPPRESSION" is documented in HubSpot's docs, but "SUPPRESSED" isn't. We've seen "SUPPRESSED" events, so "SUPPRESSION" events might not actually occur.
        "UNBOUNCE",  # This is not actually a type reported by HubSpot, but the absence of the "type" field means its an UNBOUNCE type.
    ] = Field(
        default="UNBOUNCE",
        # Don't schematize the default value.
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )


class EmailEventsResponse(BaseModel, extra="forbid"):
    hasMore: bool
    offset: str
    events: list[EmailEvent]


# Custom objects can be associated with contacts, companies, deals, tickets, and any other custom
# object. For simplicity we are only capturing associations with the non-custom objects for now.
class CustomObject(BaseCRMObject):
    ASSOCIATED_ENTITIES = [Names.contacts, Names.companies, Names.deals, Names.tickets]

    contacts: list[int] = []
    companies: list[int] = []
    deals: list[int] = []
    tickets: list[int] = []


# There's quite a bit more information available from v3/schemas, but none of it seems particularly
# useful right now.
class CustomObjectSchema(BaseDocument, extra="allow"):
    name: str
    archived: bool


# This is the shape of a response from the V3 search API for custom objects. As above, we are
# modeling only the minimum needed to get the IDs and modification time.
class CustomObjectSearchResult(BaseModel):
    class Properties(BaseModel):
        hs_lastmodifieddate: AwareDatetime

        @model_validator(mode="before")
        def set_lastmodifieddate(cls, values):
            assert isinstance(values, dict)

            # The "Contacts" object uses `lastmodifieddate`, while everything
            # else uses `hs_lastmodifieddate`.
            if "lastmodifieddate" in values:
                values["hs_lastmodifieddate"] = datetime.fromisoformat(
                    values.pop("lastmodifieddate")
                )
            elif "hs_lastmodifieddate" in values:
                values["hs_lastmodifieddate"] = datetime.fromisoformat(
                    values.pop("hs_lastmodifieddate")
                )
            return values

    id: int
    properties: Properties
