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
    "communication_preferences.read",
    "communication_preferences.read_write",
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
    campaigns = auto()
    companies = auto()
    contacts = auto()
    deals = auto()
    engagements = auto()
    line_items = auto()
    properties = auto()
    tickets = auto()
    contact_lists = auto()
    engagements_calls = auto()
    engagements_emails = auto()
    engagements_meetings = auto()
    engagements_notes = auto()
    engagements_tasks = auto()
    ticket_pipelines = auto()
    marketing_emails = auto()
    deal_pipelines = auto()
    subscription_changes = auto()
    email_events = auto()
    marketing_forms = auto()
    owners = auto()
    products = auto()
    workflows = auto()
    goal_targets = auto()
    feedback_submissions = auto()
    email_subscriptions = auto()
    contacts_lists_subscription = auto()

class PropertyNames(StrEnum):
    emails = auto()
    companies = auto()
    deals = auto()
    engagements = auto()
    tickets = auto()
    calls = auto()
    notes = auto()
    tasks = auto()
    meetings = auto()
    contacts = auto()
    forms = auto()
    line_items = auto()
    products = auto()
    workflows = auto()
    goal_targets = auto()
    feedback_submissions = auto()
    email_subscriptions = auto()

class EnforceUrl(str):
    owners = "/crm/v3/owners/"
    forms = "/marketing/v3/forms"
    workflows = "/automation/v3/workflows/"
    email_subscriptions = "/communication-preferences/v3/definitions"
    contact_subscription = "/contacts/v1/lists/all/contacts/all"
    marketing_emails = "/marketing/v3/emails/"
    contact_lists = "/contacts/v1/lists"

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
    PRIMARY_KEY: ClassVar[list[str]] = ["/id"]
    PROPERTY_SEARCH_NAME: ClassVar[str] = None
    ASSOCIATED_ENTITIES: ClassVar[list[str]]
    CACHED_PROPERTIES: ClassVar[Properties]
    IGNORE_PROPERTY_SEARCH: ClassVar[bool] = False
    ENFORCE_URL: ClassVar[str]

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


class V1CRMObject(BaseDocument, extra="allow"):
    # Class-scoped metadata attached to concrete subclasses.
    NAME: ClassVar[str]
    PRIMARY_KEY: ClassVar[list[str]] = ["/id"]
    PROPERTY_SEARCH_NAME: ClassVar[str]
    ASSOCIATED_ENTITIES: ClassVar[list[str]]
    CACHED_PROPERTIES: ClassVar[Properties]
    IGNORE_PROPERTY_SEARCH: ClassVar[bool] = False
    ENFORCE_URL: ClassVar[str]

class V1CustomCRMObject(BaseDocument, extra="allow"):
    # Class-scoped metadata attached to concrete subclasses.
    NAME: ClassVar[str]
    PRIMARY_KEY: ClassVar[list[str]] = ["/id"]
    PROPERTY_SEARCH_NAME: ClassVar[str]
    ASSOCIATED_ENTITIES: ClassVar[list[str]]
    CACHED_PROPERTIES: ClassVar[Properties]
    IGNORE_PROPERTY_SEARCH: ClassVar[bool] = False
    ENFORCE_URL: ClassVar[str]

    offset: int = Field(alias="vid-offset")
    hasMore: bool = Field(alias="has-more")

class V1CustomCRMObject2(BaseDocument, extra="allow"):
    # Class-scoped metadata attached to concrete subclasses.
    NAME: ClassVar[str]
    PRIMARY_KEY: ClassVar[list[str]] = ["/id"]
    PROPERTY_SEARCH_NAME: ClassVar[str]
    ASSOCIATED_ENTITIES: ClassVar[list[str]]
    CACHED_PROPERTIES: ClassVar[Properties]
    IGNORE_PROPERTY_SEARCH: ClassVar[bool] = False
    ENFORCE_URL: ClassVar[str]

    hasMore: bool = Field(alias="has-more")

CustomCRMObject = TypeVar("CustomCRMObject", bound=V1CRMObject)
CustomCRMObject2 = TypeVar("CustomCRMObject2", bound=V1CustomCRMObject2)
CustomCRMObjectV1 = TypeVar("CustomCRMObjectV1", bound=V1CustomCRMObject)

class CustomObject(BaseDocument, extra="allow"):

    NAME: ClassVar[str]
    PRIMARY_KEY: ClassVar[list[str]]
    PROPERTY_SEARCH_NAME: ClassVar[str]
    IGNORE_PROPERTY_SEARCH: ClassVar[bool] = False
    ASSOCIATED_ENTITIES: ClassVar[list[str]] = []

    createdAt: AwareDatetime
    updatedAt: AwareDatetime
    results: list[int] = []

class Company(BaseCRMObject):
    NAME = Names.companies
    PROPERTY_SEARCH_NAME = PropertyNames.companies
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals]

    contacts: list[int] = []
    deals: list[int] = []


class Contact(BaseCRMObject):
    NAME = Names.contacts
    PROPERTY_SEARCH_NAME = PropertyNames.contacts
    ASSOCIATED_ENTITIES = [Names.companies]

    companies: list[int] = []


class Deal(BaseCRMObject):
    NAME = Names.deals
    PROPERTY_SEARCH_NAME = PropertyNames.deals
    ASSOCIATED_ENTITIES = [Names.contacts, Names.engagements, Names.line_items]

    contacts: list[int] = []
    engagements: list[int] = []
    line_items: list[int] = []


class Engagement(BaseCRMObject):
    NAME = Names.engagements
    PROPERTY_SEARCH_NAME = PropertyNames.engagements
    ASSOCIATED_ENTITIES = [Names.deals]

    deals: list[int] = []


class Ticket(BaseCRMObject):
    NAME = Names.tickets
    PROPERTY_SEARCH_NAME = PropertyNames.tickets
    ASSOCIATED_ENTITIES = [Names.contacts, Names.engagements, Names.line_items]

    contacts: list[int] = []
    engagements: list[int] = []
    line_items: list[int] = []
    
class ContactLists(V1CRMObject):
    NAME = Names.contact_lists
    PRIMARY_KEY = ["/listId"]
    ASSOCIATED_ENTITIES = []
    IGNORE_PROPERTY_SEARCH = True
    ENFORCE_URL = EnforceUrl.contact_lists

    listId: str
    lists: list[int] = []

class ContactSubscription(V1CRMObject):
    NAME = Names.contacts_lists_subscription
    ASSOCIATED_ENTITIES = []
    PRIMARY_KEY = ["/vid"]
    ENFORCE_URL = EnforceUrl.contact_subscription
    IGNORE_PROPERTY_SEARCH = True

    vid: str
    contacts: list[int] = []

class EngagementEmails(BaseCRMObject):
    NAME = Names.engagements_emails
    PROPERTY_SEARCH_NAME = PropertyNames.emails
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals, Names.companies, Names.tickets]

    contacts: list[int] = []
    deals: list[int] = []
    companies: list[int] = []
    tickets: list[int] = []
    results: list[int] = []

class EngagementCalls(BaseCRMObject):
    NAME = Names.engagements_calls
    PROPERTY_SEARCH_NAME = PropertyNames.calls
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals, Names.companies, Names.tickets]

    contacts: list[int] = []
    deals: list[int] = []
    companies: list[int] = []
    tickets: list[int] = []
    results: list[int] = []

class EngagementMeetings(BaseCRMObject):
    NAME = Names.engagements_meetings
    PROPERTY_SEARCH_NAME = PropertyNames.meetings
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals, Names.companies, Names.tickets]

    contacts: list[int] = []
    deals: list[int] = []
    companies: list[int] = []
    tickets: list[int] = []
    results: list[int] = []

class EngagementNotes(BaseCRMObject):
    NAME = Names.engagements_notes
    PROPERTY_SEARCH_NAME = PropertyNames.notes
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals, Names.companies, Names.tickets]

    contacts: list[int] = []
    deals: list[int] = []
    companies: list[int] = []
    tickets: list[int] = []
    results: list[int] = []

class EngagementTasks(BaseCRMObject):
    NAME = Names.engagements_tasks
    PROPERTY_SEARCH_NAME = PropertyNames.tasks
    ASSOCIATED_ENTITIES = [Names.contacts, Names.deals, Names.companies, Names.tickets]
    
    contacts: list[int] = []
    deals: list[int] = []
    companies: list[int] = []
    tickets: list[int] = []
    results: list[int] = []

class TicketPipelines(BaseCRMObject):
    NAME = Names.ticket_pipelines
    PRIMARY_KEY = ['/pipelineId']
    ASSOCIATED_ENTITIES = []

    pipelineId: str
    results: list[int] = []

class MarketingEmails(V1CRMObject):
    NAME = Names.marketing_emails
    ENFORCE_URL = EnforceUrl.marketing_emails
    IGNORE_PROPERTY_SEARCH = True
    ASSOCIATED_ENTITIES = []

    id: int
    updatedAt: AwareDatetime
    results: list[  int] = []

class SubscriptionChanges(V1CRMObject):
    NAME = Names.subscription_changes
    PRIMARY_KEY = ['/portalId']
    ASSOCIATED_ENTITIES = []

    portalId: str
    timeline: list[int] = []

class Campaigns(V1CRMObject):
    NAME = Names.campaigns
    ASSOCIATED_ENTITIES = []

    id: str
    timeline: list[int] = []

class DealPipelines(V1CRMObject):
    NAME = Names.deal_pipelines
    PRIMARY_KEY = ['/pipelineId']
    ASSOCIATED_ENTITIES = []

    pipelineId: str
    results: list[int] = []

class EmailEvents(V1CRMObject):
    NAME = Names.email_events
    ASSOCIATED_ENTITIES = []

    id: str
    events: list[int] = []

class MarketingForms(V1CRMObject):
    NAME = Names.marketing_forms
    ENFORCE_URL = EnforceUrl.forms
    PROPERTY_SEARCH_NAME = PropertyNames.forms
    IGNORE_PROPERTY_SEARCH = True
    ASSOCIATED_ENTITIES = []

    id: int
    results: list[int] = []

class Owners(V1CRMObject):
    NAME = Names.owners
    ENFORCE_URL = EnforceUrl.owners
    IGNORE_PROPERTY_SEARCH = True
    ASSOCIATED_ENTITIES = []

    id: int
    updatedAt: AwareDatetime
    results: list[int] = []

class LineItems(BaseCRMObject):
    NAME = Names.line_items
    IGNORE_PROPERTY_SEARCH = True
    PROPERTY_SEARCH_NAME = PropertyNames.line_items
    ASSOCIATED_ENTITIES = []

    results: list[int] = []

class Products(BaseCRMObject):
    NAME = Names.products
    IGNORE_PROPERTY_SEARCH = True
    PROPERTY_SEARCH_NAME = PropertyNames.products
    ASSOCIATED_ENTITIES = []

    results: list[int] = []

class Workflows(V1CRMObject):
    NAME = Names.workflows
    IGNORE_PROPERTY_SEARCH = True
    ENFORCE_URL = EnforceUrl.workflows
    ASSOCIATED_ENTITIES = []
    PROPERTY_SEARCH_NAME = PropertyNames.workflows

    id: int
    updatedAt: AwareDatetime
    workflows: list[int] = []

class Goals(BaseCRMObject):
    NAME = Names.goal_targets
    ASSOCIATED_ENTITIES = []
    PROPERTY_SEARCH_NAME = PropertyNames.goal_targets

    results: list[int] = []

class FeedbackSubmissions(BaseCRMObject):
    NAME = Names.feedback_submissions
    ASSOCIATED_ENTITIES = [Names.contacts]
    PROPERTY_SEARCH_NAME = PropertyNames.feedback_submissions

    results: list[int] = []

class EmailSubscriptions(V1CRMObject):
    NAME = Names.email_subscriptions
    PRIMARY_KEY = ['/id']
    ASSOCIATED_ENTITIES = []
    ENFORCE_URL = EnforceUrl.email_subscriptions
    IGNORE_PROPERTY_SEARCH = True

    id: str
    updatedAt: AwareDatetime
    createdAt: AwareDatetime
    subscriptionDefinitions: list[int] = []

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

class WorkflowResult(BaseModel, Generic[Item], extra="forbid"):
    workflows: list[Item]

class SubscriptionResult(BaseModel, Generic[Item], extra="forbid"):
    subscriptionDefinitions: list[Item]


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
    class Item(BaseModel):
        id: int
        updatedAt: datetime

    results: list[Item]
    hasMore: bool
    offset: str

class OldRecentCampaigns(BaseModel):
    
    class Item(BaseModel):

        id: int
        lastUpdatedTime: int

    campaigns : list[Item]
    hasMore: bool
    offset: str



################################# v3




class OldRecentMarketingEmails(BaseModel):

    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime
    
    results: list[Item]


class OldRecentEngagementsEmails(BaseModel):
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentEngagementsTasks(BaseModel):
    
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentEngagementsNotes(BaseModel):
    
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentEngagementsCalls(BaseModel):
    
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentEngagementsMeetings(BaseModel):
    
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentMarketingForms(BaseModel):

    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime

    results: list[Item]

class OldRecentOwners(BaseModel):

    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime

    results: list[Item]
    
class OldRecentLineItems(BaseModel):
    
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentProducts(BaseModel):
    
    class Item(BaseModel):
        id: int
        createdAt: AwareDatetime

    results: list[Item]

class OldRecentWorkflows(BaseModel):

    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime

    workflows: list[Item]

class OldRecentGoals(BaseModel):
    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime

    results: list[Item]

class OldRecentFeedbackSubmissions(BaseModel):
    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime

    results: list[Item]

class OldRecentEmailSubscriptions(BaseModel):

    class Item(BaseModel):
        id: int
        updatedAt: AwareDatetime

    subscriptionDefinitions: list[Item]
