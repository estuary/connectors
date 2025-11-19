from __future__ import annotations
import urllib.parse
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Generic,
    Literal,
    Self,
    TypeVar,
)

from estuary_cdk.capture.common import (
    AccessToken,
    BaseDocument,
    BaseOAuth2Credentials,
    CRON_REGEX,
    OAuth2Spec,
    ReductionStrategy,
    ResourceConfigWithSchedule,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from pydantic import AwareDatetime, BaseModel, Field, model_validator, ValidationInfo

OBJECT_TYPE_IDS = {"contact": "0-1", "contact_list": "0-45"}
ContactTypeId = Literal["0-1"]

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
    "forms",
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


class HubspotResourceConfigWithSchedule(ResourceConfigWithSchedule):
    schedule: str = Field(
        default="",
        title="Calculated Property Refresh Schedule",
        description="Schedule to automatically refresh calculated properties. Accepts a cron expression.",
        pattern=CRON_REGEX
    )


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
    useLegacyNamingForCustomObjects: bool = Field(
        title="Use Legacy Naming for Custom Objects",
        description="If selected, the legacy naming convention for custom objects is used. Otherwise, all discovered bindings for custom objects will have 'custom_' prepended to their names.",
        default=False,
        json_schema_extra={"x-hidden-field": True},
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
    forms = auto()
    form_submissions = auto()
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
    marketing_emails = auto()
    contact_lists = auto()
    contact_list_memberships = auto()
    feedback_submissions = auto()
    goals = auto()
    workflows = auto()


# A Property is a HubSpot or HubSpot-user defined attribute that's
# attached to a HubSpot CRM object.
class Property(BaseDocument, extra="allow"):
    name: str = ""
    calculated: bool = False
    hubspotObject: str = "unknown"  # Added by us.


class Properties(BaseDocument, extra="allow"):
    results: list[Property]


class DealPipeline(BaseDocument, extra="allow"):
    createdAt: AwareDatetime | None
    updatedAt: AwareDatetime | None


class DealPipelines(BaseDocument, extra="allow"):
    results: list[DealPipeline]


class Owner(BaseDocument, extra="allow"):
    createdAt: AwareDatetime | None
    updatedAt: AwareDatetime | None


class Form(BaseDocument, extra="allow"):
    id: str
    createdAt: AwareDatetime | None
    updatedAt: AwareDatetime | None


class FormSubmissionContext:
    def __init__(self, form_id: str):
        self.form_id = form_id


class FormSubmission(BaseDocument, extra="allow"):
    submittedAt: int
    formId: str

    @model_validator(mode="before")
    def set_form_id(cls, values: dict[str, Any], info: ValidationInfo):
        if not info.context or not isinstance(info.context, FormSubmissionContext):
            raise RuntimeError(
                f"Validation context is not set or is not of type FormSubmissionContext: {info.context}"
            )

        assert "formId" not in values
        values["formId"] = info.context.form_id
        return values


class MarketingEmail(BaseDocument, extra="allow"):
    id: str
    updatedAt: AwareDatetime


# Base Struct for all CRM Objects within HubSpot.
class BaseCRMObject(BaseDocument, extra="allow"):
    ASSOCIATED_ENTITIES: ClassVar[list[str]]

    class History(BaseModel, extra="allow"):
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

    # During calculated properties refreshes, partial documents containing only calculated properties
    # are emitted. In order to merge the properties and histories of these partial documents into the
    # previously captured data, merge reduction strategies need specified for the `properties` and
    # `propertiesWithHistory` fields.
    properties: Annotated[
        dict[str, str | None],
        Field(json_schema_extra={"reduce": {"strategy": ReductionStrategy.MERGE}}),
    ]
    propertiesWithHistory: Annotated[
        dict[str, list[History]] | None,
        # The merge reduction strategy is only allowed for objects and arrays. Since propertiesWithHistory can be null,
        # we use a conditional to only apply the reduce annotation when the field is an object.
        Field(json_schema_extra={"if": {"type": "object"}, "then": {"reduce": {"strategy": ReductionStrategy.MERGE}}}),
    ] = None

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
        #
        # Only remove properties that are empty strings. Hubspot sometimes returns empty strings 
        # for formmatted string fields, like a date, number, or email. An empty string
        # causes the inferred format to get dropped from the collection's inferred schema.
        # We want to maintain those formats. Omitting properties that are empty strings
        # is a little broad - we'd ideally only do so for string fields that have an expected
        # format - but that more precise removal requires a larger effort than I'd like to
        # take on right now.
        self.properties = {k: v for k, v in self.properties.items() if v != ""}
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


class FeedbackSubmission(BaseCRMObject):
    ASSOCIATED_ENTITIES = []


class Goals(BaseCRMObject):
    ASSOCIATED_ENTITIES = []


# An Association, as returned by the v4 associations API.
class Association(BaseModel, extra="allow"):
    class Type(BaseModel, extra="allow"):
        category: Literal["HUBSPOT_DEFINED", "USER_DEFINED"]
        # Type IDs are defined here: https://developers.hubspot.com/docs/api/crm/associations
        typeId: int
        # Ex "Child Company", "Parent Company".
        # These seem potentially useful, but they're not available when doing
        # paged reads of CRM entities, so they're omitted in output so that
        # paged vs batched reads of CRM records can have a consistent shape.
        label: str | None

    class From(BaseModel, extra="allow"):
        id: int

    class To(BaseModel, extra="allow"):
        toObjectId: int
        associationTypes: list["Association.Type"]

    from_: From = Field(alias="from")
    to: list[To]

    # TODO(johnny): This can potentially have a `pager`, but I'm leaving it out
    # to see if it's ever encountered. If it is, I think we handle by ignoring
    # it and having a position of "we'll get (only) the first page".
    # TODO(whb): We did encounter this, and for now have decided to ignore any
    # extra pages.
    class Cursor(BaseModel, extra="allow"):
        after: str
        link: str

    class Paging(BaseModel, extra="allow"):
        next: "Association.Cursor"

    paging: Paging | None = None


# An unconstrained Item type.
Item = TypeVar("Item")


# Common shape of a v3 API paged listing for a GET request to the objects endpoint for a particular
# object.
class PageResult(BaseModel, Generic[Item], extra="allow"):
    class Cursor(BaseModel, extra="allow"):
        after: str
        link: str

    class Paging(BaseModel, extra="allow"):
        next: "PageResult.Cursor"

    results: list[Item]
    paging: Paging | None = None


# Common shape of a v3 search API listing, which is the same as PageResult but includes a field for
# the total number of records returned, and doesn't have a "link" in the paging.next object.
class SearchPageResult(BaseModel, Generic[Item], extra="allow"):
    class Cursor(BaseModel, extra="allow"):
        after: str

    class Paging(BaseModel, extra="allow"):
        next: "SearchPageResult.Cursor"

    results: list[Item]
    paging: Paging | None = None
    total: int


# Common shape of a v3 API batch read.
class BatchResult(BaseModel, Generic[Item], extra="allow"):
    class Error(BaseModel, extra="allow"):
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


class EmailEventsResponse(BaseModel, extra="allow"):
    hasMore: bool
    offset: str
    events: list[EmailEvent]


class Workflow(BaseDocument, extra="allow"):
    id: int
    updatedAt: AwareDatetime
    insertedAt: AwareDatetime


class WorkflowsResponse(BaseModel, extra="allow"):
    workflows: list[Workflow]


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


class List(BaseDocument, extra="allow"):
    objectTypeId: str


class ContactList(BaseDocument, extra="allow"):
    listId: int
    objectTypeId: ContactTypeId
    updatedAt: AwareDatetime
    additionalProperties: dict[str, Any]

    def get_cursor_value(self) -> datetime:
        if "hs_lastmodifieddate" not in self.additionalProperties:
            return self.updatedAt

        raw_last_modified_timestamp = self.additionalProperties["hs_lastmodifieddate"]
        last_modified_timestamp = int(raw_last_modified_timestamp) / 1000

        return datetime.fromtimestamp(last_modified_timestamp, tz=UTC)


class ListSearch(BaseModel, extra="allow"):
    lists: list[Annotated[ContactList | List, Field(union_mode="left_to_right")]]

    hasMore: bool
    offset: int


class ContactListMembershipResponse(BaseModel, extra="allow"):
    class Cursor(BaseModel, extra="allow"):
        after: str

    class Paging(BaseModel, extra="allow"):
        next: ContactListMembershipResponse.Cursor | None = None

    results: list[ContactListMembership]
    paging: Paging | None = None

    @model_validator(mode="before")
    @classmethod
    def _set_membership_list_ids(cls, values: dict[str, Any], info: ValidationInfo):
        if not info.context or not isinstance(info.context, ContactList):
            raise RuntimeError(
                f"Validation context is not set or is not of type ContactList: {info.context}"
            )

        for membership in values["results"]:
            membership["listId"] = info.context.listId

        return values

    def get_next_cursor(self) -> str | None:
        if self.paging is None or self.paging.next is None:
            return None

        return self.paging.next.after


class ContactListMembership(BaseDocument, extra="allow"):
    # The enriched list membership entity, containing the id of the list that contact belongs to.
    #
    # This corresponds to the schema returned by the "/crm/v3/lists/{listId}/memberships" endpoint
    # (https://developers.hubspot.com/docs/api-reference/crm-lists-v3/memberships/get-crm-v3-lists-listId-memberships).
    #
    # The alternative "/crm/v3/lists/records/{objectTypeId}/{recordId}/memberships" endpoint
    # (https://developers.hubspot.com/docs/api-reference/crm-lists-v3/memberships/get-crm-v3-lists-records-objectTypeId-recordId-memberships)
    # yields more data, but since getting added to a list doesn't update a contact's
    # `lastmodifieddate` there's no way to tell which contacts to request memberships for.
    # List `updatedAt` fields do not get updated either, but we assume there will always be
    # less lists than contacts to query.

    listId: int
    recordId: str
    membershipTimestamp: AwareDatetime
