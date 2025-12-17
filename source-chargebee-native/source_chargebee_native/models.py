from datetime import datetime, UTC, timedelta
from typing import Annotated, Generic, TypeVar, Literal, List, ClassVar
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    model_validator,
    field_validator,
)
import re

from estuary_cdk.flow import BasicAuth
from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
    ConnectorState as GenericConnectorState,
)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


class ApiKey(BasicAuth):
    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string"},
    )
    username: str = Field(
        title="API Key",
        json_schema_extra={"secret": True},
        alias="api_key",
    )
    password: str = Field(
        default="",
        title="Password (Not Required)",
        description="This field is always blank for Chargebee authentication.",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    site: str = Field(
        description="The site prefix of your Chargebee account.",
        title="Site",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data before this date will not be replicated. If left blank, defaults to 30 days before current time.",
        title="Start Date",
        default_factory=default_start_date,
        le=datetime.now(tz=UTC),
    )
    product_catalog: Literal["1.0", "2.0"] = Field(
        description="The product catalog version to use.",
        title="Product Catalog",
        default="1.0",
    )

    class Advanced(BaseModel, extra="forbid"):
        limit: Annotated[
            int,
            Field(
                description="Limit used in queries to Chargebee API. This should be left as the default value unless connector errors indicate a smaller limit is required.",
                title="Limit",
                default=100,
                gt=0,
                le=100,
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )


ConnectorState = GenericConnectorState[ResourceState]


ListItem = TypeVar("ListItem", bound=BaseModel)


class ChargebeeConfiguration(BaseModel, extra="allow"):
    domain: str
    product_catalog_version: Literal["v1", "v2"]

    @property
    def product_catalog(self) -> Literal["1.0", "2.0"]:
        version_map: dict[Literal["v1", "v2"], Literal["1.0", "2.0"]] = {"v1": "1.0", "v2": "2.0"}
        return version_map[self.product_catalog_version]


class APIResponse(BaseModel, Generic[ListItem], extra="allow"):
    next_offset: str | None = None
    list: List[ListItem] | None = None
    configurations: List[ChargebeeConfiguration] | None = None

    @model_validator(mode="before")
    @classmethod
    def handle_single_object(cls, data: dict) -> dict:
        if isinstance(data, dict):
            if "configurations" in data:
                return data
            elif "list" not in data:
                data = {"list": [data], "next_offset": None}
        return data


class ChargebeeResource(BaseDocument, extra="allow"):
    pass


class IncrementalChargebeeResource(ChargebeeResource):
    RESOURCE_KEY: ClassVar[str]
    CURSOR_FIELD: ClassVar[str] = "updated_at"
    ID_FIELD: ClassVar[str] = "id"
    DELETED_FIELD: ClassVar[str] = "deleted"

    id: str = Field(exclude=True)
    cursor_value: int = Field(exclude=True)
    deleted: bool = Field(exclude=True)

    @classmethod
    def get_resource_key_json_path(cls) -> str:
        return f"/{cls.RESOURCE_KEY}/id"

    @staticmethod
    def validate_required_class_attrs(class_obj, attrs: List[str]) -> None:
        for attr in attrs:
            if not hasattr(class_obj, attr) or getattr(class_obj, attr) == "":
                raise TypeError(f"Class {class_obj.__name__} must define class attribute '{attr}'")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        IncrementalChargebeeResource.validate_required_class_attrs(
            cls,
            [
                "RESOURCE_KEY",
                "CURSOR_FIELD",
                "ID_FIELD",
                "DELETED_FIELD",
            ],
        )

    @model_validator(mode="before")
    @classmethod
    def set_id_and_updated_at(cls, data):
        if (
            isinstance(data, dict)
            and cls.RESOURCE_KEY in data
            and isinstance(data[cls.RESOURCE_KEY], dict)
        ):
            data["id"] = data[cls.RESOURCE_KEY][cls.ID_FIELD]
            data["cursor_value"] = data[cls.RESOURCE_KEY][cls.CURSOR_FIELD]
            data["deleted"] = data[cls.RESOURCE_KEY].get(cls.DELETED_FIELD, False)
        return data


class AssociationConfig(BaseModel):
    parent_resource: str
    parent_response_key: str
    parent_key_field: str
    endpoint_pattern: str  # e.g. "{parent}/{id}/retrieve_with_scheduled_changes"
    parent_filter_params: dict[str, str] | None = None
    returns_list: bool

    @field_validator("endpoint_pattern")
    @classmethod
    def validate_endpoint_pattern(cls, v: str) -> str:
        required_placeholders = {"parent", "id"}
        found_placeholders = set(re.findall(r"\{([^}]+)\}", v))

        if found_placeholders != required_placeholders:
            raise ValueError(
                f"Endpoint pattern must contain exactly {{parent}} and {{id}} placeholders. "
                f"Found: {found_placeholders}"
            )
        return v


class Customer(IncrementalChargebeeResource):
    RESOURCE_KEY = "customer"

    class CustomerData(BaseModel, extra="allow"):
        id: str

    customer: CustomerData


class Comment(IncrementalChargebeeResource):
    RESOURCE_KEY = "comment"
    CURSOR_FIELD = "created_at"

    class CommentData(BaseModel, extra="allow"):
        id: str

    comment: CommentData


class Subscription(IncrementalChargebeeResource):
    RESOURCE_KEY = "subscription"

    class SubscriptionData(BaseModel, extra="allow"):
        id: str

    subscription: SubscriptionData


class Addon(IncrementalChargebeeResource):
    RESOURCE_KEY = "addon"

    class AddonData(BaseModel, extra="allow"):
        id: str

    addon: AddonData


class AttachedItem(IncrementalChargebeeResource):
    RESOURCE_KEY = "attached_item"

    class AttachedItemData(BaseModel, extra="allow"):
        id: str

    attached_item: AttachedItemData


class Coupon(IncrementalChargebeeResource):
    RESOURCE_KEY = "coupon"

    class CouponData(BaseModel, extra="allow"):
        id: str

    coupon: CouponData


class CreditNote(IncrementalChargebeeResource):
    RESOURCE_KEY = "credit_note"

    class CreditNoteData(BaseModel, extra="allow"):
        id: str

    credit_note: CreditNoteData


class Event(IncrementalChargebeeResource):
    RESOURCE_KEY = "event"
    CURSOR_FIELD = "occurred_at"

    class EventData(BaseModel, extra="allow"):
        id: str

    event: EventData


class HostedPage(IncrementalChargebeeResource):
    RESOURCE_KEY = "hosted_page"

    class HostedPageData(BaseModel, extra="allow"):
        id: str

    hosted_page: HostedPageData


class Invoice(IncrementalChargebeeResource):
    RESOURCE_KEY = "invoice"

    class InvoiceData(BaseModel, extra="allow"):
        id: str

    invoice: InvoiceData


class Item(IncrementalChargebeeResource):
    RESOURCE_KEY = "item"

    class ItemData(BaseModel, extra="allow"):
        id: str

    item: ItemData


class ItemPrice(IncrementalChargebeeResource):
    RESOURCE_KEY = "item_price"

    class ItemPriceData(BaseModel, extra="allow"):
        id: str

    item_price: ItemPriceData


class ItemFamily(IncrementalChargebeeResource):
    RESOURCE_KEY = "item_family"

    class ItemFamilyData(BaseModel, extra="allow"):
        id: str

    item_family: ItemFamilyData


class Order(IncrementalChargebeeResource):
    RESOURCE_KEY = "order"

    class OrderData(BaseModel, extra="allow"):
        id: str

    order: OrderData


class PaymentSource(IncrementalChargebeeResource):
    RESOURCE_KEY = "payment_source"

    class PaymentSourceData(BaseModel, extra="allow"):
        id: str

    payment_source: PaymentSourceData


class Plan(IncrementalChargebeeResource):
    RESOURCE_KEY = "plan"

    class PlanData(BaseModel, extra="allow"):
        id: str

    plan: PlanData


class PromotionalCredits(IncrementalChargebeeResource):
    RESOURCE_KEY = "promotional_credit"
    CURSOR_FIELD = "created_at"

    class PromotionalCreditData(BaseModel, extra="allow"):
        id: str

    promotional_credit: PromotionalCreditData


class Quote(IncrementalChargebeeResource):
    RESOURCE_KEY = "quote"

    class QuoteData(BaseModel, extra="allow"):
        id: str

    quote: QuoteData


class Transaction(IncrementalChargebeeResource):
    RESOURCE_KEY = "transaction"

    class TransactionData(BaseModel, extra="allow"):
        id: str

    transaction: TransactionData


class VirtualBankAccount(IncrementalChargebeeResource):
    RESOURCE_KEY = "virtual_bank_account"

    class VirtualBankAccountData(BaseModel, extra="allow"):
        id: str

    virtual_bank_account: VirtualBankAccountData
