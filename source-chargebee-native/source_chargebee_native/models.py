from datetime import datetime, UTC, timedelta
from typing import Generic, TypeVar, Literal, List, ClassVar
from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    model_validator,
)

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
    )
    product_catalog: Literal["1.0"] = Field(
        description="The product catalog version to use.",
        title="Product Catalog",
        default="1.0",
    )


ConnectorState = GenericConnectorState[ResourceState]


ListItem = TypeVar("ListItem", bound=BaseModel)


class APIResponse(BaseModel, Generic[ListItem], extra="allow"):
    next_offset: str | None = None
    list: List[ListItem] | None = None


class ChargebeeResource(BaseDocument, extra="allow"):
    pass


class IncrementalChargebeeResource(ChargebeeResource):
    RESOURCE_KEY: ClassVar[str]
    RESOURCE_KEY_JSON_PATH: ClassVar[str]
    CURSOR_FIELD: ClassVar[str] = "updated_at"
    ID_FIELD: ClassVar[str] = "id"
    DELETED_FIELD: ClassVar[str] = "deleted"
    TIMESTAMP_FIELDS: ClassVar[List[str]] = [
        "created_at",
        "updated_at",
        "archived_at",
        "vat_number_validated_time",
        "start_date",
        "trial_start",
        "trial_end",
        "current_term_start",
        "current_term_end",
        "next_billing_at",
        "started_at",
        "archived_at",
        "pause_date",
        "resume_date",
        "cancelled_at",
        "cancel_schedule_created_at",
        "due_since",
        "changes_scheduled_at",
        "invoice_date",
        "date",
        "activated_date",
        "contract_start",
        "contract_end",
        "scheduled_at",
        "actual_delivered_at",
        "claim_expiry_date",
        "gifted_at",
        "cancelled_at",
        "paused_at",
        "resumed_at",
        "scheduled_pause_at",
        "scheduled_resume_at",
        "event_time",
        "due_date",
        # TODO(jsmith): more fields to list
    ]

    id: str = Field(exclude=True)
    updated_at: AwareDatetime = Field(exclude=True)
    deleted: bool = Field(exclude=True)

    @staticmethod
    def validate_required_class_attrs(class_obj, attrs: List[str]) -> None:
        """Validate that the class has the required class attributes defined and non-empty."""
        for attr in attrs:
            if not hasattr(class_obj, attr) or getattr(class_obj, attr) == "":
                raise TypeError(
                    f"Class {class_obj.__name__} must define class attribute '{attr}'"
                )

    @classmethod
    def convert_timestamp_fields(cls, data: dict, field_names: List[str]) -> None:
        if not isinstance(data, dict):
            return

        for field in field_names:
            if field in data and isinstance(data[field], int):
                data[field] = datetime.fromtimestamp(data[field], tz=UTC)

        for key, value in data.items():
            if isinstance(value, dict):
                cls.convert_timestamp_fields(value, field_names)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        cls.convert_timestamp_fields(item, field_names)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        IncrementalChargebeeResource.validate_required_class_attrs(
            cls,
            [
                "RESOURCE_KEY",
                "RESOURCE_KEY_JSON_PATH",
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
            data["updated_at"] = data[cls.RESOURCE_KEY][cls.CURSOR_FIELD]
            data["deleted"] = data[cls.RESOURCE_KEY].get(cls.DELETED_FIELD, False)

            if cls.TIMESTAMP_FIELDS:
                cls.convert_timestamp_fields(data, cls.TIMESTAMP_FIELDS)

        return data


class Customer(IncrementalChargebeeResource):
    RESOURCE_KEY = "customer"
    RESOURCE_KEY_JSON_PATH = "/customer/id"

    class CustomerData(BaseModel, extra="allow"):
        id: str

    customer: CustomerData


class Subscription(IncrementalChargebeeResource):
    RESOURCE_KEY = "subscription"
    RESOURCE_KEY_JSON_PATH = "/subscription/id"

    class SubscriptionData(BaseModel, extra="allow"):
        id: str

    subscription: SubscriptionData


class Addon(IncrementalChargebeeResource):
    RESOURCE_KEY = "addon"
    RESOURCE_KEY_JSON_PATH = "/addon/id"

    class AddonData(BaseModel, extra="allow"):
        id: str

    addon: AddonData


class AttachedItem(IncrementalChargebeeResource):
    RESOURCE_KEY = "attached_item"
    RESOURCE_KEY_JSON_PATH = "/attached_item/id"

    class AttachedItemData(BaseModel, extra="allow"):
        id: str

    attached_item: AttachedItemData


class Coupon(IncrementalChargebeeResource):
    RESOURCE_KEY = "coupon"
    RESOURCE_KEY_JSON_PATH = "/coupon/id"

    class CouponData(BaseModel, extra="allow"):
        id: str

    coupon: CouponData


class CreditNote(IncrementalChargebeeResource):
    RESOURCE_KEY = "credit_note"
    RESOURCE_KEY_JSON_PATH = "/credit_note/id"

    class CreditNoteData(BaseModel, extra="allow"):
        id: str

    credit_note: CreditNoteData


class Event(IncrementalChargebeeResource):
    RESOURCE_KEY = "event"
    RESOURCE_KEY_JSON_PATH = "/event/id"
    CURSOR_FIELD = "occurred_at"

    class EventData(BaseModel, extra="allow"):
        id: str

    event: EventData


class HostedPage(IncrementalChargebeeResource):
    RESOURCE_KEY = "hosted_page"
    RESOURCE_KEY_JSON_PATH = "/hosted_page/id"

    class HostedPageData(BaseModel, extra="allow"):
        id: str

    hosted_page: HostedPageData


class Invoice(IncrementalChargebeeResource):
    RESOURCE_KEY = "invoice"
    RESOURCE_KEY_JSON_PATH = "/invoice/id"

    class InvoiceData(BaseModel, extra="allow"):
        id: str

    invoice: InvoiceData


class Item(IncrementalChargebeeResource):
    RESOURCE_KEY = "item"
    RESOURCE_KEY_JSON_PATH = "/item/id"

    class ItemData(BaseModel, extra="allow"):
        id: str

    item: ItemData


class ItemPrice(IncrementalChargebeeResource):
    RESOURCE_KEY = "item_price"
    RESOURCE_KEY_JSON_PATH = "/item_price/id"

    class ItemPriceData(BaseModel, extra="allow"):
        id: str

    item_price: ItemPriceData


class ItemFamily(IncrementalChargebeeResource):
    RESOURCE_KEY = "item_family"
    RESOURCE_KEY_JSON_PATH = "/item_family/id"

    class ItemFamilyData(BaseModel, extra="allow"):
        id: str

    item_family: ItemFamilyData


class Order(IncrementalChargebeeResource):
    RESOURCE_KEY = "order"
    RESOURCE_KEY_JSON_PATH = "/order/id"

    class OrderData(BaseModel, extra="allow"):
        id: str

    order: OrderData


class PaymentSource(IncrementalChargebeeResource):
    RESOURCE_KEY = "payment_source"
    RESOURCE_KEY_JSON_PATH = "/payment_source/id"

    class PaymentSourceData(BaseModel, extra="allow"):
        id: str

    payment_source: PaymentSourceData


class Plan(IncrementalChargebeeResource):
    RESOURCE_KEY = "plan"
    RESOURCE_KEY_JSON_PATH = "/plan/id"

    class PlanData(BaseModel, extra="allow"):
        id: str

    plan: PlanData


class Quote(IncrementalChargebeeResource):
    RESOURCE_KEY = "quote"
    RESOURCE_KEY_JSON_PATH = "/quote/id"

    class QuoteData(BaseModel, extra="allow"):
        id: str

    quote: QuoteData


class Transaction(IncrementalChargebeeResource):
    RESOURCE_KEY = "transaction"
    RESOURCE_KEY_JSON_PATH = "/transaction/id"

    class TransactionData(BaseModel, extra="allow"):
        id: str

    transaction: TransactionData


class VirtualBankAccount(IncrementalChargebeeResource):
    RESOURCE_KEY = "virtual_bank_account"
    RESOURCE_KEY_JSON_PATH = "/virtual_bank_account/id"

    class VirtualBankAccountData(BaseModel, extra="allow"):
        id: str

    virtual_bank_account: VirtualBankAccountData
