from pydantic import AwareDatetime, BaseModel, Field

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    BaseDocument,
    ResourceConfig,
    ResourceState,
)


class EndpointConfig(BaseModel):
    apiToken: str = Field(
        description="API Token to use for Authentication",
        title="API Token",
        json_schema_extra={"secret": True},
    )


ConnectorState = GenericConnectorState[ResourceState]

class Account(BaseDocument, extra="forbid"):
    
    class ShippingAddress(BaseModel, extra="allow"):
        pass

    class ExternalAccounts(BaseModel, extra="allow"):
        pass

    class Address(BaseModel, extra="allow"):
        pass

    class BillingInfo(BaseModel, extra="allow"):
        pass

    class CustomFields(BaseModel, extra="allow"):
        pass
    

    id: str
    object: str
    state: str
    hosted_login_token: str
    shipping_address: ShippingAddress
    has_live_subscription: bool
    has_active_subscription: bool
    has_future_subscription: bool
    has_canceled_subscription: bool
    has_paused_subscription: bool
    has_past_due_invoice: bool
    created_at: AwareDatetime
    updated_at: AwareDatetime
    deleted_at: AwareDatetime
    code: str
    username: str
    email: str
    override_business_entity_id: str
    preferred_locale: str
    preferred_time_zone: str
    cc_emails: str
    first_name: str
    last_name: str
    company: str
    vat_number: str
    tax_exempt: bool
    exemption_certificate: str
    externalAccounts: ExternalAccounts
    parent_account_id: str
    bill_to: str
    dunning_campaign_id: str
    invoice_template_id: str
    address: Address
    billing_info: BillingInfo
    custom_fields: CustomFields




