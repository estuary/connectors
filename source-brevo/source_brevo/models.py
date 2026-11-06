from typing import ClassVar, Literal

from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.capture.common import (
    ResourceState,
)
from estuary_cdk.capture.document import BaseDocument
from estuary_cdk.flow import AccessToken
from pydantic import AwareDatetime, BaseModel, Field


class ApiKey(AccessToken):
    credentials_title: Literal["API Key"] = Field(
        default="API Key",
        json_schema_extra={"type": "string", "order": 0},
    )
    access_token: str = Field(
        title="API Key",
        json_schema_extra={"secret": True, "order": 1},
    )


class EndpointConfig(BaseModel):
    credentials: ApiKey = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class BrevoResource(BaseDocument, extra="allow"):
    """Base for every captured Brevo document.

    `PATH` is the collection endpoint relative to the API root and `ITEMS_KEY`
    the response envelope key holding the array — Brevo names that key after the
    entity rather than using a generic `data` wrapper, so it differs per
    endpoint. `PAGE_SIZE` is the endpoint's documented maximum `limit`, or None
    for the endpoints that accept no pagination parameters at all."""

    NAME: ClassVar[str]
    PATH: ClassVar[str]
    ITEMS_KEY: ClassVar[str]
    PAGE_SIZE: ClassVar[int | None] = None


class ContactAttribute(BrevoResource):
    NAME: ClassVar[str] = "contacts_attributes"
    PATH: ClassVar[str] = "contacts/attributes"
    ITEMS_KEY: ClassVar[str] = "attributes"


class ContactList(BrevoResource):
    NAME: ClassVar[str] = "contacts_lists"
    PATH: ClassVar[str] = "contacts/lists"
    ITEMS_KEY: ClassVar[str] = "lists"
    PAGE_SIZE: ClassVar[int | None] = 50


class ContactFolder(BrevoResource):
    NAME: ClassVar[str] = "contacts_folders"
    PATH: ClassVar[str] = "contacts/folders"
    ITEMS_KEY: ClassVar[str] = "folders"
    PAGE_SIZE: ClassVar[int | None] = 50


class ContactSegment(BrevoResource):
    NAME: ClassVar[str] = "contacts_segments"
    PATH: ClassVar[str] = "contacts/segments"
    ITEMS_KEY: ClassVar[str] = "segments"
    PAGE_SIZE: ClassVar[int | None] = 50


class Sender(BrevoResource):
    NAME: ClassVar[str] = "senders"
    PATH: ClassVar[str] = "senders"
    ITEMS_KEY: ClassVar[str] = "senders"


class Webhook(BrevoResource):
    NAME: ClassVar[str] = "webhooks"
    PATH: ClassVar[str] = "webhooks"
    ITEMS_KEY: ClassVar[str] = "webhooks"


class Contact(BrevoResource):
    NAME: ClassVar[str] = "contacts"
    PATH: ClassVar[str] = "contacts"
    ITEMS_KEY: ClassVar[str] = "contacts"
    PAGE_SIZE: ClassVar[int | None] = 1000
    KEY: ClassVar[list[str]] = ["/id"]

    id: int
    modifiedAt: AwareDatetime


SNAPSHOT_RESOURCES: list[type[BrevoResource]] = [
    ContactAttribute,
    ContactList,
    ContactFolder,
    ContactSegment,
    Sender,
    Webhook,
]
