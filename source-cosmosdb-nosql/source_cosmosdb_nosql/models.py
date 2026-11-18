import re
from datetime import timedelta
from typing import Annotated, ClassVar, Literal, Self

from estuary_cdk.capture.common import (
    BaseDocument,
    ConnectorState as GenericConnectorState,
    ResourceConfig as CommonResourceConfig,
    ResourceState,
)
from pydantic import AwareDatetime, BaseModel, Field, field_validator, model_validator


class AccountKeyCredentials(BaseModel):
    credentials_title: Literal["Account Key"] = Field(
        default="Account Key", json_schema_extra={"type": "string", "order": 0},
    )
    key: str = Field(
        title="Account Key",
        description="Your Cosmos DB account key. A read-only key is sufficient.",
        min_length=1, repr=False, json_schema_extra={"secret": True},
    )


class ServicePrincipalCredentials(BaseModel):
    credentials_title: Literal["Service Principal"] = Field(
        default="Service Principal", json_schema_extra={"type": "string", "order": 0},
    )
    tenant_id: str = Field(
        title="Tenant ID", description="Your Microsoft Entra tenant ID.", min_length=1,
    )
    client_id: str = Field(
        title="Client ID", description="Your application's client ID.", min_length=1,
    )
    client_secret: str = Field(
        title="Client Secret", description="Your application's client secret.",
        min_length=1, repr=False, json_schema_extra={"secret": True},
    )


class Advanced(BaseModel):
    disable_endpoint_discovery: bool = Field(
        default=False,
        title="Disable Endpoint Discovery",
        description="Connect only to the configured endpoint, for example when using the emulator or a private endpoint.",
    )
    priority: Literal["High", "Low"] | None = Field(
        default=None, title="Request Priority",
        description="Priority for change feed and partition key sampling requests. Low priority requests are throttled first. Enable priority-based execution on your account before setting this.",
    )
    throughput_bucket: int | None = Field(
        default=None, title="Throughput Bucket", ge=1, le=5,
        description="Throughput bucket ID (1–5) assigned to connector requests. Configure the bucket's throughput allocation on your Cosmos DB account first.",
    )
    max_item_count: int | None = Field(
        default=None, title="Page Size Hint", gt=0,
        description="Maximum number of items requested per change feed page. The service may return fewer items. Leave unset to use the SDK default.",
    )
    start_time: Literal["Beginning", "Now"] | AwareDatetime = Field(
        default="Beginning", title="Start Position",
        description="Where a new capture starts: Beginning, Now, or an RFC 3339 timestamp with a timezone. Saved continuations take precedence; changing this does not reset an existing capture.",
    )

    @field_validator("start_time", mode="before")
    @classmethod
    def validate_start_time(cls, value):
        if isinstance(value, (int, float)) or (
            isinstance(value, str) and value not in ("Beginning", "Now")
            and not re.fullmatch(r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})", value)
        ):
            raise ValueError("Start position must be Beginning, Now, or an RFC 3339 timestamp.")
        return value


class EndpointConfig(BaseModel):
    endpoint: str = Field(
        title="Endpoint",
        description="Your Cosmos DB account URI, for example https://myaccount.documents.azure.com:443/.",
        min_length=1,
        json_schema_extra={"order": 0},
    )
    credentials: AccountKeyCredentials | ServicePrincipalCredentials = Field(
        discriminator="credentials_title", title="Authentication",
        description="An account key, or Microsoft Entra authentication with a service principal. Assign Cosmos DB Built-in Data Reader to the service principal for the databases you capture.",
        json_schema_extra={"order": 1},
    )
    database: str = Field(
        default="",
        title="Database",
        description="A single Cosmos DB database to capture. Choose this, Databases, or All Databases.",
        json_schema_extra={"order": 2},
    )
    databases: list[Annotated[str, Field(min_length=1)]] = Field(
        default_factory=list, title="Databases",
        description="The Cosmos DB databases to capture together. Bindings include the database name. Choose this, Database, or All Databases.",
        json_schema_extra={"order": 3, "uniqueItems": True},
    )
    all_databases: bool = Field(
        default=False, title="All Databases",
        description="Discover containers across all databases in your account. Requires permission to list databases. Choose this, Database, or Databases.",
        json_schema_extra={"order": 4},
    )
    advanced: Advanced = Field(
        default_factory=Advanced,
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @model_validator(mode="after")
    def validate_selections(self) -> Self:
        if sum((bool(self.database), bool(self.databases), self.all_databases)) != 1:
            raise ValueError("Configure exactly one of database, databases, or all_databases.")
        if len(self.databases) != len(set(self.databases)):
            raise ValueError("Databases must not contain duplicate names.")
        return self


class ResourceConfig(CommonResourceConfig):
    PATH_POINTERS: ClassVar[list[str]] = ["/database", "/container"]

    database: str | None = Field(
        default=None, title="Database", min_length=1,
        description="The container's database. Required when capturing multiple or all databases; omit for the single Database setting.",
    )

    name: str = Field(
        alias="container",
        title="Container",
        description="The Cosmos DB container to capture.",
        min_length=1,
    )

    interval: timedelta = Field(
        default=timedelta(minutes=1),
        title="Polling Interval",
        description="Interval between change feed polls for this container.",
        gt=timedelta(0),
        json_schema_extra={"nonsensitive": True},
    )

    def path(self) -> list[str]:
        return [self.database, self.name] if self.database else [self.name]


ConnectorState = GenericConnectorState[ResourceState]


class Document(BaseDocument, extra="allow"):
    class Meta(BaseDocument.Meta):
        partitionKeyState: str | None = Field(
            default=None,
            description="For each partition key path, in order: value, null, or undefined, joined by commas.",
        )

    meta_: Meta = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default_factory=lambda: Document.Meta(op="u"),
        alias="_meta",
        description="Document metadata",
    )
    id: str


class PartitionKey(BaseModel):
    paths: list[str]


class Container(BaseModel):
    id: str
    partitionKey: PartitionKey


class Database(BaseModel):
    id: str


class ChangeFeedContinuation(BaseModel, extra="allow"):
    class Beginning(BaseModel, extra="allow"):
        Type: Literal["Beginning"]

    class PointInTime(BaseModel, extra="allow"):
        Type: Literal["PointInTime"]
        PointInTimeMs: int | float

    class FeedRanges(BaseModel, extra="allow"):
        class FeedRange(BaseModel, extra="allow"):
            token: str | None

        continuation: list[FeedRange]

    v: Literal["v2"]
    startFrom: Beginning | PointInTime = Field(discriminator="Type")
    continuation: FeedRanges
