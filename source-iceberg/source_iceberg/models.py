from datetime import timedelta
from typing import Annotated, Any, ClassVar, Literal
from urllib.parse import urlsplit

from estuary_cdk.capture.common import BaseDocument, BaseResourceConfig, ResourceState
from estuary_cdk.capture.common import ConnectorState as GenericConnectorState
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema


# Sections without required keys must require auth_type, or a section that omits it
# matches several oneOf branches and fails JSON schema validation.
def require_auth_type(schema: dict[str, Any]) -> None:
    schema["required"] = ["auth_type", *schema.get("required", [])]


class IAMConfig(BaseModel):
    model_config = ConfigDict(title="Use Catalog IAM Role", json_schema_extra=require_auth_type)

    auth_type: Literal["AWSIAM"] = Field(
        default="AWSIAM",
        title="Authentication Type",
        description="Use the catalog's IAM role configured under Credentials.",
    )


class NoAuthConfig(BaseModel):
    model_config = ConfigDict(title="No Authentication", json_schema_extra=require_auth_type)

    auth_type: Literal["None"] = Field(
        default="None",
        title="Authentication Type",
        description="Connect to your catalog without authentication.",
    )


class AWSIAMConfig(IAMConfig):
    model_config = ConfigDict(title="AWS IAM Role", json_schema_extra=require_auth_type)

    auth_type: Literal["AWSIAM"] = Field(
        default="AWSIAM",
        title="Authentication Type",
        description="Sign catalog requests with an IAM role that Estuary assumes.",
    )
    aws_region: str = Field(
        title="AWS Region",
        description="AWS region of your resource.",
        min_length=1,
    )
    aws_role_arn: str = Field(
        title="AWS Role ARN",
        description="AWS role with access to your resource, which Estuary will assume.",
        min_length=1,
    )
    signing_name: str = Field(
        default="glue",
        title="Signing Name",
        description="AWS service to sign requests for: glue for Glue Data Catalog or s3tables for S3 Tables.",
        min_length=1,
    )
    # Estuary supplies these temporary credentials after assuming the configured role.
    aws_access_key_id: SkipJsonSchema[str] = Field(default="", repr=False)
    aws_secret_access_key: SkipJsonSchema[str] = Field(default="", repr=False)
    aws_session_token: SkipJsonSchema[str] = Field(default="", repr=False)


class TokenConfig(BaseModel):
    model_config = ConfigDict(title="Bearer Token", json_schema_extra=require_auth_type)

    auth_type: Literal["Bearer Token"] = Field(
        default="Bearer Token",
        title="Authentication Type",
        description="Authenticate to your catalog with a bearer token.",
    )
    token: str = Field(
        title="Bearer Token",
        description="Bearer token for your catalog.",
        min_length=1,
        repr=False,
        json_schema_extra={"secret": True},
    )


class OAuthConfig(BaseModel):
    model_config = ConfigDict(title="OAuth 2.0 Client Credentials", json_schema_extra=require_auth_type)

    auth_type: Literal["OAuth2"] = Field(
        default="OAuth2",
        title="Authentication Type",
        description="Authenticate to your catalog with OAuth2 client credentials.",
    )
    credential: str = Field(
        title="Client Credential",
        description="OAuth2 client credential for your catalog, in id:secret format.",
        min_length=1,
        repr=False,
        json_schema_extra={"secret": True},
    )
    token: str = Field(
        default="",
        title="Bearer Token",
        description="Optional initial bearer token. Your client credential is used when it expires.",
        repr=False,
        json_schema_extra={"secret": True},
    )
    scope: str = Field(
        default="",
        title="Scope",
        description="OAuth2 authorization scope for your catalog, such as PRINCIPAL_ROLE:your_principal. Leave blank to use catalog.",
    )
    oauth2_server_uri: str = Field(
        default="",
        title="OAuth 2.0 Server URI",
        description="Token endpoint URL or path relative to your catalog URL. Leave blank to use v1/oauth/tokens.",
    )

    @field_validator("oauth2_server_uri")
    @classmethod
    def valid_oauth2_server_uri(cls, value: str) -> str:
        url = urlsplit(value)
        if (url.scheme and (url.scheme not in ("http", "https") or not url.netloc)) or (url.netloc and not url.scheme):
            raise ValueError("OAuth2 server URI must be an HTTP(S) URL or a path relative to the catalog URL")
        return value


class SigV4Config(BaseModel):
    model_config = ConfigDict(title="AWS SigV4 Access Keys", json_schema_extra=require_auth_type)

    auth_type: Literal["AWS SigV4"] = Field(
        default="AWS SigV4",
        title="Authentication Type",
        description="Sign catalog requests with your AWS credentials.",
    )
    aws_region: str = Field(
        default="us-east-1",
        title="AWS Region",
        description="AWS region of your catalog.",
        min_length=1,
    )
    signing_name: str = Field(
        default="glue",
        title="Signing Name",
        description="AWS service to sign requests for: glue for Glue Data Catalog or s3tables for S3 Tables.",
        min_length=1,
    )
    aws_access_key_id: str = Field(
        title="AWS Access Key ID",
        description="Access key ID for your catalog.",
        min_length=1,
    )
    aws_secret_access_key: str = Field(
        title="AWS Secret Access Key",
        description="Secret access key for your catalog.",
        min_length=1,
        repr=False,
        json_schema_extra={"secret": True},
    )
    aws_session_token: str = Field(
        default="",
        title="AWS Session Token",
        description="Session token when you use temporary AWS credentials.",
        repr=False,
        json_schema_extra={"secret": True},
    )


class CatalogStorageConfig(BaseModel):
    model_config = ConfigDict(title="Catalog Storage Credentials", json_schema_extra=require_auth_type)

    auth_type: Literal["None"] = Field(
        default="None",
        title="Authentication Type",
        description="Read storage with the settings and credentials returned by your catalog.",
    )


class S3Config(BaseModel):
    model_config = ConfigDict(title="S3 Access Keys", json_schema_extra=require_auth_type)

    auth_type: Literal["AWSAccessKey"] = Field(
        default="AWSAccessKey",
        title="Authentication Type",
        description="Read storage using your AWS credentials.",
    )
    endpoint: str = Field(
        default="",
        title="S3 Endpoint",
        description="Endpoint URL for your S3-compatible storage. Leave blank to use the catalog's endpoint.",
    )
    region: str = Field(
        default="us-east-1",
        title="S3 Region",
        description="Region of your S3 bucket.",
        min_length=1,
    )
    access_key_id: str = Field(
        title="Access Key ID",
        description="Access key ID for your S3 storage.",
        min_length=1,
    )
    secret_access_key: str = Field(
        title="Secret Access Key",
        description="Secret access key for your S3 storage.",
        min_length=1,
        repr=False,
        json_schema_extra={"secret": True},
    )
    session_token: str = Field(
        default="",
        title="Session Token",
        description="Session token when you use temporary AWS credentials for S3.",
        repr=False,
        json_schema_extra={"secret": True},
    )
    path_style_access: bool = Field(
        default=False,
        title="Path Style Access",
        description="Use path-style addressing for your S3-compatible storage.",
    )


class EndpointConfig(BaseModel):
    catalog_uri: str = Field(
        title="Catalog URI",
        description="URL of your Iceberg REST catalog.",
        min_length=1,
    )
    warehouse: str = Field(
        default="",
        title="Warehouse",
        description="Warehouse name or location passed to your catalog. Leave blank to use its default.",
    )
    credentials: Annotated[NoAuthConfig | OAuthConfig | TokenConfig | SigV4Config | AWSIAMConfig, Field(discriminator="auth_type")] = Field(
        default=NoAuthConfig(),
        title="Credentials",
        description="Authentication method for your Iceberg REST catalog.",
        json_schema_extra={"x-iam-auth": True},
    )
    namespaces: list[str] = Field(
        default=[],
        title="Namespaces",
        description="Discover only these namespaces. Use dots between nested namespace levels. Leave empty to discover all namespaces.",
    )
    s3: Annotated[CatalogStorageConfig | S3Config | IAMConfig, Field(discriminator="auth_type")] = Field(
        default=CatalogStorageConfig(),
        title="S3 Configuration",
        description="Storage settings that override or augment the FileIO properties returned by your catalog.",
    )

    @model_validator(mode="after")
    def storage_authentication(self) -> "EndpointConfig":
        if isinstance(self.s3, IAMConfig) and not isinstance(self.credentials, AWSIAMConfig):
            raise ValueError("S3 AWSIAM authentication requires AWSIAM catalog credentials")
        return self


class ResourceConfig(BaseResourceConfig):
    PATH_POINTERS: ClassVar[list[str]] = ["/namespace", "/table"]

    namespace: str = Field(
        title="Namespace",
        description="Namespace containing your Iceberg table.",
        min_length=1,
    )
    table: str = Field(
        title="Table",
        description="Name of your Iceberg table.",
        min_length=1,
    )
    interval: timedelta = Field(
        default=timedelta(minutes=5),
        title="Polling Interval",
        description="Interval between checks for new snapshots of this table.",
        gt=timedelta(),
        json_schema_extra={"nonsensitive": True},
    )

    def path(self) -> list[str]:
        return [self.namespace, self.table]


ConnectorState = GenericConnectorState[ResourceState]


class CatalogErrorResponse(BaseModel):
    class Error(BaseModel):
        stack: list[str] | None = None

    error: Error


class Document(BaseDocument, extra="allow"):
    class Meta(BaseDocument.Meta):
        snapshot_id: int
        file: str
        row: int

    meta_: Meta = Field(alias="_meta")
