import sys
from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    model_validator,
)

AuthTypeAccessKey = Literal["AWSAccessKey"]
AuthTypeIAM = Literal["AWSIAM"]
CatalogTypeGlue = Literal["AWS Glue"]
CatalogTypeRest = Literal["Iceberg REST Server"]


class AccessKeyCredentials(BaseModel):
    class Config:
        title: str = "Access Key"

    auth_type: AuthTypeAccessKey = Field(
        default="AWSAccessKey", json_schema_extra={"order": 0, "type": "string"}
    )
    aws_access_key_id: str = Field(
        min_length=1,
        title="AWS Access Key ID",
        description="Access Key ID for accessing AWS services.",
        json_schema_extra={"order": 1},
        alias="awsAccessKeyId",
    )
    aws_secret_access_key: str = Field(
        min_length=1,
        title="AWS Secret Access Key",
        description="Secret Access Key for accessing AWS services.",
        json_schema_extra={"order": 2, "secret": True},
        alias="awsSecretAccessKey",
    )


class IAMCredentials(BaseModel):
    class Config:
        title: str = "AWS IAM"

    auth_type: AuthTypeIAM = Field(
        default="AWSIAM", json_schema_extra={"order": 0, "type": "string"}
    )
    aws_region: str = Field(
        min_length=1,
        title="AWS Region",
        description="AWS Region of your resource.",
        json_schema_extra={"order": 1},
    )
    aws_role_arn: str = Field(
        min_length=1,
        title="AWS Role ARN",
        description="AWS Role which has access to the resource which will be assumed by Flow.",
        json_schema_extra={"order": 2},
    )
    # Short-lived STS tokens injected at runtime by Flow's control plane.
    aws_access_key_id: str = Field(
        default="",
        title="AWS Access Key ID",
        description="AWS Access Key ID for accessing AWS services. "
        + "Automatically injected by Estuary",
        json_schema_extra={"order": 3, "x-hidden-field": True},
    )
    aws_secret_access_key: str = Field(
        default="",
        title="AWS Secret Access Key",
        description="AWS Secret Access Key for accessing AWS services."
        + "Automatically injected by Estuary",
        json_schema_extra={"order": 4, "x-hidden-field": True},
    )
    aws_session_token: str = Field(
        default="",
        title="AWS Session Token",
        description="AWS Session Token for accessing AWS services."
        + "Automatically injected by Estuary",
        json_schema_extra={"order": 5, "x-hidden-field": True},
    )


class RestCatalogConfig(BaseModel):
    class Config:
        title: str = "REST"

    catalog_type: CatalogTypeRest = Field(
        default="Iceberg REST Server", json_schema_extra={"type": "string", "order": 0}
    )
    uri: str = Field(
        title="URI",
        description="URI identifying the REST catalog, in the format of 'https://yourserver.com/catalog'.",
        json_schema_extra={"order": 1},
    )
    credential: str | None = Field(
        default=None,
        title="Credential",
        description="Credential for connecting to the catalog.",
        json_schema_extra={"order": 2, "secret": True},
    )
    token: str | None = Field(
        default=None,
        title="Token",
        description="Token for connecting to the catalog.",
        json_schema_extra={"order": 3, "secret": True},
    )
    warehouse: str = Field(
        title="Warehouse",
        description="Warehouse to connect to.",
        json_schema_extra={"order": 4},
    )
    scope: str | None = Field(
        default=None,
        title="Scope",
        description="Desired scope of the requested security token.",
        json_schema_extra={"order": 5},
    )


class GlueCatalogConfig(BaseModel):
    class Config:
        title: str = "AWS Glue"

    catalog_type: CatalogTypeGlue = Field(
        default="AWS Glue", json_schema_extra={"type": "string"}
    )
    glue_id: str | None = Field(
        default=None,
        title="Glue Catalog ID",
        description="Glue Catalog ID to use. If not specified, defaults to the account ID of the configured credentials.",
        json_schema_extra={"order": 1},
    )


class AdvancedConfig(BaseModel):
    feature_flags: str | None = Field(
        default=None,
        title="Feature Flags",
        description="This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support.",
    )


class EndpointConfig(BaseModel):
    credentials: AccessKeyCredentials | IAMCredentials = Field(
        discriminator="auth_type",
        title="Authentication",
        json_schema_extra={
            "order": 2,
            "x-iam-auth": True,
            "default": {"auth_type": "AWSAccessKey"},
        },
    )
    bucket: str = Field(
        description="The S3 bucket to write data files to.",
        json_schema_extra={"order": 3},
    )
    prefix: str = Field(
        default="",
        description="Optional prefix that will be used to store objects.",
        json_schema_extra={"order": 4},
    )
    region: str = Field(
        description="AWS region.",
        json_schema_extra={"order": 5},
    )
    namespace: Annotated[str, StringConstraints(pattern="^[^.]*$")] = Field(
        description="Namespace for bound collection tables (unless overridden within the binding resource configuration).",
        json_schema_extra={"order": 6},
    )
    upload_interval: timedelta = Field(
        default=timedelta(minutes=5),
        description="Frequency at which files will be uploaded. Must be a valid ISO8601 duration string no greater than 4 hours.",
        json_schema_extra={"order": 7},
    )
    s3_endpoint: str = Field(
        default="",
        description="Custom S3 endpoint URL. If not provided, the default AWS S3 endpoint for the specified region will be used.",
        json_schema_extra={"order": 8},
    )
    catalog: RestCatalogConfig | GlueCatalogConfig = Field(
        discriminator="catalog_type",
        title="Catalog",
        json_schema_extra={"order": 9},
    )
    advanced: AdvancedConfig | None = Field(
        default=None,
        title="Advanced Options",
        description="Options for advanced users. You should not typically need to modify these.",
        json_schema_extra={"order": 10, "advanced": True},
    )

    @model_validator(mode="before")
    @classmethod
    def transform_legacy_credentials(cls, data: Any):
        if not isinstance(data, dict):
            return data

        legacy_id = data.get("aws_access_key_id")
        legacy_secret = data.get("aws_secret_access_key")
        has_legacy = legacy_id is not None or legacy_secret is not None
        has_new = data.get("credentials") is not None

        if has_legacy and has_new:
            print(
                (
                    "Credentials have been found in both the legacy"
                    " (top-level aws_access_key_id/aws_secret_access_key keys)"
                    " and new formats (credentials object)."
                    " The former will be ignored in favour of the latter"
                ),
                file=sys.stderr,
                flush=True,
            )
        elif has_legacy:
            data["credentials"] = {
                "auth_type": "AWSAccessKey",
                "awsAccessKeyId": legacy_id,
                "awsSecretAccessKey": legacy_secret,
            }
        elif not has_new:
            raise ValueError("must provide credentials")

        data.pop("aws_access_key_id", None)
        data.pop("aws_secret_access_key", None)

        return data
