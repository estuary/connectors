from datetime import timedelta
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field, StringConstraints

CatalogTypeGlue = Literal["AWS Glue"]
CatalogTypeRest = Literal["Iceberg REST Server"]


class RestCatalogConfig(BaseModel):
    class Config:
        title="REST"

    catalog_type: CatalogTypeRest = Field(
        default="Iceberg REST Server",
        json_schema_extra={"type": "string", "order": 0}
    )
    uri: str = Field(
        title="URI",
        description="URI identifying the REST catalog, in the format of 'https://yourserver.com/catalog'.",
        json_schema_extra={"order": 1},
    )
    credential: Optional[str] = Field(
        default=None,
        title="Credential",
        description="Credential for connecting to the catalog.",
        json_schema_extra={"order": 2, "secret": True},
    )
    token: Optional[str] = Field(
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


class GlueCatalogConfig(BaseModel):
    class Config:
        title="AWS Glue"

    catalog_type: CatalogTypeGlue = Field(
        default="AWS Glue",
        json_schema_extra={"type": "string"}
    )

class AdvancedConfig(BaseModel):
    feature_flags: Optional[str] = Field(
        default=None,
        title="Feature Flags",
        description="This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support.",
    )


class EndpointConfig(BaseModel):
    aws_access_key_id: str = Field(
        title="AWS Access Key ID",
        description="Access Key ID for accessing AWS services.",
        json_schema_extra={"order": 0},
    )
    aws_secret_access_key: str = Field(
        title="AWS Secret Access Key",
        description="Secret Access Key for accessing AWS services.",
        json_schema_extra={"order": 1, "secret": True},
    )
    bucket: str = Field(
        description="The S3 bucket to write data files to.",
        json_schema_extra={"order": 2},
    )
    prefix: str = Field(
        default="",
        description="Optional prefix that will be used to store objects.",
        json_schema_extra={"order": 3},
    )
    region: str = Field(
        description="AWS region.",
        json_schema_extra={"order": 4},
    )
    namespace: Annotated[str, StringConstraints(pattern="^[^.]*$")] = Field(
        description="Namespace for bound collection tables (unless overridden within the binding resource configuration).",
        json_schema_extra={"order": 5},
    )
    upload_interval: timedelta = Field(
        default=timedelta(minutes=5),
        description="Frequency at which files will be uploaded. Must be a valid ISO8601 duration string no greater than 4 hours.",
        json_schema_extra={"order": 6},
    )
    catalog: RestCatalogConfig | GlueCatalogConfig = Field(
        discriminator="catalog_type",
        title="Catalog",
        json_schema_extra={"order": 7},
    )
    advanced: Optional[AdvancedConfig] = Field(
        default=None,
        title="Advanced Options",
        description="Options for advanced users. You should not typically need to modify these.",
        json_schema_extra={"order": 8, "advanced": True},
    )
