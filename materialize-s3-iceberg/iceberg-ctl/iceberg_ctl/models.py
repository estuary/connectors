from datetime import timedelta
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints


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
