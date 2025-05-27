from typing import Optional, List
from datetime import datetime, timezone
from pydantic import BaseModel, Field, AwareDatetime

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
    ConnectorState as GenericConnectorState,
)

ConnectorState = GenericConnectorState[ResourceState]

class EndpointConfig(BaseModel):
    api_key: str = Field(
        description="Qualtrics API Key",
        title="API Key",
        json_schema_extra={"secret": True},
    )
    data_center: str = Field(
        description="Qualtrics Data Center",
        title="Data Center",
    )
    start_time: Optional[datetime] = Field(
        default=None,
        description="When to start collecting data from. If not specified, defaults to 1 hour ago. Use ISO format (e.g., '2024-03-01T00:00:00Z').",
        title="Start Time",
    )


class FullRefreshResource(BaseDocument, extra="allow"):
    pass

class Survey(BaseDocument, extra="allow"):
    pass

class SurveysResponse(BaseModel, extra="forbid"):
    result: list[Survey]
    meta: list[FullRefreshResource]

class SurveyQuestion(BaseDocument, extra="allow"):
    pass

class SurveyQuestionResponse(BaseModel, extra="forbid"):
    result: list[SurveyQuestion]
    meta: list[FullRefreshResource]

