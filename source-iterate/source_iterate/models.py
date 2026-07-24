from typing import AsyncGenerator, Callable

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceConfig,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
    LogCursor,
    Logger,
)
from estuary_cdk.http import HTTPSession, AccessToken

from pydantic import BaseModel, Field


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
    )


ConnectorState = GenericConnectorState[ResourceState]


class FullRefreshResource(BaseDocument, extra="allow"):
    pass


class SurveysResponse(BaseModel, extra="ignore"):
    results: list[FullRefreshResource]


class SurveyResponsesResponse(BaseModel, extra="ignore"):
    class Results(BaseModel, extra="allow"):
        count: int
        list: list[FullRefreshResource]

    results: Results

    class Links(BaseModel, extra="ignore"):
        next: str

    # links is not present on the last page of results.
    links: Links | None = None


FullRefreshFn = Callable[
    [HTTPSession, Logger],
    AsyncGenerator[FullRefreshResource, None],
]
