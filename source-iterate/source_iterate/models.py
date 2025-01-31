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


class SurveysResponse(BaseModel, extra="forbid"):
    results: list[FullRefreshResource]


class SurveyResponsesResponse(BaseModel, extra="allow"):
    class Results(BaseModel, extra="forbid"):
        count: int
        list: list[FullRefreshResource]

    results: Results

    class Links(BaseModel, extra="forbid"):
        next: str

    # links is not present on the last page of results.
    links: Links | None = None


FullRefreshFn = Callable[
    [HTTPSession, Logger],
    AsyncGenerator[FullRefreshResource, None],
]
