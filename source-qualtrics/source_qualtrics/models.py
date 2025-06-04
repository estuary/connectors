from datetime import datetime, timedelta, UTC
from pydantic import BaseModel, Field, field_validator
from typing import (
    AsyncGenerator,
    Callable,
    Generic,
    Optional,
    TypeVar,
    ClassVar,
    Literal,
)
from logging import Logger
from estuary_cdk.capture.common import LogCursor

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from estuary_cdk.http import HTTPSession

ConnectorState = GenericConnectorState[ResourceState]


class Auth(AccessToken):
    credentials_title: Literal["Private App Credentials"] = Field(
        default="Private App Credentials", json_schema_extra={"type": "string"}
    )
    access_token: str = Field(
        title="API Token. Found in Account Settings > Qualtrics IDs under 'API' section.",
        json_schema_extra={"secret": True},
    )


class EndpointConfig(BaseModel):
    credentials: Auth = Field(
        discriminator="credentials_title",
        title="Authentication",
    )
    data_center: str = Field(
        description="Your Qualtrics data center ID (e.g., 'fra1', 'syd1'). Found in Account Settings > Qualtrics IDs.",
        title="Data Center ID",
        examples=["fra1", "syd1", "dub1"],
    )
    start_date: datetime = Field(
        description="Start date for fetching survey responses (ISO format). Defaults to 6 months ago.",
        title="Start Date",
        default_factory=lambda: (datetime.now(tz=UTC) - timedelta(days=180)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ),
    )

    @field_validator("data_center")
    @classmethod
    def validate_data_center(cls, value: str) -> str:
        return value.strip().lower()

    @property
    def base_url(self) -> str:
        return f"https://{self.data_center}.qualtrics.com/API/v3"


TApiResponse = TypeVar("TApiResponse", bound=BaseModel)


class Meta(BaseModel):
    httpStatus: str
    error: Optional[dict] = None  # Contains errorMessage when present


class ApiResponse(BaseModel, Generic[TApiResponse]):
    result: Optional[TApiResponse] = None
    meta: Meta


class PaginatedResponse(BaseModel):
    elements: list[dict] = []
    nextPage: Optional[str] = None


class QuestionsResponse(BaseModel):
    questions: dict[str, dict] = {}


class ExportCreationRequest(BaseModel):
    format: Literal["ndjson"]
    startDate: str
    compress: bool = True  # Always compress to avoid 1.8GB limit


class ExportStartResponse(BaseModel):
    progressId: str
    percentComplete: float = 0.0
    status: str


class ExportStatusResponse(BaseModel):
    progressId: str
    percentComplete: float
    status: str


class QualtricsResource(BaseDocument):
    RESOURCE_NAME: ClassVar[str]
    ENDPOINT: ClassVar[str]

    @property
    def resource_name(self) -> str:
        return self.RESOURCE_NAME

    @property
    def endpoint(self) -> str:
        return self.ENDPOINT.format(**self.model_dump())

    @classmethod
    def get_resource_key_json_path(cls) -> list[str]: ...


class Survey(QualtricsResource, extra="allow"):
    RESOURCE_NAME: ClassVar[str] = "surveys"
    ENDPOINT: ClassVar[str] = "surveys"

    id: str


class SurveyQuestion(QualtricsResource, extra="allow"):
    RESOURCE_NAME: ClassVar[str] = "survey_questions"
    ENDPOINT: ClassVar[str] = "surveys/{survey_id}/questions"

    surveyId: str
    questionId: str


class SurveyResponse(QualtricsResource, extra="allow"):
    RESOURCE_NAME: ClassVar[str] = "survey_responses"
    ENDPOINT: ClassVar[str] = "surveys/{survey_id}/export-responses"

    surveyId: str
    responseId: str
    recordedDate: datetime
    startDate: Optional[datetime] = None
    endDate: Optional[datetime] = None

    @classmethod
    def get_resource_key_json_path(cls) -> list[str]:
        return ["/surveyId", "/responseId"]


FullRefreshResourceFetchFn = Callable[
    [HTTPSession, EndpointConfig, Logger], AsyncGenerator[BaseDocument, None]
]

IncrementalResourceFetchChangesFn = Callable[
    [HTTPSession, EndpointConfig, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]
