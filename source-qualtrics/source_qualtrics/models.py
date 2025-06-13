from datetime import datetime, timedelta, UTC
from pydantic import AwareDatetime, BaseModel, Field, field_validator
from typing import (
    Annotated,
    AsyncGenerator,
    Callable,
    Generic,
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
from estuary_cdk.http import HTTPMixin

ConnectorState = GenericConnectorState[ResourceState]


class Auth(AccessToken):
    credentials_title: Literal["Private App Credentials"] = Field(
        default="Private App Credentials", json_schema_extra={"type": "string"}
    )
    access_token: str = Field(
        title="API Token",
        description="Found in Account Settings > Qualtrics IDs under 'API' section.",
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
    start_date: AwareDatetime = Field(
        description="Start date for fetching survey responses (ISO format). Defaults to 6 months ago.",
        title="Start Date",
        default_factory=lambda: (datetime.now(tz=UTC) - timedelta(days=180)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ),
    )

    class Advanced(BaseModel):
        window_size: Annotated[
            int,
            Field(
                description="Window size in days for incremental streams.",
                title="Window Size",
                default=30,
                gt=0,
            ),
        ]

    advanced: Advanced = Field(
        default_factory=Advanced,  # type: ignore
        title="Advanced Config",
        description="Advanced settings for the connector.",
        json_schema_extra={"advanced": True},
    )

    @field_validator("data_center")
    @classmethod
    def validate_data_center(cls, value: str) -> str:
        return value.strip().lower()


TApiResponse = TypeVar("TApiResponse", bound=BaseModel)


class Meta(BaseModel, extra="allow"):
    httpStatus: str
    error: dict | None = None


class ApiResponse(BaseModel, Generic[TApiResponse]):
    result: TApiResponse | None = None
    meta: Meta


class PaginatedResponse(BaseModel, extra="allow"):
    elements: list[dict] = []
    nextPage: str | None = None


class QuestionsResponse(BaseModel, extra="allow"):
    elements: dict[str, dict] = {}


class ExportCreationRequest(BaseModel, extra="allow"):
    format: Literal["ndjson"]
    startDate: str | None = (
        "1970-01-01T01:00:00Z"  # Qualtrics API defaults to this value if not provided
    )
    endDate: str | None = None
    compress: bool = True  # Always compress to avoid 1.8GB limit


class ExportResponse(BaseModel, extra="allow"):
    percentComplete: float = Field(ge=0.0)
    status: Literal["inProgress", "failed", "complete"]


class ExportStartResponse(ExportResponse):
    progressId: str
    fileId: str | None = None


class ExportStatusResponse(ExportResponse):
    fileId: str | None = None


class QualtricsResource(BaseDocument):
    RESOURCE_NAME: ClassVar[str]
    ENDPOINT: ClassVar[str]

    @property
    def resource_name(self) -> str:
        return self.RESOURCE_NAME

    @property
    def endpoint(self) -> str:
        return self.ENDPOINT.format(**self.model_dump())


class IncrementalResource(QualtricsResource):
    @classmethod
    def get_resource_key_json_path(cls) -> list[str]:
        raise NotImplementedError(
            "Incremental resources must implement get_resource_key_json_path"
        )


class Survey(QualtricsResource, extra="allow"):
    RESOURCE_NAME: ClassVar[str] = "surveys"
    ENDPOINT: ClassVar[str] = "surveys"

    id: str


class SurveyQuestion(QualtricsResource, extra="allow"):
    RESOURCE_NAME: ClassVar[str] = "survey_questions"
    ENDPOINT: ClassVar[str] = "surveys/{survey_id}/questions"

    QuestionID: str


class SurveyResponse(IncrementalResource, extra="allow"):
    RESOURCE_NAME: ClassVar[str] = "survey_responses"
    ENDPOINT: ClassVar[str] = "surveys/{survey_id}/export-responses"

    # Ensures that the model validates assignment of the surveyId
    # field which is not required in the API response, but is required
    # for the resource key and set manually in the connector
    model_config = {"validate_assignment": True}

    class Values(BaseModel, extra="allow"):
        recordedDate: datetime

    responseId: str
    surveyId: str = Field(
        default="DEFAULT",
        json_schema_extra=lambda x: x.pop("default"),  # type: ignore
    )
    values: Values

    @classmethod
    def get_resource_key_json_path(cls) -> list[str]:
        return ["/surveyId", "/responseId"]

    @field_validator("surveyId")
    def validate_response_id(cls, value: str) -> str:
        if not value or value.strip() == "" or value == "DEFAULT":
            # This validation at first glance seems like it could cause issues if a
            # surveyId returned from Qualtrics' API is "DEFAULT", but in practice,
            # the API does not return "DEFAULT" as a surveyId. It is only used as a
            # placeholder in the model to ensure that the surveyId is always set to a valid value.
            # The Qualtrics API docs shows that survey IDs follow the pattern ^SV_[a-zA-Z0-9]{11,15}$
            # however, validating against that pattern is not strictly necessary
            # since the API will not return "DEFAULT" as a surveyId.
            raise ValueError(
                "surveyId is invalid or empty. It must be a non-empty string and cannot be 'DEFAULT'."
            )
        return value


FullRefreshResourceFetchFn = Callable[
    [HTTPMixin, str, Logger], AsyncGenerator[BaseDocument, None]
]

IncrementalResourceFetchChangesFn = Callable[
    [HTTPMixin, str, int, Logger, LogCursor],
    AsyncGenerator[BaseDocument | LogCursor, None],
]
