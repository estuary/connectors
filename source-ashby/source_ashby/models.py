from __future__ import annotations

from abc import ABCMeta
from typing import ClassVar

from estuary_cdk.capture.common import (
    BaseDocument,
    ResourceState,
)
from estuary_cdk.capture.common import (
    ConnectorState as GenericConnectorState,
)
from estuary_cdk.flow import AccessToken
from pydantic import BaseModel, Field, JsonValue


class EndpointConfig(BaseModel):
    credentials: AccessToken = Field(
        discriminator="credentials_title",
        title="Authentication",
        json_schema_extra={"order": 0},
    )


ConnectorState = GenericConnectorState[ResourceState]


class ResponseMeta(BaseModel, extra="allow"):
    success: bool
    moreDataAvailable: bool = False
    nextCursor: str | None = None
    syncToken: str | None = None
    errors: list[str] = []


class ApiKeyInfo(BaseModel, extra="allow"):
    scopes: list[str] = []


class ApiKeyInfoError(BaseModel, extra="allow"):
    code: str
    message: str


class ApiKeyInfoResponse(BaseModel, extra="allow"):
    results: ApiKeyInfo | None = None
    errorInfo: ApiKeyInfoError | None = None


class AshbyEntity(BaseDocument, extra="allow", metaclass=ABCMeta):
    name: ClassVar[str]
    required_scope: ClassVar[str]
    path: ClassVar[str]
    base_request_body: ClassVar[dict[str, JsonValue]] = {}

    id: str


class Applications(AshbyEntity):
    name: ClassVar[str] = "applications"
    required_scope: ClassVar[str] = "candidates:read"
    path: ClassVar[str] = "application.list"


class Approvals(AshbyEntity):
    name: ClassVar[str] = "approvals"
    required_scope: ClassVar[str] = "approvals:read"
    path: ClassVar[str] = "approval.list"


class CandidateTags(AshbyEntity):
    name: ClassVar[str] = "candidate_tags"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "candidateTag.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class Candidates(AshbyEntity):
    name: ClassVar[str] = "candidates"
    required_scope: ClassVar[str] = "candidates:read"
    path: ClassVar[str] = "candidate.list"


class CustomFields(AshbyEntity):
    name: ClassVar[str] = "custom_fields"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "customField.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class Departments(AshbyEntity):
    name: ClassVar[str] = "departments"
    required_scope: ClassVar[str] = "organization:read"
    path: ClassVar[str] = "department.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class FeedbackFormDefinitions(AshbyEntity):
    name: ClassVar[str] = "feedback_form_definitions"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "feedbackFormDefinition.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class InterviewPlans(AshbyEntity):
    name: ClassVar[str] = "interview_plans"
    required_scope: ClassVar[str] = "interviews:read"
    path: ClassVar[str] = "interviewPlan.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class InterviewSchedules(AshbyEntity):
    name: ClassVar[str] = "interview_schedules"
    required_scope: ClassVar[str] = "interviews:read"
    path: ClassVar[str] = "interviewSchedule.list"


class ChildEntityMixin(metaclass=ABCMeta):
    parent_entity: ClassVar[type[AshbyEntity]]
    parent_id_field: ClassVar[str]


class InterviewEvents(AshbyEntity, ChildEntityMixin):
    name: ClassVar[str] = "interview_events"
    required_scope: ClassVar[str] = "interviews:read"
    path: ClassVar[str] = "interviewEvent.list"
    parent_entity: ClassVar[type[AshbyEntity]] = InterviewSchedules
    parent_id_field: ClassVar[str] = "interviewScheduleId"


class InterviewerPools(AshbyEntity):
    name: ClassVar[str] = "interviewer_pools"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "interviewerPool.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {
        "includeArchivedPools": True,
        "includeArchivedTrainingStages": True,
    }


class Interviews(AshbyEntity):
    name: ClassVar[str] = "interviews"
    required_scope: ClassVar[str] = "interviews:read"
    path: ClassVar[str] = "interview.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {
        "includeArchived": True,
        "includeNonSharedInterviews": True,
    }


class JobTemplates(AshbyEntity):
    name: ClassVar[str] = "job_templates"
    required_scope: ClassVar[str] = "jobs:read"
    path: ClassVar[str] = "jobTemplate.list"


class Jobs(AshbyEntity):
    name: ClassVar[str] = "jobs"
    required_scope: ClassVar[str] = "jobs:read"
    path: ClassVar[str] = "job.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {
        "includeUnpublishedJobPostingsIds": True
    }


class Locations(AshbyEntity):
    name: ClassVar[str] = "locations"
    required_scope: ClassVar[str] = "organization:read"
    path: ClassVar[str] = "location.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {
        "includeArchived": True,
        "includeLocationHierarchy": True,
    }


class Offers(AshbyEntity):
    name: ClassVar[str] = "offers"
    required_scope: ClassVar[str] = "offers:read"
    path: ClassVar[str] = "offer.list"


class Openings(AshbyEntity):
    name: ClassVar[str] = "openings"
    required_scope: ClassVar[str] = "jobs:read"
    path: ClassVar[str] = "opening.list"


class Projects(AshbyEntity):
    name: ClassVar[str] = "projects"
    required_scope: ClassVar[str] = "candidates:read"
    path: ClassVar[str] = "project.list"


class SurveyFormDefinitions(AshbyEntity):
    name: ClassVar[str] = "survey_form_definitions"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "surveyFormDefinition.list"


class Users(AshbyEntity):
    name: ClassVar[str] = "users"
    required_scope: ClassVar[str] = "organization:read"
    path: ClassVar[str] = "user.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeDeactivated": True}


class AshbySnapshotEntity(AshbyEntity, metaclass=ABCMeta):
    pass


class ArchiveReasons(AshbySnapshotEntity):
    name: ClassVar[str] = "archive_reasons"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "archiveReason.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class JobPostings(AshbySnapshotEntity):
    name: ClassVar[str] = "job_postings"
    required_scope: ClassVar[str] = "jobs:read"
    path: ClassVar[str] = "jobPosting.list"


class Sources(AshbySnapshotEntity):
    name: ClassVar[str] = "sources"
    required_scope: ClassVar[str] = "hiringProcessMetadata:read"
    path: ClassVar[str] = "source.list"
    base_request_body: ClassVar[dict[str, JsonValue]] = {"includeArchived": True}


class InterviewStages(AshbySnapshotEntity, ChildEntityMixin):
    name: ClassVar[str] = "interview_stages"
    required_scope: ClassVar[str] = "interviews:read"
    path: ClassVar[str] = "interviewStage.list"
    parent_entity: ClassVar[type[AshbyEntity]] = InterviewPlans
    parent_id_field: ClassVar[str] = "interviewPlanId"


INCREMENTAL_STREAMS: list[type[AshbyEntity]] = [
    Applications,
    Approvals,
    CandidateTags,
    Candidates,
    CustomFields,
    Departments,
    FeedbackFormDefinitions,
    InterviewEvents,
    InterviewPlans,
    InterviewSchedules,
    InterviewerPools,
    Interviews,
    JobTemplates,
    Jobs,
    Locations,
    Offers,
    Openings,
    Projects,
    SurveyFormDefinitions,
    Users,
]

SNAPSHOT_STREAMS: list[type[AshbySnapshotEntity]] = [
    ArchiveReasons,
    InterviewStages,
    JobPostings,
    Sources,
]
