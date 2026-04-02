import dataclasses
from datetime import datetime, timedelta, UTC
from typing import ClassVar, Literal

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
from estuary_cdk.flow import (
    ClientCredentialsOAuth2Credentials,
    OAuth2ClientCredentialsPlacement,
    OAuth2TokenFlowSpec,
)
from estuary_cdk.http import TokenSource

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


OAUTH2_SPEC = OAuth2TokenFlowSpec(
    accessTokenUrlTemplate="https://auth.greenhouse.io/token",
    accessTokenResponseMap={
        "access_token": "/access_token",
    },
)


def default_start_date():
    dt = datetime.now(tz=UTC) - timedelta(days=30)
    return dt


@dataclasses.dataclass
class GreenhouseTokenSource(TokenSource):
    """Custom TokenSource that converts Greenhouse's expires_at (ISO datetime)
    response field into expires_in (seconds) so the CDK can cache tokens."""

    class AccessTokenResponse(TokenSource.AccessTokenResponse):
        expires_at: AwareDatetime

        @model_validator(mode="after")
        def _compute_expires_in(self):
            remaining = (self.expires_at - datetime.now(tz=UTC)).total_seconds()
            self.expires_in = max(int(remaining), 0)
            return self


class GreenhouseClientCredentials(
    ClientCredentialsOAuth2Credentials.with_client_credentials_placement(
        OAuth2ClientCredentialsPlacement.HEADERS
    )
):
    model_config = ConfigDict(
        title="Client Credentials",
    )

    credentials_title: Literal["Client Credentials"] = Field(
        default="Client Credentials",
    )


class EndpointConfig(BaseModel):
    credentials: GreenhouseClientCredentials = Field(
        title="Authentication",
    )
    start_date: AwareDatetime = Field(
        description="UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present.",
        title="Start Date",
        default_factory=default_start_date,
        ge=EPOCH,
    )


ConnectorState = GenericConnectorState[ResourceState]


class GreenhouseResource(BaseDocument, extra="allow"):
    name: ClassVar[str]
    path: ClassVar[str]

    id: int
    created_at: AwareDatetime
    updated_at: AwareDatetime


# Incremental streams
class Applications(GreenhouseResource):
    name: ClassVar[str] = "applications"
    path: ClassVar[str] = "applications"


class ApplicationStages(GreenhouseResource):
    name: ClassVar[str] = "application_stages"
    path: ClassVar[str] = "application_stages"


class AppliedCandidateTags(GreenhouseResource):
    name: ClassVar[str] = "applied_candidate_tags"
    path: ClassVar[str] = "applied_candidate_tags"


class ApprovalFlows(GreenhouseResource):
    name: ClassVar[str] = "approval_flows"
    path: ClassVar[str] = "approval_flows"


class ApproverGroups(GreenhouseResource):
    name: ClassVar[str] = "approver_groups"
    path: ClassVar[str] = "approver_groups"


class Approvers(GreenhouseResource):
    name: ClassVar[str] = "approvers"
    path: ClassVar[str] = "approvers"


class Attachments(GreenhouseResource):
    name: ClassVar[str] = "attachments"
    path: ClassVar[str] = "attachments"


class Candidates(GreenhouseResource):
    name: ClassVar[str] = "candidates"
    path: ClassVar[str] = "candidates"


class CandidateAttributeTypes(GreenhouseResource):
    name: ClassVar[str] = "candidate_attribute_types"
    path: ClassVar[str] = "candidate_attribute_types"


class CandidateEducations(GreenhouseResource):
    name: ClassVar[str] = "candidate_educations"
    path: ClassVar[str] = "candidate_educations"


class CandidateEmployments(GreenhouseResource):
    name: ClassVar[str] = "candidate_employments"
    path: ClassVar[str] = "candidate_employments"


class CandidateTags(GreenhouseResource):
    name: ClassVar[str] = "candidate_tags"
    path: ClassVar[str] = "candidate_tags"


class CloseReasons(GreenhouseResource):
    name: ClassVar[str] = "close_reasons"
    path: ClassVar[str] = "close_reasons"


class CustomFields(GreenhouseResource):
    name: ClassVar[str] = "custom_fields"
    path: ClassVar[str] = "custom_fields"


class CustomFieldOptions(GreenhouseResource):
    name: ClassVar[str] = "custom_field_options"
    path: ClassVar[str] = "custom_field_options"


class DemographicAnswers(GreenhouseResource):
    name: ClassVar[str] = "demographic_answers"
    path: ClassVar[str] = "demographic_answers"


class DemographicQuestions(GreenhouseResource):
    name: ClassVar[str] = "demographic_questions"
    path: ClassVar[str] = "demographic_questions"


class DemographicQuestionSets(GreenhouseResource):
    name: ClassVar[str] = "demographic_question_sets"
    path: ClassVar[str] = "demographic_question_sets"


class Departments(GreenhouseResource):
    name: ClassVar[str] = "departments"
    path: ClassVar[str] = "departments"


class EEOC(GreenhouseResource):
    name: ClassVar[str] = "eeoc"
    path: ClassVar[str] = "eeoc"


class EmailTemplates(GreenhouseResource):
    name: ClassVar[str] = "email_templates"
    path: ClassVar[str] = "email_templates"


class Interviews(GreenhouseResource):
    name: ClassVar[str] = "interviews"
    path: ClassVar[str] = "interviews"


class Interviewers(GreenhouseResource):
    name: ClassVar[str] = "interviewers"
    path: ClassVar[str] = "interviewers"


class JobNotes(GreenhouseResource):
    name: ClassVar[str] = "job_notes"
    path: ClassVar[str] = "job_notes"


class JobPosts(GreenhouseResource):
    name: ClassVar[str] = "job_posts"
    path: ClassVar[str] = "job_posts"


class JobInterviewStages(GreenhouseResource):
    name: ClassVar[str] = "job_interview_stages"
    path: ClassVar[str] = "job_interview_stages"


class Jobs(GreenhouseResource):
    name: ClassVar[str] = "jobs"
    path: ClassVar[str] = "jobs"


class Offers(GreenhouseResource):
    name: ClassVar[str] = "offers"
    path: ClassVar[str] = "offers"


class Offices(GreenhouseResource):
    name: ClassVar[str] = "offices"
    path: ClassVar[str] = "offices"


class Openings(GreenhouseResource):
    name: ClassVar[str] = "openings"
    path: ClassVar[str] = "openings"


class ProspectPools(GreenhouseResource):
    name: ClassVar[str] = "prospect_pools"
    path: ClassVar[str] = "prospect_pools"


class ProspectPoolStages(GreenhouseResource):
    name: ClassVar[str] = "prospect_pool_stages"
    path: ClassVar[str] = "prospect_pool_stages"


class RejectionReasons(GreenhouseResource):
    name: ClassVar[str] = "rejection_reasons"
    path: ClassVar[str] = "rejection_reasons"


class ScorecardQuestions(GreenhouseResource):
    name: ClassVar[str] = "scorecard_questions"
    path: ClassVar[str] = "scorecard_questions"


class Scorecards(GreenhouseResource):
    name: ClassVar[str] = "scorecards"
    path: ClassVar[str] = "scorecards"


class Sources(GreenhouseResource):
    name: ClassVar[str] = "sources"
    path: ClassVar[str] = "sources"


class UserEmails(GreenhouseResource):
    name: ClassVar[str] = "user_emails"
    path: ClassVar[str] = "user_emails"


class UserJobPermissions(GreenhouseResource):
    name: ClassVar[str] = "user_job_permissions"
    path: ClassVar[str] = "user_job_permissions"


class UserRoles(GreenhouseResource):
    name: ClassVar[str] = "user_roles"
    path: ClassVar[str] = "user_roles"


class Users(GreenhouseResource):
    name: ClassVar[str] = "users"
    path: ClassVar[str] = "users"


INCREMENTAL_RESOURCES: list[type[GreenhouseResource]] = [
    Applications,
    ApplicationStages,
    AppliedCandidateTags,
    ApprovalFlows,
    ApproverGroups,
    Approvers,
    Attachments,
    Candidates,
    CandidateAttributeTypes,
    CandidateEducations,
    CandidateEmployments,
    CandidateTags,
    CloseReasons,
    CustomFields,
    CustomFieldOptions,
    DemographicAnswers,
    DemographicQuestions,
    DemographicQuestionSets,
    Departments,
    EEOC,
    EmailTemplates,
    Interviews,
    Interviewers,
    JobInterviewStages,
    JobNotes,
    JobPosts,
    Jobs,
    Offers,
    Offices,
    Openings,
    ProspectPools,
    ProspectPoolStages,
    RejectionReasons,
    ScorecardQuestions,
    Scorecards,
    Sources,
    UserEmails,
    UserJobPermissions,
    UserRoles,
    Users,
]
