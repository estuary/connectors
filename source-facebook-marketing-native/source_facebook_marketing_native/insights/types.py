"""Type definitions for Facebook Insights jobs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, TypeAlias, TypedDict

from pydantic import BaseModel, ConfigDict

from ..client import FacebookError, FacebookPaging


class TimeRange(TypedDict):
    """Date range for insights queries. Format: YYYY-MM-DD."""

    since: str
    until: str


InsightRecord: TypeAlias = dict[str, Any]


class JobScope(StrEnum):
    """
    Hierarchy level of an insights job.

    Jobs can query at four levels, each with different filter behavior:
    - ACCOUNT: No filtering, queries entire ad account
    - CAMPAIGNS: Filters by campaign.id IN [list]
    - ADSETS: Filters by adset.id IN [list]
    - ADS: Filters by ad.id IN [list] (atomic level, cannot split further)
    """

    ACCOUNT = "account"
    CAMPAIGNS = "campaigns"
    ADSETS = "adsets"
    ADS = "ads"


class InternalJobErrorType(StrEnum):
    """
    Error types for internally-generated job failures.

    These are used when the job manager creates FacebookAPIError
    exceptions for job status failures (not from Facebook's API directly).
    """

    JOB_FAILURE = "JobFailure"
    JOB_SKIPPED = "JobSkipped"
    JOB_TIMEOUT = "JobTimeout"
    JOB_RETRY_EXHAUSTED = "JobRetryExhausted"


class AsyncJobStatus(StrEnum):
    """Status values for async insights jobs."""

    JOB_NOT_STARTED = "Job Not Started"
    JOB_STARTED = "Job Started"
    JOB_RUNNING = "Job Running"
    JOB_COMPLETED = "Job Completed"
    JOB_FAILED = "Job Failed"
    JOB_SKIPPED = "Job Skipped"


class FilterField(StrEnum):
    """Filter field names for Facebook insights queries."""

    CAMPAIGN = "campaign.id"
    ADSET = "adset.id"
    AD = "ad.id"


# Errors that should propagate up (not retried at status-check level)
PROPAGATING_JOB_ERRORS: frozenset[str] = frozenset({
    InternalJobErrorType.JOB_FAILURE,
    InternalJobErrorType.JOB_SKIPPED,
    InternalJobErrorType.JOB_TIMEOUT,
})


@dataclass
class InsightsFilter:
    """
    Encapsulates Facebook's filtering parameter for insights queries.

    Used to narrow job scope by filtering to specific entity IDs using
    the IN operator. This enables efficient logarithmic splitting by
    batching multiple IDs in a single filter rather than creating
    individual jobs per entity.
    """

    field: FilterField
    value: list[str]
    operator: Literal["IN"] = "IN"

    def to_dict(self) -> dict[str, str | list[str]]:
        """Return the filter in the format expected by Facebook's API."""
        return {
            "field": self.field,
            "operator": self.operator,
            "value": self.value,
        }


@dataclass
class InsightsJob:
    """
    Represents a single insights job in the processing queue.

    Jobs are defined by their scope (hierarchy level) and optionally
    a list of entity IDs to filter. When a job fails, it can be split
    into smaller jobs through binary splitting or hierarchy descent.

    Attributes:
        scope: Hierarchy level (ACCOUNT, CAMPAIGNS, ADSETS, ADS).
        entity_ids: Optional list of entity IDs to filter by.
        depth: Number of splits from root ACCOUNT job (0 = root).
        parent_scope: Scope of the parent job that created this one.
    """

    scope: JobScope
    entity_ids: list[str] | None = None
    depth: int = 0
    parent_scope: JobScope | None = None

    def can_split(self) -> bool:
        """
        Determine if this job can be subdivided into smaller jobs.

        Split logic:
        - ACCOUNT scope: Can always split (discover campaigns)
        - Multiple entity_ids: Can binary split the list in half
        - Single entity at CAMPAIGNS/ADSETS: Can descend to children
        - Single entity at ADS: Cannot split (atomic level)

        Returns:
            True if the job can be split further, False otherwise.
        """
        if self.scope == JobScope.ACCOUNT:
            return True

        if self.entity_ids and len(self.entity_ids) > 1:
            return True

        if self.scope in (JobScope.CAMPAIGNS, JobScope.ADSETS):
            return True

        return False


@dataclass
class JobOutcome:
    """Base class for job execution outcomes."""

    pass


@dataclass
class JobSuccess(JobOutcome):
    """Job completed successfully. Records were streamed to queue."""

    records_count: int = 0


@dataclass
class JobSplit(JobOutcome):
    """Job was split into child jobs."""

    children: list[InsightsJob]


class AsyncJobSubmissionResponse(BaseModel):
    """Response from submitting an async insights job."""

    report_run_id: str
    error: FacebookError | None = None

    model_config = ConfigDict(extra="allow")


class AsyncJobStatusResponse(BaseModel):
    """Response from checking async insights job status."""

    id: str
    async_status: AsyncJobStatus
    async_percent_completion: int
    error: FacebookError | None = None

    model_config = ConfigDict(extra="allow")


class FacebookPaginatedInsightsResponse(BaseModel):
    """Paginated response for insights data from async jobs."""

    data: list[dict[str, Any]]
    paging: FacebookPaging | None = None
    error: FacebookError | None = None

    model_config = ConfigDict(extra="allow")
