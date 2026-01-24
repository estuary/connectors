from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import AwareDatetime, BaseModel


class ExportJobError(Exception):
    """Exception raised for errors when executing a export job."""
    def __init__(self, message: str, data_type: str, start: datetime, end: datetime | None, error: str | None = None):
        self.message = message
        self.errors = error
        self.data_type = data_type
        self.start = start
        self.end = end

        self.details: dict[str, Any] = {
            "data_type": data_type,
            "start": self.start,
            "end": self.end,
            "message": self.message
        }

        if self.errors:
            self.details["errors"] = self.errors

        super().__init__(self.details)

    def __str__(self):
        return self.message

    def __repr__(self):
        return (
            f"{self.__class__.__name__}: {self.message},"
            f"errors: {self.errors}"
        )


class TruncatedExportJobError(ExportJobError):
    ...


class ExportJobTimeoutError(ExportJobError):
    ...


class ExportJobState(StrEnum):
    ENQUEUED = "enqueued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class JobDetails(BaseModel):
    id: int
    jobState: ExportJobState


class GetRecentExportJobsResponse(BaseModel):
    jobs: list[JobDetails]


class StartExportResponse(BaseModel):
    jobId: int


class GetExportFilesResponse(BaseModel):
    class ExportFile(BaseModel):
        file: str
        url: str

    # exportTruncated is True if the total export size
    # exceeded the max allowed 100GB size & was truncated
    # (i.e. we need to request a smaller date range to
    # ensure we don't miss data)
    exportTruncated: bool
    jobId: int
    jobState: ExportJobState
    files: list[ExportFile]