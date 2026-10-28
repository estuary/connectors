from logging import Logger

from pydantic import BaseModel, ValidationError as PydanticValidationError

from estuary_cdk.http import HTTPError, Headers

# Fault codes the connector reacts to.
# https://learn.microsoft.com/en-us/advertising/guides/handle-service-errors-exceptions?view=bingads-13
# https://learn.microsoft.com/en-us/advertising/guides/operation-error-codes?view=bingads-13
INTERNAL_ERROR = 0
INVALID_CREDENTIALS = 105
USER_IS_NOT_AUTHORIZED = 106
CALL_RATE_EXCEEDED = 117
CONCURRENT_REQUEST_OVER_LIMIT = (207, "ConcurrentRequestOverLimit")

ACCESS_DENIED_CODES = {INVALID_CREDENTIALS, USER_IS_NOT_AUTHORIZED}

# Bounds retries of internal errors. Without a callback the CDK retries 5xx
# responses indefinitely.
MAX_INTERNAL_ERROR_ATTEMPTS = 5


class FaultError(BaseModel, extra="allow"):
    Code: int
    ErrorCode: str | None = None
    Message: str | None = None


class Fault(BaseModel, extra="allow"):
    """A Microsoft Advertising fault body.

    Which error list is populated depends on the fault type and service:
    `Errors` for AdApiFaultDetail, `OperationErrors` and `BatchErrors` for
    ApiFaultDetail and ApiFault.
    https://learn.microsoft.com/en-us/advertising/campaign-management-service/adapifaultdetail?view=bingads-13
    """

    TrackingId: str | None = None
    Errors: list[FaultError] | None = None
    OperationErrors: list[FaultError] | None = None
    BatchErrors: list[FaultError] | None = None

    @property
    def errors(self) -> list[FaultError]:
        return [
            *(self.Errors or []),
            *(self.OperationErrors or []),
            *(self.BatchErrors or []),
        ]

    def codes(self) -> set[int]:
        return {e.Code for e in self.errors}

    def has(self, code: int, error_code: str | None = None) -> bool:
        return any(
            e.Code == code and (error_code is None or e.ErrorCode == error_code)
            for e in self.errors
        )

    def describe(self) -> str:
        messages = "; ".join(
            f"{e.Code} {e.ErrorCode or ''}: {e.Message or ''}".strip()
            for e in self.errors
        )
        return f"{messages} (TrackingId {self.TrackingId})"


def parse_fault(body: bytes | str) -> Fault | None:
    try:
        return Fault.model_validate_json(body)
    except PydanticValidationError:
        return None


def fault_of(err: HTTPError) -> Fault | None:
    """The fault in an `HTTPError`'s response body, if it has one."""
    if "Response:" not in err.message:
        return None
    return parse_fault(err.message.split("Response:", 1)[1].strip())


def should_retry(
    log: Logger, status: int, headers: Headers, body: bytes, attempt: int
) -> bool:
    """Retry only internal errors, a bounded number of times.

    Call-rate (117) and concurrent-report (207) faults are not retried: the
    CDK would retry them about once a second with no limit, which could keep a
    sliding-window throttle from ever clearing. They fail the task instead, and
    it restarts from its last checkpoint.
    """
    if attempt >= MAX_INTERNAL_ERROR_ATTEMPTS:
        log.debug(
            "not retrying: reached the maximum attempts for internal errors",
            {"attempt": attempt, "status": status},
        )
        return False

    fault = parse_fault(body)
    if fault is None:
        # Not a Microsoft fault, such as a gateway error.
        return True

    return fault.codes() == {INTERNAL_ERROR}
