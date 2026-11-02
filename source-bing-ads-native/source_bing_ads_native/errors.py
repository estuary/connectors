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

# Bounds retries. Without a callback the CDK retries 5xx responses
# indefinitely. The CDK waits before each retry, from 1 s doubling to 30 s,
# so 8 attempts wait about 90 s in all: longer than the 60 s Microsoft says to
# wait after a call-rate fault (117).
MAX_ATTEMPTS = 8


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


def parse_fault(body: bytes) -> Fault | None:
    """The Microsoft fault in a response body, or None if it isn't one.

    Every field of `Fault` is optional, so any JSON object parses. A body
    counts as a fault only if it lists at least one error."""
    try:
        fault = Fault.model_validate_json(body)
    except PydanticValidationError:
        return None
    return fault if fault.errors else None


def fault_of(err: HTTPError) -> Fault | None:
    """The fault in an `HTTPError`'s response body, if it has one."""
    if err.body is None:
        return None
    return parse_fault(err.body)


def should_retry(
    log: Logger, status: int, headers: Headers, body: bytes, attempt: int
) -> bool:
    """Retry internal errors (0), call-rate faults (117) and concurrent-report
    faults (207), up to MAX_ATTEMPTS. Microsoft says to wait 60 s after a 117,
    and for running reports to finish after a 207; the CDK's growing wait
    between retries covers both. Any other fault fails at once.

    The CDK only calls this for 5xx responses. A fault sent as a 4xx fails at
    once whatever this returns. Which status the REST API uses for 117 and 207
    isn't documented.
    """
    if attempt >= MAX_ATTEMPTS:
        log.debug(
            "not retrying: reached the maximum attempts",
            {"attempt": attempt, "status": status},
        )
        return False

    fault = parse_fault(body)
    if fault is None:
        # Not a Microsoft fault, such as a gateway error.
        return True

    return all(_is_retryable(e) for e in fault.errors)


def _is_retryable(error: FaultError) -> bool:
    if error.Code in (INTERNAL_ERROR, CALL_RATE_EXCEEDED):
        return True
    # Code 207 is also used for an unrelated address error, so match the name.
    return (error.Code, error.ErrorCode) == CONCURRENT_REQUEST_OVER_LIMIT
