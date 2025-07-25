import asyncio
import re
from dataclasses import dataclass, field
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Generic,
    Protocol,
    TypeVar,
)

from estuary_cdk.http import HTTPSession
from estuary_cdk.incremental_json_processor import IncrementalJsonProcessor
from pydantic import BaseModel, ValidationError

from source_monday.models import (
    GraphQLError,
    GraphQLResponseRemainder,
)

from .constants import API, API_VERSION

# Monday.com API allows up to 10M complexity points per minute. We conservatively trigger
# complexity waits when remaining budget drops below 10k to prevent hitting the hard limit.
# This provides buffer for concurrent requests and unpredictable query complexity.
LOW_COMPLEXITY_BUDGET = 10000

# Monday.com complexity budget resets every 60 seconds. We wait this duration when
# hitting complexity limits before retrying queries.
COMPLEXITY_RESET_WAIT_SECONDS = 60

# Maximum number of retry attempts for transient GraphQL query failures
MAX_RETRY_ATTEMPTS = 5

# Error codes for specific exceptions returned by the GraphQL API
INTERNAL_SERVER_ERROR_CODE = "INTERNAL_SERVER_ERROR"
CURSOR_EXCEPTION_CODE = "CursorException"
USER_UNAUTHORIZED_CODE = "USER_UNAUTHORIZED"
USER_NOT_AUTHENTICATED_CODE = "USER_NOT_AUTHENTICATED"
GRAPHQL_VALIDATION_FAILED_CODE = "GRAPHQL_VALIDATION_FAILED"
INVALID_GRAPHQL_REQUEST_CODE = "INVALID_GRAPHQL_REQUEST"
PARSING_ERROR_CODE = "PARSING_ERROR"
UNDEFINED_FIELD_CODE = "undefinedField"
UNDEFINED_TYPE_CODE = "undefinedType"
BAD_REQUEST_CODE = "BadRequest"
ARGUMENT_LITERAL_IS_INCOMPATIBLE_CODE = "argumentLiteralsIncompatible"
RATE_LIMIT_EXCEEDED_CODE = "RATE_LIMIT_EXCEEDED"
COMPLEXITY_EXCEPTION_CODE = "ComplexityException"
COLUMN_VALUE_EXCEPTION_CODE = "ColumnValueException"
RESOURCE_NOT_FOUND_EXCEPTION_CODE = "ResourceNotFoundException"

RETRIABLE_ERRORS = {
    INTERNAL_SERVER_ERROR_CODE,  # 500 - Server errors are often transient
    RATE_LIMIT_EXCEEDED_CODE,
    COMPLEXITY_EXCEPTION_CODE,
}

NON_RETRIABLE_ERRORS = {
    USER_UNAUTHORIZED_CODE,
    USER_NOT_AUTHENTICATED_CODE,
    GRAPHQL_VALIDATION_FAILED_CODE,
    INVALID_GRAPHQL_REQUEST_CODE,
    PARSING_ERROR_CODE,
    UNDEFINED_FIELD_CODE,
    UNDEFINED_TYPE_CODE,
    BAD_REQUEST_CODE,
    ARGUMENT_LITERAL_IS_INCOMPATIBLE_CODE,
    RESOURCE_NOT_FOUND_EXCEPTION_CODE,
    COLUMN_VALUE_EXCEPTION_CODE,
    CURSOR_EXCEPTION_CODE,
}

STATUS_CODE_PRIORITY = {
    401: 1,
    403: 2,
    404: 3,
    400: 4,
    429: 5,
    500: 6,
    502: 7,
    503: 8,
    200: 99,  # Success (shouldn't raise but included for completeness)
}

DEFAULT_ERROR_STATUS_CODE = 500
DEFAULT_RETRY_WAIT_SECONDS = 2

TGraphqlData = TypeVar("TGraphqlData", bound=BaseModel)
TRemainder = TypeVar("TRemainder", bound=GraphQLResponseRemainder, contravariant=True)


@dataclass
class BoardNullTracker:
    """
    Track boards that have null values for restricted fields during streaming.

    When boards have authorization restrictions, restricted fields will be null in the response.
    This tracker helps correlate detected nulls with actual USER_UNAUTHORIZED errors received in
    the remainder of the GraphQL response.
    """

    boards_with_nulls: dict[str, set[str]] = field(
        default_factory=dict
    )  # board_id -> {null_fields}

    def track_board_with_null_field(self, board_id: str, field_name: str) -> None:
        if board_id not in self.boards_with_nulls:
            self.boards_with_nulls[board_id] = set()
        self.boards_with_nulls[board_id].add(field_name)

    def correlate_with_errors(
        self, log: Logger, auth_errors: list[GraphQLError]
    ) -> None:
        """
        Correlate detected nulls with actual authorization errors and log warnings.

        For each error path like ['boards', N, 'activity_logs']:
        1. Extract the field name ('activity_logs')
        2. Check if we detected any boards with null values for that field
        3. Log warnings for confirmed unauthorized boards
        """
        if not auth_errors or not self.boards_with_nulls:
            return

        error_fields = set()
        for error in auth_errors:
            if (
                error.path
                and len(error.path) >= 3
                and error.path[0] == "boards"
                and isinstance(error.path[2], str)
            ):
                # ['boards', N, 'activity_logs'] -> 'activity_logs'
                field_name = error.path[2]
                error_fields.add(field_name)

        for board_id, null_fields in self.boards_with_nulls.items():
            for field_name in null_fields:
                if field_name in error_fields:
                    log.warning(
                        f"Board {board_id} unauthorized for {field_name} - data excluded from results",
                        {
                            "board_id": board_id,
                            "restricted_field": field_name,
                            "operation": "board_field_access",
                        },
                    )


class RemainderProcessor(Protocol, Generic[TRemainder]):
    """
    A protocol for processing the remainder of a GraphQL response.

    Useful for extracting cursors or other data from the remainder while still allowing the main
    query to yield individual results. See `CursorCollector` in `source_monday.graphql.items` for an example implementation.
    """

    def process(self, log: Logger, remainder: TRemainder) -> None: ...
    def get_result(self, log: Logger) -> Any: ...


class GraphQLQueryError(RuntimeError):
    def __init__(self, errors: list[GraphQLError]):
        super().__init__(f"GraphQL query returned errors. Errors: {errors}")
        self.errors = errors


def _add_query_complexity(log: Logger, query: str) -> str:
    normalized_query = " ".join(query.split())

    # Pattern to match the opening brace of the query body
    # This handles: query { ... } or query Name { ... } or query Name($var: Type) { ... }
    pattern = r"(query(?:\s+\w+)?(?:\s*\([^)]*\))?\s*\{\s*)"
    match = re.search(pattern, normalized_query, re.IGNORECASE)

    if match:
        insert_pos = match.end()
        complexity_field = "complexity { query after reset_in_x_seconds } "
        query = (
            normalized_query[:insert_pos]
            + complexity_field
            + normalized_query[insert_pos:]
        )
    else:
        log.error(
            "Failed to modify query to include complexity field",
            {"original_query": normalized_query},
        )
        raise ValueError("Query modification failed. Cannot find query body.")

    return query


def _get_primary_error_info(
    errors: list[GraphQLError],
) -> tuple[int, str | None, bool, int]:
    """
    Analyze multiple errors and determine the primary status code and whether to retry.

    Returns: (status_code, error_code, should_retry, wait_seconds)
    """
    if not errors:
        return DEFAULT_ERROR_STATUS_CODE, None, True, DEFAULT_RETRY_WAIT_SECONDS

    error_details = []
    for error in errors:
        if error.extensions:
            status_code = error.extensions.status_code if error.extensions.status_code else DEFAULT_ERROR_STATUS_CODE
            code = error.extensions.code
            error_details.append(
                {
                    "status_code": status_code,
                    "code": code,
                    "error": error,
                    "priority": STATUS_CODE_PRIORITY.get(status_code, 99)
                    if status_code
                    else 99,
                }
            )

    if not error_details:
        # No extensions found, treat as generic server error
        return DEFAULT_ERROR_STATUS_CODE, None, True, DEFAULT_RETRY_WAIT_SECONDS

    error_details.sort(key=lambda x: x["priority"])
    primary_error = error_details[0]

    code = primary_error["code"]
    status_code = primary_error["status_code"]

    if code in NON_RETRIABLE_ERRORS:
        return status_code, code, False, 0
    elif code == CURSOR_EXCEPTION_CODE:
        return status_code, code, False, 0
    elif code == RATE_LIMIT_EXCEEDED_CODE:
        return status_code, code, True, 60
    elif code == COMPLEXITY_EXCEPTION_CODE:
        return status_code, code, True, 30
    elif code in RETRIABLE_ERRORS:
        return status_code, code, True, 2
    else:
        if status_code >= 500:
            return status_code, code, True, 2
        elif status_code >= 400:
            return status_code, code, False, 0
        else:
            return status_code, code, True, 2


async def execute_query(
    cls: type[TGraphqlData],
    http: HTTPSession,
    log: Logger,
    json_path: str,
    query: str,
    variables: dict[str, Any] | None = None,
    remainder_cls: type[TRemainder] = GraphQLResponseRemainder,
    remainder_processor: RemainderProcessor | None = None,
    null_tracker: BoardNullTracker | None = None,
) -> AsyncGenerator[TGraphqlData, None]:
    log.debug(
        f"Executing GraphQL query: {query}",
        {"json_path": json_path, "variables": variables},
    )

    if remainder_processor is not None:
        if not hasattr(remainder_processor, "process"):
            raise ValueError(
                "RemainderProcessor must implement 'process' and 'get_result' methods."
            )
        if not hasattr(remainder_processor, "get_result"):
            raise ValueError(
                "RemainderProcessor must implement 'process' and 'get_result' methods."
            )

    attempt = 1
    modified_query = _add_query_complexity(log, query)
    reset_in_seconds = None

    while True:
        try:
            _, body = await http.request_stream(
                log,
                API,
                method="POST",
                headers={"API-Version": API_VERSION},
                json=(
                    {"query": modified_query, "variables": variables}
                    if variables
                    else {"query": modified_query}
                ),
            )

            processor = IncrementalJsonProcessor(
                body(),
                json_path,
                cls,
                remainder_cls,
            )

            async for item in processor:
                yield item

            remainder = processor.get_remainder()

            if remainder.data and remainder.data.complexity:
                reset_in_seconds = remainder.data.complexity.reset_in_x_seconds

            if remainder.has_errors():
                if null_tracker:
                    auth_errors = [
                        err
                        for err in remainder.get_errors()
                        if err.extensions
                        and err.extensions.code == USER_UNAUTHORIZED_CODE
                    ]
                    other_errors = [
                        err
                        for err in remainder.get_errors()
                        if not (
                            err.extensions
                            and err.extensions.code == USER_UNAUTHORIZED_CODE
                        )
                    ]

                    if auth_errors:
                        null_tracker.correlate_with_errors(log, auth_errors)
                        if other_errors:
                            raise GraphQLQueryError(other_errors)
                        else:
                            log.debug(
                                "All GraphQL errors were USER_UNAUTHORIZED and handled via null tracking"
                            )
                            return
                    else:
                        raise GraphQLQueryError(remainder.get_errors())
                else:
                    raise GraphQLQueryError(remainder.get_errors())

            if remainder_processor is not None:
                remainder_processor.process(log, remainder)

            if remainder.data and remainder.data.complexity:
                log.debug(
                    "GraphQL query executed successfully",
                    {
                        "query": modified_query,
                        "variables": variables,
                        "complexity": remainder.data.complexity.query,
                        "reset_in_seconds": remainder.data.complexity.reset_in_x_seconds,
                        "after": remainder.data.complexity.after,
                    },
                )

                if remainder.data.complexity.after <= LOW_COMPLEXITY_BUDGET:
                    log.warning(
                        "Complexity budget critically low - waiting for reset",
                        {
                            "remaining_complexity": remainder.data.complexity.after,
                            "threshold": LOW_COMPLEXITY_BUDGET,
                            "reset_wait_seconds": remainder.data.complexity.reset_in_x_seconds,
                            "query_complexity": remainder.data.complexity.query,
                        },
                    )
                    await asyncio.sleep(remainder.data.complexity.reset_in_x_seconds)

            return

        except GraphQLQueryError as e:
            status_code, error_code, should_retry, wait_seconds = (
                _get_primary_error_info(e.errors)
            )
            if reset_in_seconds is not None:
                wait_seconds = reset_in_seconds

            log_context = {
                "primary_status_code": status_code,
                "primary_error_code": error_code,
                "all_errors": [
                    {
                        "message": err.message,
                        "code": err.extensions.code if err.extensions else None,
                        "status_code": err.extensions.status_code
                        if err.extensions
                        else None,
                    }
                    for err in e.errors
                ],
                "attempt": attempt,
                "max_attempts": MAX_RETRY_ATTEMPTS,
                "will_retry": should_retry and attempt < MAX_RETRY_ATTEMPTS,
            }

            if not should_retry:
                if error_code == CURSOR_EXCEPTION_CODE:
                    log.error("Cursor expired - need new cursor", log_context)
                    # TODO(jsmith): If this becomes a recurrent issue, consider
                    # implementing a cursor refresh instead of failing
                else:
                    log.error(
                        f"Non-retriable error with status {status_code}", log_context
                    )
                raise

            if attempt >= MAX_RETRY_ATTEMPTS:
                log.error("Max retries exhausted", log_context)
                raise

            if error_code == RATE_LIMIT_EXCEEDED_CODE:
                log.warning(f"Rate limit hit, waiting {wait_seconds}s", log_context)
            elif error_code == COMPLEXITY_EXCEPTION_CODE:
                log.warning(
                    f"Complexity limit hit, waiting {wait_seconds}s", log_context
                )
            else:
                wait_seconds = min(wait_seconds * attempt, 60)
                log.warning(f"Retriable error, waiting {wait_seconds}s", log_context)

            await asyncio.sleep(wait_seconds)
            attempt += 1

        except ValidationError as e:
            log.error(
                "Response validation failed",
                {
                    "error": str(e),
                    "attempt": attempt,
                },
            )
            raise

        except Exception as e:
            if attempt >= MAX_RETRY_ATTEMPTS:
                log.error(
                    "Unexpected error after max retries",
                    {
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "attempt": attempt,
                    },
                )
                raise

            wait_seconds = min(2 * attempt, 30)
            log.warning(
                f"Unexpected error, retrying in {wait_seconds}s",
                {
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "attempt": attempt,
                },
            )
            await asyncio.sleep(wait_seconds)
            attempt += 1
