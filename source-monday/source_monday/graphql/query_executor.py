import asyncio
import re
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

TGraphqlData = TypeVar("TGraphqlData", bound=BaseModel)
TRemainder = TypeVar("TRemainder", bound=GraphQLResponseRemainder, contravariant=True)


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


async def execute_query(
    cls: type[TGraphqlData],
    http: HTTPSession,
    log: Logger,
    json_path: str,
    query: str,
    variables: dict[str, Any] | None = None,
    remainder_cls: type[TRemainder] = GraphQLResponseRemainder,
    remainder_processor: RemainderProcessor | None = None,
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

            if remainder.has_errors():
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
                        }
                    )
                    await asyncio.sleep(remainder.data.complexity.reset_in_x_seconds)

            return

        except GraphQLQueryError as e:
            for error in e.errors:
                if error.extensions:
                    if error.extensions.complexity and error.extensions.maxComplexity:
                        if error.extensions.complexity > error.extensions.maxComplexity:
                            log.error(
                                "GraphQL query permanently exceeds complexity limit - cannot be retried",
                                {
                                    "query_complexity": error.extensions.complexity,
                                    "max_allowed_complexity": error.extensions.maxComplexity,
                                    "complexity_ratio": round(error.extensions.complexity / error.extensions.maxComplexity, 2),
                                    "query_preview": query[:200] + "..." if len(query) > 200 else query,
                                    "variables": variables,
                                    "json_path": json_path,
                                    "response_model": cls.__name__,
                                },
                            )
                            raise

                if (
                    "ComplexityException" in str(error)
                    or "complexity" in str(error).lower()
                ):
                    log.warning(
                        f"Hit complexity limit during query execution (attempt {attempt})",
                        {
                            "error": str(error),
                            "will_retry_in_seconds": COMPLEXITY_RESET_WAIT_SECONDS,
                        },
                    )
                    await asyncio.sleep(COMPLEXITY_RESET_WAIT_SECONDS)
                    attempt += 1
                    break

            if attempt == MAX_RETRY_ATTEMPTS:
                log.error(
                    "GraphQL streaming query failed permanently",
                    {
                        "total_attempts": attempt,
                        "final_error": str(e),
                        "variables": variables,
                        "json_path": json_path,
                        "response_model": cls.__name__,
                    },
                )
                raise

            retry_delay = attempt * 2
            log.warning(
                "GraphQL query failed - retrying with exponential backoff",
                {
                    "error": str(e),
                    "attempt": attempt,
                    "max_attempts": MAX_RETRY_ATTEMPTS,
                    "query_preview": modified_query[:100] + "..." if len(modified_query) > 100 else modified_query,
                    "variables": variables,
                    "json_path": json_path,
                    "response_model": cls.__name__,
                    "retry_delay_seconds": retry_delay,
                },
            )
            await asyncio.sleep(retry_delay)
            attempt += 1

        except Exception as e:
            if attempt == MAX_RETRY_ATTEMPTS or isinstance(e, ValidationError):
                log.error(
                    "GraphQL streaming query failed permanently",
                    {
                        "total_attempts": attempt,
                        "final_error": str(e),
                        "variables": variables,
                        "json_path": json_path,
                        "response_model": cls.__name__,
                    },
                )
                raise

            log.warning(
                "GraphQL streaming query failed, will retry",
                {
                    "error": str(e),
                    "attempt": attempt,
                    "query": modified_query,
                    "variables": variables,
                    "json_path": json_path,
                    "response_model": cls.__name__,
                    "next_retry_delay_s": attempt * 2,
                },
            )

            await asyncio.sleep(attempt * 2)
            attempt += 1
