import asyncio
from logging import Logger
from pydantic import BaseModel
from typing import Any, Generic, TypeVar

from estuary_cdk.http import HTTPSession
from source_shopify_native.models import (
    GraphQLErrorCode,
    GraphQLResponse,
)

from .common import VERSION


T = TypeVar('T')


DEFAULT_MAXIMUM_AVAILABLE = 2000
DEFAULT_RESTORE_RATE = 100
INITIAL_RETRY_DELAY = 2
MAX_RETRY_DELAY = 60
MAX_RETRY_COUNT = 10


class QueryError(RuntimeError):
    """Exception raise for non-retryable errors when executing a GraphQL query."""
    def __init__(
        self,
        message: str,
        query: str | None = None,
        errors: list[str] | None = None,
    ):
        self.message = message
        self.query = query
        self.errors = errors

        self.details: dict[str, Any] = {"message": self.message}

        if self.errors:
            self.details["errors"] = self.errors
        if self.query:
            self.details["query"] = self.query

        super().__init__(self.details)

class ShopifyGraphQLClient:
    def __init__(self, http: HTTPSession, store: str):
        self.http = http
        self.url = f"https://{store}.myshopify.com/admin/api/{VERSION}/graphql.json"
        # maximum_available and restore_rate can be different depending on the 
        # user's Shopify tier, and they are refreshed each time we receive a
        # successful response.
        self.maximum_available = DEFAULT_MAXIMUM_AVAILABLE
        self.restore_rate = DEFAULT_RESTORE_RATE

    async def request(self, query: str, data_model: type[T], log: Logger,) -> T:
        retry_count = 0
        retry_delay = INITIAL_RETRY_DELAY
        while True:
            response = GraphQLResponse[data_model].model_validate_json(
                await self.http.request(
                    log, self.url, method="POST", json={"query": query}
                )
            )

            if response.extensions and response.extensions.cost:
                throttle_status = response.extensions.cost.throttleStatus
                self.maximum_available = throttle_status.maximumAvailable
                self.restore_rate = throttle_status.restoreRate

            if response.errors is not None:
                is_throttled = False
                is_retryable = False
                fatal_errors: list[str] = []
                for error in response.errors:
                    match error.extensions.code:
                        case GraphQLErrorCode.THROTTLED:
                            is_throttled = True
                        case GraphQLErrorCode.INTERNAL_SERVER_ERROR:
                            is_retryable = True
                        case GraphQLErrorCode.UNDEFINED_FIELD | GraphQLErrorCode.ACCESS_DENIED | GraphQLErrorCode.SHOP_INACTIVE | GraphQLErrorCode.MAX_COST_EXCEEDED | GraphQLErrorCode.BAD_REQUEST:
                            fatal_errors.append(error.message)
                        case _:
                            raise QueryError(f"Unexpected GraphQLErrorCode: {error.extensions.code}", query=query, errors=[error.message])

                err_count = len(fatal_errors)
                if err_count > 0:
                    msg = f"{err_count} error{"s" if err_count > 1 else ""} when executing query."
                    raise QueryError(msg, query, fatal_errors)

                if is_throttled:
                    delay = self.maximum_available / self.restore_rate
                    log.info(f"Throttled by API. Sleeping {delay} seconds until cost points have been restored.")
                    await asyncio.sleep(delay)
                    continue

                if is_retryable:
                    if retry_count > MAX_RETRY_COUNT:
                        msg = f"Maximum number of retries exceeded ({MAX_RETRY_COUNT})."
                        raise QueryError(msg, query)

                    retry_count += 1
                    log.warning(f"Received {GraphQLErrorCode.INTERNAL_SERVER_ERROR} on attempt {retry_count} (will retry after {retry_delay} seconds)")
                    await asyncio.sleep(retry_delay)

                    retry_delay = min(MAX_RETRY_DELAY, retry_delay * 2)
                    continue

            if response.data is None:
                msg = "Query returned no data and no errors."
                raise QueryError(msg, query)

            return response.data
