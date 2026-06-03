from typing import Protocol

from estuary_cdk.capture.common import FetchChangesFn, FetchPageFn, RecurringFetchPageFn
from estuary_cdk.capture.document import BaseDocument


# To avoid storing all fetch_changes and fetch_page function in memory, factory functions
# are used to create the fetch functions on demand for a specific account.
class FetchChangesFnFactory(Protocol):
    """Factory function for creating incremental fetch functions for a specific account."""

    def __call__(self, account_id: str | None) -> FetchChangesFn[BaseDocument]: ...


class FetchPageFnFactory(Protocol):
    """Factory function for creating backfill fetch functions for a specific account."""

    def __call__(
        self, account_id: str | None
    ) -> FetchPageFn[BaseDocument] | RecurringFetchPageFn[BaseDocument] | None: ...
