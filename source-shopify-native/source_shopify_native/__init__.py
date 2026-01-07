from logging import Logger
from typing import Callable, Awaitable, Union

from estuary_cdk.flow import (
    ConnectorSpec,
    ValidationError,
)
from estuary_cdk.capture import (
    BaseCaptureConnector,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.capture.common import ResourceConfig

from .resources import all_resources, validate_credentials
from .models import (
    ConnectorState,
    EndpointConfig,
    OAUTH2_SPEC,
    ShopifyOpen,
)


# Custom Request type that uses ShopifyOpen for cleaning hybrid state during parsing.
# This is necessary because Flow's checkpoint merging can create hybrid state when
# migrating from flat to dict-based format, which Pydantic cannot parse.
ShopifyRequest = Union[
    request.Spec,
    request.Discover[EndpointConfig],
    request.Validate[EndpointConfig, ResourceConfig],
    ShopifyOpen,
]


def _bindings_use_multi_store_keys(bindings: list | None) -> bool:
    """Check if any binding uses the multi-store key structure (/_meta/store in key)."""
    if not bindings:
        return False
    return any("/_meta/store" in b.collection.key for b in bindings)


def _validate_multi_store_transition(
    validate_req: request.Validate[EndpointConfig, ResourceConfig],
) -> None:
    """Validate that adding multiple stores to a legacy capture requires backfill acknowledgment.

    Raises ValidationError if:
    - Existing capture uses legacy keys (["/id"] without /_meta/store)
    - Multiple stores are now configured
    - User hasn't incremented backfill counters for all bindings
    """
    # New capture - no validation needed
    if validate_req.lastCapture is None:
        return

    # Already using multi-store keys - no validation needed
    if _bindings_use_multi_store_keys(validate_req.lastCapture.bindings):
        return

    # Single store - no validation needed (legacy mode continues)
    if len(validate_req.config.stores) == 1:
        return

    # Multiple stores on legacy capture - require backfill acknowledgment
    if not validate_req.lastCapture.bindings:
        return

    prev_backfill_counters = {
        b.resourceConfig.name: b.backfill
        for b in validate_req.lastCapture.bindings
    }

    unacknowledged_bindings = [
        binding.resourceConfig.name
        for binding in validate_req.bindings
        if binding.resourceConfig.name in prev_backfill_counters
        and binding.backfill <= prev_backfill_counters[binding.resourceConfig.name]
    ]

    if unacknowledged_bindings:
        raise ValidationError([
            "Adding multiple stores to an existing capture requires a full backfill because "
            "the document key structure must change to include the store identifier. "
            f"Please increment the backfill counter for these bindings: {', '.join(unacknowledged_bindings)}"
        ])


def _validate_key_structure_mismatch(
    validate_req: request.Validate[EndpointConfig, ResourceConfig],
    use_multi_store_keys: bool,
) -> None:
    """Validate that bindings with mismatched key structures require backfill.

    This handles the case where:
    1. A legacy capture transitioned to multi-store keys
    2. Some bindings were disabled during the transition
    3. Those disabled bindings still have ["/id"] keys
    4. When re-enabled, they need backfill to get ["/_meta/store", "/id"] keys
    """
    if validate_req.lastCapture is None:
        return

    if not use_multi_store_keys:
        return  # Legacy mode, no key mismatch possible

    # Build map of previous backfill counters
    prev_backfill_counters = {
        b.resourceConfig.name: b.backfill
        for b in (validate_req.lastCapture.bindings or [])
    }

    # Build map of previous collection keys
    prev_collection_keys = {
        b.resourceConfig.name: b.collection.key
        for b in (validate_req.lastCapture.bindings or [])
    }

    expected_key = ["/_meta/store", "/id"]
    mismatched_bindings: list[str] = []

    for binding in validate_req.bindings:
        name = binding.resourceConfig.name
        prev_key = prev_collection_keys.get(name)

        # If binding existed before and has old key structure
        if prev_key is not None and "/_meta/store" not in prev_key:
            # Check if backfill was acknowledged
            prev_backfill = prev_backfill_counters.get(name, 0)
            if binding.backfill <= prev_backfill:
                mismatched_bindings.append(name)

    if mismatched_bindings:
        raise ValidationError([
            f"The following bindings have outdated key structure (['/id'] instead of {expected_key}) "
            "and require backfill to include the store identifier. "
            f"Please increment the backfill counter for: {', '.join(mismatched_bindings)}"
        ])


def _should_use_multi_store_keys(
    bindings: list | None,
    num_stores: int,
) -> bool:
    """Determine whether to use multi-store keys in collection key structure.

    Returns True (use ["/_meta/store", "/id"]) for:
    - New captures (no existing bindings)
    - Existing captures already using multi-store keys
    - Legacy captures transitioning to multiple stores (after backfill acknowledgment)

    Returns False (use ["/id"]) for:
    - Legacy captures with single store (backward compatibility)
    """
    # No existing bindings = new capture, always use multi-store keys
    if not bindings:
        return True

    # Check existing key structure
    if _bindings_use_multi_store_keys(bindings):
        return True

    # Legacy capture - use multi-store keys only if multiple stores configured
    # (validation already ensured backfill was acknowledged)
    return num_stores > 1


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return ShopifyRequest

    async def spec(self, log: Logger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=OAUTH2_SPEC,
            documentationUrl="https://go.estuary.dev/source-shopify-native",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: Logger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        # Discover is only called for new captures, always use multi-store keys
        resources = await all_resources(log, self, discover.config, use_multi_store_keys=True)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)

        # Validate multi-store transition (legacy capture adding stores needs backfill)
        _validate_multi_store_transition(validate)

        # Determine key structure based on existing bindings
        use_multi_store_keys = _should_use_multi_store_keys(
            validate.lastCapture.bindings if validate.lastCapture else None,
            len(validate.config.stores),
        )

        # Validate that bindings with old key structure get backfilled
        # (handles re-enabling bindings that were disabled during multi-store transition)
        _validate_key_structure_mismatch(validate, use_multi_store_keys)

        resources = await all_resources(
            log, self, validate.config, use_multi_store_keys=use_multi_store_keys
        )
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        # Determine key structure from existing bindings
        use_multi_store_keys = _should_use_multi_store_keys(
            open.capture.bindings,
            len(open.capture.config.stores),
        )

        resources = await all_resources(
            log, self, open.capture.config,
            should_cancel_ongoing_job=True,
            use_multi_store_keys=use_multi_store_keys,
        )
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
