from logging import Logger

from estuary_cdk.logger import FlowLogger
from typing import Callable, Awaitable

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

from estuary_cdk.capture import Request

from .resources import all_resources, validate_credentials
from .models import (
    ConnectorState,
    EndpointConfig,
    OAUTH2_SPEC,
)


def _bindings_use_multi_store_keys(bindings: list[request.ValidateBinding]) -> bool:
    """Check if any binding uses the multi-store key structure (/_meta/store in key)."""
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

    # Multiple stores on legacy capture - require backfill acknowledgment since
    # lastCapture bindings did not have _meta/store in all bindings
    prev_backfill_counters = {
        b.resourceConfig.name: b.backfill
        for b in validate_req.lastCapture.bindings
    }

    unacknowledged_bindings: list[str] = []
    for binding in validate_req.bindings:
        name = binding.resourceConfig.name

        is_existing_binding = name in prev_backfill_counters
        backfill_acknowledged = binding.backfill > prev_backfill_counters.get(name, 0)

        if is_existing_binding and not backfill_acknowledged:
            unacknowledged_bindings.append(name)

    if unacknowledged_bindings:
        msgs = [
            "Adding multiple stores to an existing capture requires a full backfill because "
            "the document key structure must change to include the store identifier. "
            "Please increment the backfill counter for these bindings:"
        ]
        msgs.extend([f"- {name}" for name in unacknowledged_bindings])
        raise ValidationError(msgs)


def _validate_key_structure_mismatch(
    validate_req: request.Validate[EndpointConfig, ResourceConfig],
    use_store_in_key: bool,
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

    if not use_store_in_key:
        return  # Single store mode, no key mismatch possible

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

        has_legacy_key = prev_key is not None and "/_meta/store" not in prev_key
        backfill_acknowledged = binding.backfill > prev_backfill_counters.get(name, 0)

        if has_legacy_key and not backfill_acknowledged:
            mismatched_bindings.append(name)

    if mismatched_bindings:
        raise ValidationError([
            f"The following bindings have outdated key structure (['/id'] instead of {expected_key}) "
            "and require backfill to include the store identifier. "
            f"Please increment the backfill counter for: {', '.join(mismatched_bindings)}"
        ])


def _should_use_store_in_key(
    bindings: list | None,
    num_stores: int,
) -> bool:
    """Determine whether to include /_meta/store in collection keys for existing captures.

    Returns True (use ["/_meta/store", "/id"]) when:
    - Multiple stores are configured
    - Existing bindings already use store in key

    Returns False (use ["/id"]) when:
    - Legacy single-store capture that hasn't transitioned

    Note: New captures always use ["/_meta/store", "/id"] via discover().
    This function is only used for validate/open of existing captures.
    """
    # If bindings already use store in key, continue using it
    if bindings and _bindings_use_multi_store_keys(bindings):
        return True

    # Use store in key only when multiple stores configured
    return num_stores > 1


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

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
        resources = await all_resources(log, self, discover.config, use_store_in_key=True)
        return common.discovered(resources)

    async def validate(
        self,
        log: Logger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)

        # Validate multi-store transition (requires backfill for key change)
        _validate_multi_store_transition(validate)

        # Determine key structure (affects collection keys only)
        use_store_in_key = _should_use_store_in_key(
            validate.lastCapture.bindings if validate.lastCapture else None,
            len(validate.config.stores),
        )

        # Validate key structure mismatch
        _validate_key_structure_mismatch(validate, use_store_in_key)

        resources = await all_resources(
            log, self, validate.config, use_store_in_key=use_store_in_key
        )
        resolved = common.resolve_bindings(validate.bindings, resources)
        return common.validated(resolved)

    async def open(
        self,
        log: FlowLogger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState],
    ) -> tuple[response.Opened, Callable[[Task], Awaitable[None]]]:
        config = open.capture.config

        # Persist migrated config
        log.debug(f"Config migration flag: {getattr(config, '_was_migrated', False)} - {config._was_migrated}")
        if config._was_migrated:
            log.info("Persisting migrated config to multi-store format")
            encrypted_config = await self._encrypt_config(log, config)
            log.event.config_update(
                "Migrating legacy single-store config to multi-store format.",
                encrypted_config
            )

        # Determine key structure from bindings
        use_store_in_key = _should_use_store_in_key(
            open.capture.bindings,
            len(config.stores),
        )

        resources = await all_resources(
            log, self, config,
            should_cancel_ongoing_job=True,
            use_store_in_key=use_store_in_key,
        )
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
