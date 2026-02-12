from typing import Awaitable, Callable

from estuary_cdk.capture import (
    BaseCaptureConnector,
    Request,
    Task,
    common,
    request,
    response,
)
from estuary_cdk.capture.common import ResourceConfig
from estuary_cdk.flow import (
    CaptureBinding,
    ConnectorSpec,
    ValidationError,
)
from estuary_cdk.logger import FlowLogger

from .models import (
    ConnectorState,
    EndpointConfig,
)
from .resources import all_resources, validate_credentials


def _validate_binding_keys(
    validate_req: request.Validate[EndpointConfig, ResourceConfig],
) -> None:
    """Require backfill when transitioning from single-store to multi-store keys.

    When adding multiple stores to a legacy capture, existing bindings need backfill
    because the document key structure must change from ["/id"] to ["/_meta/store", "/id"].

    In a mixed-key capture (see discover()), bindings added after the initial capture
    already have composite keys and won't need backfill here — only legacy ["/id"]
    bindings require it.
    """
    num_stores = len(validate_req.config.stores)
    if num_stores <= 1 or validate_req.lastCapture is None:
        return

    prev_bindings = {
        b.resourceConfig.name: b
        for b in validate_req.lastCapture.bindings
    }

    if all("/_meta/store" in b.collection.key for b in prev_bindings.values()):
        return

    needs_backfill: list[str] = []
    for binding in validate_req.bindings:
        name = binding.resourceConfig.name
        prev = prev_bindings.get(name)
        if prev is None:
            continue
        if "/_meta/store" in prev.collection.key:
            continue
        if binding.backfill > prev.backfill:
            continue
        needs_backfill.append(name)

    if needs_backfill:
        raise ValidationError([
            "Adding multiple stores to an existing capture requires a full backfill because "
            "the document key structure must change to include the store identifier. "
            f"Please increment the backfill counter for: {', '.join(needs_backfill)}"
        ])


def _should_use_store_in_key(
    bindings: list[request.ValidateBinding] | list[CaptureBinding] | None,
    num_stores: int,
) -> bool:
    """Determine whether to include /_meta/store in collection keys for existing captures.

    Returns True (use ["/_meta/store", "/id"]) when either:
    - Multiple stores are configured, OR
    - Existing bindings already use store in key (prevents key regression
      when going from multi-store back to single-store)

    Returns False (use ["/id"]) when:
    - Legacy single-store capture that hasn't transitioned

    Note: New captures always use ["/_meta/store", "/id"] via discover().
    This function is only used for validate/open of existing captures.
    """
    # If bindings already use store in key, continue using it
    if bindings and any("/_meta/store" in b.collection.key for b in bindings):
        return True

    # Use store in key only when multiple stores configured
    return num_stores > 1


class Connector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
):
    def request_class(self):
        return Request[EndpointConfig, ResourceConfig, ConnectorState]

    async def spec(self, log: FlowLogger, _: request.Spec) -> ConnectorSpec:
        return ConnectorSpec(
            configSchema=EndpointConfig.model_json_schema(),
            oauth2=None,
            documentationUrl="https://go.estuary.dev/source-shopify-native",
            resourceConfigSchema=ResourceConfig.model_json_schema(),
            resourcePathPointers=ResourceConfig.PATH_POINTERS,
        )

    async def discover(
        self, log: FlowLogger, discover: request.Discover[EndpointConfig]
    ) -> response.Discovered[ResourceConfig]:
        # Always discover with composite keys ["/_meta/store", "/id"]. This means legacy
        # single-store captures may have mixed keys: old bindings retain ["/id"] while
        # newly discovered bindings get ["/_meta/store", "/id"]. This is intentional —
        # it reduces the number of bindings requiring backfill if a store is added later,
        # since only the legacy ["/id"] bindings need a key change.
        resources = await all_resources(
            log, self, discover.config, use_store_in_key=True
        )
        return common.discovered(resources)

    async def validate(
        self,
        log: FlowLogger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)

        num_stores = len(validate.config.stores)
        use_store_in_key = _should_use_store_in_key(
            validate.lastCapture.bindings if validate.lastCapture else None,
            num_stores,
        )
        _validate_binding_keys(validate)

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

        if config._was_migrated:
            encrypted_config = await self._encrypt_config(log, config)
            log.event.config_update(
                "Migrating legacy single-store config to multi-store format.",
                encrypted_config,
            )

        use_store_in_key = _should_use_store_in_key(
            open.capture.bindings,
            len(config.stores),
        )

        resources = await all_resources(
            log,
            self,
            config,
            should_cancel_ongoing_job=True,
            use_store_in_key=use_store_in_key,
        )
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
