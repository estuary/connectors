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
    ConnectorSpec,
    ValidationError,
)
from estuary_cdk.logger import FlowLogger

from .models import (
    ConnectorState,
    EndpointConfig,
)
from .resources import all_resources, validate_credentials


def _validate_config(
    validate_req: request.Validate[EndpointConfig, ResourceConfig],
) -> None:
    """Validate config constraints around composite keys and multi-store.

    1. Block multi-store when composite keys are disabled (legacy captures).
    2. Require re-discovery when the composite key flag is toggled so the
       catalog reflects the new key structure.
    3. When composite keys are being enabled, require dataflow reset for all
       bindings that still have non-composite keys.
    """
    config = validate_req.config

    # Legacy captures can't add stores without enabling composite keys.
    if not config.advanced.should_use_composite_key and len(config.stores) > 1:
        raise ValidationError(
            [
                "Adding multiple stores to a capture requires enabling composite keys. "
                "Set advanced.should_use_composite_key to true and backfill all enabled "
                "bindings with a dataflow reset to change collection keys to include "
                "the store identifier (/_meta/store)."
            ]
        )

    if validate_req.lastCapture is None:
        return

    # Extract the previous composite key flag directly from the raw config
    # dict rather than parsing a full EndpointConfig. The previous config may
    # contain SOPS-encrypted credential fields (e.g. client_id_sops) that
    # would fail strict model validation.
    prev_raw = validate_req.lastCapture.config.config
    if isinstance(prev_raw, dict) and "stores" not in prev_raw and "store" in prev_raw:
        # Legacy flat config predates composite keys.
        prev_use_composite_key = False
    else:
        prev_advanced = prev_raw.get("advanced", {}) if isinstance(prev_raw, dict) else {}
        prev_use_composite_key = prev_advanced.get("should_use_composite_key", True) if isinstance(prev_advanced, dict) else True

    # Detect whether the composite key flag was toggled since the last capture.
    flag_toggled = (
        config.advanced.should_use_composite_key
        != prev_use_composite_key
    )

    # Toggling the flag changes collection keys, so the user must re-discover
    # to update the catalog before validation can proceed. Skip this check if
    # the current bindings already reflect the expected key structure, meaning
    # the user has already re-discovered after toggling.
    if flag_toggled:
        if config.advanced.should_use_composite_key:
            already_discovered = all(
                "/_meta/store" in b.collection.key for b in validate_req.bindings
            )
        else:
            already_discovered = all(
                "/_meta/store" not in b.collection.key for b in validate_req.bindings
            )

        if not already_discovered:
            current = "enabled" if config.advanced.should_use_composite_key else "disabled"
            raise ValidationError(
                [
                    f"Composite keys have been {current}. "
                    "Please click 'Discover' to update the catalog with the new "
                    "collection key structure before publishing."
                ]
            )

    # When composite keys are enabled, verify existing non-composite bindings
    # have been backfilled (dataflow reset) so the key transition is safe.
    if config.advanced.should_use_composite_key:
        prev_bindings = {
            b.resourceConfig.name: b for b in validate_req.lastCapture.bindings
        }
        if any("/_meta/store" not in b.collection.key for b in prev_bindings.values()):
            needs_backfill: list[str] = []
            for binding in validate_req.bindings:
                prev = prev_bindings.get(binding.resourceConfig.name)
                if prev is None:
                    continue
                if "/_meta/store" in prev.collection.key:
                    continue  # Already composite
                if binding.backfill > prev.backfill:
                    continue  # Backfill acknowledged
                needs_backfill.append(binding.resourceConfig.name)
            if needs_backfill:
                raise ValidationError(
                    [
                        "Enabling composite keys requires a dataflow reset to change "
                        "collection keys from [/id] to [/_meta/store, /id]. "
                        f"Please increment the backfill counter for: {', '.join(needs_backfill)}"
                    ]
                )


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
        resources = await all_resources(log, self, discover.config)
        return common.discovered(resources)

    async def validate(
        self,
        log: FlowLogger,
        validate: request.Validate[EndpointConfig, ResourceConfig],
    ) -> response.Validated:
        await validate_credentials(log, self, validate.config)
        _validate_config(validate)

        resources = await all_resources(log, self, validate.config)
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

        resources = await all_resources(
            log, self, config, should_cancel_ongoing_job=True
        )
        resolved = common.resolve_bindings(open.capture.bindings, resources)
        return common.open(open, resolved)
