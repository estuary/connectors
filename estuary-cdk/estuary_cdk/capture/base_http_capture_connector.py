from typing import Any
from logging import Logger

from pydantic import BaseModel

from . import request, BaseCaptureConnector
from .common import ConnectorState
from ..flow import (
    EndpointConfig,
    ResourceConfig,
    RotatingOAuth2Credentials,
)
from ..encryption import encrypt, decrypt
from ..http import HTTPError, HTTPMixin, TokenSource


class BaseHTTPCaptureConnector(
    BaseCaptureConnector[EndpointConfig, ResourceConfig, ConnectorState],
    HTTPMixin,
):
    # _encrypt_config calls the config encryption endpoint & returns the encrypted config.
    async def _encrypt_config(
        self,
        log: Logger,
        config: EndpointConfig,
    ) -> bytes:
        assert isinstance(config, BaseModel)
        ENCRYPTION_URL = "https://config-encryption.estuary.dev/v1/encrypt-config"

        body = {
            # mode="json" converts Python-specific concepts (like datetimes) to valid JSON.
            "config": config.model_dump(mode="json"),
            "schema": config.model_json_schema(),
        }

        encrypted_config = await self.request(log, ENCRYPTION_URL, "POST", json=body)

        return encrypted_config

    async def _setup(
        self,
        log: Logger,
        open: request.Open[EndpointConfig, ResourceConfig, ConnectorState]
    ):
        assert isinstance(self.token_source, TokenSource)

        # Rotate refresh tokens for credentials with periodically expiring refresh tokens.
        if isinstance(self.token_source.credentials, RotatingOAuth2Credentials):
            try:
                # Fetch a fresh set of tokens.
                resp_1 = await self.token_source.initialize_oauth_tokens(log, self)

                # Update the state with the refresh token we received to ensure the
                # connector's state always has a valid refresh token.
                self._checkpoint(
                    state=ConnectorState(
                        refresh_token=encrypt(
                            resp_1.refresh_token,
                            open.capture.config.credentials.refresh_token, # type: ignore
                        ),
                    )
                )

            except HTTPError as err:
                # Is there a better way to detect if the config's refresh token is invalid based on the connector?
                # Maybe allow connectors to specify an error code & message for expired refresh tokens?
                if  500 > err.code >= 400 and "invalid_grant" in err.message:
                    assert isinstance(open.state.refresh_token, str)
                    assert isinstance(open.capture.config.credentials, RotatingOAuth2Credentials) # type: ignore

                    old_config_token = open.capture.config.credentials.refresh_token # type: ignore

                    # Set token source to use decrypted token from state.
                    self.token_source.credentials.refresh_token = decrypt(open.state.refresh_token, old_config_token)

                    # Get new config token.
                    new_config_token = (await self.token_source.initialize_oauth_tokens(
                        log,
                        self,
                    )).refresh_token

                    # Get new state token.
                    new_state_token = (await self.token_source.initialize_oauth_tokens(
                        log,
                        self,
                    )).refresh_token

                    # Emit a checkpoint with the new state token.
                    self._checkpoint(
                        state=ConnectorState(
                            refresh_token=encrypt(
                                value=new_state_token,
                                # The state token is always encrypted with the current config's token, _not_ the
                                # to-be-published config's token. This is done to prevent breaking tasks & requiring
                                # users to reauthenticate. If we used the to-be-published config's token to encrypt but for some
                                # reason the publication doesn't go through, the connector's state will have a token encrypted
                                # by a token we can no longer access.
                                key=old_config_token,
                            ),
                        )
                    )

                    # Update the connector's config with the new config token.
                    open.capture.config.credentials.refresh_token = new_config_token # type: ignore

                    # Encrypt the updated config and emit an event telling the control plane to publish a new spec.
                    encrypted_config = await self._encrypt_config(log, open.capture.config)
                    log.info("Some structured log with the encrypted config", {
                        # Name might need to be different. Need to coordinate with what the control plane expects.
                        "newConfig": encrypted_config,
                        # Need to confirm what eventType this should be.
                        "eventType": "configUpdate",
                    })
                else:
                    raise
