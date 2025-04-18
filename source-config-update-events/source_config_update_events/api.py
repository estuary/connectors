from datetime import datetime, UTC
import json
from typing import AsyncGenerator, Any
from pydantic import BaseModel
import secrets
import base64

from estuary_cdk.capture.common import LogCursor
from estuary_cdk.http import HTTPSession
from estuary_cdk.logger import FlowLogger

from .models import EndpointConfig, FakeOAuth2Credentials


def generate_fake_refresh_token(length_bytes=32):
    token_bytes = secrets.token_bytes(length_bytes)
    return base64.urlsafe_b64encode(token_bytes).rstrip(b'=').decode('utf-8')


async def _encrypt_config(
    http: HTTPSession,
    log: FlowLogger,
    config: EndpointConfig,
) -> dict[str, Any]:
    assert isinstance(config, BaseModel)
    ENCRYPTION_URL = "https://config-encryption.estuary.dev/v1/encrypt-config"

    body = {
        # mode="json" converts Python-specific concepts (like datetimes) to valid JSON.
        "config": config.model_dump(mode="json"),
        "schema": config.model_json_schema(),
    }

    encrypted_config = await http.request(log, ENCRYPTION_URL, "POST", json=body, _with_token = False)

    return json.loads(encrypted_config.decode('utf-8'))


async def fetch_resources(
    http: HTTPSession,
    config: EndpointConfig,
    log: FlowLogger,
    log_cursor: LogCursor,
) -> AsyncGenerator[LogCursor, None]:
    # Log out a configUpdate event.

    # Generate an updated config.
    new_config = EndpointConfig(
        start_date=datetime.now(tz=UTC),
        credentials=FakeOAuth2Credentials(
            credentials_title="Fake OAuth Credentials",
            client_id=config.credentials.client_id,
            client_secret=config.credentials.client_secret,
            refresh_token=generate_fake_refresh_token()
        ),
        advanced=config.advanced
    )

    # Encrypt the updated config if the associated setting is checked. The connector defaults to always encrypting the config.
    if config.advanced.should_encrypt:
        encrypted_config = await _encrypt_config(http, log, new_config)
    else:
        encrypted_config = new_config.model_dump()

    log.event.config_update("Some structured log with an updated, encrypted config", encrypted_config)

    yield datetime.now(tz=UTC)
