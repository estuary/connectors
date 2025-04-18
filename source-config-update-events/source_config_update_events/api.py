from datetime import datetime, UTC
import json
from typing import AsyncGenerator, Any, Mapping
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


def _sort_dict(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        return {k: _sort_dict(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, list):
        return [_sort_dict(item) for item in obj]
    else:
        return obj


async def _encrypt_config(
    http: HTTPSession,
    log: FlowLogger,
    config: EndpointConfig,
) -> str:
    assert isinstance(config, BaseModel)
    ENCRYPTION_URL = "https://config-encryption.estuary.dev/v1/encrypt-config"

    unencrypted_config = config.model_dump(mode="json", include=config.model_fields_set)

    body = {
        # mode="json" converts Python-specific concepts (like datetimes) to valid JSON.
        # include=config.model_fields_set ensures only the fields that are explicitly set on the
        # model. This means any default values that are set are included, but any fields that are unset
        # & fallback to some default value are left unset.
        "config": _sort_dict(unencrypted_config),
        "schema": config.model_json_schema(),
    }

    encrypted_config = await http.request(log, ENCRYPTION_URL, "POST", json=body, _with_token = False)

    # return json.loads(encrypted_config.decode('utf-8'))
    decoded_encrypted_config = encrypted_config.decode('utf-8')

    log.info("Encrypted config options:", {
        "json.loads(decoded_encrypted_config)": json.loads(decoded_encrypted_config)
    })

    # return decoded_encrypted_config

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
        # start_date=datetime.now(tz=UTC),
        credentials=FakeOAuth2Credentials(
            credentials_title="Fake OAuth Credentials",
            client_id=config.credentials.client_id,
            client_secret=config.credentials.client_secret,
            refresh_token=generate_fake_refresh_token()
        ),
        # advanced=config.advanced
    )

    # Encrypt the updated config if the associated setting is checked. The connector defaults to always encrypting the config.
    if config.advanced.should_encrypt:
        encrypted_config = await _encrypt_config(http, log, new_config)
    else:
        encrypted_config = new_config.model_dump()

    log.event.config_update("Some structured log with an updated, encrypted config", encrypted_config)

    yield datetime.now(tz=UTC)
