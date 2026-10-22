#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import time
from typing import MutableMapping

from airbyte_cdk.models import AirbyteControlConnectorConfigMessage, AirbyteControlMessage, AirbyteMessage, OrchestratorType, Type


def emit_configuration_as_airbyte_control_message(config: MutableMapping):
    """
    WARNING: deprecated - emit_configuration_as_airbyte_control_message is being deprecated in favor of the MessageRepository mechanism.
    See the airbyte_cdk.sources.message package
    """
    airbyte_message = create_connector_config_control_message(config)
    print(airbyte_message.model_dump_json(exclude_unset=True))


def create_connector_config_control_message(config):
    control_message = AirbyteControlMessage(
        type=OrchestratorType.CONNECTOR_CONFIG,
        emitted_at=time.time() * 1000,
        connectorConfig=AirbyteControlConnectorConfigMessage(config=config),
    )
    return AirbyteMessage(type=Type.CONTROL, control=control_message)
