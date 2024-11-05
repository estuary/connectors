import logging
from typing import Any, List, Mapping

from airbyte_cdk.config_observation import create_connector_config_control_message
from airbyte_cdk.entrypoint import AirbyteEntrypoint
from airbyte_cdk.sources import Source
from airbyte_cdk.sources.message import InMemoryMessageRepository, MessageRepository

logger = logging.getLogger("airbyte_logger")


class MigrateAccountId:

    message_repository: MessageRepository = InMemoryMessageRepository()
    migrate_from_key: str = "account_id"
    migrate_to_key: str = "account_ids"

    @classmethod
    def should_migrate(cls, config: Mapping[str, Any]) -> bool:
        return False if config.get(cls.migrate_to_key) else True

    @classmethod
    def transform(cls, config: Mapping[str, Any]) -> Mapping[str, Any]:
        config[cls.migrate_to_key] = [config[cls.migrate_from_key]]
        # return transformed config
        return config

    @classmethod
    def modify_and_save(cls, config_path: str, source: Source, config: Mapping[str, Any]) -> Mapping[str, Any]:
        # modify the config
        migrated_config = cls.transform(config)
        # save the config
        source.write_config(migrated_config, config_path)
        # return modified config
        return migrated_config

    @classmethod
    def emit_control_message(cls, migrated_config: Mapping[str, Any]) -> None:
        # add the Airbyte Control Message to message repo
        cls.message_repository.emit_message(create_connector_config_control_message(migrated_config))
        # emit the Airbyte Control Message from message queue to stdout
        for message in cls.message_repository._message_queue:
            print(message.json(exclude_unset=True))

    @classmethod
    def migrate(cls, args: List[str], source: Source) -> None:
        # get config path
        config_path = AirbyteEntrypoint(source).extract_config(args)
        # proceed only if `--config` arg is provided
        if config_path:
            # read the existing config
            config = source.read_config(config_path)
            # migration check
            if cls.should_migrate(config):
                cls.emit_control_message(
                    cls.modify_and_save(config_path, source, config),
                )