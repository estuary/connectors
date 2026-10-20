#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

# Importing `.entrypoint` is load-bearing beyond the re-export: at module scope it runs
# `init_logger("airbyte")`, which raises the "airbyte" logger to INFO. Without it the CDK's
# own retry/backoff logging sits below the default WARNING threshold and is silently dropped.
from .entrypoint import AirbyteEntrypoint
from .logger import AirbyteLogger

__all__ = ["AirbyteEntrypoint", "AirbyteLogger"]
