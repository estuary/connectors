from datetime import datetime, UTC
from estuary_cdk.http import HTTPSession
from logging import Logger
from pydantic import TypeAdapter
import json
import pytz
from typing import Iterable, Any, Callable, Awaitable, AsyncGenerator, Dict
import asyncio
import itertools
from copy import deepcopy

from estuary_cdk.capture.common import (
    PageCursor,
    LogCursor,
)
