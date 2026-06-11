import asyncio
from . import Connector

asyncio.run(Connector().serve())
