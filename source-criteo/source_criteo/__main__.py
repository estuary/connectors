import asyncio

import source_criteo

asyncio.run(source_criteo.Connector().serve())
