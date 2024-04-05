import asyncio

import source_netsuite

asyncio.run(source_netsuite.Connector().serve())
