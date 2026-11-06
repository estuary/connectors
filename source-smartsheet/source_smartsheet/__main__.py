import asyncio

import source_smartsheet

asyncio.run(source_smartsheet.Connector().serve())
