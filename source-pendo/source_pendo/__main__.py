import asyncio
import source_pendo

asyncio.run(source_pendo.Connector().serve())
