import asyncio
import source_genesys

asyncio.run(source_genesys.Connector().serve())
