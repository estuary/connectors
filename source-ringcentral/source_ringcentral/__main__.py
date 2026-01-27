import asyncio
import source_ringcentral

asyncio.run(source_ringcentral.Connector().serve())
