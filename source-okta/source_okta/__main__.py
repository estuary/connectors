import asyncio
import source_okta

asyncio.run(source_okta.Connector().serve())
