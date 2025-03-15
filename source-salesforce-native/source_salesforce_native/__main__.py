import asyncio
import source_salesforce_native

asyncio.run(source_salesforce_native.Connector().serve())
