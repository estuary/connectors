import asyncio

import source_azure_servicebus

asyncio.run(source_azure_servicebus.Connector().serve())
