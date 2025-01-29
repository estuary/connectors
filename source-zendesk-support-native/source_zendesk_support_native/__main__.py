import asyncio
import source_zendesk_support_native

asyncio.run(source_zendesk_support_native.Connector().serve())
