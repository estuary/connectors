import asyncio
import source_datadog

asyncio.run(source_datadog.Connector().serve())
