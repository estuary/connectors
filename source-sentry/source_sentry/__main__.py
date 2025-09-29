import asyncio
import source_sentry

asyncio.run(source_sentry.Connector().serve())
