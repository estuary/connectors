import asyncio
import source_hackernews

asyncio.run(source_hackernews.Connector().serve())
