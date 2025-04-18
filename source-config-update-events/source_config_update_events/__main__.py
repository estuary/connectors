import asyncio
import source_config_update_events

asyncio.run(source_config_update_events.Connector().serve())
