import asyncio

import source_incident_io

asyncio.run(source_incident_io.Connector().serve())
