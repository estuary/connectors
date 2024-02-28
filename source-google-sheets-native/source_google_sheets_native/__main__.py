import asyncio
import source_google_sheets_native

asyncio.run(source_google_sheets_native.Connector().serve())
