import asyncio
import source_sage_intacct

asyncio.run(source_sage_intacct.Connector().serve())
