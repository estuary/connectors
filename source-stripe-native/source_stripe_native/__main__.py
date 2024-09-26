import asyncio
import source_stripe_native

asyncio.run(source_stripe_native.Connector().serve())
