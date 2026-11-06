import asyncio

import source_mailchimp_native

asyncio.run(source_mailchimp_native.Connector().serve())
