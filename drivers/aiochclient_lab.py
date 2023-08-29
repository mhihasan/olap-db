import os

from aiochclient import ChClient
from aiohttp import ClientSession

CLICKHOUSE_URL = os.getenv('CLICKHOUSE_URL', 'http://localhost:8123')

async def main():
    async with ClientSession() as s:
        async with ChClient(s, url=CLICKHOUSE_URL) as client:
            is_alive = await client.is_alive()
            await client.execute("SELECT 1")
            await client.execute()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
