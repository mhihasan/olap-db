from aiochclient import ChClient
from aiohttp import ClientSession


async def main():
    async with ClientSession() as s:
        client = ChClient(s)
        assert await client.is_alive()  # returns True if connection is Ok
        r = await client.execute("SELECT 1")  # returns list of rows
        print(r)

        # or use async context manager
        async with ChClient(s) as client:
            await client.execute("SELECT 1")

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
