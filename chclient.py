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

"""
clickhouse-client \
    --host avw5r4qs3y.us-east-2.aws.clickhouse.cloud \
    --secure \
    --port 9440 \
    --password Myp@ssw0rd \
    --query "
    INSERT INTO rankings_v1 
    SELECT
        domain,
        date,
        url,
        term,
        rank, 
        volume, 
        cpc 
    FROM input('id UInt32, type String, author String, timestamp DateTime, comment String, children Array(UInt32)')
    FORMAT CSV 
" < comments.tsv

clickhouse-client -q "INSERT INTO rankings_v1 FORMAT CSV" <  data/rankings_1.csv
"""
