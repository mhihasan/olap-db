import os

from clickhouse_driver import Client as ClickhouseClient

regular_credentials = {
    'host': '38.130.229.181',
    'database': 'content_inventory',
    'user': 'content_inventory_user',
    'password': 'AB0przbfN97cosoriuHrKZ2'
}

default_credentials = {
    'host': '38.130.229.181',
    # 'port': 9000,
    'password': 'xr0przbfN97cosoriuHrKZ2',
    "database": "content_inventory"
}


def main():
    with ClickhouseClient(**regular_credentials) as client:
        print("Connected!")
        print(client.execute("SHOW TABLES"))
        # client.execute('SHOW TABLES')
        # client.execute('SHOW CREATE TABLE content_inventory.ranking_urls')
        # client.execute('SELECT * FROM content_inventory.ranking_urls LIMIT 10')


if __name__ == '__main__':
    main()
