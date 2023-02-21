import argparse
import subprocess
import time

DB_NAME = 'content_inventory'
TABLE_NAME = 'rankings_v1'

def ingest(db_name=DB_NAME, table_name=TABLE_NAME):
    for i in range(1, 7):
        t1 = time.perf_counter()
        print(f'Ingesting data/rankings_{i}.csv')
        subprocess.Popen(f'docker-compose run clickhouse-server clickhouse-client -d {db_name} -h clickhouse-server -q "INSERT INTO {table_name} FORMAT CSV" <  data/rankings_{i}.csv', shell=True)
        print(f'Finished in {time.perf_counter() - t1} seconds')


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=str, default=DB_NAME)
    parser.add_argument('--table', type=str, default=TABLE_NAME)

    args = parser.parse_args()
    ingest(args.db, args.table)


if __name__ == '__main__':
    cli()
