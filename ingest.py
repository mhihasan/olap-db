import argparse
import os
import subprocess
import time

DB_NAME = 'content_inventory'
TABLE_NAME = 'rankings_v1'

"""
docker-compose run clickhouse-server clickhouse-client -d content_inventory -h clickhouse-server -q "INSERT INTO rankings_v1 FORMAT CSV" <  rankings_data_en-us/rankings_en-us_1.csv
"""


def ingest(db_name=DB_NAME, table_name=TABLE_NAME):
    for i in range(1, 2138):
        t1 = time.perf_counter()
        print(f'Ingesting data/rankings_{i}.csv')
        subprocess.Popen(f'docker-compose run clickhouse-server clickhouse-client -d {db_name} -h clickhouse-server -q "INSERT INTO {table_name} FORMAT CSV" <  rankings_data_en-us/rankings_{i}.csv', shell=True)
        print(f'Finished in {time.perf_counter() - t1} seconds')


def ingest_file(file_name):
    print(f"Ingesting {file_name}")
    t1 = time.perf_counter()
    subprocess.Popen(
        f'docker-compose run clickhouse-server clickhouse-client -d {DB_NAME} -h clickhouse-server -q "INSERT INTO {TABLE_NAME} FORMAT CSV" <  {file_name}',
        shell=True)
    print(f'Finished in {time.perf_counter() - t1} seconds')



def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', type=str, default=DB_NAME)
    parser.add_argument('--table', type=str, default=TABLE_NAME)

    args = parser.parse_args()
    ingest(args.db, args.table)


if __name__ == '__main__':
    with open('ingest.sh', 'w+') as file_writer:
        file_writer.write("#!/usr/bin/env bash")
        file_writer.write("set -e")
        folder_names = ['rankings_data_en-us', 'rankings_data_en-uk', 'rankings_data']
        for folder_name in folder_names:
            files = os.listdir(folder_name)
            for f in files:
                file_writer.write(f'docker-compose run clickhouse-server clickhouse-client -d {DB_NAME} -h clickhouse-server -q "INSERT INTO {TABLE_NAME} FORMAT CSV" < {folder_name}/{f}')
