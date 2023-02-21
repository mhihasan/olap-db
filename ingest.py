import os
import subprocess

DB_NAME = 'content_inventory'
TABLE_NAME = 'rankings_v1'

def ingest():
    for root, dirs, files in os.walk('data'):
        for file in files:
            print(file)
            subprocess.Popen(f'docker-compose run clickhouse-server clickhouse-client -d {DB_NAME} -h clickhouse-server -q "INSERT INTO {TABLE_NAME} FORMAT CSV" <  data/{file}', shell=True)


if __name__ == '__main__':
    ingest()
