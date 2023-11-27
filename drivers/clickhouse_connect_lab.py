import ast
import os
import time

import clickhouse_connect
import pandas as pd
os.environ['AWS_PROFILE'] = 'mm'

# client = clickhouse_connect.get_client(host='154.27.75.107', port=8123, username='default')
# print(client.execute('SHOW DATABASES'))
# import clickhouse_connect

client = clickhouse_connect.get_client(host='154.27.75.*', username='default', port=8123)

DATABASE_NAME = 'content_inventory'
TABLE_NAME = 'ranking_urls'


def get_df(filename='ranking_urls_short.csv'):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    df = pd.read_csv(os.path.join(base_dir, f'../{filename}'))
    df['date'] = pd.to_datetime(df['date'])
    for col in ['category_strings' , 'serp_features']:
        df[col] = df[col].apply(ast.literal_eval)
    return df


def main():
    t = time.perf_counter()
    # df = pd.read_csv('s3://zappa-marketmuse-admin-prod/ranking_urls/en-us/2/0.csv')
    df = get_df(filename='ranking_urls.csv')
    print("Read CSV: ", time.perf_counter() - t)
    t = time.perf_counter()
    print(client.insert_df(table=TABLE_NAME, df=df, database=DATABASE_NAME))
    print("Inserted: ", time.perf_counter() - t)


if __name__ == '__main__':
    main()
