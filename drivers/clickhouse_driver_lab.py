import ast
import os
import time

import pandas as pd
from clickhouse_driver import Client


def get_df(filename='ranking_urls_short.csv'):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    df = pd.read_csv(os.path.join(base_dir, f'../{filename}'))
    df['date'] = pd.to_datetime(df['date'])
    for col in ['category_strings' , 'serp_features']:
        df[col] = df[col].apply(ast.literal_eval)
    return df


def download_csv_from_s3(locale, page_no, chunk_no):
    df = pd.read_csv(f's3://zappa-marketmuse-admin-prod/ranking_urls/{locale}/{page_no}/{chunk_no}.csv')
    return df


def ingest_df(ranking_urls_df):
    with Client(host='154.27.75.107', settings={'use_numpy': True}) as client:
        client.insert_dataframe(
            query='INSERT INTO content_inventory.ranking_urls VALUES',
            dataframe=ranking_urls_df,
            settings={'types_check': True}
        )


def ingest(locale, page_no, chunk_no):
    df = download_csv_from_s3(locale, page_no, chunk_no)
    ingest_df(df)


def main():
    t1 = time.perf_counter()
    df = get_df(filename='ranking_urls.csv')
    print("Read CSV: ", time.perf_counter() - t1)
    t2 = time.perf_counter()
    ingest(df)
    print("Inserted: ", time.perf_counter() - t2)


if __name__ == '__main__':
    main()
