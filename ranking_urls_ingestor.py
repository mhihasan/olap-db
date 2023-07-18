import argparse
import ast
import itertools
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime
import boto3
import pandas as pd
import sentry_sdk
from clickhouse_driver import Client
from dotenv import load_dotenv
from sentry_sdk.integrations.logging import LoggingIntegration


dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

sentry_logging = LoggingIntegration(
    level=logging.INFO,  # Capture info and above as breadcrumbs
    event_level=logging.ERROR  # Send errors as events
)
sentry_sdk.init(
    dsn=os.getenv('SENTRY_DSN', 'https://88125568e9f7416c8be655f47eed151e@o10787.ingest.sentry.io/1890285'),
    integrations=[sentry_logging, ],
    environment=os.environ.get('ENVIRONMENT', 'prod')
)
sentry_sdk.set_tag('script', 'ranking_urls_generator')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb')

DEFAULT_PAGE_SIZE = int(os.getenv('DEFAULT_PAGE_SIZE', 5000000))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1000))
BUCKET_NAME = 'zappa-marketmuse-admin-prod'
NUM_FILES_IN_A_CHUNK = 10


def log(*message):
    print(f'{datetime.utcnow().isoformat()}: {message}')


def download_csv_from_s3(locale, page_no, chunk_no):
    df = pd.read_csv(f's3://{BUCKET_NAME}/ranking_urls/{locale}/{page_no}/{chunk_no}.csv')
    df['date'] = pd.to_datetime(df['date'])
    for col in ['category_strings', 'serp_features']:
        df[col] = df[col].apply(ast.literal_eval)
    return df


def ingest_df(ranking_urls_df):
    with Client(host='154.27.75.107', settings={'use_numpy': True}) as client:
        client.insert_dataframe(
            query='INSERT INTO content_inventory.ranking_urls VALUES',
            dataframe=ranking_urls_df,
            settings={'types_check': True}
        )


class FlagDynamoDB:
    def __init__(self):
        self.dynamodb = dynamodb
        self.table_name = 'ranking_urls_generator_flags'
        self.table = self.dynamodb.Table(self.table_name)

    def exists(self, locale, page_no, chunk_no):
        response = self.table.get_item(Key={'locale_page': f'{locale}_{page_no}', 'chunk': chunk_no})
        return response.get('Item', None) is not None

    def create(self, locale, page_no, chunk_no):
        self.table.put_item(Item={'locale_page': f'{locale}_{page_no}', 'chunk': chunk_no})


def ingest(kwargs):
    locale, page_no, chunk_no = kwargs['locale'], kwargs['page_no'], kwargs['chunk_no']

    flag_db = FlagDynamoDB()
    if flag_db.exists(locale, page_no, chunk_no):
        log(f'Already ingested {locale}/{page_no}/{chunk_no}.csv')
        return

    log(f'Ingesting {locale}/{page_no}/{chunk_no}.csv')
    t1 = time.time()
    # df = download_csv_from_s3(locale, page_no, chunk_no)
    log(f'Downloaded {locale}/{page_no}/{chunk_no}.csv in {time.time() - t1} seconds')
    t2 = time.time()
    # ingest_df(df)
    log(f'Inserted {locale}/{page_no}/{chunk_no}.csv in {time.time() - t2} seconds')

    flag_db.create(locale, page_no, chunk_no)


def run_concurrent_process(fn, input_params, *, max_concurrency):
    input_params_iter = iter(input_params)
    total_tasks = len(input_params)
    total_completed_tasks = 0

    with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        futures = {executor.submit(fn, param): param for param in itertools.islice(input_params_iter, max_concurrency)}

        while futures:
            finished_tasks, _ = wait(futures, return_when=FIRST_COMPLETED)

            for task in finished_tasks:
                if task.exception():
                    raise task.exception()

                param = futures.pop(task)
                log(f"Finished task {param}")

            total_completed_tasks += len(finished_tasks)
            log(
                f"Completed tasks: {total_completed_tasks}/{total_tasks}, {round(total_completed_tasks * 100 / total_tasks, 2)}%"  # noqa
            )

            for param in itertools.islice(input_params_iter, len(finished_tasks)):
                futures[executor.submit(fn, param)] = param

        executor.shutdown()


def ingest_ranking_urls(locale, page_no, start_chunk_no=0):
    """
    Ingests ranking urls data into clickhouse
    """
    total_files = DEFAULT_PAGE_SIZE // CHUNK_SIZE
    files = range(total_files)
    total_files_to_be_ingested = len(files) // NUM_FILES_IN_A_CHUNK
    params = [{"locale": locale, "page_no": page_no, "chunk_no": chunk_no} for chunk_no in range(total_files_to_be_ingested) if chunk_no > start_chunk_no]
    run_concurrent_process(ingest, params, max_concurrency=10)


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locale", type=str, required=True)
    parser.add_argument("--page_no", type=int, default=1)
    parser.add_argument("--start_chunk_no", type=int, default=0)

    args = parser.parse_args()

    ingest_ranking_urls(args.locale, args.page_no, args.start_chunk_no)


# export PYTHONUNBUFFERED=1 && nohup python ranking_urls_ingestor.py --locale=en-us --start_chunk_no=0 --page_no=1> ranking_urls_ingestor_en_us_1.log &


if __name__ == '__main__':
    t = time.perf_counter()
    cli()
    log(f'Elapsed in {time.perf_counter() - t} seconds')
