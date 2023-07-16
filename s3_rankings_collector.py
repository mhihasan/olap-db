import argparse
import asyncio
import csv
import itertools
import logging
import os
import time
from datetime import datetime
from io import StringIO

import aioboto3
import asyncpg
import sentry_sdk
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
sentry_sdk.set_tag('script', 's3_rankings_collector')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

aioboto3_session = aioboto3.Session()

POINT_IN_TIME_TIMESTAMP = datetime.utcnow().timestamp() - 86400 * 30 * 3
BUCKET_NAME = 'zappa-marketmuse-admin-prod'
DEFAULT_PAGE_SIZE = os.getenv('DEFAULT_PAGE_SIZE', 5000000)
CHUNK_SIZE = os.getenv('CHUNK_SIZE', 1000)


def log(*message):
    print(f'{datetime.utcnow().isoformat()}: {message}')


def write_to_csv(data, filename):
    file_path = os.path.join(os.path.dirname(__file__), filename)

    with open(file_path, "w") as f:
        csv_writer = csv.DictWriter(f, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)


def convert_list_of_dicts_to_csv(data):
    with StringIO() as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        csv_data = csv_file.getvalue()
    return csv_data


async def upload_csv_data_to_s3(bucket_name, key, list_of_dicts):
    async with aioboto3_session.resource('s3') as s3_resource:
        bucket = await s3_resource.Bucket(bucket_name)
        csv_data = convert_list_of_dicts_to_csv(list_of_dicts)

        await bucket.put_object(Key=key, Body=csv_data.encode())


def _chunkify(arr, n):
    return [arr[i : i + n] for i in range(0, len(arr), n)]


SERP_INDEX_TABLES = {
    'en-us': 'topic_serp_index_prod',
    "en-uk": "topic_serp_index_en_uk_prod",
    "en-au": "topic_serp_index_en_au_prod",
    "en-ca": "topic_serp_index_en_ca_prod",
    "en-nz": "topic_serp_index_en_nz_prod",
}


async def get_index(table, topic):
    response = await table.get_item(Key={'topic': topic}, AttributesToGet=['historical_serp_data'])
    s3_keys = [{'serp_rankings': r['serp_rankings']} for r in response.get('Item', {}).get('historical_serp_data', []) if int(r['timestamp']) > POINT_IN_TIME_TIMESTAMP and r.get('serp_rankings')]
    return s3_keys[:3]


async def get_indices(topics, locale):
    async with aioboto3_session.resource('dynamodb', region_name='us-east-1') as dynamo_resource:
        table = await dynamo_resource.Table(SERP_INDEX_TABLES[locale])
        result = await asyncio.gather(*[get_index(table, topic) for topic in topics])
        return list(itertools.chain.from_iterable(result))


async def get_s3_ranking_keys(locale, topics, page_no):
    topics_processed = 0
    total_topics = len(topics)
    t_start = time.perf_counter()

    for chunk_no, chunk in enumerate(_chunkify(topics, CHUNK_SIZE)):
        indices = await get_indices(chunk, locale)

        if not indices:
            log(f'No indices found for {locale}/{page_no}/{chunk_no}.csv')
            continue

        await upload_csv_data_to_s3(bucket_name=BUCKET_NAME,
                                    key=f'{locale}/{page_no}/{chunk_no}.csv',
                                    list_of_dicts=indices)
        topics_processed += CHUNK_SIZE

        t_end = time.perf_counter()
        log(f'Processed {topics_processed} topics : {round(topics_processed * 100 / total_topics * 2, 2)}% in {round(t_end - t_start, 2)} seconds')


def get_credentials(locale):
    env_var_suffix = locale.upper().replace("-", "_")
    return {
        'host': os.getenv(f'DB_SCHEDULER_HOST_{env_var_suffix}'),
        'database': os.getenv(f'DB_SCHEDULER_NAME_{env_var_suffix}'),
        'user': os.getenv(f'DB_SCHEDULER_USER_{env_var_suffix}'),
        'password': os.getenv(f'DB_SCHEDULER_PASSWORD_{env_var_suffix}'),
    }


async def fetch_tracked_topics(locale, page_no, page_size):
    credentials = get_credentials(locale)
    pool = await asyncpg.create_pool(**credentials)
    async with pool.acquire() as connection:
        query = f"""
            SELECT topic
            FROM topics_to_schedule
            WHERE last_time_scheduled > {POINT_IN_TIME_TIMESTAMP}
            ORDER BY last_time_scheduled
            LIMIT {page_size} OFFSET {page_no * page_size};
        """

        records = await connection.fetch(query)
    return [record['topic'] for record in records]


async def main(locale, page_no, page_size):
    topics = await fetch_tracked_topics(locale, page_no, page_size)
    log(f'Found {len(topics)} topics')
    await get_s3_ranking_keys(locale, topics, page_no)
    log(f'Uploaded ranking keys to S3')


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locale", type=str, required=True)
    parser.add_argument("--page_no", type=int, default=1)
    parser.add_argument("--page_size", type=int, default=DEFAULT_PAGE_SIZE)

    args = parser.parse_args()
    asyncio.run(main(
        locale=args.locale, page_no=args.page_no, page_size=args.page_size
    ))


# export PYTHONUNBUFFERED=1 && nohup python s3_rankings_collector.py --locale=en-us --page_no=2 > s3_ranking_keys_en_us.log &
if __name__ == '__main__':
    t = time.perf_counter()
    cli()
    elapsed = time.perf_counter() - t
    log(f'executed in {elapsed:0.2f} seconds.')

