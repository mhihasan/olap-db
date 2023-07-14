import argparse
import asyncio
import csv
import dataclasses
import itertools
import json
import logging
import os
import random
import time
from datetime import datetime
from io import StringIO
from typing import List

import aioboto3
import tldextract
from dotenv import load_dotenv
from faker import Faker

fake = Faker()

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

aioboto3_session = aioboto3.Session()

DEFAULT_PAGE_SIZE = int(os.getenv('DEFAULT_PAGE_SIZE', 5000000))
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 1000))
BUCKET_NAME = 'zappa-marketmuse-admin-prod'
NUM_FILES_IN_A_CHUNK = 10


def log(*message):
    print(f'{datetime.utcnow().isoformat()}: {message}')


def _chunkify(arr, n):
    return [arr[i : i + n] for i in range(0, len(arr), n)]


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

        try:
            await bucket.put_object(Key=key, Body=csv_data.encode())
        except Exception as e:
            log(f'Error uploading data to s3: {e}')


SERP_FEATURES = (
    "bottom_ads",
    "featured_snippet",
    "image_pack",
    "knowledge_card",
    "knowledge_panel",
    "local_pack",
    "news_box",
    "organic",
    "related_questions",
    "related_searches",
    "reviews",
    "shopping_results",
    "site_links",
    "spelling",
    "top_ads",
    "twitter",
    "videos",
    "jobs_pack",
    "recipes",
    "popular_products"
)


@dataclasses.dataclass
class Ranking:
    domain: str
    root_domain: str
    date: str
    url_bone: str
    url: str
    term: str
    rank: int
    volume: int
    cpc: float  # cpc = round(random.uniform(0, 10), 2)
    competition: float  # between 0 and 1
    category_strings: List[str]
    mm_difficulty: float
    traffic: float
    traffic_pct: float
    serp_features: List[str]
    results_count: int


def get_url_bone(url: str) -> str:
    prefixes = ("https://www.", "https://", "http://www.", "http://",)
    for prefix in prefixes:
        if url.lower().startswith(prefix):
            url = url[len(prefix):]
    if url.endswith("/"):
        url = url[0:-1]
    return url

CATEGORY_STRINGS = [fake.sentence(nb_words=random.randint(1, 3)).split(".")[0] for i in range(1, 10)]
def rankings_to_clickhouse_schema(term, rankings, timestamp, total_results_count=None):
    """
    domain: LowCardinality(String),
    date: Date,
    term: String,
    url: Url,
    rank: Uint8,
    volume: Uint32,
    cpc: UFloat
    """

    date = datetime.fromtimestamp(int(timestamp)).date().strftime("%Y-%m-%d")
    data = []
    for ranking in rankings:
        url = ranking.get("url")
        rank = ranking.get("position")

        if url is None or rank is None:
            continue

        url_bone = get_url_bone(url)
        tld_extract_result = tldextract.extract(url)
        root_domain = tld_extract_result.registered_domain
        domain = tld_extract_result.fqdn
        if domain.startswith("www."):
            domain = domain[4:]

        volume = fake.pyint(min_value=10, max_value=50000000, step=10)
        cpc = round(random.uniform(0, 10), 2)
        competition = round(random.uniform(0, 1), 6)
        category_strings = random.choices(CATEGORY_STRINGS, k=3)
        mm_difficulty = round(random.uniform(0, 2), 9)
        traffic = round(random.uniform(0, 10000000000), 9)
        traffic_pct = round(random.uniform(0, 1), 6)
        serp_features = random.choices(SERP_FEATURES, k=3)
        results_count = total_results_count or int(random.uniform(100, 10000000000))

        ranking = Ranking(
            domain=domain,
            root_domain=root_domain,
            date=date,
            url_bone=url_bone,
            url=url,
            term=term,
            rank=rank,
            volume=volume,
            cpc=cpc,
            competition=competition,
            category_strings=category_strings,
            mm_difficulty=mm_difficulty,
            traffic=traffic,
            traffic_pct=traffic_pct,
            serp_features=serp_features,
            results_count=results_count,
        )

        data.append(dataclasses.asdict(ranking))

    return data


async def get_rankings_keys(bucket, s3_key):
    obj = await bucket.Object(s3_key)
    try:
        resp = await obj.get()
    except Exception as e:
        return []

    csv_string = (await resp['Body'].read()).decode('utf-8')
    csv_file = csv.DictReader(StringIO(csv_string))
    s3_keys = [row['serp_rankings'] for row in csv_file]
    return s3_keys


async def get_ranking_keys_in_batch(bucket_name, s3_keys):
    async with aioboto3_session.resource('s3') as s3_resource:
        bucket = await s3_resource.Bucket(bucket_name)
        result = await asyncio.gather(*[get_rankings_keys(bucket, s3_key) for s3_key in s3_keys])

    return list(itertools.chain.from_iterable(result))


async def fetch_singe_rankings(bucket, s3_key):
    obj = await bucket.Object(s3_key)
    resp = await obj.get()
    data = (await resp['Body'].read()).decode('utf-8')
    json_data = json.loads(data)
    term = s3_key.split("_")[0]
    timestamp = int(s3_key.split("_")[-1])
    total_results = json_data[1]
    ranking_urls = rankings_to_clickhouse_schema(term, json_data[0], timestamp, total_results)
    return ranking_urls


async def fetch_rankings(s3_keys):
    async with aioboto3_session.resource('s3') as s3_resource:
        bucket = await s3_resource.Bucket('serp-rankings-prod')
        result = await asyncio.gather(*[fetch_singe_rankings(bucket, s3_key) for s3_key in s3_keys])

    return list(itertools.chain.from_iterable(result))


async def collect_rankings_from_chunk(bucket_name, locale, page_no, chunk_index, chunk):
    keys = [f'{locale}/{page_no}/{i}.csv' for i in chunk]
    s3_keys = await get_ranking_keys_in_batch(bucket_name, keys)
    rankings = await fetch_rankings(s3_keys)
    await upload_csv_data_to_s3(
        bucket_name,
        key=f'ranking_urls/{locale}/{page_no}/{chunk_index}.csv',
        list_of_dicts=rankings
    )


async def collect_rankings(bucket_name, locale, page_no):
    files_generated = 0
    total_files = DEFAULT_PAGE_SIZE // CHUNK_SIZE
    files = range(total_files)
    t = time.perf_counter()
    for i, chunk in enumerate(_chunkify(files, NUM_FILES_IN_A_CHUNK)):
        await collect_rankings_from_chunk(
            bucket_name=bucket_name, locale=locale, page_no=page_no, chunk_index=i, chunk=chunk
        )
        files_generated += 1
        log(f'Generated {files_generated} out of {total_files} files: {round(files_generated/total_files, 2)}%, {round(time.perf_counter() - t, 2)} seconds elapsed.')


async def main(locale, page_no):
    await collect_rankings(BUCKET_NAME, locale, page_no)


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locale", type=str, required=True)
    parser.add_argument("--page_no", type=int, default=1)

    args = parser.parse_args()
    asyncio.run(main(
        locale=args.locale, page_no=args.page_no
    ))

# export PYTHONUNBUFFERED=1 && nohup python ranking_urls_generator.py --locale=en-us --page_no=1 > ranking_urls_generator_en_us_1.log &


if __name__ == '__main__':
    t = time.perf_counter()
    cli()
    elapsed = time.perf_counter() - t
    log(f'executed in {elapsed:0.2f} seconds.')

