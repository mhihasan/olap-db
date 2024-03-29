import argparse
import asyncio
import csv
import dataclasses
import os
import random
import time
from datetime import datetime
from typing import List

import tldextract
from dotenv import load_dotenv


dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

from serp_vault.serp_query import SerpQuery
from sqlalchemy import select, desc

from historical_serps_data.models import TopicsToSchedule
from historical_serps_data.models.enums import TopicStatusEnum
from historical_serps_data.session import historical_serps_session_maker

from faker import Faker

fake = Faker()

DEFAULT_PAGE_SIZE = 20000
SERP_FETCHING_CONCURRENCY = 100

# import itertools
# from concurrent.futures import FIRST_COMPLETED, wait, ProcessPoolExecutor
def log(*message):
    print(f'{datetime.now().isoformat()}: {message}')


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
    subdomain: str
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


def rankings_to_clickhouse_schema(term, serps):
    """
    domain: LowCardinality(String),
    date: Date,
    term: String,
    url: Url,
    rank: Uint8,
    volume: Uint32,
    cpc: UFloat
    """
    if serps is None:
        return []

    date = datetime.fromtimestamp(int(serps.timestamp)).date().strftime("%Y-%m-%d")
    data = []
    for ranking in (serps.rankings or []):
        url = ranking.get("url")
        rank = ranking.get("position")

        if url is None or rank is None:
            continue

        url_bone = get_url_bone(url)
        tld_extract_result = tldextract.extract(url)
        domain = f'{tld_extract_result.domain}.{tld_extract_result.suffix}'
        subdomain = tld_extract_result.subdomain if tld_extract_result.subdomain and tld_extract_result.subdomain != 'www' else None

        volume = fake.pyint(min_value=10, max_value=50000000, step=10)
        cpc = round(random.uniform(0, 10), 2)
        competition = round(random.uniform(0, 1), 6)
        category_strings = [fake.sentence(nb_words=random.randint(1, 3)) for i in range(1, 10)]
        mm_difficulty = round(random.uniform(0, 2), 9)
        traffic = round(random.uniform(0, 10000000000), 9)
        traffic_pct = round(random.uniform(0, 1), 6)
        serp_features = random.sample(SERP_FEATURES, random.randint(0, len(SERP_FEATURES)))
        results_count = int(random.uniform(100, 10000000000))

        ranking = Ranking(
            domain=domain,
            subdomain=subdomain,
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


def get_hs_session(locale):
    return historical_serps_session_maker(locale)


def fetch_tracked_topics(locale, page_no, page_size):
    offset = (page_no - 1) * page_size
    HsDataSession = get_hs_session(locale)
    ts = 1678887190  # 2023-03-15 13:33:10
    with HsDataSession() as session:
        query = (
            select(TopicsToSchedule.topic)
            .where(TopicsToSchedule.status == TopicStatusEnum.processed.name, TopicsToSchedule.last_update_timestamp > ts, TopicsToSchedule.tracked == True)
            .order_by(desc(TopicsToSchedule.last_update_timestamp))
            .limit(page_size)
            .offset(offset)
        )
        return session.execute(query).scalars().all()


def write_to_csv(data, filename):
    file_path = os.path.join(os.path.dirname(__file__), filename)

    with open(file_path, "w") as f:
        csv_writer = csv.DictWriter(f, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)


def _chunkify(arr, n):
    return [arr[i : i + n] for i in range(0, len(arr), n)]


async def get_serps(terms, locale):
    serp_query = SerpQuery()
    try:
        return await serp_query.get_recent_serps(
            topics=terms, locale=locale, fetch=["rankings"]
        )
    except Exception as e:
        log(f"Error while fetching serps for {terms}: {e}")
        return {}


async def generate_rankings_data2(locale, page_no=1, page_size=DEFAULT_PAGE_SIZE):
    while True:
        topics = fetch_tracked_topics(locale, page_no, page_size)
        log(f"topics fetched {len(topics)}")
        if not topics:
            break

        rankings_data = []

        for chunk in _chunkify(topics, SERP_FETCHING_CONCURRENCY):
            response = await get_serps(chunk, locale)
            log(f"Found SERPs for {len(response.keys())} topics")

            for topic, serps in response.items():
                data = rankings_to_clickhouse_schema(topic, serps)
                log(f'Generate {len(data)} rankings for {topic}')

                if data:
                    rankings_data.extend(data)

        if rankings_data:
            parent_dir = f'rankings_data_{locale}'
            if not os.path.exists(parent_dir):
                os.mkdir(parent_dir)

            write_to_csv(rankings_data, f"{parent_dir}/rankings_{locale}_{page_no}.csv")
            log(f"Finished page {page_no}")
        else:
            log(f"No data found for locale {locale} page {page_no}")

        page_no += 1
        # break


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locale", type=str, required=True)
    parser.add_argument("--page_no", type=int, default=1)
    parser.add_argument("--page_size", type=int, default=DEFAULT_PAGE_SIZE)

    args = parser.parse_args()
    t1 = time.perf_counter()
    asyncio.run(generate_rankings_data2(
        locale=args.locale, page_no=args.page_no, page_size=args.page_size
    ))
    log(f"Finished in {time.perf_counter() - t1} seconds")


if __name__ == "__main__":
    asyncio.run(generate_rankings_data2('en-ca'))
