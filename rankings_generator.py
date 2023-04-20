import argparse
import asyncio
import csv
import os
import random
from datetime import datetime
from urllib.parse import urlparse

from dotenv import load_dotenv
from serp_vault.responses import Serps

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

from serp_vault.serp_query import SerpQuery
from sqlalchemy import select

from historical_serps_data.models import TopicsToSchedule
from historical_serps_data.session import historical_serps_session_maker

from faker import Faker

fake = Faker()

DEFAULT_PAGE_SIZE = 10000

import itertools
from concurrent.futures import FIRST_COMPLETED, wait, ProcessPoolExecutor


def run_concurrent_process(fn, input_params, *, max_concurrency):
    input_params_iter = iter(input_params)
    total_tasks = len(input_params)
    total_completed_tasks = 0

    with ProcessPoolExecutor(max_workers=max_concurrency) as executor:
        futures = {executor.submit(fn, param): param for param in itertools.islice(input_params_iter, max_concurrency)}

        while futures:
            finished_tasks, _ = wait(futures, return_when=FIRST_COMPLETED)

            for task in finished_tasks:
                param = futures.pop(task)
                print("Finished param", param)

            total_completed_tasks += len(finished_tasks)
            print(
                f"Completed tasks: {total_completed_tasks}/{total_tasks}, {round(total_completed_tasks * 100 / total_tasks, 2)}%"  # noqa
            )

            for param in itertools.islice(input_params_iter, len(finished_tasks)):
                futures[executor.submit(fn, param)] = param

        executor.shutdown()


def get_hs_session(locale):
    return historical_serps_session_maker(locale)


def fetch_tracked_topics(locale, page_no, page_size):
    offset = (page_no - 1) * page_size
    HsDataSession = get_hs_session(locale)
    with HsDataSession() as session:
        query = select(TopicsToSchedule).where(TopicsToSchedule.tracked==True).limit(page_size).offset(offset)
        return session.execute(query).scalars().all()


def url_to_domain(url):
    domain_with_www = urlparse(url).netloc
    if domain_with_www.startswith('www'):
        return domain_with_www.split('.', 1)[1]

    return domain_with_www


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
    date = datetime.fromtimestamp(int(serps.timestamp)).date().strftime('%Y-%m-%d')
    data = []
    for ranking in (serps.rankings or []):
        url = ranking.get('url')
        rank = ranking.get('position')

        if url is None or rank is None:
            continue

        domain = url_to_domain(url)

        volume = fake.pyint(min_value=10, max_value=50000000, step=10)
        cpc = round(random.uniform(0, 10), 2)

        data.append({
            'domain': domain,
            'date': date,
            'term': term,
            'url': url,
            'rank': rank,
            'volume': volume,
            'cpc': cpc
        })

    return data


def write_to_csv(data, filename):
    file_path = os.path.join(os.path.dirname(__file__), f'rankings_data/{filename}')

    with open(file_path, 'w') as f:
        csv_writer = csv.DictWriter(f, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)


def _chunkify(arr, n):
    return [arr[i: i + n] for i in range(0, len(arr), n)]


def generate_rankings_data(params):
    locale = params.get('locale')
    page_no = params.get('page_no')
    page_size = params.get('page_size', DEFAULT_PAGE_SIZE)
    topics = fetch_tracked_topics(locale, page_no, page_size)
    serp_query = SerpQuery()

    rankings_data = []

    for chunk in _chunkify(topics, 100):
        terms = [t.topic for t in chunk]

        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(serp_query.get_recent_serps(topics=terms, locale=locale, fetch=['rankings']))

        for topic, serps in response.items():
            rankings_data.extend(rankings_to_clickhouse_schema(topic, serps))

    write_to_csv(rankings_data, f'{locale}_rankings_{page_no}.csv')


async def get_serps(serp_query, terms, locale):
    try:
        return await serp_query.get_recent_serps(topics=terms, locale=locale, fetch=['rankings'])
    except Exception as e:
        return Serps()

def generate_rankings_data2(locale, page_no=1, page_size=DEFAULT_PAGE_SIZE):
    serp_query = SerpQuery()
    loop = asyncio.get_event_loop()

    while True:
        topics = fetch_tracked_topics(locale, page_no, page_size)
        if not topics:
            break

        rankings_data = []

        for chunk in _chunkify(topics, 100):
            terms = [t.topic for t in chunk]

            response = loop.run_until_complete(get_serps(serp_query, terms, locale))

            for topic, serps in response.items():
                rankings_data.extend(rankings_to_clickhouse_schema(topic, serps))

        write_to_csv(rankings_data, f'{locale}_rankings_{page_no}.csv')

        print(f"{datetime.now().isoformat()}: Finished page", page_no)
        page_no += 1


def main(locale):
    pages = [{'locale': locale, 'page_no': i} for i in list(range(1, 100))]
    run_concurrent_process(generate_rankings_data, pages, max_concurrency=10)


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('--locale', type=str, required=True)

    args = parser.parse_args()
    generate_rankings_data2(args.locale)


if __name__ == '__main__':
    # main('en-us')
    cli()
    # generate_rankings_data2('en-us')
