import argparse
import asyncio
import csv
import os
import random
import time
from datetime import datetime
from urllib.parse import urlparse

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

DEFAULT_PAGE_SIZE = 1000
SERP_FETCHING_CONCURRENCY = 100

# import itertools
# from concurrent.futures import FIRST_COMPLETED, wait, ProcessPoolExecutor


def log(*message):
    print(f'{datetime.now().isoformat()}: {message}')


# def run_concurrent_process(fn, input_params, *, max_concurrency):
#     input_params_iter = iter(input_params)
#     total_tasks = len(input_params)
#     total_completed_tasks = 0
#
#     with ProcessPoolExecutor(max_workers=max_concurrency) as executor:
#         futures = {
#             executor.submit(fn, param): param
#             for param in itertools.islice(input_params_iter, max_concurrency)
#         }
#
#         while futures:
#             finished_tasks, _ = wait(futures, return_when=FIRST_COMPLETED)
#
#             for task in finished_tasks:
#                 param = futures.pop(task)
#                 log("Finished param", param)
#
#             total_completed_tasks += len(finished_tasks)
#             log(
#                 f"Completed tasks: {total_completed_tasks}/{total_tasks}, {round(total_completed_tasks * 100 / total_tasks, 2)}%"  # noqa
#             )
#
#             for param in itertools.islice(input_params_iter, len(finished_tasks)):
#                 futures[executor.submit(fn, param)] = param
#
#         executor.shutdown()


def get_hs_session(locale):
    return historical_serps_session_maker(locale)


def fetch_tracked_topics(locale, page_no, page_size):
    offset = (page_no - 1) * page_size
    HsDataSession = get_hs_session(locale)
    with HsDataSession() as session:
        query = (
            select(TopicsToSchedule.topic)
            .where(TopicsToSchedule.status == TopicStatusEnum.processed.name, TopicsToSchedule.tracked == True)
            .order_by(desc(TopicsToSchedule.last_update_timestamp))
            .limit(page_size)
            .offset(offset)
        )
        return session.execute(query).scalars().all()


def url_to_domain(url):
    domain_with_www = urlparse(url).netloc
    if domain_with_www.startswith("www"):
        return domain_with_www.split(".", 1)[1]

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
    if serps is None:
        return []

    date = datetime.fromtimestamp(int(serps.timestamp)).date().strftime("%Y-%m-%d")
    data = []
    for ranking in (serps.rankings or []):
        url = ranking.get("url")
        rank = ranking.get("position")

        if url is None or rank is None:
            continue

        domain = url_to_domain(url)

        volume = fake.pyint(min_value=10, max_value=50000000, step=10)
        cpc = round(random.uniform(0, 10), 2)

        data.append(
            {
                "domain": domain,
                "date": date,
                "term": term,
                "url": url,
                "rank": rank,
                "volume": volume,
                "cpc": cpc,
            }
        )

    return data


def write_to_csv(data, filename):
    file_path = os.path.join(os.path.dirname(__file__), f"rankings_data/{filename}")

    with open(file_path, "w") as f:
        csv_writer = csv.DictWriter(f, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)


def _chunkify(arr, n):
    return [arr[i : i + n] for i in range(0, len(arr), n)]


# def generate_rankings_data(params):
#     locale = params.get("locale")
#     page_no = params.get("page_no")
#     page_size = params.get("page_size", DEFAULT_PAGE_SIZE)
#     topics = fetch_tracked_topics(locale, page_no, page_size)
#     serp_query = SerpQuery()
#
#     rankings_data = []
#
#     for chunk in _chunkify(topics, 10):
#         terms = [t.topic for t in chunk]
#
#         response = asyncio.run(
#             serp_query.get_recent_serps(topics=terms, locale=locale, fetch=["rankings"])
#         )
#
#         for topic, serps in response.items():
#             data = rankings_to_clickhouse_schema(topic, serps)
#             if data:
#                 rankings_data.extend(data)
#
#     if rankings_data:
#         write_to_csv(rankings_data, f"{locale}_rankings_{page_no}.csv")
#     else:
#         log(f"No data found for locale {locale} page {page_no}")


async def get_serps(terms, locale):
    serp_query = SerpQuery()
    try:
        return await serp_query.get_recent_serps(
            topics=terms, locale=locale, fetch=["rankings"]
        )
    except Exception as e:
        log(f"Error while fetching serps for {terms}: {e}")
        return {}


def generate_rankings_data2(locale, page_no=1, page_size=DEFAULT_PAGE_SIZE):
    while True:
        topics = fetch_tracked_topics(locale, page_no, page_size)
        log(f"topics fetched {len(topics)}")
        if not topics:
            break

        rankings_data = []

        for chunk in _chunkify(topics, SERP_FETCHING_CONCURRENCY):
            response = asyncio.run(get_serps(chunk, locale))

            for topic, serps in response.items():
                data = rankings_to_clickhouse_schema(topic, serps)
                log(f'Generate {len(data)} rankings for {topic}')

                if data:
                    rankings_data.extend(data)

        if rankings_data:
            write_to_csv(rankings_data, f"rankings_{locale}_{page_no}.csv")
            log(f"{datetime.now().isoformat()}: Finished page", page_no)
        else:
            log(f"No data found for locale {locale} page {page_no}")

        page_no += 1


# def main(locale):
#     pages = [{"locale": locale, "page_no": i} for i in list(range(1, 100))]
#     run_concurrent_process(generate_rankings_data, pages, max_concurrency=10)


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locale", type=str, required=True)
    parser.add_argument("--page_no", type=int, default=1)
    parser.add_argument("--page_size", type=int, default=DEFAULT_PAGE_SIZE)

    args = parser.parse_args()
    t1 = time.perf_counter()
    generate_rankings_data2(
        locale=args.locale, page_no=args.page_no, page_size=args.page_size
    )
    log(f"Finished in {time.perf_counter() - t1} seconds")


if __name__ == "__main__":
    # main('en-us')
    cli()
    # generate_rankings_data2('en-us')
