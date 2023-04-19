import csv
import itertools
import os.path
import random
import time
from concurrent.futures import ProcessPoolExecutor,  wait, FIRST_COMPLETED

from faker import Faker

fake = Faker()


def generate_rankings(serial_no):
    if os.path.exists(f'data/rankings_{serial_no}.csv'):
        print(f'File data/rankings_{serial_no}.csv already exists. Skipping...')
        return

    print(f'Generating data/rankings_{serial_no}.csv')
    t1 = time.perf_counter()

    RANKINGS_PER_DOMAIN = 5000000
    TOTAL_DOMAINS = 4
    # Generate random data for the table
    data = []
    # url = fake.url()

    # domain = urlparse(url).netloc
    for i in range(TOTAL_DOMAINS):
        domain = fake.domain_name()
        for j in range(RANKINGS_PER_DOMAIN):
            url = f'https://{domain}/{fake.uri_path()}'
            date = fake.date_between(start_date='-360d', end_date='today')
            term = fake.word()
            rank = random.randint(1, 100)
            volume = fake.pyint(min_value=10, max_value=50000000, step=10)
            cpc = round(random.uniform(0, 10), 2)
            data.append({"domain": domain, "date": date, "url": url, "term": term, "rank": rank, "volume": volume, "cpc": cpc})

    # Write the data to a file
    with open(f'data/rankings_{serial_no}.csv', 'w') as f:
        csv_writer = csv.DictWriter(f, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)

    print(f'Finished data/rankings_{serial_no}.csv in {time.perf_counter() - t1} seconds')


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


def main():
    run_concurrent_process(generate_rankings, range(50, 301), max_concurrency=10)


if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()
    print(f"Finished in {t2 - t1} seconds")