import csv
import random
import time
from urllib.parse import urlparse

from faker import Faker

fake = Faker()

RANKINGS_PER_DOMAIN = 1000000
TOTAL_DOMAINS = 10


def main():
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
    with open('data/rankings_6.csv', 'w') as f:
        csv_writer = csv.DictWriter(f, fieldnames=data[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(data)


if __name__ == '__main__':
    t1 = time.perf_counter()
    main()
    t2 = time.perf_counter()
    print(f"Finished in {t2 - t1} seconds")