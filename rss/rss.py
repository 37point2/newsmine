#!/usr/bin/env python3.5

from datetime import datetime
import os
import time
import traceback

from confluent_kafka import Producer
from prometheus_client import start_http_server, Summary, Counter
import requests

import file_utils as fu
from rss_feeds import get_feeds
import time_utils as tu

PRODUCER = Producer({'bootstrap.servers':'127.0.0.1:9092'})

KAFKA_PRODUCER_ERRORS = Counter('kafka_producer_errors', 'Exceptions publishing to kafka')

DATA_PATH = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)

def delivery_report(err, msg):
    if err is not None:
        KAFKA_PRODUCER_ERRORS.inc()
        print('Message delivery failed: {}'.format(err))

def process_feed(feed):

    feed_name = '{}_{}_{}'.format(
        feed.region,
        feed.topic,
        fu.url_to_file_name(feed.url),
    )
    feed_name = feed_name.replace(' ', '_')

    summary_name = '{}_feed_processing_seconds'.format(feed_name)
    process_feed_time = Summary(summary_name, 'Time spent processing feed')

    errors_name = '{}_feed_processing_exceptions'.format(feed_name)
    count_errors = Counter(errors_name, 'Exceptions processing feed')

    @process_feed_time.time()
    def _process(feed):
        hour = tu.ts_to_hour(datetime.now().isoformat())
        output_dir = os.path.join(
            DATA_PATH,
            fu.datestamp_to_path(hour),
        )
        os.makedirs(output_dir, exist_ok=True)

        output_file = fu.url_to_file_name(feed.url) + fu.RAW_FEED
        output_path = os.path.join(output_dir, output_file)
        
        with open(output_path, 'w') as out:
            r = requests.get(feed.url, timeout=15)
            r.raise_for_status()
            out.write(r.text)

        PRODUCER.poll(0)
        PRODUCER.produce('raw_rss', output_path.encode('utf-8'), callback=delivery_report)

    try:
        _process(feed)
        return 0
    except Exception as e:
        count_errors.inc()
        return 1


def main():
    feeds = get_feeds()
    errors = {feed.url:0 for feed in feeds}

    while(True):
        for feed in feeds:
            if errors[feed.url] >= 100:
                continue

            errors[feed.url] += process_feed(feed)

        time.sleep(30 * 60)


if __name__ == '__main__':
    start_http_server(8000)

    main()