#!/usr/bin/env python3.5

from datetime import datetime
import json
import os
import time
import traceback

from confluent_kafka import Producer
import requests

import file_utils as fu
from prometheus_helpers import Registry
from rss_feeds import get_feeds
import time_utils as tu

PRODUCER = Producer({'bootstrap.servers':'127.0.0.1:9092'})

REGISTRY = Registry(8000)
KAFKA_PRODUCER_ERRORS = REGISTRY.get_counter(
    'newsmine_rss_kafka_producer_errors',
    'Exceptions publishing to kafka',
)

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
    process_feed_time = REGISTRY.get_summary(summary_name, 'Time spent processing feed')

    errors_name = '{}_feed_processing_exceptions'.format(feed_name)
    count_errors = REGISTRY.get_counter(errors_name, 'Exceptions processing feed')

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

        output = {
            'name':fu.baseurl_to_file_name(feed.url),
            'path':output_path,
            'region':feed.region,
            'topic':feed.topic,
            'url':feed.url,
        }

        PRODUCER.poll(0)
        PRODUCER.produce(
            'raw_rss',
            json.dumps(output).encode('utf-8'),
            callback=delivery_report,
        )

    try:
        _process(feed)
        return 0
    except Exception as e:
        print('{}'.format(repr(e)))
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
    main()