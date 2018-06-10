#!/usr/bin/env python3.5

from contextlib import contextmanager
from datetime import datetime
import json
import os
import sys

from confluent_kafka import Consumer, Producer, KafkaError
import requests

from article_parser import ArticleParser
import file_utils as fu
from prometheus_helpers import Registry
import time_utils as tu


REGISTRY = Registry(8100 + int(sys.argv[1]))
PROCESS_MESSAGE_ERRORS = REGISTRY.get_counter(
    'newsmine_articles_process_message_errors',
    'newsmine_articles process message errors',
)
KAFKA_PRODUCER_ERRORS = REGISTRY.get_counter(
    'newsmine_article_kafka_producer_errors',
    'Exceptions publishing to kafka',
)

def delivery_report(err, msg):
    if err is not None:
        KAFKA_PRODUCER_ERRORS.inc()
        print('Message delivery failed: {}'.format(err))


@contextmanager
def get_consumer():
    consumer = Consumer(
        {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'newsmine_articles',
            'default.topic.config': {
                'auto.offset.reset': 'beginning',
            },
        },
    )

    yield consumer

    consumer.close()


@contextmanager
def get_producer():
    producer = Producer({'bootstrap.servers':'127.0.0.1:9092'})

    yield producer

    producer.close()


def validate(article):
    """Just accessing all expected members."""
    _ = article['name']
    _ = article['path']
    _ = article['region']
    _ = article['topic']
    _ = article['url']
    return


@PROCESS_MESSAGE_ERRORS.count_exceptions()
def process_message(message):
    article = json.loads(message)

    validate(article)

    article_request = REGISTRY.get_summary(
        '{}_requests'.format(article['name']),
        '{}_requests'.format(article['name']),
    )

    @article_request.time()
    def _get_article(article):
        data_path = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)
        output_dir = os.path.join(
            data_path,
            article['region'],
            article['topic'],
            tu.ts_to_month(datetime.now().isoformat()),
            article['name'],
        )
        os.makedirs(output_dir, exist_ok=True)

        output_file = fu.url_to_file_name(article['url']) + fu.RAW_ARTICLE
        output_path = os.path.join(output_dir, output_file)

        # Short circuit
        if os.path.exists(output_path):
            return None

        with open(output_path, 'w') as out:
            res = requests.get(article['url'], timeout=15)
            res.raise_for_status()
            out.write(res.text)

        return output_path


    article_parse = REGISTRY.get_summary(
        '{}_parse'.format(article['name']),
        '{}_parse'.format(article['name']),
    )

    @article_parse.time()
    def _parse_article(article_path, article):
        data_path = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)
        output_dir = os.path.join(
            data_path,
            article['region'],
            article['topic'],
            tu.ts_to_month(datetime.now().isoformat()),
            article['name'],
        )
        os.makedirs(output_dir, exist_ok=True)

        output_file = fu.url_to_file_name(article['url']) + fu.PARSED_ARTICLE
        output_path = os.path.join(output_dir, output_file)

        parser = ArticleParser()

        title, _, authors, publish_date = parser.parse(
            article_path,
            output_path,
            article['url'],
        )

        return {
            'title': title,
            'authors': authors,
            'publish_date': publish_date or datetime.now().isoformat(),
            'path': output_path,
            'article': article,
        }

    article_path = _get_article(article)
    if article_path:
        return _parse_article(article_path, article)


def main():

    with get_producer() as producer:
        with get_consumer() as consumer:
            consumer.subscribe(['raw_rss'])

            while True:
                message = consumer.poll(1.0)

                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(message.error())
                        break

                try:
                    parsed_article = process_message(message.value().decode('utf-8'))
                    print('{}'.format(json.dumps(parsed_article)))

                    producer.poll(0)
                    producer.produce(
                        'parsed_articles',
                        json.dumps(parsed_article).encode('utf-8'),
                        callback=delivery_report,
                    )
                except Exception as e:
                    print('{}'.format(repr(e)))
                    pass


if __name__ == '__main__':
    main()