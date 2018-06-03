#!/usr/bin/env python3.5

from airflow import DAG
from airflow import operators
from airflow import hooks
from datetime import datetime, timedelta
import os

from article_parser import ArticleParser
import feed
import file_utils as fu
import time_utils as tu

def parse_articles(ts, **kwargs):
    """
    Get articles from postgres db and parse them
    """
    month = tu.ts_to_month(ts)

    parser = ArticleParser()

    data_path = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)

    print('Get articles')
    pg_hook = hooks.PostgresHook(postgres_conn_id='rss_urls_id')

    feeds = feed.get_feeds(pg_hook)

    get_articles_cmd = '''SELECT id, rss_id, url, file_path FROM rss_articles'''

    conn = pg_hook.get_conn()
    curr = pg_hook.get_cursor()
    curr.execute(get_articles_cmd)

    rows = curr.fetchall()

    for row in rows:
        try:
            src_id = row[0]
            rss_id = row[1]
            url = row[2]
            input_path = row[3]

            feed_tuple = feed.feed_from_rss_id(feeds, rss_id)

            feed_name, region, topic = feed.parts(feed_tuple)

            print('Removing article')
            remove_article_cmd = '''DELETE FROM rss_articles WHERE id = {}'''.format(src_id)
            curr.execute(remove_article_cmd)
            conn.commit()
            print('Done removing article')

            output_dir = os.path.join(data_path, region, topic, month, feed_name)
            os.makedirs(output_dir, exist_ok=True)

            if not os.path.exists(input_path):
                print('Source file {} does not exist'.format(input_path))
                continue

            output_file = fu.url_to_file_name(url) + fu.PARSED_ARTICLE
            output_path = os.path.join(output_dir, output_file)

            # Short circuit
            if os.path.exists(output_path):
                print('Path already exists, skipping')
                continue

            title, _, authors, publish_date = parser.parse(input_path, output_path, url)

            insert_parsed_article_cmd = '''INSERT INTO parsed_articles
                    (rss_id, url, title, authors, file_path, publish_date, added)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING'''
            print('Inserting parsed article')
            article = (
                rss_id,
                url,
                title,
                str(authors),
                output_path,
                publish_date or datetime.now().date(),
                datetime.now().date(),
            )
            pg_hook.run(insert_parsed_article_cmd, parameters=article)
            print('Done inserting article')
        except Exception as e:
            #
            # Log exception
            #
            print(e)

    curr.close()
    conn.close()

    return 0


def generate_summaries(ts, **kwargs):
    """
    Call gensim on parsed articles
    """
    return 0

def get_named_entities(ts, **kwargs):
    """
    Get named entities from parsed articles
    """
    return 0

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email':[None],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0,
}

dag = DAG(
    dag_id='parse_articles_dag',
    default_args=default_args,
    start_date=datetime(2018,6,1),
    schedule_interval=timedelta(minutes=60),
)

task1 = operators.PythonOperator(
    task_id='parse_articles',
    provide_context=True,
    python_callable=parse_articles,
    dag=dag,
)

task2 = operators.PythonOperator(
    task_id='gensim',
    provide_context=True,
    python_callable=generate_summaries,
    dag=dag,
)

task3 = operators.PythonOperator(
    task_id='get_named_entities',
    provide_context=True,
    python_callable=get_named_entities,
    dag=dag,
)

task1 >> task2 >> task3