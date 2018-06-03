#!/usr/bin/env python3.5

from airflow import DAG
from airflow import operators
from airflow import hooks
from datetime import datetime, timedelta
import os
import requests

import feed
import file_utils as fu
import time_utils as tu
from feed_parser import get_parser

def fetch_feed_data(ts, **kwargs):
    """
    Get feeds and load them into the postgres db
    """
    ts = tu.ts_to_hour(ts)
    data_path = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)

    print('Get data from enabled feeds')
    pg_hook = hooks.PostgresHook(postgres_conn_id='rss_urls_id')

    fetch_feeds_cmd = '''SELECT id, url, errors
            FROM rss_urls
            WHERE enabled is True'''

    conn = pg_hook.get_conn()
    curr = pg_hook.get_cursor()
    curr.execute(fetch_feeds_cmd)

    rows = curr.fetchall()

    for row in rows:
        try:
            src_id = row[0]
            feed_url = row[1]
            errors = row[2]

            if errors > 100:
                #
                # Should probably stop grabbing this feed
                #
                disable_feed_cmd = '''UPDATE rss_urls 
                                        SET enabled = {} 
                                        WHERE id = {}'''.format(False, src_id)
                curr.execute(disable_feed_cmd)
                conn.commit()

            print(str(src_id) + ', ' + str(feed_url))

            output_dir = os.path.join(data_path, fu.datestamp_id_to_path(ts, src_id))
            os.makedirs(output_dir, exist_ok=True)

            output_file = fu.url_to_file_name(feed_url) + fu.RAW_FEED
            output_path = os.path.join(output_dir, output_file)
            
            with open(output_path, 'w') as out:
                r = requests.get(feed_url, timeout=5)
                print('Id: {}\tResponse: {}\tErrors: {}\tURL: {}'.format(
                        src_id,
                        r.status_code,
                        errors,
                        feed_url))
                out.write(r.text)

        except Exception as e:
            #
            # Increment errors and log exception
            #
            increment_error_cmd = '''UPDATE rss_urls 
                                        SET errors = errors + 1 
                                        WHERE id = {}'''.format(src_id)
            curr.execute(increment_error_cmd)
            conn.commit()
            print(e)

    curr.close()
    conn.close()

    return 0


def parse_feed_data(ts, **kwargs):
    """
    Parse feeds from folders on disk
    """
    ts = tu.ts_to_hour(ts)
    data_path = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)
    data_dirs = os.path.join(data_path, fu.datestamp_to_path(ts))

    if not os.path.exists(data_dirs) and os.path.isdir(data_dirs):
        print('Error: Unknown directory {}'. format(data_dirs), file=sys.stderr)
        return -1

    pg_hook = hooks.PostgresHook(postgres_conn_id='rss_urls_id')

    insert_cmd = '''INSERT INTO rss_feeds
            (rss_id, url, title, description, pub_date, added)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING'''

    paths = fu.walk_dirs(data_dirs, fu.RAW_FEED)
    for path in paths:
        src_id = fu.path_to_id(path)
        parser = get_parser(path)

        if parser == None:
            continue

        articles = parser.parse()
        for article in articles:
            row = (
                    src_id,
                    article.url,
                    article.title,
                    article.description,
                    article.pub_date,
                    datetime.now().date())

            print(row)

            pg_hook.run(insert_cmd, parameters=row)

    return 0


def get_articles(ts, **kwargs):
    """
    Parse feeds from folders on disk
    """
    hour = tu.ts_to_hour(ts)
    month = tu.ts_to_month(ts)
    data_path = os.path.join(fu.get_mount_folder(), fu.FEED_BASE)
    data_dirs = os.path.join(data_path, fu.datestamp_to_path(hour))

    if not os.path.exists(data_dirs) and os.path.isdir(data_dirs):
        print('Error: Unknown directory {}'. format(data_dirs), file=sys.stderr)
        return -1

    pg_hook = hooks.PostgresHook(postgres_conn_id='rss_urls_id')
    
    feeds = feed.get_feeds(pg_hook)

    get_articles_cmd = '''SELECT id, rss_id, url FROM rss_feeds'''

    conn = pg_hook.get_conn()
    curr = pg_hook.get_cursor()


    curr.execute(get_articles_cmd)
    rows = curr.fetchall()
    for row in rows:
        src_id = row[0]
        rss_id = row[1]
        url = row[2]
        
        feed_tuple = feed.feed_from_rss_id(feeds, rss_id)

        feed_name, region, topic = feed.parts(feed_tuple)

        print(str(src_id) + ', ' +str(rss_id) + ', ' + str(url))

        print('Removing article')
        remove_article_cmd = '''DELETE FROM rss_feeds WHERE id = {}'''.format(src_id)
        curr.execute(remove_article_cmd)
        conn.commit()
        print('Done removing article')

        output_dir = os.path.join(data_path, region, topic, month, feed_name)
        os.makedirs(output_dir, exist_ok=True)

        output_file = fu.url_to_file_name(url) + fu.RAW_ARTICLE
        output_path = os.path.join(output_dir, output_file)

        # Short circuit
        if os.path.exists(output_path):
            print('Path already exists, skipping')
            continue

        try:
            with open(output_path, 'w') as out:
                r = requests.get(url, timeout=5)
                print('Id: {}\tResponse: {}\tURL: {}'.format(
                        src_id,
                        r.status_code,
                        url))
                if r.status_code == 200:
                    out.write(r.text)
                    raw_article_cmd = '''INSERT INTO rss_articles
                            (rss_id, url, file_path, added)
                            VALUES
                            (%s, %s, %s, %s)
                            ON CONFLICT (url) DO NOTHING'''
                    print('Inserting article')
                    article = (rss_id, url, output_path, datetime.now().date())
                    pg_hook.run(raw_article_cmd, parameters=article)
                    print('Done inserting article')
        except Exception as e:
            #
            # Log exception
            #
            print(e)

    curr.close()
    conn.close()
    
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
        dag_id='get_feeds_dag',
        default_args=default_args,
        start_date=datetime(2018,2,18),
        schedule_interval=timedelta(minutes=60))


task1 = operators.PythonOperator(
        task_id='fetch_feed_data',
        provide_context=True,
        python_callable=fetch_feed_data,
        dag=dag)

task2 = operators.PythonOperator(
        task_id='parse_feed_data',
        provide_context=True,
        python_callable=parse_feed_data,
        dag=dag)

task3 = operators.PythonOperator(
        task_id='get_articles',
        provide_context=True,
        python_callable=get_articles,
        dag=dag)

task1 >> task2
