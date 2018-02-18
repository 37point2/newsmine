#!/usr/bin/env python3.5

from airflow import DAG
from airflow import operators
from airflow import hooks
from datetime import datetime, timedelta
import requests
import os
import sys

from rss import get_feeds, Feed
from file_utils import get_mount_folder, datestamp_id_to_path, url_to_file_name

def ts_to_hour(ts):
    if '.' in ts:
        #
        # Hack off the milliseconds
        #
        ts = ts.split('.')[0]
    datestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
    return datetime.strftime(datestamp, '%Y-%m-%dT%H')

def load_data(ds, **kwargs):
    """
    Get feeds and load them into the postgres db
    """

    print(ds)
    sys.exit(0)

    pg_hook = hooks.PostgresHook(postgres_conn_id='rss_urls_id')

    insert_cmd = '''INSERT INTO rss_urls
            (url, region, topic, enabled, added, modified)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET 
                enabled = %s,
                modified = %s'''

    for feed in get_feeds():
        row = (
                feed.url,
                feed.region,
                feed.topic,
                feed.enabled,
                datetime.now().date(),
                datetime.now().date(),
                feed.enabled,
                datetime.now().date())

        pg_hook.run(insert_cmd, parameters=row)

    return 0


def fetch_feed_data(ts, **kwargs):
    """
    Get feeds and load them into the postgres db
    """
    ts = ts_to_hour(ts)
    data_path = os.path.join(get_mount_folder(), 'feeds')

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

            output_dir = os.path.join(data_path, datestamp_id_to_path(ts, src_id))
            os.makedirs(output_dir, exist_ok=True)

            output_file = url_to_file_name(feed_url) + '.raw_feed'
            output_path = os.path.join(output_dir, output_file)
            
            with open(output_path, 'w') as out:
                r = requests.get(feed_url, timeout=5)
                print('Id: {}\tResponse: {}\tURL: {}'.format(src_id, r.status_code, feed_url))
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
        dag_id='import_rss_dag',
        default_args=default_args,
        start_date=datetime(2018,2,18,4,00,00),
        schedule_interval=timedelta(minutes=30))


task1 = operators.PythonOperator(
        task_id='load_data',
        provide_context=True,
        python_callable=load_data,
        dag=dag)

task2 = operators.PythonOperator(
        task_id='fetch_feed_data',
        provide_context=True,
        python_callable=fetch_feed_data,
        dag=dag)

task1 >> task2