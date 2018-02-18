#!/usr/bin/env python3.5

from airflow import DAG
from airflow import operators
from airflow import hooks
from datetime import datetime, timedelta
import os
import requests

from file_utils import get_mount_folder, \
        datestamp_id_to_path, \
        datestamp_to_path, \
        url_to_file_name, \
        path_to_id
from feed_parser import get_parser

def ts_to_hour(ts):
    if '.' in ts:
        #
        # Hack off the milliseconds
        #
        ts = ts.split('.')[0]
    datestamp = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S')
    return datetime.strftime(datestamp, '%Y-%m-%dT%H')


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

    return 0


def walk_dirs(path):
    paths = []
    for root, dirs, files in os.walk(path):
        print(root, dirs, files)
        for name in dirs:
            for subdir in walk_dirs(name):
                paths.append(os.path.join(root, subdir))
        for name in files:
            paths.append(os.path.join(root, name))
    return paths


def parse_feed_data(ts, **kwargs):
    """
    Parse feeds from folders on disk
    """
    ts = ts_to_hour(ts)
    data_path = os.path.join(get_mount_folder(), 'feeds')
    data_dirs = os.path.join(data_path, datestamp_to_path(ts))

    if not os.path.exists(data_dirs) and os.path.isdir(data_dirs):
        print('Error: Unknown directory {}'. format(data_dirs), file=sys.stderr)
        return -1

    pg_hook = hooks.PostgresHook(postgres_conn_id='rss_urls_id')
    conn = pg_hook.get_conn()
    curr = pg_hook.get_cursor()

    insert_cmd = '''INSERT INTO rss_feeds
            (rss_id, url, title, description, pub_date, added)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING'''

    paths = walk_dirs(data_dirs)
    for path in paths:
        src_id = path_to_id(path)
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

task1 >> task2
