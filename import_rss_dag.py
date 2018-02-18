#!/usr/bin/env python3.5

from airflow import DAG
from airflow import operators
from airflow import hooks
from datetime import datetime, timedelta
import os

from rss import get_feeds, Feed

def load_data(ds, **kwargs):
    """
    Get feeds and load them into the postgres db
    """
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
