#!/usr/bin/env python3.5

import sys

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

def make_database(drop=False):
    """
    Create the database and tables
    """

    db_name = 'RssDB'
    user_name = 'airflow'
    rss_urls_table = 'rss_urls'
    rss_feeds_table = 'rss_feeds'
    rss_articles_table = 'rss_articles'
    parsed_articles_table = 'parsed_articles'

    engine = create_engine('postgresql+psycopg2://{}@localhost/{}'.format(user_name, db_name))

    if not database_exists(engine.url):
        create_database(engine.url)

    conn = psycopg2.connect(database=db_name, user=user_name)

    curr = conn.cursor()

    if drop:
        #
        # Are you really sure you want to do this?
        # ids for rss feeds are used in filesystem paths
        #
        # drop_rss_urls_table = 'DROP TABLE {}'.format(rss_urls_table)
        # curr.execute(drop_rss_urls_table)
        # conn.commit()

        drop_rss_feeds_table = 'DROP TABLE {}'.format(rss_feeds_table)
        curr.execute(drop_rss_feeds_table)
        conn.commit()

        drop_rss_articles_table = 'DROP TABLE {}'.format(rss_articles_table)
        curr.execute(drop_rss_articles_table)
        conn.commit()

    create_rss_urls_table = '''CREATE TABLE IF NOT EXISTS {}
            (
                id          SERIAL,
                url         TEXT NOT NULL UNIQUE,
                region      TEXT NOT NULL,
                topic       TEXT NOT NULL,
                enabled     BOOLEAN NOT NULL,
                errors      INT DEFAULT 0,
                added       timestamp,
                modified    timestamp
            )
            '''.format(rss_urls_table)
    curr.execute(create_rss_urls_table)
    conn.commit()

    create_rss_feeds_table = '''CREATE TABLE IF NOT EXISTS {}
            (
                id          SERIAL,
                rss_id      INT NOT NULL,
                url         TEXT NOT NULL UNIQUE,
                title       TEXT,
                description TEXT,
                pub_date    TEXT,
                added       timestamp
            )

            '''.format(rss_feeds_table)
    curr.execute(create_rss_feeds_table)
    conn.commit()

    create_rss_articles_table = '''CREATE TABLE IF NOT EXISTS {}
            (
                id          SERIAL,
                rss_id      INT NOT NULL,
                url         TEXT NOT NULL UNIQUE,
                file_path   TEXT,
                added       timestamp
            )

            '''.format(rss_articles_table)
    curr.execute(create_rss_articles_table)
    conn.commit()

    create_parsed_articles_table = '''CREATE TABLE IF NOT EXISTS {}
            (
                id              SERIAL,
                rss_id          INT NOT NULL,
                url             TEXT NOT NULL UNIQUE,
                authors         TEXT,
                file_path       TEXT,
                publish_date    timestamp,
                added           timestamp
            )
            '''.format(parsed_articles_table)
    curr.execute(create_parsed_articles_table)
    conn.commit()

    try:
        alter_parsed_articles_table = '''ALTER TABLE IF EXISTS {} 
            ADD title TEXT'''.format(parsed_articles_table)
        curr.execute(alter_parsed_articles_table)
        conn.commit()
    except psycopg2.ProgrammingError:
        pass

    conn.close()

if __name__ == '__main__':
    print(sys.argv)
    if (len(sys.argv) > 1) and (sys.argv[1] is "drop"):
        make_database(True)
    else:
        make_database()