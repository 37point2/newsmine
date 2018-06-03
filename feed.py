#!/usr/bin/env python3.5

import file_utils as fu

def get_feeds(pg_hook):
    """Returns sorted list of feeds"""

    get_feeds_cmd = '''SELECT id, url, region, topic from rss_urls'''

    conn = pg_hook.get_conn()
    curr = pg_hook.get_cursor()

    curr.execute(get_feeds_cmd)
    feeds = curr.fetchall()
    # Add empty option
    feeds.append((0, 'Unknown', 'Unknown', 'Unknown'))
    feeds = sorted(feeds, key=lambda feed: feed[0])

    curr.close()
    conn.close()

    return feeds

def feed_from_rss_id(feeds, rss_id):
    """Gets a feed from rss_id"""
    
    num_feeds = len(feeds)

    if (rss_id < num_feeds) and (feeds[rss_id][0] == rss_id):
        feed = feeds[rss_id]
    else:
        print('Sort failed')
        for item in feeds:
            if item[0] == rss_id:
                feed = item
                break
            if item[0] > rss_id:
                # No need to check the rest of the items
                break
    if feed == 0:
        feed = feeds[0]

    return feed

def parts(feed):
    """Returns tuple of feed parts"""

    # url_to_file_name can also be used on strings
    feed_name = fu.baseurl_to_file_name(feed[1])
    region = fu.url_to_file_name(feed[2])
    topic = fu.url_to_file_name(feed[3])

    return (feed_name, region, topic)