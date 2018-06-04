#!/usr/bin/env python3.5

import sys
from collections import namedtuple

Feed = namedtuple('feed', ['url', 'region', 'topic', 'enabled'])

def print_unknown_line(line, region, topic):
    print(
        'Unknown line:\n\t{}\n\tRegion: {}\n\tTopic: {}'.format(line, region, topic),
        file=sys.stderr,
    )


def get_feeds(file_name='rss.txt'):
    print('Getting feeds from {}'.format(file_name))

    valid_topics = [
        'People and Society',
        'Government and Politics',
        'Economy',
        'Energy',
        'Communications',
        'Transportation',
        'Military and Security',
        'Transnational Issues',
        'World News',
        'Regional and Cities',
        'Environment',
    ]

    region = ''
    topic = ''
    feeds = []
    with open(file_name, 'r') as fd:
        # TODO: refactor rss.txt to enable single line reading
        for line in fd.readlines():
            # Strip trailing newline
            line = line.split('\n')[0]

            if line.startswith('# '):
                # Could be region, topic, or disabled feed
                line = line.split('# ')[-1]
                if line.startswith('http'):
                    if (region is not '') and (topic is not ''):
                        feeds.append(Feed(line, region, topic, False))
                    else:
                        # Error case
                        print_unknown_line(line, region, topic)
                elif line in valid_topics:
                    topic = line
                else:
                    # Set region and reset topic
                    region = line
                    topic = ''
            elif line.startswith('http'):
                feeds.append(Feed(line, region, topic, True))
            elif (line is '#') or (line is ''):
                # Allow empty comment lines
                continue
            else:
                # Error case
                print_unknown_line(line, region, topic)

    return feeds


if __name__ == '__main__':
    # For parsing verification purposes
    # To easily see errors, './rss.py > /dev/null'
    for feed in get_feeds():
        print(feed)