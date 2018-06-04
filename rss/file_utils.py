#!/usr/bin/env python3.5

import os
import sys
import re

FEED_BASE = 'feeds'
RAW_FEED = '.raw_feed'
PARSED_FEED = '.parsed_feed'
RAW_ARTICLE = '.raw_article'
PARSED_ARTICLE = '.parsed_article'
SUMMARIZED_ARTICLE = '.summ_article'
KEYWORDS_ARTICLE = '.key_article'


def walk_dirs(path, ext=None):
    paths = []
    for root, dirs, files in os.walk(path):
        print(root, dirs, files)
        for name in dirs:
            for subdir in walk_dirs(name):
                paths.append(os.path.join(root, subdir))
        for name in files:
            paths.append(os.path.join(root, name))

    paths = [path for path in paths if (not ext or path.endswith(ext))]

    return paths


def get_mount_folder():
    return os.path.join('/mnt', 'data')


def path_to_id(path):
    if os.path.isfile(path):
        path = os.path.dirname(path)

    # Cannot use basename as it returns '' for a directory
    return int(os.path.split(path)[-1])


def datestamp_to_path(ds):
    #
    # Expects datestamp in format 2018-02-18T00:00:00
    #
    ds = ds.replace('T', '-')
    ds = ds.replace(':', '-')
    return os.path.join(*ds.split('-'))


def datestamp_id_to_path(ds, src_id):
    #
    # Used to get a path to a source's working directory
    #
    return os.path.join(datestamp_to_path(ds), str(src_id))


def baseurl_to_file_name(url):
    # Remove protocol if exists
    if '://' in url:
        url = url.split('://')[1]

    # Remove path if exists
    if '/' in url:
        url = url.split('/')[0]

    return url_to_file_name(url)

def url_to_file_name(url):
    #
    # Nearly impossible to know all the things that could
    # Instead define what we'll accept and then clobber the rest
    #
    def transform(c):
        pattern = re.compile('[a-z0-9]')
        if pattern.match(c):
            return c
        return '_'

    output = ''
    for c in url.lower():
        output += transform(c)

    # Make slightly prettier names
    output = re.sub('_+', '_', output)

    return output


if __name__ == '__main__':
    print(datestamp_to_path('2018-02-18T00:00:00'))
    assert(datestamp_to_path('2018-02-18T00:00:00') == '2018/02/18/00/00/00')

    print(url_to_file_name('http://hosted.ap.org/lineups/USHEADS-rss_2.0.xml?SITE=SCAND&SECTION=HOME'))
    assert(url_to_file_name('http://hosted.ap.org/lineups/USHEADS-rss_2.0.xml?SITE=SCAND&SECTION=HOME') ==
            'http_hosted_ap_org_lineups_usheads_rss_2_0_xml_site_scand_section_home')