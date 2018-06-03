#!/usr/bin/env python3.5

import os
import sys
import re
import types
from io import BytesIO
from lxml import etree
from collections import namedtuple

Article = namedtuple('article', ['url', 'title', 'description', 'pub_date'])

def clean_element(element):
    """
    Take a string and strip html/xml from it
    """
    if isinstance(element, str) or isinstance(element, bytes):
        return re.sub(r'<[^>]*>', '', element)
    else:
        return ''


def get_parser(path):
    """

    """
    raw = open(path, 'r').read()

    header = raw[:1000]
    if 'atom' in header:
        pattern = re.compile(r'version=.2')
        if pattern.search(header):
            return Atom2Parser(path)
    else:
        print('Error: Unknown xml feed format {}'.format(path), sys.stderr)
        return None

class FeedParser(object):

    def __init__(self, path):
        self.path = path
        self.raw = open(path, 'rb').read()


    def parse(self):
        """
        Use 'get_parser' method to get a Parser
        """
        raise NotImplementedError


class Atom2Parser(FeedParser):

    def parse(self):
        tree = etree.parse(BytesIO(self.raw))
        root = tree.getroot()

        if len(root.getchildren()) != 1:
            print('Error: Unexpected xml in {}'.format(self.path), file=sys.stderr)
            return None

        articles = []
        for element in root.getchildren()[0].getchildren():
            if element.tag == 'item':
                item = element.getchildren()
                title = ''
                description = ''
                link = ''
                pub_date = ''
                for elmnt in item:
                    if elmnt.tag == 'title':
                        title = clean_element(elmnt.text)
                    elif elmnt.tag == 'description':
                        description = clean_element(elmnt.text)
                    elif elmnt.tag == 'link':
                        link = clean_element(elmnt.text)
                    elif elmnt.tag == 'pubDate':
                        pub_date = clean_element(elmnt.text)
                if (title == '') or (description == '') or (link == '') or (pub_date == ''):
                    print('Error: malformed item')
                    print('Article:\n\tTitle: {}\n\tDescription: {}\n\tLink: {}\n\tPubDate: {}'.format(
                        title,
                        description,
                        link,
                        pub_date), file=sys.stderr)
                    continue
                article = Article(link, title, description, pub_date)
                articles.append(article)

        print('Found {} articles from {}'.format(len(articles), self.path))
        return articles


if __name__ == '__main__':
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = '/mnt/data/feeds/2018/02/18/00/1/http_rss_cnn_com_rss_cnn_topstories_rss.raw_feed'
    parser = get_parser(path)
    articles = parser.parse()
    # for article in articles:
    #     print(article)