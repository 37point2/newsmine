#!/usr/bin/env python3.5

from newspaper import Article

import file_utils as fu

def newspaper_parser(url, input_html):
    """Basic newspaper parser"""
    article = Article(url)

    article.download(input_html=input_html)
    article.parse()

    return (article.title, article.text, article.authors, article.publish_date)    


class ArticleParser(object):

    def __init__(self):
        """Build parser objects"""

        # Only support newspaper parser right now
        self.parsers = {'default':newspaper_parser}

    def parse(self, input_path, output_path, url):
        """Parse a downloaded article"""
        title = ''
        text = ''
        authors = []
        published_date = ''

        base_url = fu.baseurl_to_file_name(url)

        parser = self.parsers.get(base_url) or self.parsers.get('default')

        with open(input_path, 'r') as fd:
            input_html = fd.read()

            title, text, authors, publish_date = parser(url, input_html)

        with open(output_path, 'w') as fd:
            fd.write(text)

        return (title, text, authors, published_date)
