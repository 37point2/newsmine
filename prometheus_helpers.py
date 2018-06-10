#!/usr/bin/env python3.5

from prometheus_client import start_http_server, Summary, Counter

class Registry:

    def __init__(self, port):
        start_http_server(port)
        self.registry = {}


    def _get_metric(self, name):
        return self.registry.get(name, None)


    def _put_metric(self, name, metric):
        self.registry[name] = metric


    def get_counter(self, name, summary):
        if not self._get_metric(name):
            self._put_metric(name, Counter(name, summary))
        return self._get_metric(name)


    def get_summary(self, name, summary):
        if not self._get_metric(name):
            self._put_metric(name, Summary(name, summary))
        return self._get_metric(name)