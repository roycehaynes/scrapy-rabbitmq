__author__ = 'roycehaynes'

from scrapy.dupefilter import BaseDupeFilter

import time
import connection

from scrapy.dupefilter import BaseDupeFilter
from scrapy.utils.request import request_fingerprint


class RFPDupeFilter(BaseDupeFilter):
    """RabbitMQ-based request duplication filter"""

    def __init__(self, server, key):
        """Initialize duplication filter

        Parameters
        ----------
        server : RabbitMQ instance
        key : str
            Where to store fingerprints
        """
        self.server = server
        self.key = key

    @classmethod
    def from_settings(cls, settings):
        server = connection.from_settings(settings)
        # create one-time key. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        key = "dupefilter:%s" % int(time.time())
        return cls(server, key)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        fp = request_fingerprint(request)

        added = self.server.basic_publish(
            exchange='',
            routing_key=self.key,
            body=fp
        )

        return not added

    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.server.queue_purge(self.key)

