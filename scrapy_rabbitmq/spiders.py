__author__ = 'roycehaynes'

import scrapy_rabbitmq.connection as connection

from scrapy.spider import Spider
from scrapy import signals
from scrapy.exceptions import DontCloseSpider


class RabbitMQMixin(object):
    """ A RabbitMQ Mixin used to read URLs from a RabbitMQ queue.
    """

    rabbitmq_key = None

    def __init__(self):
        self.server = None

    def setup_rabbitmq(self):
        """ Setup RabbitMQ connection.

            Call this method after spider has set its crawler object.
        :return: None
        """

        if not self.rabbitmq_key:
            self.rabbitmq_key = '{}:start_urls'.format(self.name)

        self.server = connection.from_settings(self.crawler.settings)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def next_request(self):
        """ Provides a request to be scheduled.
        :return: Request object or None
        """

        method_frame, header_frame, url = self.server.basic_get(queue=self.rabbitmq_key)

        if url:
            return self.make_requests_from_url(url)

    def schedule_next_request(self):
        """ Schedules a request, if exists.

        :return:
        """
        req = self.next_request()

        if req:
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """ Waits for request to be scheduled.

        :return: None
        """
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """ Avoid waiting for spider.
        :param args:
        :param kwargs:
        :return: None
        """
        self.schedule_next_request()


class RabbitMQSpider(RabbitMQMixin, Spider):
    """ Spider that reads urls from RabbitMQ queue when idle.
    """

    def set_crawler(self, crawler):
        super(RabbitMQSpider, self).set_crawler(crawler)
        self.setup_rabbitmq()
