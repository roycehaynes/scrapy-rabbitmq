
from scrapy_rabbitmq.connection import from_settings
from twisted.internet.threads import deferToThread
from scrapy.utils.serialize import ScrapyJSONEncoder

EXCHANGE_NAME = ''


class RabbitMQPipeline(object):
    """Pushes serialized item into a RabbitMQ list/queue"""

    def __init__(self, server, exchange_name):
        self.server = server
        self.exchange_name = exchange_name
        self.encoder = ScrapyJSONEncoder()

    @classmethod
    def from_settings(cls, settings):
        server = from_settings(settings)
        exchange_name = settings.get('RABBITMQ_EXCHANGE_NAME', EXCHANGE_NAME)
        return cls(server, exchange_name)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        key = self.item_key(item, spider)
        data = self.encoder.encode(item)
        self.server.basic_publish(exchange=self.exchange_name,
                                  routing_key=key,
                                  body=data)
        return item

    def item_key(self, item, spider):
        """Returns RabbitMQ key based on given spider"""
        return "%s:items" % spider.name
