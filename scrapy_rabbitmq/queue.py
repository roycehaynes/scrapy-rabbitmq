from scrapy.utils.reqser import request_to_dict, request_from_dict

try:
    import cPickle as pickle
except ImportError:
    import pickle


class Base(object):
    """Per-spider queue/stack base class"""

    def __init__(self, server, spider, key, exchange=None):
        """Initialize per-spider RabbitMQ queue.

        Parameters:
            server -- rabbitmq connection
            spider -- spider instance
            key -- key for this queue (e.g. "%(spider)s:queue")
        """
        self.server = server
        self.spider = spider
        self.key = key % {'spider': spider.name}

    def _encode_request(self, request):
        """Encode a request object"""
        return pickle.dumps(request_to_dict(request, self.spider), protocol=-1)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        return request_from_dict(pickle.loads(encoded_request), self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.queue_purge(self.key)


class SpiderQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        response = self.server.queue_declare(self.key, passive=True)
        return response.message_count

    def push(self, request):
        """Push a request"""
        self.server.basic_publish(
            exchange='',
            routing_key=self.key,
            body=request
        )

    def pop(self):
        """Pop a request"""

        method_frame, header, body = self.server.basic_get(queue=self.key)

        if body:
            return self._decode_request(body)


__all__ = ['SpiderQueue']
