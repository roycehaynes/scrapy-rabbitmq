from scrapy_rabbitmq import connection
from unittest import TestCase
from tests.common.rabbitmqctl_cmd import create_user, delete_user
import pika


class ConnectionTestCase(TestCase):
    def setUp(self):
        self.user = 'admin'
        self.password = '123456'
        create_user(self.user, self.password)

    def tearDown(self):
        delete_user(self.user)

    def test_pika_conn(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost', credentials=credentials, channel_max=2, retry_delay=50))
        connection.close()

    def test_from_setings_auth(self):
        credentials = connection.credentials(self.user, self.password)
        settings = {
            'RABBITMQ_CONNECTION_PARAMETERS': {
                'host': 'localhost',
                'credentials': credentials
            }
        }

        ch = connection.from_settings(settings)
        ch.close()
