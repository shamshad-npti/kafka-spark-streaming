"""
test class
"""
import json
import os
import time
import unittest
from datetime import datetime
from kafka import SimpleClient
from kafka import KafkaProducer
from stream.trans_sales_processor import TransSalesStreamProcessor, MysqlUtils

FIXTURE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "fixtures", "data.json")

class TestTransSalesStreamProcessor(unittest.TestCase):
    """
    test cases for sales stream processor (exactly once)
    """
    server = "localhost:9092"
    topics = None

    def setUp(self):

        ident = datetime.now().strftime("%Y%m%d_%H%M%S")
        database = "test_{}".format(ident)

        MysqlUtils.database = os.environ.get("mysql.database", database)
        MysqlUtils.username = os.environ.get("mysql.username", "marker")
        MysqlUtils.password = os.environ.get("mysql.password", "marker-secure")
        MysqlUtils.host = os.environ.get("mysql.host", "localhost")
        MysqlUtils.port = os.environ.get("mysql.port", "3306")

        if self.topics is None:
            self.topics = "test_topic_%s" % (ident)

        self.client = SimpleClient(self.server)
        self.client.ensure_topic_exists(self.topics)

        self.stream_processor = TransSalesStreamProcessor(
            batch_duration=1,
            bootstrap_servers="localhost:9092",
            topics=[self.topics],
        )

        with open(FIXTURE_PATH) as data:
            self.messages = json.load(data)

    def test_streaming(self):
        result = [()]

        def message_handler(data):
            if isinstance(data, list):
                result[0] = data

        self.stream_processor.handler = message_handler

        self.stream_processor.start_streaming(with_await=False)

        producer = KafkaProducer(
            bootstrap_servers=self.server,
            value_serializer=lambda message: json.dumps(message).encode("utf-8")
        )

        time.sleep(2)

        for message in self.messages:
            producer.send(self.topics, message)

        time.sleep(2)

        expected = {
            1: 1100.0,
            2: 600.0
        }

        actual = {row["store_id"]: row["total_sales_price"] for row in result[0]}

        self.assertDictEqual(expected, actual)

    def tearDown(self):
        self.stream_processor.stop()
        MysqlUtils.cleanup()
