"""
Test processor
"""
import json
import os
import shutil
import time
import unittest
from datetime import datetime
from kafka import SimpleClient
from kafka import KafkaProducer
from stream.sales_stream_processor import StreamingProcessor

FIXTURE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "fixtures", "data.json")

class TestSalesProcessor(unittest.TestCase):
    """
    test cases sales processor
    """
    server = "localhost:9092"
    topics = None

    def setUp(self):

        ident = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.checkpoints = ".check-%s" % (ident)
        if self.topics is None:
            self.topics = "test-topic-%s" % (ident)

        self.client = SimpleClient(self.server)
        self.client.ensure_topic_exists(self.topics)

        self.stream_processor = StreamingProcessor(
            batch_duration=1,
            bootstrap_servers="localhost:9092",
            topics=[self.topics],
            checkpoint=self.checkpoints
        )

        with open(FIXTURE_PATH) as data:
            self.messages = json.load(data)

    def test_streaming(self):
        result = [None]

        def message_handler(rdd):
            data = rdd.collect()
            if len(data) == 1:
                result[0] = data[0]

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
        actual = {row["store_id"]: row["total_sales_price"] for row in json.loads(result[0])}

        self.assertDictEqual(expected, actual)

    def tearDown(self):
        if self.stream_processor.streaming_context:
            self.stream_processor.streaming_context.stop()
        self.stream_processor.spark_context.stop()
        try:
            shutil.rmtree(self.checkpoints)
        except BaseException:
            pass
