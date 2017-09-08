"""
A simple spark streaming processor to
aggregate price by store id
"""
import argparse
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

GROUP_ID = "sales-stream-processor"
CHECKPOINT_PATH = ".scc-checkpoint"

def _extract_price(data):
    data = data[1]
    store_id = data["store_id"]
    return [(store_id, item["total_price_paid"]) for item in data["items"]]

def _aggregate_price(price, running_price):
    return (running_price or 0.0) + sum(price)

def _as_dict(row):
    return {"store_id": row[0], "total_sales_price": row[1]}

class StreamingProcessor(object):
    def __init__(self, batch_duration, bootstrap_servers, topics, **kwargs):
        # create spark config
        conf = SparkConf().setAppName("sales-stream-processor")

        # create spark context
        spark_context = SparkContext.getOrCreate(conf=conf)

        # desable detailed logging
        spark_context.setLogLevel("WARN")

        self.spark_context = spark_context
        self.batch_duration = batch_duration
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.checkpoint = kwargs.get("checkpoint", None) or CHECKPOINT_PATH
        self.streaming_context = None

        if callable(kwargs.get("handler", None)):
            self.handler = kwargs["handler"]
        else:
            self.handler = self._console_handler

    @staticmethod
    def _console_handler(rdd):
        data = rdd.collect()
        if len(data) == 1:
            print data[0]

    def start_streaming(self, with_await=True):
        """
        start processing stream
        """

        # create a streaming context
        streaming_context = StreamingContext.getOrCreate(self.checkpoint, self._setup_streaming)

        self.streaming_context = streaming_context

        # start streaming
        streaming_context.start()
        if with_await:
            streaming_context.awaitTermination()

    def _setup_streaming(self):
        streaming_context = StreamingContext(self.spark_context, self.batch_duration)
        streaming_context.checkpoint(self.checkpoint)

        # kafka parameters to connect to server
        kafka_params = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": GROUP_ID,
            "zookeeper.connection.timeout.ms": "30000",
        }

        # create a dstream
        dstream = KafkaUtils.createDirectStream(
            ssc=streaming_context,
            topics=self.topics,
            kafkaParams=kafka_params,
            valueDecoder=lambda value: json.loads(value.decode("utf-8"))
        )

        # apply transformation
        # 1. flatMap nested price information to [(store_id, price)]
        # 2. reduce the price within the stream
        # 3. update aggregated state with new price in stream
        # 4. For printing only
        #   a. convert entire stream to a list of tuple(store_id, price)
        #   b. convert the list to json string
        aggregated_stream = dstream.flatMap(_extract_price)\
            .reduceByKey(lambda x, y: x + y)\
            .updateStateByKey(_aggregate_price)\
            .map(_as_dict)\
            .map(lambda x: [x])\
            .reduce(lambda x, y: x + y)\
            .map(lambda x: json.dumps(x, indent=2))

        # print result
        aggregated_stream.foreachRDD(self.handler)

        return streaming_context

def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap_servers",
        help="kafka servers host[:port]",
        default="localhost:9092"
    )

    parser.add_argument(
        "--batch_duration",
        help="batch duration in seconds",
        type=int,
        default=30
    )

    parser.add_argument(
        "--topics",
        help="kafka topic to receive message from",
        nargs="+",
        default=["sales_receipts"]
    )

    args = parser.parse_args()

    StreamingProcessor(
        batch_duration=args.batch_duration,
        bootstrap_servers=args.bootstrap_servers,
        topics=args.topics
    ).start_streaming()

if __name__ == '__main__':
    _main()
