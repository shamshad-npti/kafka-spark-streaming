import argparse
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

GROUP_ID = "sales-stream-processor"

def _extract_price(data):
    data = data[1]
    store_id = data["store_id"]
    return [(store_id, item["total_price_paid"]) for item in data["items"]]

def _aggregate_price(price, running_price):
    return (running_price or 0.0) + sum(price)

def _as_dict(row):
    return {"store_id": row[0], "total_sales_price": row[1]}

def _start_streaming(args):

    # create spark config
    conf = SparkConf().setAppName("sales-stream-processor")

    # create spark context
    spark_context = SparkContext.getOrCreate(conf=conf)

    # desable detailed logging
    spark_context.setLogLevel("WARN")

    # create a streaming context
    streaming_context = StreamingContext(spark_context, args.batch_duration)

    # kafka parameters to connect to server
    kafka_params = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": GROUP_ID,
        "zookeeper.connection.timeout.ms": "30000",
    }

    # create a dstream
    dstream = KafkaUtils.createDirectStream(
        ssc=streaming_context,
        topics=args.topics,
        kafkaParams=kafka_params,
        valueDecoder=lambda value: json.loads(value.decode("utf-8"))
    )

    # apply transformation
    aggregated_stream = dstream.flatMap(_extract_price)\
        .reduceByKey(lambda x, y: x + y)\
        .updateStateByKey(_aggregate_price)\
        .map(_as_dict)\
        .map(lambda x: [x])\
        .reduce(lambda x, y: x + y)\
        .map(lambda x: json.dumps(x, indent=2))

    # print result
    aggregated_stream.pprint()

    # set-up checkpoint
    streaming_context.checkpoint(".scc-checkpoint")

    # start streaming
    streaming_context.start()
    streaming_context.awaitTermination()

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
        default=5
    )

    parser.add_argument(
        "--topics",
        help="kafka topic to receive message from",
        nargs="+",
        default=["sales_receipts"]
    )

    args = parser.parse_args()
    _start_streaming(args)

if __name__ == '__main__':
    _main()
