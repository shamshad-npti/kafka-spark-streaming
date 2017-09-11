"""
A simple spark streaming processor to
aggregate price by store id. (Semantics: `exactly-once`)
"""
import argparse
import json
import threading
from datetime import datetime
from collections import defaultdict
from pyspark import SparkConf, SparkContext, TaskContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from sqlalchemy.sql import text
from sqlalchemy import create_engine

GROUP_ID = "sales-stream-processor"
CHECKPOINT_PATH = ".scc-checkpoint"

OFFSET_UPSERT_QEURY = """
INSERT INTO `offsets` (`topic`, `partition`, `offset`) VALUES(:topic, :partition, :offset)
ON DUPLICATE KEY UPDATE `offset` = :offset
"""

SALES_UPSERT_QUERY = """
INSERT INTO `sales` (`store_id`, `date`, `total_sales_price`) VALUES (:store_id, :date, :total_sales_price)
ON DUPLICATE KEY UPDATE `total_sales_price` = `total_sales_price` + :total_sales_price
"""

SELECT_SALES_QUERY = "SELECT `store_id`, `total_sales_price` FROM `sales` WHERE `date` = :date"
SELECT_OFFSETS_QUERY = "SELECT `topic`, `partition`, `offset` FROM `offsets`"


class MysqlUtils(object):
    """
    utility classs for setting up and cleaning database
    """
    OFFSET_CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS `offsets` (
        `topic` VARCHAR(200) NOT NULL,
        `partition` INT NOT NULL,
        `offset` BIGINT,
        PRIMARY KEY(`topic`, `partition`)
    ) ENGINE=InnoDB
    """

    SALES_CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS `sales` (
        `store_id` INT NOT NULL,
        `date` DATE NOT NULL,
        `total_sales_price` DOUBLE,
        PRIMARY KEY(`store_id`, `date`)
    ) ENGINE=InnoDB
    """

    TABLES = ["offsets", "sales"]

    host = None
    port = None
    username = None
    password = None
    database = None

    @classmethod
    def url(cls, with_database=True):
        """
        get database connection url
        """
        conn_url = "mysql://{username}:{password}@{host}:{port}"
        if with_database:
            conn_url += '/{database}'
        return conn_url.format(
            username=cls.username,
            password=cls.password,
            host=cls.host,
            port=cls.port,
            database=cls.database
        )

    @classmethod
    def init(cls, clean=False):
        """
        initialize databse, optionally clean the table
        """
        engine = create_engine(cls.url(with_database=False))
        with engine.begin() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS `%s`" %  (cls.database))
            conn.execute("USE `%s`" % (cls.database))
            conn.execute(cls.OFFSET_CREATE_TABLE_QUERY)
            conn.execute(cls.SALES_CREATE_TABLE_QUERY)

            if clean:
                for table in cls.TABLES:
                    conn.execute("TRUNCATE TABLE `%s`" % (table))

    @classmethod
    def cleanup(cls):
        """
        remove tables and schema, useful for writing test cases
        """
        engine = create_engine(cls.url())

        with engine.begin() as conn:
            for table in cls.TABLES:
                conn.execute("DROP TABLE `%s`" % (table))
            conn.execute("DROP SCHEMA `%s`" % (cls.database))


def _process(timeunit, rdd):
    offsets = rdd.offsetRanges()
    timestr = timeunit.strftime("%Y-%m-%d")
    url = MysqlUtils.url()

    def _process_partition(messages):
        offset = offsets[TaskContext.get().partitionId()]
        result = defaultdict(float)

        for (_, message) in messages:
            price = sum(item['total_price_paid'] for item in message['items'])
            result[message['store_id']] += price

        engine = create_engine(url)

        # avoid transactional deadlock
        result = sorted(result.iteritems())

        with engine.begin() as conn:
            for store_id, price in result:
                conn.execute(
                    text(SALES_UPSERT_QUERY),
                    store_id=store_id,
                    date=timestr,
                    total_sales_price=price
                )

            conn.execute(
                text(OFFSET_UPSERT_QEURY),
                topic=offset.topic,
                partition=offset.partition,
                offset=offset.untilOffset
            )

        return [len(result)]

    # make sure transformation get applied
    rdd.mapPartitions(_process_partition).reduce(lambda x, y: x + y)


class TransSalesStreamProcessor(object):
    """
    transactional stream handler
    """
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

        self.mysql_url = MysqlUtils.url()
        self.__stopped = False
        MysqlUtils.init()

    @staticmethod
    def _fetch_sales_data(url):
        engine = create_engine(url)
        result = []
        with engine.begin() as conn:
            resultset = conn.execute(
                text(SELECT_SALES_QUERY),
                date=datetime.today().strftime("%Y-%m-%d")
            )

            result = [{
                'store_id': row['store_id'],
                'total_sales_price': row['total_sales_price']
            } for row in resultset.fetchall()]

        return result

    @staticmethod
    def _fetch_offsets(url):
        engine = create_engine(url)
        result = dict()
        with engine.begin() as conn:
            resultset = conn.execute(text(SELECT_OFFSETS_QUERY))
            result = {
                TopicAndPartition(
                    topic=row['topic'],
                    partition=int(row['partition'])
                ): long(row['offset']) for row in resultset.fetchall()
            }

        return result

    @staticmethod
    def _console_handler(result):
        if result:
            print json.dumps(result, indent=2)

    def _execute_handler(self):
        if not self.__stopped:
            self.handler(self._fetch_sales_data(self.mysql_url))
            threading.Timer(self.batch_duration, self._execute_handler).start()

    def start_streaming(self, with_await=True):
        """
        start processing stream
        """

        # create a streaming context
        self.streaming_context = self._setup_streaming()

        # start streaming
        self.streaming_context.start()
        if with_await:
            self.streaming_context.awaitTermination()

    def stop(self):
        """
        stop streaming
        """
        self.__stopped = True
        if self.streaming_context:
            self.streaming_context.stop()
        self.spark_context.stop()

    def _setup_streaming(self):
        streaming_context = StreamingContext(self.spark_context, self.batch_duration)
        # streaming_context.checkpoint(self.checkpoint)

        # kafka parameters to connect to server
        kafka_params = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': GROUP_ID,
            'zookeeper.connection.timeout.ms': '30000',
            'enable.auto.commit': 'false'
        }

        # create a dstream
        dstream = KafkaUtils.createDirectStream(
            ssc=streaming_context,
            topics=self.topics,
            fromOffsets=self._fetch_offsets(self.mysql_url),
            kafkaParams=kafka_params,
            valueDecoder=lambda value: json.loads(value.decode("utf-8"))
        )

        dstream.foreachRDD(_process)

        # set execution for handler in thread
        self._execute_handler()

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
        default=5
    )

    parser.add_argument(
        "--topics",
        help="kafka topic to receive message from",
        nargs="+",
        default=["sales_receipts"]
    )

    parser.add_argument(
        "--username",
        help="transactional database username",
        default="marker"
    )

    parser.add_argument(
        "--password",
        help="transactional database password",
        default="marker-secure"
    )

    parser.add_argument(
        "--host",
        help="transactional database host",
        default="localhost"
    )

    parser.add_argument(
        "--port",
        help="transactional database port",
        default="3306"
    )

    parser.add_argument(
        "--database",
        help="name of database",
        default="sales"
    )

    args = parser.parse_args()

    MysqlUtils.username = args.username
    MysqlUtils.password = args.password
    MysqlUtils.database = args.database
    MysqlUtils.host = args.host
    MysqlUtils.port = args.port

    TransSalesStreamProcessor(
        batch_duration=args.batch_duration,
        bootstrap_servers=args.bootstrap_servers,
        topics=args.topics
    ).start_streaming()

if __name__ == '__main__':
    _main()
