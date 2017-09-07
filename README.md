Stream Processing of Sales Order
---


# How to Run the Application on a Single Node

1. Install java and scala

```bash
# become root
sudo su
mkdir /app; cd /app

# install java, git and scala2.11
apt-get install openjdk-8-jdk openjdk-8-jre git -y
wget www.scala-lang.org/files/archive/scala-2.11.7.deb
dpkg -i scala-2.11.7.deb
rm scala-2.11.7.deb
```

2. Download and start kafka

```bash
# downlaod and extract kafka
wget http://www-eu.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz
tar -xzvf kafka_2.11-0.11.0.0.tgz
mv kafka_2.11-0.11.0.0 kafka_2.11
rm kafka_2.11-0.11.0.0.tgz

# start zookeeper 
./kafka_2.11/bin/zookeeper-server-start.sh -daemon kafka_2.11/config/zookeeper.properties

# start kafka
./kafka_2.11/bin/kafka-server-start.sh -daemon kafka_2.11/config/server.properties

# Create topic in kafka
./kafka_2.11/bin/kafka-topics.sh\
	--zookeeper "localhost:2181"\
	--create\
	--topic sales_receipts --partitions 4 --replication-factor 1
```

3. Download and configure spark environment

```bash
# download spark-2.0.2
wget https://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz
tar -xzvf spark-2.0.2-bin-hadoop2.7.tgz
mv spark-2.0.2-bin-hadoop2.7 spark-2.0.2
rm spark-2.0.2-bin-hadoop2.7.tgz
```

4. Clone application from github

```bash
git clone https://github.com/shamshad-npti/kafka-spark-streaming.git
cd kafka-spark-streaming
```

5. Setup python and application environment

```bash
wget https://bootstrap.pypa.io/get-pip.py
python get-pip.py
rm get-pip.py
pip install virtualenv
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

6. Start a producer (Just to run the demo)

```bash
export PYTHONPATH=$PYTHONPATH:`pwd`
python producer/item_receipt_producer.py &
```

7. Start Spark Streaming Context 

```bash
../spark-2.0.2/bin/spark-submit\
 	--jars jars/spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar\
 	stream/sales_stream_processor.py
```