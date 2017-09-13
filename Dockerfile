FROM debian:jessie

RUN apt-get update && apt-get upgrade

WORKDIR /app

RUN apt-get install -y git vim curl \
	&& apt-get install openjdk-8-jdk openjdk-8-jre git -y \
	&& wget www.scala-lang.org/files/archive/scala-2.11.7.deb \
	&& dpkg -i scala-2.11.7.deb \
	&& rm scala-2.11.7.deb

RUN wget http://www-eu.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz \
	&& tar -xzvf kafka_2.11-0.11.0.0.tgz \
	&& mv kafka_2.11-0.11.0.0 kafka_2.11 \
	&& rm kafka_2.11-0.11.0.0.tgz 

RUN wget https://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz \
	&& tar -xzvf spark-2.0.2-bin-hadoop2.7.tgz \
	&& mv spark-2.0.2-bin-hadoop2.7 spark-2.0.2 \
	&& rm spark-2.0.2-bin-hadoop2.7.tgz \

RUN git clone https://github.com/shamshad-npti/kafka-spark-streaming.git \
	&& cd kafka-spark-streaming \
	&& wget https://bootstrap.pypa.io/get-pip.py \
	&& python get-pip.py \
	&& rm get-pip.py \
	&& pip install virtualenv \
	&& virtualenv .venv \
	&& source .venv/bin/activate \
	&& pip install -r requirements.txt

CMD ['export PATH=$PATH:/app/kafka_2.11/bin:/app/spark-2.0.2/bin']