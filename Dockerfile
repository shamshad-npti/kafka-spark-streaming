# Dockerfile for testing, the image is pushed here: skramer/kafka-spark-mysql-testing

FROM debian:jessie

WORKDIR /app

RUN echo "deb http://httpredir.debian.org/debian/ jessie-backports main" \
	> /etc/apt/sources.list.d/debian-jessie-backports.list

RUN { \
	echo "Package: *"; \
	echo "Pin: release o=Debian,a=jessie-backports"; \
	echo "Pin-Priority: -200"; \
} > /etc/apt/preferences.d/debian-jessie-backports

RUN apt-get update; \
	install -y debian-keyring; \
	key=A4A9406876FCBD3C456770C88C718D3B5072E1F5; \
	gpg --keyserver ha.pool.sks-keyservers.net --recv-keys $key; \
	gpg --export $key > /etc/apt/trusted.gpg.d/mysql.gpg; \
	apt-key list > /dev/null;

RUN echo "deb http://repo.mysql.com/apt/debian jessie mysql-5.7" \
	> /etc/apt/sources.list.d/mysql.list

RUN apt-get update \
	&& apt-get install -y git vim curl wget \
	&& apt-get install -t jessie-backports openjdk-8-jdk openjdk-8-jre git -y \
	&& wget www.scala-lang.org/files/archive/scala-2.11.7.deb \
	&& dpkg -i scala-2.11.7.deb \
	&& rm scala-2.11.7.deb

ENV DEBIAN_FRONTEND=noninteractive

RUN { \
		echo 'mysql-server-5.6 mysql-server/root_password password insecure-password'; \
		echo 'mysql-server-5.6 mysql-server/root_password_again password insecure-password'; \
		echo 'mysql-apt-config mysql-apt-config/enable-repo select mysql-5.7'; \
	} | debconf-set-selections \
	&& apt-get install -y mysql-server


RUN wget http://www-eu.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz \
	&& tar -xzvf kafka_2.11-0.11.0.0.tgz \
	&& mv kafka_2.11-0.11.0.0 kafka_2.11 \
	&& rm kafka_2.11-0.11.0.0.tgz

RUN wget https://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz \
	&& tar -xzvf spark-2.0.2-bin-hadoop2.7.tgz \
	&& mv spark-2.0.2-bin-hadoop2.7 spark-2.0.2 \
	&& rm spark-2.0.2-bin-hadoop2.7.tgz

COPY ./init.sh /usr/local/bin/

CMD ["/bin/bash", "/usr/local/bin/init.sh"]