FROM debian:jessie

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME="/app/spark-2.0.2"
ENV KAFKA_JAR="spark-streaming-kafka-0-8-assembly_2.11-2.0.0-preview.jar"

RUN echo "deb http://httpredir.debian.org/debian/ jessie-backports main" \
    > /etc/apt/sources.list.d/debian-jessie-backports.list

RUN { \
        echo "Package: *"; \
        echo "Pin: release o=Debian,a=jessie-backports"; \
        echo "Pin-Priority: -200"; \
    } > /etc/apt/preferences.d/debian-jessie-backports

RUN key=A4A9406876FCBD3C456770C88C718D3B5072E1F5; \
    gpg --keyserver ha.pool.sks-keyservers.net --recv-keys $key; \
    gpg --export $key > /etc/apt/trusted.gpg.d/mysql.gpg; \
    apt-key list > /dev/null;

RUN echo "deb http://repo.mysql.com/apt/debian jessie mysql-5.7" \
    > /etc/apt/sources.list.d/mysql.list

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
        libmysqlclient-dev \
        python \
        python-dev \
        python-pip \
        vim \
        wget \
    && apt-get install -y --no-install-recommends -t jessie-backports \
        openjdk-8-jdk \
        openjdk-8-jre \
    && { \
        echo 'mysql-server-5.6 mysql-server/root_password password insecure-password'; \
        echo 'mysql-server-5.6 mysql-server/root_password_again password insecure-password'; \
        echo 'mysql-apt-config mysql-apt-config/enable-repo select mysql-5.7'; \
    } | debconf-set-selections \
    && apt-get install -y --no-install-recommends mysql-server \
    && wget -q www.scala-lang.org/files/archive/scala-2.11.7.deb \
    && dpkg -i scala-2.11.7.deb \
    && rm scala-2.11.7.deb \
    && wget -q http://www-eu.apache.org/dist/kafka/0.11.0.0/kafka_2.11-0.11.0.0.tgz \
    && tar -xzf kafka_2.11-0.11.0.0.tgz \
    && mv kafka_2.11-0.11.0.0 kafka_2.11 \
    && rm kafka_2.11-0.11.0.0.tgz \
    && wget -q https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz \
    && tar -xzf spark-2.2.0-bin-hadoop2.7.tgz \
    && mv spark-2.2.0-bin-hadoop2.7 spark-2.0.2 \
    && rm spark-2.2.0-bin-hadoop2.7.tgz \
    && { \
        echo "spark.driver.extraClassPath     $SPARK_HOME/custom-jars/$KAFKA_JAR"; \
        echo "spark.executor.extraClassPath   $SPARK_HOME/custom-jars/$KAFKA_JAR"; \
    } > $SPARK_HOME/conf/spark-defaults.conf \
    && mkdir -p $SPARK_HOME/custom-jars \
    && wget -q -O $SPARK_HOME/custom-jars/$KAFKA_JAR http://central.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-8-assembly_2.11/2.0.0-preview/$KAFKA_JAR \
    && apt-get autoremove -y \
        curl \
        wget \
        vim \
    && rm -rf /var/lib/apt/lists/*



COPY ./init.sh /usr/local/bin/

ENTRYPOINT bash /usr/local/bin/init.sh && bash