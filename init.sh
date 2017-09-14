/etc/init.d/mysql start
mysql -uroot -pinsecure-password -e "CREATE USER IF NOT EXISTS marker IDENTIFIED BY 'marker-secure'; GRANT ALL ON *.* TO marker;"
export KAFKA_HOME=/app/kafka_2.11
export SPARK_HOME=/app/spark-2.0.2
export PATH=$PATH:$KAFKA_HOME/bin:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

echo "$KAFKA_HOME/bin/zookeeper-server-start.sh"
bash $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5
bash $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties