#https://www.jianshu.com/p/d310a7628852

tar zxvf kafka_2.11-1.0.0.tgz

cd kafka_2.11-1.0.0

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --list --zookeeper localhost:2181

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning


