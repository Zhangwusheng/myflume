#!/usr/bin/env bash

cd /usr/hdp/current/kafka-broker/bin

kinit -k -t /etc/security/keytabs/odp.user.keytab odp/danalysis@DFS.COM

bin/odp-kafka-topics.sh --list --zookeeper ecs-92a5-0002.dfs.com:2181,ecs-92a5-0004.dfs.com:2181,ecs-92a5-0009.dfs.com:2181


diff odp-kafka-topics.sh kafka-topics.sh
17,23d16
<
< KAFKA_JAAS_CONF=$KAFKA_HOME/config/kafka_odp_jaas.conf
< if [ -f $KAFKA_JAAS_CONF ]; then
<     export KAFKA_CLIENT_KERBEROS_PARAMS="-Djava.security.auth.login.config=$KAFKA_HOME/config/kafka_odp_jaas.conf"
< fi
<
<

bin/odp-kafka-topics.sh --list --zookeeper ecs-92a5-0002.dfs.com:2181

#用kafka用户创建，odp用户不能创建
bin/odp-kafka-topics.sh --create --zookeeper ecs-92a5-0002.dfs.com:2181 --replication-factor 1 --partitions 1 --topic test


/usr/hdp/current/kafka-broker/bin/odp-kafka-console-producer.sh --broker-list localhost:9092 --topic test --security-protocol PLAINTEXTSASL
/usr/hdp/current/kafka-broker/bin/odp-kafka-console-producer.sh --bootstrap.servers
#这里zk上必须odp能读取那个目录
/usr/hdp/current/kafka-broker/bin/odp-kafka-console-consumer.sh --zookeeper ecs-92a5-0002.dfs.com:2181 --topic test --from-beginning --security-protocol PLAINTEXTSASL
/usr/hdp/current/kafka-broker/bin/odp-kafka-console-consumer.sh --new-consumer --bootstrap-server danalysis.dfs.com:6667 --security-protocol SASL_PLAINTEXT --topic test --from-beginning

#能够运行的
/usr/hdp/current/kafka-broker/bin/odp-kafka-console-producer.sh --broker-list danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667 --topic test --security-protocol PLAINTEXTSASL
/usr/hdp/current/kafka-broker/bin/odp-kafka-console-consumer.sh --zookeeper ecs-92a5-0002.dfs.com:2181 --topic test --from-beginning --security-protocol PLAINTEXTSASL
/usr/hdp/current/kafka-broker/bin/odp-kafka-console-consumer.sh --new-consumer --bootstrap-server danalysis.dfs.com:6667 --security-protocol SASL_PLAINTEXT --topic test --from-beginning

{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}



/usr/hdp/current/kafka-broker/bin/kafka-acls.sh --authorizer-properties zookeeper.connect=ecs-92a5-0002.dfs.com:2181 --list --topic test
/usr/hdp/current/kafka-broker/bin/kafka-acls.sh --authorizer-properties zookeeper.connect=ecs-92a5-0002.dfs.com:2181 --add --allow-principal User:* --group * --topic test
