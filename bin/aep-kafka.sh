#!/usr/bin/env bash

cd /usr/hdp/current/kafka-broker/bin

bin/kafka-topics.sh --list --zookeeper ecs-92a5-0002.dfs.com:2181,ecs-92a5-0004.dfs.com:2181,ecs-92a5-0009.dfs.com:2181


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

