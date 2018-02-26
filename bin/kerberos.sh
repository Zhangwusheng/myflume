#!/usr/bin/env bash



useradd odp -d /home/odp


HOSTNAME=`hostname`
sudo /usr/bin/kadmin -p root/admin -w admin -q "ank -randkey odp/${HOSTNAME}"

sudo /usr/bin/kadmin -p root/admin -w admin -q "xst -k /etc/security/keytabs/odp.user.keytab odp/${HOSTNAME}"

chmod +r /etc/security/keytabs/odp.user.keytab




kinit -k -t /etc/security/keytabs/odp.user.keytab odp/danalysis@DFS.COM

kinit -k -t /etc/security/keytabs/odp.user.keytab odp/danalysis1@DFS.COM

kinit -k -t /etc/security/keytabs/odp.user.keytab odp/danalysis2@DFS.COM

sudo /usr/bin/kadmin -p root/admin -w admin -q 'ank -randkey ${user.name}/${hostname}'
sudo /usr/bin/kadmin -p root/admin -w admin -q 'xst -k /etc/security/keytabs/${keytab.filename} ${user.name}/${hostname}'

klist

cd /usr/hdp/current/kafka-broker/bin
./kafka-topics.sh --list --zookeeper localhost:2181


192.168.1.191 ecs-92a5-0004.dfs.com ecs-92a5-0004



/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper localhost:2181

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper ecs-92a5-0004.dfs.com:2181


bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning

{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}
{"col1":"valueOfCol1","col2":"valueOfCol2","col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}





