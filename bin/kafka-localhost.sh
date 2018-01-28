#https://www.jianshu.com/p/d310a7628852

tar zxvf kafka_2.11-1.0.0.tgz

cd kafka_2.11-1.0.0

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --list --zookeeper localhost:2181

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


hbase-site.xml
<configuration>
	<property>
		<name>hbase.rootdir</name>
		<value>file:///data1/apps/hbase-1.3.1/data</value>
	</property>

	<property>
          <name>hbase.zookeeper.property.clientPort</name>
          <value>2182</value>
  </property>
</configuration>


hbase-env.sh
 export HBASE_MANAGES_ZK=false


 将HBASE的conf加入到Classpath里面去：
 File->Project Structure->Modules->data-server->Sources把目录作为source加入进去，
 intellij就会把这个目录自动加入到classpath里面去



 {"deviceId":"device1","timestamp":1517138154664,"col3":"valueOfCol3","col4":"valueOfCol4","col5":"valueOfCol5","col6":"valueOfCol6","col7":"valueOfCol7","payload":"UmVhRGF0YVBheUxvYWQ=","tenant":"tenant","tableName":"testtbl"}