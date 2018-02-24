#监控HTTP端口
aep.monitoring.port=18900

kafka.topic=test
kafka.consumer.group.id=aep-dataserver
kafka.consumer.client.id=aep_dataserver_1
kafka.consumer.auto.commit.interval.ms = 3000
kafka.bootstrap.servers=localhost:9092
#没有数据的时候睡眠的时间
kafka.maxBackoffSleep = 22

hbase.kerberosKeytab=/etc/security/keytabs/odp.user.keytab
hbase.kerberosPrincipal=odp/${HOSTNAME}@DFS.COM
#所有的表统一使用这个列簇
hbase.columnFamily=zws

#是否自动创建namespace
hbase.autoCreateNamespace=true
#如果autoCreateNamespace=false，统一写入的Namespace的名字
#当自动创建ns失败时，也会写入这个NS
hbase.uberNamespaceName=aepdataserver
#当使用uberNamespaceName，对应的tableName
hbase.uberTableName=aeptable

redis.host=localhost
redis.port=8188

ctgcache.group=group.AEP.storage
ctgcache.timeout=1000
ctgcache.user=AEP
ctgcache.passwd=Redis123
ctgcache.using_hash=false
