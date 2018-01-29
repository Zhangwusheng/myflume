package com.ctg.aep.dataserver;

/**
 * Created by zhangwusheng on 18/1/25.
 */
public class DataServerConstants {
    
    public static final String HTTP_PORT = "http.port";
    public static final String FLUME_HTTP_PORT = "flume.monitoring.port";

    //这里和命令行的-n是一致的。也和
    public static final String FLUME_AGENT_NAME = "AEP";

    public static final String KERBEROSKEYTAB="hbase.kerberosKeytab";
    public static final String KERBEROSPRINCIPAL="hbase.kerberosPrincipal";
    public static final String COLUMN_FAMILY="hbase.columnFamily";
    public static final String AUTO_CREATE_NS="hbase.autoCreateNamespace";
    public static final String UBER_NAMESPACE="hbase.uberNamespaceName";
    public static final String UBER_TABLENAME="hbase.uberTableName";
    
    
    public static final String KAFKA_PREFIX="kafka.";
    public static final String KAFKA_CONSUMER_PREFIX=KAFKA_PREFIX+"consumer.";
    public static final String KAFKA_MAXBACKOFFSLEEP=KAFKA_PREFIX+"maxBackoffSleep";


    public static final String AEP_TOPIC_NAME = KAFKA_PREFIX+"topic";
    public static final String AEP_CONSUMER_GROUP=KAFKA_CONSUMER_PREFIX+"group.id";
    public static final String AEP_BOOTSTRAP_SERVER=KAFKA_PREFIX+"bootstrap.servers";

    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
}
