package com.ctg.aep.dataserver;

/**
 * Created by zhangwusheng on 18/1/25.
 */
public class DataServerConstants {
    
    public static final String HTTP_PORT = "http.port";
    public static final String FLUME_HTTP_PORT = "flume.monitoring.port";
    
    public static final String FLUME_AGENT_NAME = "AEP";
    public static final String KERBEROSKEYTAB="hbase.kerberosKeytab";
    public static final String KERBEROSPRINCIPAL="hbase.kerberosPrincipal";
    
    public static final String KAFKA_PREFIX="aep.kafka.";
    public static final String KAFKA_CONSUMER_PREFIX=KAFKA_PREFIX+"consumer.";
    
    
    public static final String AEP_TOPIC_NAME = KAFKA_PREFIX+"topic";
    public static final String AEP_CONSUMER_GROUP=KAFKA_CONSUMER_PREFIX+"group.id";
    public static final String AEP_BOOTSTRAP_SERVER="aep.kafka.bootstrap.servers";
}
