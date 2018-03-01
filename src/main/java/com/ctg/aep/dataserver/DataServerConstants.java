package com.ctg.aep.dataserver;

/**
 * Created by zhangwusheng on 18/1/25.
 */
public class DataServerConstants {
    
    public static final String HTTP_PORT = "http.port";
    public static final String FLUME_HTTP_PORT = "flume.monitoring.port";

    //这里和命令行的-n是一致的。也和

    public static final String KERBEROSKEYTAB="hbase.kerberosKeytab";
    public static final String KERBEROSPRINCIPAL="hbase.kerberosPrincipal";
    public static final String COLUMN_FAMILY="hbase.columnFamily";
    public static final String AUTO_CREATE_NS="hbase.autoCreateNamespace";
    public static final String UBER_NAMESPACE="hbase.uberNamespaceName";
    public static final String UBER_TABLENAME="hbase.uberTableName";
    
    
    public static final String KAFKA_PREFIX="kafka.";
    public static final String KAFKA_CONSUMER_PREFIX=KAFKA_PREFIX+"consumer.";
    public static final String KAFKA_MAXBACKOFFSLEEP=KAFKA_PREFIX+"maxBackoffSleep";
    public static final String KAFKA_SECURITY_PROTOCOL=KAFKA_PREFIX+"security.protocol";
    public static final String KAFKA_DEBUG_MODE=KAFKA_PREFIX+"debug";

    public static final String AEP_TOPIC_NAME = KAFKA_PREFIX+"topic";
    public static final String AEP_CONSUMER_GROUP=KAFKA_CONSUMER_PREFIX+"group.id";
    public static final String AEP_BOOTSTRAP_SERVER=KAFKA_PREFIX+"bootstrap.servers";
    public static final String AEP_KAFKA_SECURITY_PROTOCOL=KAFKA_PREFIX+"security.protocol";
    public static final String AEP_KAFKA_KINIT_CMD=KAFKA_PREFIX+"sasl.kerberos.kinit.cmd";
    public static final String AEP_KAFKA_KERBEROS_RENEW_WINDOW=KAFKA_PREFIX+"sasl.kerberos.ticket.renew.window.factor";
    public static final String AEP_KAFKA_KERBEROS_RENEW_JITTER=KAFKA_PREFIX+"sasl.kerberos.ticket.renew.jitter";
    public static final String AEP_KAFKA_KERBEROS_RELOGIN_TIME=KAFKA_PREFIX+"sasl.kerberos.min.time.before.relogin";
    public static final String AEP_KAFKA_USE_KERBEROS=KAFKA_PREFIX+"useKerberos";
    public static final String AEP_KAFKA_JAAS_FILE=KAFKA_PREFIX+"jaasfile";
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";

    public static final String CTGCACHE_GROUP = "ctgcache.group";
    public static final String CTGCACHE_TIMEOUT = "ctgcache.timeout";
    public static final String CTGCACHE_USER = "ctgcache.user";
    public static final String CTGCACHE_PASSWD = "ctgcache.passwd";
    public static final String CTGCACHE_USING_HASH = "ctgcache.using_hash";
}
