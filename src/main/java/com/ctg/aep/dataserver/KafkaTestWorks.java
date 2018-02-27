

package com.ctg.aep.dataserver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.ctg.aep.source.kafka.AEPKafkaSourceConstants;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by zws on 1/28/18.
 * 单机版测试HBase的基础功能
 */
public class KafkaTestWorks {

    String keyTab = "/etc/security/keytabs/odp.user.keytab";
    String principal = "odp/danalysis@DFS.COM";


    public KafkaTestWorks(){

    }

    public void init() throws IOException{

        setConsumerProps();

    }

    private Properties kafkaProps;


    private void setConsumerProps() {
        kafkaProps = new Properties();

        String groupId="aep-dataserver";

        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
        //Defaults overridden based on config
        kafkaProps.put("client.id","aep_client_1");
        kafkaProps.put("auto.commit.interval.ms",3000);
        kafkaProps.put("bootstrap.servers","danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667");
//        kafkaProps.put("security.protocol","SASL_PLAINTEXT");
        kafkaProps.put("security.protocol","SASL_PLAINTEXT");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);

        kafkaProps.put("sasl.kerberos.kinit.cmd","/bin/kinit");
        kafkaProps.put("sasl.kerberos.ticket.renew.window.factor",0.80);
        kafkaProps.put("sasl.kerberos.ticket.renew.jitter",0.05);
        kafkaProps.put("sasl.kerberos.min.time.before.relogin",1 * 60 * 1000L);
    }


    public void testConsumeKafka() throws Exception{
//         KafkaSource.Subscriber subscriber;

        KafkaConsumer<String, byte[]> consumer;

        consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
        Iterator<ConsumerRecord<String, byte[]>> it;

        List<String> topics = new ArrayList<>();
        topics.add("test");
        consumer.subscribe(topics);
        it = consumer.poll(1000).iterator();

        while( true ){
            while( it.hasNext() ) {
                ConsumerRecord<String, byte[]> message = it.next();
                String kafkaKey = message.key();
                byte[] kafkaMessage = message.value();

                String strValue = new String(kafkaMessage);
                System.out.println( "key="+kafkaKey+",value="+strValue);
            }

            it = consumer.poll(1000).iterator();
        }

    }



    public void testConsumeKafkaKerberos() throws Exception{

        System.setProperty("java.security.auth.login.config","/usr/hdp/current/kafka-broker/config/kafka_odp_jaas.conf");

        Map<String,Object> configs = Maps.newHashMap();
        configs.put("sasl.kerberos.ticket.renew.window.factor",0.80);
        configs.put("sasl.kerberos.ticket.renew.jitter",0.05);
        configs.put("sasl.kerberos.min.time.before.relogin",1 * 60 * 1000L);
        configs.put("sasl.kerberos.kinit.cmd","/bin/kinit");


//        this.ticketRenewWindowFactor = ((Double)configs.get("sasl.kerberos.ticket.renew.window.factor")).doubleValue();
//        this.ticketRenewJitter = ((Double)configs.get("sasl.kerberos.ticket.renew.jitter")).doubleValue();
//        this.minTimeBeforeRelogin = ((Long)configs.get("sasl.kerberos.min.time.before.relogin")).longValue();
//        this.kinitCmd = (String)configs.get("sasl.kerberos.kinit.cmd");

        LoginManager loginManager = LoginManager.acquireLoginManager(LoginType.CLIENT,true, configs);
        System.out.println(loginManager.serviceName());
        System.out.println(loginManager.subject().toString());
        System.out.println("---------------------------------");

        testConsumeKafka();
//        PrivilegedExecutor privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(principal,keyTab);
//        privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
//            @Override
//            public Void run() throws Exception {
//                testConsumeKafka();
//                return null;
//            }
//        });

    }


    public static void main(String[] args) throws Exception {

        String CDC_HOME_PROPERTY = "aep.home.dir";
        String CDCHome = System.getProperty(CDC_HOME_PROPERTY, System.getenv("AEP_HOME"));

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        try {
            configurator.doConfigure(CDCHome + "/conf/logback-aep-dataserver.xml");
        } catch (JoranException e) {
            e.printStackTrace();
            System.exit(1);
        }

        KafkaTestApplicaiton applicaiton = new KafkaTestApplicaiton();
        applicaiton.init();
        applicaiton.testConsumeKafkaKerberos();

    }
}