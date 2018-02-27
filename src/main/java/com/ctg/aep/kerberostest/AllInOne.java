package com.ctg.aep.kerberostest;

import com.ctg.aep.source.kafka.AEPKafkaSourceConstants;
import com.ctg.itrdc.cache.common.exception.CacheConfigException;
import com.ctg.itrdc.cache.core.CacheService;
import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class AllInOne {

    private CacheService cacheService;
    private String  groupId = "group.AEP.storage";
    private long timeout = 5000;
    private String user = "AEP";
    private String passwd = "Redis123";
    private String[] groups = {groupId};


    private Admin hbaseAdmin;
    private Connection connection;
    private String keyTab = "/etc/security/keytabs/odp.user.keytab";
    private String principal = "odp/danalysis@DFS.COM";

    private  Configuration hbaseConfg =null;
    private Properties kafkaProps;
    private KafkaConsumer<String, byte[]> consumer;

    public AllInOne(){

    }

    String fileName ;
    String JassKey = "java.security.auth.login.config";
    private void resetJassFile(){
        System.setProperty(JassKey,fileName);
    }


    public void init(int n ) throws Exception{

        fileName = System.getProperty(JassKey);

        System.out.println("********************init");
        if( n == 1 ) {
            System.out.println("+++++++++++1:"+fileName);
            resetJassFile();
            cacheService = new CacheService(groups, timeout, user, passwd);
        }
        else if( n == 2 ) {
            UserGroupInformation.loginUserFromKeytab("odp/danalysis@DFS.COM","/etc/security/keytabs/odp.user.keytab");
            resetJassFile();
            hbaseConfg = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(hbaseConfg);
            hbaseAdmin = connection.getAdmin();
        }
        else if( n == 3 )
        {
            resetJassFile();
            kafkaProps = new Properties();

            String KafkaGroupId = "aep-dataserver";

            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
            kafkaProps.put("client.id", "aep_client_1");
            kafkaProps.put("auto.commit.interval.ms", 3000);
            kafkaProps.put("bootstrap.servers", "danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667");
            kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);
            consumer = new KafkaConsumer<String, byte[]>(kafkaProps);
        }
        else if( n ==4 ){
            cacheService = new CacheService(groups, timeout, user, passwd);

            System.out.println("--------------------------------------------");
//            UserGroupInformation.loginUserFromKeytab("odp/danalysis@DFS.COM","/etc/security/keytabs/odp.user.keytab");
            resetJassFile();
            hbaseConfg = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(hbaseConfg);
            hbaseAdmin = connection.getAdmin();

            System.out.println("--------------------------------------------");


        }
        else if(n==5){
            cacheService = new CacheService(groups, timeout, user, passwd);
            resetJassFile();
             kafkaProps = new Properties();

            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++6:"+fileName);

            String KafkaGroupId = "aep-dataserver";

            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
            kafkaProps.put("client.id", "aep_client_1");
            kafkaProps.put("auto.commit.interval.ms", 3000);
            kafkaProps.put("bootstrap.servers", "danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667");
            kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);
            consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++7:"+fileName);

            System.out.println("--------------------------------------------");



        }
        else if(n==6){

//
            cacheService = new CacheService(groups, timeout, user, passwd);
//
            resetJassFile();
//            UserGroupInformation.loginUserFromKeytab("odp/danalysis@DFS.COM","/etc/security/keytabs/odp.user.keytab");

            hbaseConfg = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(hbaseConfg);
            hbaseAdmin = connection.getAdmin();

            System.out.println("--------------------------------------------");

            resetJassFile();
            kafkaProps = new Properties();

            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++6:"+fileName);

            String KafkaGroupId = "aep-dataserver";

            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
            kafkaProps.put("client.id", "aep_client_1");
            kafkaProps.put("auto.commit.interval.ms", 3000);
            kafkaProps.put("bootstrap.servers", "danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667");
            kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);
            consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++7:"+fileName);

            System.out.println("--------------------------------------------");
        }
        else{
            cacheService = new CacheService(groups, timeout, user, passwd);
            resetJassFile();
            System.out.println("--------------------------------------------");

            String fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++4:"+fileName);


            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++5:"+fileName);

//            UserGroupInformation.loginUserFromKeytab("odp/danalysis@DFS.COM","/etc/security/keytabs/odp.user.keytab");
            hbaseConfg = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(hbaseConfg);
            hbaseAdmin = connection.getAdmin();

            System.out.println("--------------------------------------------");


            resetJassFile();
            kafkaProps = new Properties();

            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++6:"+fileName);

            String KafkaGroupId = "aep-dataserver";

            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_KEY_DESERIALIZER);
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_VALUE_DESERIALIZER);
            kafkaProps.put("client.id", "aep_client_1");
            kafkaProps.put("auto.commit.interval.ms", 3000);
            kafkaProps.put("bootstrap.servers", "danalysis.dfs.com:6667,danalysis2.dfs.com:6667,danalysis1.dfs.com:6667");
            kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaGroupId);

            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                    AEPKafkaSourceConstants.DEFAULT_AUTO_COMMIT);
            consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

            fileName = System.getProperty("java.security.auth.login.config");
            System.out.println("+++++++++++7:"+fileName);

            System.out.println("--------------------------------------------");



        }

        System.out.println("********************init");
    }


    public void testCreateNamespace() throws IOException {
        System.out.println("********************testCreateNamespace");

        System.out.println("1.List Namespace------------");
        for (NamespaceDescriptor namespaceDescriptor : hbaseAdmin.listNamespaceDescriptors()) {
            System.out.println(namespaceDescriptor.getName());
        }

        System.out.println("2.Create Namespace------------");
        String namespace = "aep_ns2";
        NamespaceDescriptor namespaceDescriptor;
        try {
            namespaceDescriptor = hbaseAdmin.getNamespaceDescriptor ( namespace );
            System.out.println("------------");
            System.out.println( namespaceDescriptor.toString());
        }catch(NamespaceNotFoundException e){
            System.out.println("---creating namespace...");
            namespaceDescriptor = NamespaceDescriptor.create ( namespace ).build ();
            hbaseAdmin.createNamespace ( namespaceDescriptor );
        }

        System.out.println("3.List Table------------");
        String table = "aep_tbl";
        String columnFamily = "cf";
        TableName tableName = TableName.valueOf ( namespace, table );
        Table htable = null;
        try {
            HTableDescriptor hTableDescriptor = hbaseAdmin.getTableDescriptor ( tableName );
            htable = connection.getTable ( tableName );
            System.out.println("table exists:"+hTableDescriptor);
        }catch ( TableNotFoundException ex ){
            System.out.println("4.Create Table------------");
            System.out.println("creating table");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
            hcd.setBlocksize(16*1024*1024);
            hTableDescriptor.addFamily(hcd);
            hbaseAdmin.createTable ( hTableDescriptor );
            htable = connection.getTable ( tableName );
        }

        System.out.println("********************testCreateNamespace");
    }

    public void dropNamespace() throws IOException{
        System.out.println("********************dropNamespace");
        System.out.println("1.List Namespace------------");
        for (NamespaceDescriptor namespaceDescriptor : hbaseAdmin.listNamespaceDescriptors()) {
            System.out.println(namespaceDescriptor.getName());
        }

        System.out.println("2.dropping Namespace------------");
        String namespace = "aep_ns2";
        NamespaceDescriptor namespaceDescriptor;
        try {
            namespaceDescriptor = hbaseAdmin.getNamespaceDescriptor ( namespace );
            System.out.println("------------");
            System.out.println( namespaceDescriptor.toString());
        }catch(NamespaceNotFoundException e){
            System.out.println("---creating namespace...");
            namespaceDescriptor = NamespaceDescriptor.create ( namespace ).build ();
            hbaseAdmin.createNamespace ( namespaceDescriptor );
        }

        System.out.println("3.List Table------------");
        String table = "aep_tbl";
        String columnFamily = "cf";
        TableName tableName = TableName.valueOf ( namespace, table );
        Table htable = null;
        try {
            HTableDescriptor hTableDescriptor = hbaseAdmin.getTableDescriptor ( tableName );
            htable = connection.getTable ( tableName );
            System.out.println("table exists:"+hTableDescriptor);
        }catch ( TableNotFoundException ex ){
            System.out.println("4.Create Table------------");
            System.out.println("creating table");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
            hcd.setBlocksize(16*1024*1024);
            hTableDescriptor.addFamily(hcd);
            hbaseAdmin.createTable ( hTableDescriptor );
            htable = connection.getTable ( tableName );
        }

        System.out.println("try disable table "+tableName);
        if( htable != null ) {
            if( !hbaseAdmin.isTableDisabled(tableName))
            {
                hbaseAdmin.disableTable(tableName);
                System.out.println("disable table "+tableName+" success");

            }else{
                System.out.println(" table "+tableName+" already disabled");
            }

            hbaseAdmin.deleteTable(tableName);
            System.out.println("deleting table "+tableName+" success");
        }

        System.out.println("deleting namespace "+namespace);
        hbaseAdmin.deleteNamespace(namespace);
        System.out.println("namespace dropped:"+namespace);
        System.out.println("********************dropNamespace");
    }

    public void testWrite() throws IOException{
        System.out.println("********************testWrite");
        String namespace = "aep_ns2";
        String table = "aep_tbl";
        String columnFamily = "cf";
        TableName tableName = TableName.valueOf ( namespace, table );
        System.out.println("5.Write Table------------");
        String rowKey = "row-";
        Put put = new Put(rowKey.getBytes(Charsets.UTF_8));
        put.addColumn(columnFamily.getBytes(),"deviceId".getBytes(Charsets.UTF_8),"value1".getBytes(Charsets.UTF_8));
        put.addColumn(columnFamily.getBytes(),"col2".getBytes(Charsets.UTF_8),"value21".getBytes(Charsets.UTF_8));
        put.addColumn(columnFamily.getBytes(),"col3".getBytes(Charsets.UTF_8),"value3".getBytes(Charsets.UTF_8));
        List<Row> actions= new ArrayList<>();
        actions.add(put);

        Table htable = null;
        htable = connection.getTable ( tableName );
        Object[] results = new Object[actions.size()];
        try {
            htable.batch(actions,results);
            System.out.println("write success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("********************testWrite");
    }

    private void testHbaseData() throws Exception{
        dropNamespace();
        testCreateNamespace();
        testWrite();
    }

    public void testData(int n) throws Exception{

        if( n== 1 ) {
            for (int i = 0; i < 100; i++) {
                String code = cacheService.set(groupId, "itemKey" + i, "value" + i);

                System.out.println("code=" + code);
                String value = cacheService.get(groupId, "itemKey" + i);
                System.out.println("value=" + value);
            }
        }else if( n ==2 ) {
            testHbaseData();
        }
        else if( n ==3 ) {

            Iterator<ConsumerRecord<String, byte[]>> it;

            List<String> topics = new ArrayList<>();
            topics.add("test");
            consumer.subscribe(topics);
            it = consumer.poll(1000).iterator();

            while (true) {
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> message = it.next();
                    String kafkaKey = message.key();
                    byte[] kafkaMessage = message.value();

                    String strValue = new String(kafkaMessage);
                    System.out.println("key=" + kafkaKey + ",value=" + strValue);
                }

                it = consumer.poll(1000).iterator();
            }
        }
        else if( n ==4 ) {

            for (int i = 0; i < 100; i++) {
                String code = cacheService.set(groupId, "itemKey" + i, "value" + i);

                System.out.println("code=" + code);
                String value = cacheService.get(groupId, "itemKey" + i);
                System.out.println("value=" + value);
            }

            testHbaseData();
        }
        else if( n ==5 ){
            for (int i = 0; i < 100; i++) {
                String code = cacheService.set(groupId, "itemKey" + i, "value" + i);

                System.out.println("code=" + code);
                String value = cacheService.get(groupId, "itemKey" + i);
                System.out.println("value=" + value);
            }

//            testHbaseData();


            Iterator<ConsumerRecord<String, byte[]>> it;

            List<String> topics = new ArrayList<>();
            topics.add("test");
            consumer.subscribe(topics);
            it = consumer.poll(1000).iterator();

            while (true) {
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> message = it.next();
                    String kafkaKey = message.key();
                    byte[] kafkaMessage = message.value();

                    String strValue = new String(kafkaMessage);
                    System.out.println("key=" + kafkaKey + ",value=" + strValue);
                }

                it = consumer.poll(1000).iterator();
            }

        }

        else if( n ==6 ){


            testHbaseData();


            Iterator<ConsumerRecord<String, byte[]>> it;

            List<String> topics = new ArrayList<>();
            topics.add("test");
            consumer.subscribe(topics);
            it = consumer.poll(1000).iterator();

            while (true) {
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> message = it.next();
                    String kafkaKey = message.key();
                    byte[] kafkaMessage = message.value();

                    String strValue = new String(kafkaMessage);
                    System.out.println("key=" + kafkaKey + ",value=" + strValue);
                }

                it = consumer.poll(1000).iterator();
            }

        }
        else{
            for (int i = 0; i < 100; i++) {
                String code = cacheService.set(groupId, "itemKey" + i, "value" + i);

                System.out.println("code=" + code);
                String value = cacheService.get(groupId, "itemKey" + i);
                System.out.println("value=" + value);
            }

            testHbaseData();


            Iterator<ConsumerRecord<String, byte[]>> it;

            List<String> topics = new ArrayList<>();
            topics.add("test");
            consumer.subscribe(topics);
            it = consumer.poll(1000).iterator();

            while (true) {
                while (it.hasNext()) {
                    ConsumerRecord<String, byte[]> message = it.next();
                    String kafkaKey = message.key();
                    byte[] kafkaMessage = message.value();

                    String strValue = new String(kafkaMessage);
                    System.out.println("key=" + kafkaKey + ",value=" + strValue);
                }

                it = consumer.poll(1000).iterator();
            }

        }
    }

    public static void main(String[] args) throws Exception{
        if( args.length < 1 ){
            System.out.println("usage: prog N");
            System.out.println("1:  redis");
            System.out.println("2:  hbase without UserGroupInformation.login");
            System.out.println("3:  kafka");
            System.out.println("4:  prog ");
            System.out.println("usage: prog ");
            System.out.println("usage: prog ");
            System.out.println("usage: prog ");
            System.out.println("usage: prog ");
            System.out.println("usage: prog ");
        }

        int n = Integer.parseInt(args[0]);
        AllInOne allInOne = new AllInOne();
        allInOne.init(n );
        allInOne.testData(n );
    }
}
