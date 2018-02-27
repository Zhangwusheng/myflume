package com.ctg.aep.dataserver;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.LocalDateTime;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by zws on 1/28/18.
 * 单机版测试HBase的基础功能
 */
public class HbaseTestApplicaiton {

    private Admin hbaseAdmin;
    private Connection connection;

//    String keyTab = "/etc/security/keytabs/odp.app.keytab";
//    String principal = "odp/test1a1.iot.com";
    String keyTab = "/etc/security/keytabs/odp.user.keytab";
    String principal = "odp/danalysis@DFS.COM";

    Configuration hbaseConfg =null;

    public HbaseTestApplicaiton(){

    }

    public void init() throws IOException{
        hbaseConfg = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(hbaseConfg);
        hbaseAdmin = connection.getAdmin();
    }

    public void testCreateNamespaceKerberos() throws Exception {

        PrivilegedExecutor privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(principal,keyTab);
        privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                testCreateNamespace();
                return null;
            }
        });

    }

    public void testDropNamespaceKerberos() throws Exception {

        PrivilegedExecutor privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(principal,keyTab);
        privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                dropNamespace();
                return null;
            }
        });

    }

    public void dropNamespace() throws IOException{

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

    }

    public void testWriteKerberos() throws Exception {

//        String keyTab = "/etc/security/keytabs/odp.app.keytab";
//        String principal = "odp/test1a1.iot.com";

        PrivilegedExecutor privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(principal,keyTab);
        privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                testWrite();
                return null;
            }
        });

    }

    public void testWrite() throws IOException{

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
    }
    public void testCreateNamespace() throws IOException{
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
    }

    public void test() throws Exception {
        HbaseTestApplicaiton applicaiton = this;

        for(int i=0;i<10;i++) {
            System.out.println("Cycle==============" + i);
            System.out.println("-------------------------------------");
            applicaiton.testDropNamespaceKerberos();

            System.out.println("===================================");
            applicaiton.testCreateNamespaceKerberos();

            System.out.println("++++++++++++++++++++++++");
            applicaiton.testWriteKerberos();
        }

    }

    public static void main(String[] args) throws Exception {

        org.joda.time.LocalDateTime localDateTime = new LocalDateTime();
        System.out.println( localDateTime.getYear()*100+localDateTime.getMonthOfYear());
        int yyyymm =  localDateTime.getYear()*100+localDateTime.getMonthOfYear();
        String ss2 = String.format("%d",yyyymm);
//        System.out.println(ss2);
//        System.exit(1);

        ByteBuf byteBuf = Unpooled.buffer(256);
        Random random = new Random(System.currentTimeMillis());

        byteBuf.writeBytes("deviceid1-timestamp1-".getBytes());
        byteBuf.writeInt(random.nextInt());
        String ss = ByteBufUtil.prettyHexDump(byteBuf);
        System.out.println(ss);

        byteBuf.clear();
        byteBuf.writeBytes("deviceid1-timestamp1-".getBytes());
        byteBuf.writeInt(random.nextInt());
        ss = ByteBufUtil.prettyHexDump(byteBuf);

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

        HbaseTestApplicaiton applicaiton = new HbaseTestApplicaiton();
        applicaiton.init();

        int TOTAL = Integer.parseInt(args[0]);

        for(int i=0;i<TOTAL;i++) {
            System.out.println("Cycle=============="+i);
            System.out.println("-------------------------------------");
            applicaiton.testDropNamespaceKerberos();

            System.out.println("===================================");
            applicaiton.testCreateNamespaceKerberos();

            System.out.println("++++++++++++++++++++++++");
            applicaiton.testWriteKerberos();
        }
    }
}
