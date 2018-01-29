package com.ctg.aep.dataserver;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

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

    public static void main(String[] args) throws IOException {

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
        System.out.println(ss);

        HbaseTestApplicaiton applicaiton = new HbaseTestApplicaiton();
        applicaiton.testCreateNamespace();
    }

    public void testCreateNamespaceKerberos() throws Exception {

        String keyTab = "/etc/security/keytabs/odp.app.keytab";
        String principal = "odp/test1a1.iot.com";

        PrivilegedExecutor privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(keyTab, principal);
        privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                testCreateNamespace();
                return null;
            }
        });

    }

    public void testCreateNamespace() throws IOException{

        Configuration hbaseConfg = HBaseConfiguration.create();

        connection = ConnectionFactory.createConnection(hbaseConfg);
        hbaseAdmin = connection.getAdmin();

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

        System.out.println("5.Write Table------------");
        String rowKey = "row-";
        Put put = new Put(rowKey.getBytes(Charsets.UTF_8));
        put.addColumn(columnFamily.getBytes(),"deviceId".getBytes(Charsets.UTF_8),"value1".getBytes(Charsets.UTF_8));
        put.addColumn(columnFamily.getBytes(),"col2".getBytes(Charsets.UTF_8),"value21".getBytes(Charsets.UTF_8));
        put.addColumn(columnFamily.getBytes(),"col3".getBytes(Charsets.UTF_8),"value3".getBytes(Charsets.UTF_8));
        List<Row> actions= new ArrayList<>();
        actions.add(put);

        Object[] results = new Object[actions.size()];
        try {
            htable.batch(actions,results);
            System.out.println("write success");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
