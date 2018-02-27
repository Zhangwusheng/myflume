package com.ctg.aep.kerberostest;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class HbaseComponentBase extends BaseComponent {

    private Admin hbaseAdmin;
    private Connection connection;

    private Configuration hbaseConfg =null;

    protected abstract void doInit() throws Exception;

    protected void login() throws IOException{
        UserGroupInformation.loginUserFromKeytab(getPrincipal(), getKeyTabFileName());
    }

    @Override
    public void initialize() throws Exception {

        doInit();
//        resetJassFileWithCache();
        hbaseConfg = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(hbaseConfg);
        hbaseAdmin = connection.getAdmin();
    }

    @Override
    public void doWork() throws Exception {
        testHbaseData();
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

}
