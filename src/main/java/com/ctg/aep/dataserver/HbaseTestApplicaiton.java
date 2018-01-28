package com.ctg.aep.dataserver;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by zws on 1/28/18.
 */
public class HbaseTestApplicaiton {

    private Admin hbaseAdmin;
    private Connection connection;
    public void testCreateNamespace() throws IOException{
        Configuration hbaseConfg = HBaseConfiguration.create();

            connection = ConnectionFactory.createConnection(hbaseConfg);
            hbaseAdmin =connection.getAdmin ();

            System.out.println("------------");
            for (NamespaceDescriptor namespaceDescriptor : hbaseAdmin.listNamespaceDescriptors()) {
                System.out.println(namespaceDescriptor.getName());
            }



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

        String table = "aep_tbl";
        String columnFamily = "cf";
        TableName tableName = TableName.valueOf ( namespace, table );
        Table htable = null;
        try {
            HTableDescriptor hTableDescriptor = hbaseAdmin.getTableDescriptor ( tableName );
            htable = connection.getTable ( tableName );
            System.out.println("table exists:"+hTableDescriptor);
        }catch ( TableNotFoundException ex ){

            System.out.println("creating table");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
            hcd.setBlocksize(16*1024*1024);
            hTableDescriptor.addFamily(hcd);
            hbaseAdmin.createTable ( hTableDescriptor );
            htable = connection.getTable ( tableName );
        }

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

//         ByteBuffer byteBuffer = ByteBuffer.allocate(100);
//        byteBuffer.put("deviceid1-timestamp1-".getBytes());
//        Random random = new Random(System.currentTimeMillis());
//        byteBuffer.putInt(random.nextInt());
//        byteBuffer.flip();
//        byte[] bytes = new byte[byteBuffer.position()];
//        byteBuffer.get(bytes);
//        System.out.println(bytes);
//
//        ByteBuf byteBuf = Unpooled.buffer();
//        byteBuf.writeBytes(byteBuffer);
//        String ss = ByteBufUtil.prettyHexDump(byteBuf);
//        System.out.println(ss);
//
//        byteBuffer.clear();
//        byteBuffer.put("deviceid2-timestamp2-".getBytes());
//        byteBuffer.putInt(random.nextInt());
//        byteBuffer.flip();
//        bytes = new byte[byteBuffer.position()];
//        byteBuffer.get(bytes);
//        System.out.println(bytes);


//        HbaseTestApplicaiton applicaiton = new HbaseTestApplicaiton();
//        applicaiton.testCreateNamespace();
    }
}
