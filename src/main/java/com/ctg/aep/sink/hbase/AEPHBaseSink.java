/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ctg.aep.sink.hbase;

import com.ctg.aep.data.AEPDataObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flume.*;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Flume的HbaseSink要求配置TableName，我们的需求是根据数据解析Json，
 * 根据指定的字段，自动创建namespace和table。
 */

public class AEPHBaseSink extends AbstractSink implements Configurable {
  private String tableName;
  private byte[] columnFamily;
  private HTable table;
  private long batchSize;
  private Configuration config;
  private static final Logger logger = LoggerFactory.getLogger(AEPHBaseSink.class);
  private AEPHbaseEventSerializer serializer;
  private String eventSerializerType;
  private Context serializerContext;
  private String kerberosPrincipal;
  private String kerberosKeytab;
  private boolean enableWal = true;
  private boolean batchIncrements = false;
  private Method refGetFamilyMap = null;
  private SinkCounter sinkCounter;
  private PrivilegedExecutor privilegedExecutor;
  
  private Admin hbaseAdmin;
  private Connection connection;
  private ObjectMapper objectMapper;
  private String uberNamespace;
  private String uberTableName;
  private Boolean autoCreateNamespace;
  private AEPDataObject aepDataObject;
  
  private Map<String,NamespaceDescriptor> stringNamespaceDescriptorMap = Maps.newHashMap (  );
  // Internal hooks used for unit testing.
  private DebugIncrementsCallback debugIncrCallback = null;
  
  public AEPHBaseSink () {
    this(HBaseConfiguration.create());
  }
  
  public AEPHBaseSink ( Configuration conf) {
    this.config = conf;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  AEPHBaseSink ( Configuration conf, DebugIncrementsCallback cb) {
    this(conf);
    this.debugIncrCallback = cb;
  }
  
  
  private ObjectMapper getDefaultObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    //设置将对象转换成JSON字符串时候:包含的属性不能为空或"";
    //Include.Include.ALWAYS 默认
    //Include.NON_DEFAULT 属性为默认值不序列化
    //Include.NON_EMPTY 属性为空（""）  或者为 NULL 都不序列化
    //Include.NON_NULL 属性为NULL 不序列化
    mapper.setSerializationInclusion( JsonSerialize.Inclusion.NON_EMPTY);
    
    //设置将MAP转换为JSON时候只转换值不等于NULL的
    mapper.configure( SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
    mapper.setDateFormat(new SimpleDateFormat ("yyyy-MM-ddHH:mm:ss"));
//     mapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
    
    //设置有属性不能映射成PO时不报错
    mapper.disable( DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
//     mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,false);  上一条也可以如此设置；
    
    return mapper;
  }
  
  @Override
  public void start() {
    Preconditions.checkArgument(table == null, "Please call stop " +
            "before calling start on an old instance.");
    try {
      privilegedExecutor =
              FlumeAuthenticationUtil.getAuthenticator(kerberosPrincipal, kerberosKeytab);
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException("Failed to login to HBase using "
              + "provided credentials.", ex);
    }
    try {
      table = privilegedExecutor.execute(new PrivilegedExceptionAction<HTable>() {
        @Override
        public HTable run() throws Exception {
          HTable table = new HTable(config, tableName);
          table.setAutoFlush(false);
          // Flush is controlled by us. This ensures that HBase changing
          // their criteria for flushing does not change how we flush.
          return table;
        }
      });
    } catch (Exception e) {
      sinkCounter.incrementConnectionFailedCount();
      logger.error("Could not load table, " + tableName +
              " from HBase", e);
      throw new FlumeException("Could not load table, " + tableName +
              " from HBase", e);
    }
    try {
      if (!privilegedExecutor.execute(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws IOException {
          return table.getTableDescriptor().hasFamily(columnFamily);
        }
      })) {
        throw new IOException("Table " + tableName
                + " has no such column family " + Bytes.toString(columnFamily));
      }
    } catch (Exception e) {
      //Get getTableDescriptor also throws IOException, so catch the IOException
      //thrown above or by the getTableDescriptor() call.
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException("Error getting column family from HBase."
              + "Please verify that the table " + tableName + " and Column Family, "
              + Bytes.toString(columnFamily) + " exists in HBase, and the"
              + " current user has permissions to access that table.", e);
    }
    
    super.start();
    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();
  }
  
  @Override
  public void stop() {
    try {
      if (table != null) {
        table.close();
      }
      table = null;
    } catch (IOException e) {
      throw new FlumeException("Error closing table.", e);
    }
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Context context) {
    
    logger.info ( "---------------com.ctg.aep.sink.hbase.HBaseSink.configure called" );
    
    
    autoCreateNamespace = context.getBoolean ( HBaseSinkConfigurationConstants.AUTO_CREATE_NAMESPACE,false );
    if( autoCreateNamespace ){
      logger.info ( "HBaseSinkConfigurationConstants.autoCreateNamespace set to true" );
    }
    
    uberNamespace = context.getString ( HBaseSinkConfigurationConstants.UBER_NAMESPACE_NAME );
    uberTableName = context.getString ( HBaseSinkConfigurationConstants.UBER_TABLE_NAME );
    
    objectMapper =  getDefaultObjectMapper();
    
    try {
      connection = ConnectionFactory.createConnection(this.config);
      hbaseAdmin =connection.getAdmin ();
    }
    catch ( IOException e ) {
      logger.error ( "Faile to get HbaseAdmin:"+e.getMessage () );
      throw new ConfigurationException ( e );
    }


//    tableName = context.getString( HBaseSinkConfigurationConstants.CONFIG_TABLE);
    String cf = context.getString(
            HBaseSinkConfigurationConstants.CONFIG_COLUMN_FAMILY);
    logger.info ( "cf={}",cf);
    batchSize = context.getLong(
            HBaseSinkConfigurationConstants.CONFIG_BATCHSIZE, new Long(100));
    serializerContext = new Context();
    //If not specified, will use HBase defaults.
    eventSerializerType = context.getString(
            HBaseSinkConfigurationConstants.CONFIG_SERIALIZER);
//    Preconditions.checkNotNull(tableName,
//        "Table name cannot be empty, please specify in configuration file");
//    Preconditions.checkNotNull(cf,
//        "Column family cannot be empty, please specify in configuration file");
    //Check foe event serializer, if null set event serializer type
    if (eventSerializerType == null || eventSerializerType.isEmpty()) {
      eventSerializerType =
              "com.ctg.aep.sink.hbase.SimpleHbaseEventSerializer";
      logger.info("No serializer defined, Will use default:com.ctg.aep.sink.hbase.SimpleHbaseEventSerializer");
    }
    serializerContext.putAll(context.getSubProperties(
            HBaseSinkConfigurationConstants.CONFIG_SERIALIZER_PREFIX));
    
    columnFamily = cf.getBytes(Charsets.UTF_8);
    try {
      Class<? extends AEPHbaseEventSerializer > clazz =
              (Class<? extends AEPHbaseEventSerializer >)
                      Class.forName(eventSerializerType);
      serializer = clazz.newInstance();
      serializer.configure(serializerContext);
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }
    
    kerberosKeytab = context.getString( HBaseSinkConfigurationConstants.CONFIG_KEYTAB);
    kerberosPrincipal = context.getString( HBaseSinkConfigurationConstants.CONFIG_PRINCIPAL);
    
    logger.info ( "kerberosKeytab={}",kerberosKeytab );
    logger.info ( "kerberosPrincipal={}",kerberosPrincipal );
    
    enableWal = context.getBoolean( HBaseSinkConfigurationConstants
            .CONFIG_ENABLE_WAL, HBaseSinkConfigurationConstants.DEFAULT_ENABLE_WAL);
    logger.info("The write to WAL option is set to: " + String.valueOf(enableWal));
    if (!enableWal) {
      logger.warn("HBase Sink's enableWal configuration is set to false. All " +
              "writes to HBase will have WAL disabled, and any data in the " +
              "memstore of this region in the Region Server could be lost!");
    }
    
    batchIncrements = context.getBoolean(
            HBaseSinkConfigurationConstants.CONFIG_COALESCE_INCREMENTS,
            HBaseSinkConfigurationConstants.DEFAULT_COALESCE_INCREMENTS);
    
    if (batchIncrements) {
      logger.info("Increment coalescing is enabled. Increments will be " +
              "buffered.");
      refGetFamilyMap = reflectLookupGetFamilyMap();
    }
    
    String zkQuorum = context.getString( HBaseSinkConfigurationConstants
            .ZK_QUORUM);
    Integer port = null;
    /**
     * HBase allows multiple nodes in the quorum, but all need to use the
     * same client port. So get the nodes in host:port format,
     * and ignore the ports for all nodes except the first one. If no port is
     * specified, use default.
     */
    if (zkQuorum != null && !zkQuorum.isEmpty()) {
      StringBuilder zkBuilder = new StringBuilder();
      logger.info("Using ZK Quorum: " + zkQuorum);
      String[] zkHosts = zkQuorum.split(",");
      int length = zkHosts.length;
      for (int i = 0; i < length; i++) {
        String[] zkHostAndPort = zkHosts[i].split(":");
        zkBuilder.append(zkHostAndPort[0].trim());
        if (i != length - 1) {
          zkBuilder.append(",");
        } else {
          zkQuorum = zkBuilder.toString();
        }
        if (zkHostAndPort[1] == null) {
          throw new FlumeException("Expected client port for the ZK node!");
        }
        if (port == null) {
          port = Integer.parseInt(zkHostAndPort[1].trim());
        } else if (!port.equals(Integer.parseInt(zkHostAndPort[1].trim()))) {
          throw new FlumeException("All Zookeeper nodes in the quorum must " +
                  "use the same client port.");
        }
      }
      if (port == null) {
        port = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
      }
      this.config.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
      this.config.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, port);
    }
    String hbaseZnode = context.getString(
            HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT);
    if (hbaseZnode != null && !hbaseZnode.isEmpty()) {
      this.config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, hbaseZnode);
    }
    sinkCounter = new SinkCounter(this.getName());
  }
  
  public Configuration getConfig() {
    return config;
  }
  
  private void getAEPDataObject(Event event){
    byte[] bodyBytes = event.getBody ();
    String body = new String ( bodyBytes );
    
    try {
      aepDataObject = objectMapper.readValue (bodyBytes, AEPDataObject.class );
    }
    catch ( IOException e ) {
      aepDataObject = null;
      logger.warn ( "Failed to deserialize:{} ",body );
      ByteBuf byteBuf = Unpooled.copiedBuffer ( bodyBytes );
      ByteBufUtil.prettyHexDump(byteBuf);
    }
  }
  
  /**
   * 这里在解析json失败后，返回一个已有的NS和TableName(这个NS和TableName是事先创建好的）
   * 这样不会丢失数据（这个建好的表必须符合已有的数据结构，拥有已有的列簇）
   * @param event
   * @return
   */
  
  
  private Pair<String,String> getNamespaceAndTableName(Event event){
  
    getAEPDataObject(event);
    
    if( aepDataObject != null ) {
      return new Pair<> ( aepDataObject.tenant, aepDataObject.tableName );
    }
    else {
      return new Pair<> ( uberNamespace, uberTableName );
    }
  }
  
  private Map<TableName,List<Row>> myHbaseAction = new HashedMap (  );
  private Map<TableName,HTable> myHbaseTables = new HashedMap (  );
  
  private Boolean createNamespaceOrTableIfNecessary(Pair<String,String> tableInfo) throws IOException{
    String namespace = tableInfo.getFirst ();
    NamespaceDescriptor namespaceDescriptor;
    try {
      namespaceDescriptor = hbaseAdmin.getNamespaceDescriptor ( namespace );
    }catch(NamespaceNotFoundException e){
      namespaceDescriptor = NamespaceDescriptor.create ( namespace ).build ();
      hbaseAdmin.createNamespace ( namespaceDescriptor );
    }
  
    //确保建表成功
    stringNamespaceDescriptorMap.put (namespace, namespaceDescriptor );
    TableName tableName = TableName.valueOf ( tableInfo.getFirst ( ), tableInfo.getSecond ( ) );
    try {
      hbaseAdmin.getTableDescriptor ( tableName );
      return true;
    }catch ( TableNotFoundException ex ){
      HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
  
      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      hcd.setBlocksize(1000);
      hTableDescriptor.addFamily(hcd);
  
      hbaseAdmin.createTable ( hTableDescriptor );
      
      connection.getTable ( tableName );
    }
    
    return true;
  }
  
  private Pair<String,String> namespaceNameAndTblName;
  
  private void createNamespaceOrTableIfNecessary(Event event) throws Exception{
  
    namespaceNameAndTblName = getNamespaceAndTableName(event);
    
    final String namespace = namespaceNameAndTblName.getFirst ();
    
    if( stringNamespaceDescriptorMap.containsKey ( namespace ))
    {
      return;
    }

    try {
      
      privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          createNamespaceOrTableIfNecessary(namespaceNameAndTblName);
          return null;
        }
      });
    }
    catch ( IOException e ) {
      logger.warn ( "Failed to create hbase ns or table:{} ",e );
      e.printStackTrace ( );
      return;
    }
  }
  
  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    List<Row> actions = new LinkedList<Row>();
    List<Increment> incs = new LinkedList<Increment>();
    try {
      txn.begin();
      
      if (serializer instanceof BatchAware ) {
        ((BatchAware ) serializer).onBatchStart();
      }
      
      long i = 0;
      for (; i < batchSize; i++) {
        Event event = channel.take();
        if (event == null) {
          if (i == 0) {
            status = Status.BACKOFF;
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          
          createNamespaceOrTableIfNecessary(event);
          TableName tableName = TableName.valueOf ( namespaceNameAndTblName.getFirst (),
                  namespaceNameAndTblName.getSecond ()) ;
  
          serializer.initialize(aepDataObject, event, columnFamily);
          
          if( myHbaseAction.containsKey (tableName )){
            myHbaseAction.get ( tableName ).addAll ( serializer.getActions() );
          }else{
            List<Row> rows = new LinkedList<Row>();
            rows.addAll ( serializer.getActions() );
            myHbaseAction.put ( tableName, rows);
          }
          
          
         
//          actions.addAll(serializer.getActions());
          //默认值非空，会返回这些列。这里我觉得不需要，所以就取消了
//          incs.addAll(serializer.getIncrements());
        }
      }
      if (i == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(i);
  
      putEventsAndCommit2(txn);
//      putEventsAndCommit(actions, incs, txn);
      
    } catch (Throwable e) {
      try {
        txn.rollback();
      } catch (Exception e2) {
        logger.error("Exception in rollback. Rollback might not have been " +
                "successful.", e2);
      }
      logger.error("Failed to commit transaction." +
              "Transaction rolled back.", e);
      if (e instanceof Error || e instanceof RuntimeException) {
        logger.error("Failed to commit transaction." +
                "Transaction rolled back.", e);
        Throwables.propagate(e);
      } else {
        logger.error("Failed to commit transaction." +
                "Transaction rolled back.", e);
        throw new EventDeliveryException("Failed to commit transaction." +
                "Transaction rolled back.", e);
      }
    } finally {
      txn.close();
    }
    return status;
  }
  
  private void putEventsAndCommit2(Transaction txn) throws Exception {
    
    privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
  
        for ( Map.Entry< TableName, List< Row > > tableNameListEntry : myHbaseAction.entrySet ( ) ) {
          TableName tableName = tableNameListEntry.getKey ();

          Table hTable = connection.getTable ( tableName );
          List<Row> rowList = tableNameListEntry.getValue ();
  
          for (Row r : rowList) {
            if (r instanceof Put) {
              ((Put) r).setWriteToWAL(enableWal);
            }
            // Newer versions of HBase - Increment implements Row.
            if (r instanceof Increment) {
              ((Increment) r).setWriteToWAL(enableWal);
            }
          }
          hTable.batch ( rowList );
        }
        
        return null;
      }
    });
    
//    privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
//      @Override
//      public Void run() throws Exception {
//
//        List<Increment> processedIncrements;
//        if (batchIncrements) {
//          processedIncrements = coalesceIncrements(incs);
//        } else {
//          processedIncrements = incs;
//        }
//
//        // Only used for unit testing.
//        if (debugIncrCallback != null) {
//          debugIncrCallback.onAfterCoalesce(processedIncrements);
//        }
//
//        for (final Increment i : processedIncrements) {
//          i.setWriteToWAL(enableWal);
//          table.increment(i);
//        }
//        return null;
//      }
//    });
    
    txn.commit();
  
    int nTotal = 0;
    for ( Map.Entry< TableName, List< Row > > tableNameListEntry : myHbaseAction.entrySet ( ) ) {
      
      List<Row> rowList = tableNameListEntry.getValue ();
      nTotal += rowList.size ();
      
    }
    
    sinkCounter.addToEventDrainSuccessCount(nTotal);
  }
  
  private void putEventsAndCommit(final List<Row> actions,
                                  final List<Increment> incs, Transaction txn) throws Exception {
    
    privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        for (Row r : actions) {
          if (r instanceof Put) {
            ((Put) r).setWriteToWAL(enableWal);
          }
          // Newer versions of HBase - Increment implements Row.
          if (r instanceof Increment) {
            ((Increment) r).setWriteToWAL(enableWal);
          }
        }
        table.batch(actions);
        return null;
      }
    });
    
    privilegedExecutor.execute(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        
        List<Increment> processedIncrements;
        if (batchIncrements) {
          processedIncrements = coalesceIncrements(incs);
        } else {
          processedIncrements = incs;
        }
        
        // Only used for unit testing.
        if (debugIncrCallback != null) {
          debugIncrCallback.onAfterCoalesce(processedIncrements);
        }
        
        for (final Increment i : processedIncrements) {
          i.setWriteToWAL(enableWal);
          table.increment(i);
        }
        return null;
      }
    });
    
    txn.commit();
    sinkCounter.addToEventDrainSuccessCount(actions.size());
  }
  
  /**
   * The method getFamilyMap() is no longer available in Hbase 0.96.
   * We must use reflection to determine which version we may use.
   */
  @VisibleForTesting
  static Method reflectLookupGetFamilyMap() {
    Method m = null;
    String[] methodNames = {"getFamilyMapOfLongs", "getFamilyMap"};
    for (String methodName : methodNames) {
      try {
        m = Increment.class.getMethod(methodName);
        if (m != null && m.getReturnType().equals(Map.class)) {
          logger.debug("Using Increment.{} for coalesce", methodName);
          break;
        }
      } catch (NoSuchMethodException e) {
        logger.debug("Increment.{} does not exist. Exception follows.",
                methodName, e);
      } catch (SecurityException e) {
        logger.debug("No access to Increment.{}; Exception follows.",
                methodName, e);
      }
    }
    if (m == null) {
      throw new UnsupportedOperationException(
              "Cannot find Increment.getFamilyMap()");
    }
    return m;
  }
  
  @SuppressWarnings("unchecked")
  private Map<byte[], NavigableMap<byte[], Long>> getFamilyMap(Increment inc) {
    Preconditions.checkNotNull(refGetFamilyMap,
            "Increment.getFamilymap() not found");
    Preconditions.checkNotNull(inc, "Increment required");
    Map<byte[], NavigableMap<byte[], Long>> familyMap = null;
    try {
      Object familyObj = refGetFamilyMap.invoke(inc);
      familyMap = (Map<byte[], NavigableMap<byte[], Long>>) familyObj;
    } catch (IllegalAccessException e) {
      logger.warn("Unexpected error calling getFamilyMap()", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      logger.warn("Unexpected error calling getFamilyMap()", e);
      Throwables.propagate(e);
    }
    return familyMap;
  }
  
  /**
   * Perform "compression" on the given set of increments so that Flume sends
   * the minimum possible number of RPC operations to HBase per batch.
   *
   * @param incs Input: Increment objects to coalesce.
   * @return List of new Increment objects after coalescing the unique counts.
   */
  private List<Increment> coalesceIncrements(Iterable<Increment> incs) {
    Preconditions.checkNotNull(incs, "List of Increments must not be null");
    // Aggregate all of the increment row/family/column counts.
    // The nested map is keyed like this: {row, family, qualifier} => count.
    Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters =
            Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Increment inc : incs) {
      byte[] row = inc.getRow();
      Map<byte[], NavigableMap<byte[], Long>> families = getFamilyMap(inc);
      for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : families.entrySet()) {
        byte[] family = familyEntry.getKey();
        NavigableMap<byte[], Long> qualifiers = familyEntry.getValue();
        for (Map.Entry<byte[], Long> qualifierEntry : qualifiers.entrySet()) {
          byte[] qualifier = qualifierEntry.getKey();
          Long count = qualifierEntry.getValue();
          incrementCounter(counters, row, family, qualifier, count);
        }
      }
    }
    
    // Reconstruct list of Increments per unique row/family/qualifier.
    List<Increment> coalesced = Lists.newLinkedList();
    for (Map.Entry<byte[], Map<byte[], NavigableMap<byte[], Long>>> rowEntry :
            counters.entrySet()) {
      byte[] row = rowEntry.getKey();
      Map<byte[], NavigableMap<byte[], Long>> families = rowEntry.getValue();
      Increment inc = new Increment(row);
      for (Map.Entry<byte[], NavigableMap<byte[], Long>> familyEntry : families.entrySet()) {
        byte[] family = familyEntry.getKey();
        NavigableMap<byte[], Long> qualifiers = familyEntry.getValue();
        for (Map.Entry<byte[], Long> qualifierEntry : qualifiers.entrySet()) {
          byte[] qualifier = qualifierEntry.getKey();
          long count = qualifierEntry.getValue();
          inc.addColumn(family, qualifier, count);
        }
      }
      coalesced.add(inc);
    }
    
    return coalesced;
  }
  
  /**
   * Helper function for {@link #coalesceIncrements} to increment a counter
   * value in the passed data structure.
   *
   * @param counters  Nested data structure containing the counters.
   * @param row       Row key to increment.
   * @param family    Column family to increment.
   * @param qualifier Column qualifier to increment.
   * @param count     Amount to increment by.
   */
  private void incrementCounter(
          Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters,
          byte[] row, byte[] family, byte[] qualifier, Long count) {
    
    Map<byte[], NavigableMap<byte[], Long>> families = counters.get(row);
    if (families == null) {
      families = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      counters.put(row, families);
    }
    
    NavigableMap<byte[], Long> qualifiers = families.get(family);
    if (qualifiers == null) {
      qualifiers = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
      families.put(family, qualifiers);
    }
    
    Long existingValue = qualifiers.get(qualifier);
    if (existingValue == null) {
      qualifiers.put(qualifier, count);
    } else {
      qualifiers.put(qualifier, existingValue + count);
    }
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  AEPHbaseEventSerializer getSerializer() {
    return serializer;
  }
  
  @VisibleForTesting
  @InterfaceAudience.Private
  interface DebugIncrementsCallback {
    public void onAfterCoalesce ( Iterable< Increment > increments );
  }
}