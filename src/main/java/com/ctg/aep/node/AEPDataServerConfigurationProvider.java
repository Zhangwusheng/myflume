/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ctg.aep.node;

import com.ctg.aep.dataserver.DataServerConstants;
import com.google.common.collect.Maps;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.node.AbstractConfigurationProvider;
import org.apache.flume.sink.LoggerSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;

/**
 * 把AEP自己的配置，转变成Flume的配置
 */
public class AEPDataServerConfigurationProvider extends
    AbstractConfigurationProvider {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(AEPDataServerConfigurationProvider.class);

  private final File file;
//  private Map<String,String> convertedConfig = Maps.newHashMap ( );
  private SortedMap<String,String> convertedConfig = Maps.newTreeMap();
  boolean verbose;

  public AEPDataServerConfigurationProvider ( String agentName, File file , boolean verbose) {
    super(agentName);
    this.file = file;
    this.verbose = verbose;
  }
  
  
  /**
   *
   * AEP.sources = kafkaSource
   * AEP.channels = memchannelHbase memchannelRedis
   * AEP.sinks = hbaseSink redisSink
   *
   * AEP.sources.kafkaSource.channels = memchannelHbase memchannelRedis
   *
   * AEP.sources.kafkaSource.type = org.apache.flume.source.kafka.KafkaSource
   * AEP.sources.kafkaSource.kafka.topics = ad
   * AEP.sources.kafkaSource.batchSize = 1000
   * AEP.sources.kafkaSource.batchDurationMillis=1000
   * AEP.sources.kafkaSource.kafka.consumer.group.id=aep_consumer
   * AEP.sources.kafkaSource.kafka.bootstrap.servers=
   * #设置 consumer's 属性
   * AEP.sources.kafkaSource.kafka.consumer.
   * AEP.sources.kafkaSource.kafka.consumer.timeout.ms = 100
   *
   *
   * AEP.channels.memchannelHbase.type = org.apache.flume.channel.MemoryChannel
   * AEP.channels.memchannelHbase.capacity = 100
   * AEP.channels.memchannelHbase.transactionCapacity = 100
   * AEP.channels.memchannelHbase.byteCapacityBufferPercentage = 20;
   * AEP.channels.memchannelHbase.keep-alive=3
   *
   *
   * AEP.sinks.hbaseSink.type = hbase
   * agent.sinks.hbaseSink.zookeeperQuorum = mysql3:2181,mysql4:2181,mysql5:2181
   * agent.sinks.hbaseSink.kerberosKeytab=
   * agent.sinks.hbaseSink.kerberosPrincipal =
   * agent.sinks.hbaseSink.enableWal=true
   * agent.sinks.hbaseSink.coalesceIncrements=false
   *
   */
  
  private Map<String,String> getSubProperties(String prefix,Map<String,String> parameters){
  
    Map<String,String> result = Maps.newHashMap ();
    
    for (String key : parameters.keySet()) {
      if (key.startsWith(prefix)) {
        String name = key.substring(prefix.length());
        result.put(name, parameters.get(key));
      }
    }
    
    return result;
  }
  
  private void convertAEPPropertiesToFlumeConf(Properties properties){

    convertedConfig.put ( getAgentName()+".sources","kafkaSource" );
    convertedConfig.put (getAgentName()+".channels","memchannelHbase memchannelRedis memchannelCtgCache");
    convertedConfig.put (getAgentName()+".sinks","hbaseSink redisSink ctgcacheSink");
    
    
    convertedConfig.put (getAgentName()+".sources.kafkaSource.channels","memchannelHbase memchannelRedis");
    convertedConfig.put (getAgentName()+".sources.kafkaSource.type","org.apache.flume.source.kafka.KafkaSource");
    convertedConfig.put (getAgentName()+".sources.kafkaSource.kafka.topics",properties.getProperty ( DataServerConstants.AEP_TOPIC_NAME  ));
    convertedConfig.put (getAgentName()+".sources.kafkaSource.batchSize","1000");
    convertedConfig.put (getAgentName()+".sources.kafkaSource.batchDurationMillis","1000");
    convertedConfig.put (getAgentName()+".sources.kafkaSource.kafka.consumer.group.id",properties.getProperty ( DataServerConstants.AEP_CONSUMER_GROUP  ));
    convertedConfig.put (getAgentName()+".sources.kafkaSource.kafka.bootstrap.servers",properties.getProperty ( DataServerConstants.AEP_BOOTSTRAP_SERVER ));
    convertedConfig.put (getAgentName()+".sources.kafkaSource.maxBackoffSleep",properties.getProperty ( DataServerConstants.KAFKA_MAXBACKOFFSLEEP  ));

  
    Map<String, String> map1 = toMap ( properties );
    Map<String, String> map2 = getSubProperties ( DataServerConstants.KAFKA_CONSUMER_PREFIX ,map1);
    for ( String s : map2.keySet ( ) ) {
      convertedConfig.put( getAgentName()+".sources.kafkaSource.kafka.consumer." +s,map2.get ( s ));
    }
  
  
    convertedConfig.put (getAgentName()+".channels.memchannelHbase.type","org.apache.flume.channel.MemoryChannel");
    convertedConfig.put (getAgentName()+".channels.memchannelHbase.capacity","100");
    convertedConfig.put (getAgentName()+".channels.memchannelHbase.transactionCapacity","100");
    convertedConfig.put (getAgentName()+".channels.memchannelHbase.byteCapacityBufferPercentage","20");
    convertedConfig.put (getAgentName()+".channels.memchannelHbase.keep-alive","3");

    convertedConfig.put (getAgentName()+".channels.memchannelRedis.type","org.apache.flume.channel.MemoryChannel");
    convertedConfig.put (getAgentName()+".channels.memchannelRedis.capacity","100");
    convertedConfig.put (getAgentName()+".channels.memchannelRedis.transactionCapacity","100");
    convertedConfig.put (getAgentName()+".channels.memchannelRedis.byteCapacityBufferPercentage","20");
    convertedConfig.put (getAgentName()+".channels.memchannelRedis.keep-alive","3");

    convertedConfig.put (getAgentName()+".channels.memchannelCtgCache.type","org.apache.flume.channel.MemoryChannel");
    convertedConfig.put (getAgentName()+".channels.memchannelCtgCache.capacity","100");
    convertedConfig.put (getAgentName()+".channels.memchannelCtgCache.transactionCapacity","100");
    convertedConfig.put (getAgentName()+".channels.memchannelCtgCache.byteCapacityBufferPercentage","20");
    convertedConfig.put (getAgentName()+".channels.memchannelCtgCache.keep-alive","3");

    convertedConfig.put (getAgentName()+".sinks.hbaseSink.type","com.ctg.aep.sink.hbase.AEPHBaseSink");
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.kerberosKeytab",properties.getProperty ( DataServerConstants.KERBEROSKEYTAB  ));
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.kerberosPrincipal",properties.getProperty ( DataServerConstants.KERBEROSPRINCIPAL  ));
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.columnFamily",properties.getProperty (DataServerConstants.COLUMN_FAMILY));
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.enableWal","true");
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.coalesceIncrements","false");
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.channel","memchannelHbase");
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.serializer","com.ctg.aep.sink.hbase.SimpleAEPHbaseEventSerializer");

    //是否自动创建命名空间，在序列化的时候需要用到
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.autoCreateNamespace",properties.getProperty (DataServerConstants.AUTO_CREATE_NS));
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.uberNamespaceName",properties.getProperty (DataServerConstants.UBER_NAMESPACE));
    convertedConfig.put (getAgentName()+".sinks.hbaseSink.uberTableName",properties.getProperty (DataServerConstants.UBER_TABLENAME));


    convertedConfig.put (getAgentName()+".sinks.redisSink.type","com.ctg.aep.sink.redis.AEPRedisSink");
    convertedConfig.put (getAgentName()+".sinks.redisSink.host",properties.getProperty ( DataServerConstants.REDIS_HOST  ));
    convertedConfig.put (getAgentName()+".sinks.redisSink.port",properties.getProperty ( DataServerConstants.REDIS_PORT  ));
    convertedConfig.put (getAgentName()+".sinks.redisSink.channel","memchannelRedis");

    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.channel","memchannelCtgCache");
    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.type","com.ctg.aep.sink.ctgcache.CtgCacheSink");
    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.group",properties.getProperty ( DataServerConstants.CTGCACHE_GROUP  ));
    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.passwd",properties.getProperty ( DataServerConstants.CTGCACHE_PASSWD  ));
    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.user",properties.getProperty ( DataServerConstants.CTGCACHE_USER  ));
    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.using_hash",properties.getProperty ( DataServerConstants.CTGCACHE_USING_HASH  ));
    convertedConfig.put (getAgentName()+".sinks.ctgcacheSink.timeout",properties.getProperty ( DataServerConstants.CTGCACHE_TIMEOUT  ));


    if( verbose ){
      StringBuilder sb = new StringBuilder(1024);
//      LOGGER.info ( "=================================" );
      sb.append("--------------------------\n");
      for ( String s : convertedConfig.keySet ( ) ) {
        sb.append (s+"="+convertedConfig.get (s) );
        sb.append("\n");
      }

      LOGGER.info(sb.toString());
    }
  }

  @Override
  public FlumeConfiguration getFlumeConfiguration() {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      Properties properties = new Properties();
      properties.load(reader);
  
      convertAEPPropertiesToFlumeConf(properties);
//      return new FlumeConfiguration(toMap(properties));
      return new FlumeConfiguration(convertedConfig);
    } catch (IOException ex) {
      LOGGER.error("Unable to load file:" + file
          + " (I/O failure) - Exception follows.", ex);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ex) {
          LOGGER.warn(
              "Unable to close file reader for file: " + file, ex);
        }
      }
    }
    return new FlumeConfiguration(new HashMap<String, String>());
  }
}
