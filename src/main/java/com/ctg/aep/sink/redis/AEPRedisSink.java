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
package com.ctg.aep.sink.redis;

import com.ctg.aep.data.AEPDataObject;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * 新增Redis Sink
 */

public class AEPRedisSink extends AbstractSink implements Configurable {


  private static final Logger logger = LoggerFactory.getLogger(AEPRedisSink.class);

  private String redisHost;
  private int redisPort;

  private SinkCounter sinkCounter;

  private long batchSize;
  private ObjectMapper objectMapper;

  private AEPDataObject aepDataObject;

  JedisPool jedisPool;

  public AEPRedisSink() {
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

    super.start();

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPool = new JedisPool(jedisPoolConfig,redisHost,redisPort);

    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();
  }

  @Override
  public void stop() {

    if( jedisPool != null ){
      jedisPool.close();
    }

    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Context context) {

    logger.info ( "---------------com.ctg.aep.sink.hbase.AEPRedisSink.configure called" );

    objectMapper =  getDefaultObjectMapper();

    redisHost = context.getString(RedisSinkConfigurationConstants.IP_ADDRESS);
    if( redisHost == null ){
      throw new ConfigurationException("host not configed for sink source:"+getName());
    }

    String redisPortStr = context.getString(RedisSinkConfigurationConstants.PORT);
    if( redisPortStr == null ){
      throw new ConfigurationException("port not configed for sink source:"+getName());
    }

    redisPort = Integer.parseInt(redisPortStr);
    sinkCounter = new SinkCounter(this.getName());

    logger.info("redis:host={},port={}",redisHost,redisPort);
  }


  private void getAEPDataObject(Event event){
    byte[] bodyBytes = event.getBody ();
    String body = new String ( bodyBytes );

    logger.info("Deserialize event.....");
    ByteBuf byteBuf = Unpooled.copiedBuffer ( bodyBytes );
    ByteBufUtil.prettyHexDump(byteBuf);

    try {
      aepDataObject = objectMapper.readValue (bodyBytes, AEPDataObject.class );
      logger.info("Deserialize event.....SUCCESS");
    }
    catch ( IOException e ) {
      aepDataObject = null;
      logger.warn ( "Failed to deserialize:{} ",body );
//      ByteBuf byteBuf = Unpooled.copiedBuffer ( bodyBytes );
//      ByteBufUtil.prettyHexDump(byteBuf);
    }
  }


  private void writeDataToRedis(){

    String redisKey = aepDataObject.getProductId();
    redisKey+="_";
    redisKey+=aepDataObject.getDeviceId();

    String payload = aepDataObject.getPayload();

    Map<String,Object> result;
    try {
      result = objectMapper.readValue(payload.getBytes(), Map.class);
      for (Map.Entry<String, Object> stringObjectEntry : result.entrySet()) {

      }
    } catch (IOException e) {
      logger.info("decode payload failed:"+payload);
      return;
    }


    try (Jedis jedis = jedisPool.getResource()) {
      for (Map.Entry<String, Object> stringObjectEntry : result.entrySet()) {
        String itemKey =redisKey+"_"+stringObjectEntry.getKey();
        String value = stringObjectEntry.getValue().toString();
        jedis.set(itemKey,value);
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();

    try {
      txn.begin();

      Event event = channel.take();

      txn.commit();

      if( event == null ){
        return Status.BACKOFF;
      }

      getAEPDataObject(event);

      if( this.aepDataObject == null ){
        return Status.READY;
      }

      writeDataToRedis();

      return Status.READY;

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

}
