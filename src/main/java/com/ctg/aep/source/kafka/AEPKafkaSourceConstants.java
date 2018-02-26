/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ctg.aep.source.kafka;

import org.apache.kafka.clients.CommonClientConfigs;

public class AEPKafkaSourceConstants {

  public static final String KAFKA_PREFIX = "kafka.";
  public static final String KAFKA_CONSUMER_PREFIX = KAFKA_PREFIX + "consumer.";
  public static final String DEFAULT_KEY_DESERIALIZER =
      "org.apache.kafka.common.serialization.StringDeserializer";
  public static final String DEFAULT_VALUE_DESERIALIZER =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  public static final String BOOTSTRAP_SERVERS =
      KAFKA_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String TOPICS = KAFKA_PREFIX + "topics";
  public static final String TOPICS_REGEX = TOPICS + "." + "regex";
  public static final String DEFAULT_AUTO_COMMIT =  "false";
  public static final String BATCH_SIZE = "batchSize";

  public static final String USE_KERBEROS = "useKerberos";
  public static final String JAAS_FILE = "jaasfile";

  public static final String BATCH_DURATION_MS = "batchDurationMillis";
  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int DEFAULT_BATCH_DURATION = 1000;
  public static final String DEFAULT_GROUP_ID = "flume";

  public static final String MIGRATE_ZOOKEEPER_OFFSETS = "migrateZookeeperOffsets";
  public static final boolean DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS = true;

  public static final String AVRO_EVENT = "useFlumeEventFormat";
  public static final boolean DEFAULT_AVRO_EVENT = false;

  /* Old Properties */
  public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";
  public static final String TOPIC = "topic";
  public static final String OLD_GROUP_ID = "groupId";

  // flume event headers
  public static final String TOPIC_HEADER = "topic";
  public static final String KEY_HEADER = "key";
  public static final String TIMESTAMP_HEADER = "timestamp";
  public static final String PARTITION_HEADER = "partition";


  //Kerberos 相关
  public static final String SECURITY_PROTOCOL_CONFIG =
          KAFKA_PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

  public static final String KINIT_CMD="sasl.kerberos.kinit.cmd";
  public static final String KAFKA_KINIT_CMD =
          KAFKA_PREFIX + KINIT_CMD;

  public static final String KERBEROS_RENEW_WINDOW="sasl.kerberos.ticket.renew.window.factor";

  public static final String KAFKA_KERBEROS_RENEW_WINDOW =
          KAFKA_PREFIX + KERBEROS_RENEW_WINDOW;

  public static final String KERBEROS_RENEW_JITTER ="sasl.kerberos.ticket.renew.jitter";
  public static final String KAFKA_KERBEROS_RENEW_JITTER =
          KAFKA_PREFIX + KERBEROS_RENEW_JITTER;


  public static final String KERBEROS_RELOGIN_TIME="sasl.kerberos.min.time.before.relogin";
  public static final String KAFKA_KERBEROS_RELOGIN_TIME =
          KAFKA_PREFIX + KERBEROS_RELOGIN_TIME;

}
