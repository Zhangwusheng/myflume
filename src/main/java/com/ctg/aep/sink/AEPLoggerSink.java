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

package com.ctg.aep.sink;

import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/***
 * 把数据美化打印出来而已，不仅打印数据，还打印Header
 */
public class AEPLoggerSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
          .getLogger(AEPLoggerSink.class);

  // Default Max bytes to dump
  public static final int DEFAULT_MAX_BYTE_DUMP = 16;

  // Max number of bytes to be dumped
  private int maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;

  public static final String MAX_BYTES_DUMP_KEY = "maxBytesToLog";

  @Override
  public void configure(Context context) {
    logger.info("LoggerSink Called .....");
    String strMaxBytes = context.getString(MAX_BYTES_DUMP_KEY);
    if (!Strings.isNullOrEmpty(strMaxBytes)) {
      try {
        maxBytesToLog = Integer.parseInt(strMaxBytes);
      } catch (NumberFormatException e) {
        logger.warn(String.format(
                "Unable to convert %s to integer, using default value(%d) for maxByteToDump",
                strMaxBytes, DEFAULT_MAX_BYTE_DUMP));
        maxBytesToLog = DEFAULT_MAX_BYTE_DUMP;
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;

    try {
      transaction.begin();
      event = channel.take();

      if (event != null) {
        ByteBuf buf = Unpooled.buffer();

        if (logger.isInfoEnabled()) {

          Map<String,String> headers = event.getHeaders();
          for (Map.Entry<String, String> stringStringEntry : headers.entrySet()) {
            buf.writeBytes(stringStringEntry.getKey().getBytes());
            buf.writeBytes("->".getBytes());
            buf.writeBytes(stringStringEntry.getValue().getBytes());
            buf.writeBytes(",".getBytes());
          }

          buf.writeBytes("Body:".getBytes());
          if( event.getBody() == null ){
            buf.writeBytes("null".getBytes());
          }else{
            buf.writeBytes(event.getBody());
          }

          String s = ByteBufUtil.prettyHexDump(buf);
          logger.info("Event Received:\n"+s);
        }
      } else {
        // No event found, request back-off semantics from the sink runner
        result = Status.BACKOFF;
      }
      transaction.commit();
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to log event: " + event, ex);
    } finally {
      transaction.close();
    }

    return result;
  }
}
