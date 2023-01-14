/*
 * Copyright (c) 2019-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core.support;

import static com.github.sonus21.rqueue.utils.Constants.REDIS_KEY_SEPARATOR;

import com.github.sonus21.rqueue.core.RqueueMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

public final class RqueueMessageUtils {

  private static final String META_DATA_KEY_PREFIX = "__rq::m-mdata::";

  private RqueueMessageUtils() {
  }

  public static String getMessageMetaId(String queueName, String messageId) {
    return META_DATA_KEY_PREFIX + queueName + REDIS_KEY_SEPARATOR + messageId;
  }

  public static Object convertMessageToObject(
      RqueueMessage message, MessageConverter messageConverter) {
    return convertMessageToObject(new GenericMessage<>(message.getMessage()), messageConverter);
  }

  public static Object convertMessageToObject(
      Message<String> message, MessageConverter messageConverter) {
    return messageConverter.fromMessage(message, null);
  }

  public static RqueueMessage buildPeriodicMessage(
      MessageConverter converter,
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      long period,
      MessageHeaders messageHeaders) {
    Message<?> msg = converter.toMessage(message, messageHeaders);
    if (msg == null) {
      throw new MessageConversionException("Message could not be build (null)");
    }
    Object payload = msg.getPayload();
    long processAt = System.currentTimeMillis() + period;
    String strMessage;
    if (payload instanceof String) {
      strMessage = (String) payload;
    } else if (payload instanceof byte[]) {
      strMessage = new String((byte[]) payload);
    } else {
      throw new MessageConversionException("Message payload is neither String nor byte[]");
    }
    RqueueMessage rqueueMessage =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(strMessage)
            .processAt(processAt)
            .retryCount(retryCount)
            .period(period)
            .build();
    if (messageId != null) {
      rqueueMessage.setId(messageId);
    }
    return rqueueMessage;
  }

  public static RqueueMessage buildMessage(
      MessageConverter converter,
      String queueName,
      String messageId,
      Object message,
      Integer retryCount,
      Long delay,
      MessageHeaders messageHeaders) {
    Message<?> msg = converter.toMessage(message, messageHeaders);
    if (msg == null) {
      throw new MessageConversionException("Message could not be build (null)");
    }
    long queuedTime = System.nanoTime();
    long processAt = System.currentTimeMillis();
    if (delay != null) {
      processAt += delay;
    }
    Object payload = msg.getPayload();
    String strMessage;
    if (payload instanceof String) {
      strMessage = (String) payload;
    } else if (payload instanceof byte[]) {
      strMessage = new String((byte[]) payload);
    } else {
      throw new MessageConversionException("Message payload is neither String nor byte[]");
    }
    RqueueMessage rqueueMessage =
        RqueueMessage.builder()
            .retryCount(retryCount)
            .queuedTime(queuedTime)
            .id(UUID.randomUUID().toString())
            .queueName(queueName)
            .message(strMessage)
            .processAt(processAt)
            .build();
    if (messageId != null) {
      rqueueMessage.setId(messageId);
    }
    return rqueueMessage;
  }

  public static List<RqueueMessage> generateMessages(
      MessageConverter converter, String queueName, int count) {
    return generateMessages(converter, UUID.randomUUID().toString(), queueName, null, null, count);
  }

  public static RqueueMessage generateMessage(MessageConverter converter, String queueName) {
    return generateMessages(converter, UUID.randomUUID().toString(), queueName, null, null, 1)
        .get(0);
  }

  public static List<RqueueMessage> generateMessages(
      MessageConverter converter, String queueName, long delay, int count) {
    return generateMessages(converter, UUID.randomUUID().toString(), queueName, null, delay, count);
  }

  public static List<RqueueMessage> generateMessages(
      MessageConverter converter,
      Object object,
      String queueName,
      Integer retryCount,
      Long delay,
      int count) {
    List<RqueueMessage> messages = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      messages.add(buildMessage(converter, queueName, null, object, retryCount, delay, null));
    }
    return messages;
  }

  public static Message<?> cloneMessage(Message<?> message) {
    return new GenericMessage<>(message.getPayload(), new HashMap<>(message.getHeaders()));
  }
}
