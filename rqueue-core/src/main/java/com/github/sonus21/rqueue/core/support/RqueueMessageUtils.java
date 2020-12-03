/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.core.support;

import com.github.sonus21.rqueue.core.RqueueMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

public final class RqueueMessageUtils {
  private RqueueMessageUtils() {}

  private static final String META_DATA_KEY_PREFIX = "__rq::m-mdata::";
  private static final String KEY_SEPARATOR = "::";

  public static String getMessageMetaId(String queueName, String messageId) {
    return META_DATA_KEY_PREFIX + queueName + KEY_SEPARATOR + messageId;
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
      Object message,
      long period,
      MessageHeaders messageHeaders) {
    Message<?> msg = converter.toMessage(message, messageHeaders);
    if (msg == null) {
      throw new MessageConversionException("Message could not be build (null)");
    }
    Object payload = msg.getPayload();
    if (payload instanceof String) {
      return RqueueMessage.builder()
          .period(period)
          .id(UUID.randomUUID().toString())
          .queueName(queueName)
          .message((String) payload)
          .processAt(System.currentTimeMillis() + period)
          .build();
    }
    if (payload instanceof byte[]) {
      return RqueueMessage.builder()
          .period(period)
          .id(UUID.randomUUID().toString())
          .queueName(queueName)
          .message(new String((byte[]) msg.getPayload()))
          .processAt(System.currentTimeMillis() + period)
          .build();
    }
    throw new MessageConversionException("Message payload is neither String nor byte[]");
  }

  public static RqueueMessage buildMessage(
      MessageConverter converter,
      Object object,
      String queueName,
      Integer retryCount,
      Long delay,
      MessageHeaders messageHeaders) {
    Message<?> msg = converter.toMessage(object, messageHeaders);
    if (msg == null) {
      throw new MessageConversionException("Message could not be build (null)");
    }
    long queuedTime = System.nanoTime();
    long processAt = System.currentTimeMillis();
    if (delay != null) {
      processAt += delay;
    }
    Object payload = msg.getPayload();
    if (payload instanceof String) {
      return RqueueMessage.builder()
          .retryCount(retryCount)
          .queuedTime(queuedTime)
          .id(UUID.randomUUID().toString())
          .queueName(queueName)
          .message((String) payload)
          .processAt(processAt)
          .build();
    }
    if (payload instanceof byte[]) {
      return RqueueMessage.builder()
          .retryCount(retryCount)
          .queuedTime(queuedTime)
          .id(UUID.randomUUID().toString())
          .queueName(queueName)
          .message(new String((byte[]) msg.getPayload()))
          .processAt(processAt)
          .build();
    }
    throw new MessageConversionException("Message payload is neither String nor byte[]");
  }

  public static List<RqueueMessage> generateMessages(
      MessageConverter converter, String queueName, int count) {
    return generateMessages(converter, "Test Object", queueName, null, null, count);
  }

  public static List<RqueueMessage> generateMessages(
      MessageConverter converter, String queueName, long delay, int count) {
    return generateMessages(converter, "Test object", queueName, null, delay, count);
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
      messages.add(buildMessage(converter, object, queueName, retryCount, delay, null));
    }
    return messages;
  }
}
