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

package com.github.sonus21.rqueue.utils;

import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;
import static org.springframework.util.Assert.notEmpty;

import com.github.sonus21.rqueue.core.RqueueMessage;
import java.util.List;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

public final class MessageUtils {
  private static final String META_DATA_KEY_PREFIX = "__rq::m-mdata::";

  private MessageUtils() {}

  public static Object convertMessageToObject(
      RqueueMessage message, MessageConverter messageConverter) {
    return convertMessageToObject(new GenericMessage<>(message.getMessage()), messageConverter);
  }

  public static RqueueMessage buildMessage(
      MessageConverter messageConverter,
      String queueName,
      Object message,
      Integer retryCount,
      Long delayInMilliSecs) {
    Message<?> msg = messageConverter.toMessage(message, null);
    if (msg == null) {
      throw new MessageConversionException("Message could not be build (null)");
    }
    return new RqueueMessage(queueName, (String) msg.getPayload(), retryCount, delayInMilliSecs);
  }

  public static Object convertMessageToObject(
      Message<String> message, MessageConverter messageConverter) {
    return messageConverter.fromMessage(message, null);
  }

  public static Object convertMessageToObject(
      Message<String> message, List<MessageConverter> messageConverters) {
    notEmpty(messageConverters, "messageConverters cannot be empty");
    for (MessageConverter messageConverter : messageConverters) {
      try {
        return messageConverter.fromMessage(message, null);
      } catch (Exception e) {
      }
    }
    return null;
  }

  public static String getMessageMetaId(String messageId) {
    return META_DATA_KEY_PREFIX + messageId;
  }

  public static long getExpiryTime(long visibilityTimeout) {
    return System.currentTimeMillis() + visibilityTimeout - DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }
}
