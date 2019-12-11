/*
 * Copyright 2019 Sonu Kumar
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

package com.github.sonus21.rqueue.producer;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.GenericMessage;

class MessageWriter {
  private static final long MIN_DELAY_TIME = 100;
  private static Logger logger = LoggerFactory.getLogger(RqueueMessageSender.class);
  private RqueueMessageTemplate rqueueMessageTemplate;
  private CompositeMessageConverter messageConverter;

  MessageWriter(
      RqueueMessageTemplate rqueueMessageTemplate, List<MessageConverter> messageConverters) {
    this(rqueueMessageTemplate, new CompositeMessageConverter(messageConverters));
  }

  MessageWriter(
      RqueueMessageTemplate rqueueMessageTemplate,
      CompositeMessageConverter compositeMessageConverter) {
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    messageConverter = compositeMessageConverter;
  }

  boolean pushMessage(String queueName, Object message, Integer retryCount, Long delayInMilliSecs) {
    RqueueMessage rqueueMessage = buildMessage(queueName, message, retryCount, delayInMilliSecs);
    try {
      if (delayInMilliSecs == null || delayInMilliSecs <= MIN_DELAY_TIME) {
        rqueueMessageTemplate.add(queueName, rqueueMessage);
      } else {
        rqueueMessageTemplate.addWithDelay(queueName, rqueueMessage);
      }
    } catch (Exception e) {
      logger.error("Message could not be pushed ", e);
      return false;
    }
    return true;
  }

  private RqueueMessage buildMessage(
      String queueName, Object message, Integer retryCount, Long delayInMilliSecs) {
    Message<?> msg = messageConverter.toMessage(message, null);
    if (msg == null) {
      throw new MessageConversionException("Message could not be build (null)");
    }
    return new RqueueMessage(queueName, (String) msg.getPayload(), retryCount, delayInMilliSecs);
  }

  Object convertMessageToObject(RqueueMessage message) {
    return messageConverter.fromMessage(new GenericMessage<>(message.getMessage()), null);
  }

  List<MessageConverter> getMessageConverters() {
    return messageConverter.getConverters();
  }
}
