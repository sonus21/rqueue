package com.github.sonus21.rqueue.producer;

import com.github.sonus21.rqueue.utils.Validator;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import org.springframework.messaging.converter.MessageConverter;

/**
 *
 */
public class RqueueMessageSender {
  private MessageWriter messageWriter;

  public RqueueMessageSender(
      RqueueMessageTemplate messageTemplate, MessageConverter messageConverter) {
    this.messageWriter = new MessageWriter(messageTemplate, messageConverter);
  }

  public RqueueMessageSender(RqueueMessageTemplate messageTemplate) {
    this(messageTemplate, new GenericMessageConverter());
  }

  public boolean put(String queueName, Object message) {
    Validator.validateQueueNameAndMessage(queueName, message);
    return messageWriter.pushMessage(queueName, message, null, null);
  }

  public boolean put(String queueName, Object message, Long delayInMilliSecs) {
    if (delayInMilliSecs == null) {
      throw new IllegalArgumentException("delayInMilliSecs can not be null");
    }
    Validator.validateQueueNameAndMessage(queueName, message);
    return messageWriter.pushMessage(queueName, message, null, delayInMilliSecs);
  }

  public boolean put(String queueName, Object message, Integer retryCount, Long delayInMilliSecs) {
    Validator.validateQueueNameAndMessage(queueName, message);
    if (retryCount == null) {
      throw new IllegalArgumentException("retryCount can not be null");
    }
    if (delayInMilliSecs == null) {
      throw new IllegalArgumentException("delayInMilliSecs can not be null");
    }
    return messageWriter.pushMessage(queueName, message, retryCount, delayInMilliSecs);
  }

  public MessageConverter getMessageConverter() {
    return messageWriter.getMessageConverter();
  }

  public void setMessageConverter(MessageConverter messageConverter) {
    if (messageConverter == null) {
      throw new IllegalArgumentException("messageConverter can not be null");
    }
    this.messageWriter.setMessageConverter(messageConverter);
  }
}
