package com.github.sonus21.rqueue.producer;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.utils.Validator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;

/** */
public class RqueueMessageSender {
  private MessageWriter messageWriter;

  private RqueueMessageSender(
      RqueueMessageTemplate messageTemplate,
      List<MessageConverter> messageConverters,
      boolean addDefault) {
    if (CollectionUtils.isEmpty(messageConverters)) {
      throw new IllegalArgumentException("messageConverters can  not be empty");
    }
    if (addDefault) {
      messageConverters.add(new GenericMessageConverter());
    }
    this.messageWriter = new MessageWriter(messageTemplate, new ArrayList<>(messageConverters));
  }

  public RqueueMessageSender(RqueueMessageTemplate messageTemplate) {
    this(messageTemplate, Collections.singletonList(new GenericMessageConverter()), false);
  }

  public RqueueMessageSender(
      RqueueMessageTemplate messageTemplate, List<MessageConverter> messageConverters) {
    this(messageTemplate, messageConverters, true);
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
}
