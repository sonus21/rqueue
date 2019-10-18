package com.github.sonus21.rqueue.producer;

import com.github.sonus21.rqueue.constants.Constants;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;

class MessageWriter {
  private RqueueMessageTemplate rqueueMessageTemplate;
  private CompositeMessageConverter messageConverter;
  private static Logger logger = LoggerFactory.getLogger(RqueueMessageSender.class);

  MessageWriter(
      RqueueMessageTemplate rqueueMessageTemplate, List<MessageConverter> messageConverters) {
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.messageConverter = new CompositeMessageConverter(messageConverters);
  }

  boolean pushMessage(String queueName, Object message, Integer retryCount, Long delayInMilliSecs) {
    RqueueMessage rqueueMessage = buildMessage(queueName, message, retryCount, delayInMilliSecs);
    try {
      if (delayInMilliSecs == null) {
        rqueueMessageTemplate.add(queueName, rqueueMessage);
      } else {
        rqueueMessageTemplate.addToZset(Constants.getZsetName(queueName), rqueueMessage);
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
}
