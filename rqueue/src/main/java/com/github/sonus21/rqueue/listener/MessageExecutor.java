package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import org.slf4j.Logger;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

class MessageExecutor implements Runnable {
  private final ConsumerQueueDetail queueDetail;
  private final Message<String> message;
  private final RqueueMessage rqueueMessage;
  private final RqueueMessageHandler messageHandler;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final Logger logger;

  MessageExecutor(
      RqueueMessage message,
      ConsumerQueueDetail queueDetail,
      RqueueMessageHandler messageHandler,
      RqueueMessageTemplate rqueueMessageTemplate,
      Logger logger) {
    message.updateAccessTime();
    this.rqueueMessage = message;
    this.queueDetail = queueDetail;
    this.message = new GenericMessage<String>(message.getMessage(), queueDetail.getHeaders());
    this.messageHandler = messageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.logger = logger;
  }

  @Override
  public void run() {
    int retryCount =
        rqueueMessage.getRetryCount() == null
            ? queueDetail.getNumRetries()
            : rqueueMessage.getRetryCount();
    boolean executed = false;
    // DLQ is  not specified
    if (retryCount == -1 && queueDetail.getDlqName().isEmpty()) {
      retryCount = Integer.MAX_VALUE;
    }
    do {
      try {
        messageHandler.handleMessage(message);
        executed = true;
      } catch (Exception e) {
        logger.warn("Message '{}' is failing", message.getPayload(), e);
        retryCount--;
      }
    } while (retryCount > 0 && !executed);
    if (!executed) {
      if (queueDetail.getDlqName().isEmpty()) {
        logger.warn(
            "Message '{}' discarded, queue: {}", message.getPayload(), queueDetail.getQueueName());
      } else {
        rqueueMessage.updateReEnqueuedAt();
        rqueueMessageTemplate.add(queueDetail.getDlqName(), rqueueMessage);
      }
    }
  }
}
