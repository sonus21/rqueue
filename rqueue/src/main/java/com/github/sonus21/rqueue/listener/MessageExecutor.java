/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.utils.QueueInfo;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanCreationNotAllowedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

class MessageExecutor implements Runnable {
  private final ConsumerQueueDetail queueDetail;
  private final Message<String> message;
  private final RqueueMessage rqueueMessage;
  private final RqueueMessageHandler messageHandler;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final Logger logger;
  private static final int MAX_RETRY_COUNT = Integer.MAX_VALUE;
  private static final long DELTA_BETWEEN_RE_ENQUEUE_TIME = 5000L;
  private final Long accessTime;

  MessageExecutor(
      RqueueMessage message,
      ConsumerQueueDetail queueDetail,
      RqueueMessageHandler messageHandler,
      RqueueMessageTemplate rqueueMessageTemplate,
      Logger logger) {
    this.rqueueMessage = message;
    this.queueDetail = queueDetail;
    this.message =
        new GenericMessage<>(
            message.getMessage(), QueueInfo.getQueueHeaders(queueDetail.getQueueName()));
    this.messageHandler = messageHandler;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.logger = logger;
    this.accessTime = System.currentTimeMillis();
  }

  private int getMaxRetryCount() {
    int maxRetryCount =
        rqueueMessage.getRetryCount() == null
            ? queueDetail.getNumRetries()
            : rqueueMessage.getRetryCount();
    // DLQ is  not specified so retry it for max number of counts
    if (maxRetryCount == -1 && queueDetail.getDlqName().isEmpty()) {
      maxRetryCount = MAX_RETRY_COUNT;
    }
    return maxRetryCount;
  }

  private long getMaxProcessingTime() {
    return QueueInfo.getMessageReEnqueueTime() - DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }

  private void handlePostProcessing(boolean executed, int currentFailureCount, int maxRetryCount) {
    try {
      String processingQueueName = QueueInfo.getProcessingQueueName(queueDetail.getQueueName());
      if (!executed) {
        // move to DLQ
        if (!queueDetail.getDlqName().isEmpty()) {
          RqueueMessage newMessage = rqueueMessage.clone();
          newMessage.setFailureCount(currentFailureCount);
          newMessage.setAccessTime(accessTime);
          newMessage.updateReEnqueuedAt();
          rqueueMessageTemplate.add(queueDetail.getDlqName(), newMessage);
          rqueueMessageTemplate.removeFromZset(processingQueueName, rqueueMessage);
        } else if (currentFailureCount < maxRetryCount) {
          // replace the existing message with the update message
          // this will reflect the retry count
          RqueueMessage newMessage = rqueueMessage.clone();
          newMessage.setAccessTime(accessTime);
          newMessage.setFailureCount(currentFailureCount);
          newMessage.updateReEnqueuedAt();
          rqueueMessageTemplate.replaceMessage(processingQueueName, rqueueMessage, newMessage);
        } else {
          // discard this message
          logger.warn("Message {} discarded due to retry limit", rqueueMessage);
          rqueueMessageTemplate.removeFromZset(processingQueueName, rqueueMessage);
        }
      } else {
        // delete it from processing queue
        rqueueMessageTemplate.removeFromZset(processingQueueName, rqueueMessage);
      }
    } catch (Exception e) {
      logger.error("Error occurred in post processing", e);
    }
  }

  @Override
  public void run() {
    boolean executed = false;
    int currentFailureCount = rqueueMessage.getFailureCount();
    int maxRetryCount = getMaxRetryCount();
    long maxRetryTime = getMaxProcessingTime();
    do {
      try {
        messageHandler.handleMessage(message);
        executed = true;
      } catch (BeanCreationNotAllowedException e) {
        // application is being shutdown
        return;
      } catch (Exception e) {
        logger.warn("Message '{}' is failing", message.getPayload(), e);
        currentFailureCount += 1;
      }
    } while (currentFailureCount < maxRetryCount
        && !executed
        && System.currentTimeMillis() < maxRetryTime);
    handlePostProcessing(executed, currentFailureCount, maxRetryCount);
  }
}
