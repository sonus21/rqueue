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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.processor.MessageProcessor;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.QueueUtils;
import java.lang.ref.WeakReference;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

class MessageExecutor extends MessageContainerBase implements Runnable {
  private static final int MAX_RETRY_COUNT = Integer.MAX_VALUE;
  private final QueueDetail queueDetail;
  private final Message<String> message;
  private final RqueueMessage rqueueMessage;

  MessageExecutor(
      RqueueMessage message,
      QueueDetail queueDetail,
      WeakReference<RqueueMessageListenerContainer> container) {
    super(container);
    rqueueMessage = message;
    this.queueDetail = queueDetail;
    this.message =
        new GenericMessage<>(
            message.getMessage(), QueueUtils.getQueueHeaders(queueDetail.getQueueName()));
  }

  private int getMaxRetryCount() {
    int maxRetryCount =
        rqueueMessage.getRetryCount() == null
            ? queueDetail.getNumRetries()
            : rqueueMessage.getRetryCount();
    // DLQ is not specified so retry it for max number of counts
    if (maxRetryCount == -1 && !queueDetail.isDlqSet()) {
      maxRetryCount = MAX_RETRY_COUNT;
    }
    return maxRetryCount;
  }

  private Object getPayload() {
    return MessageUtils.convertMessageToObject(message, getMessageConverters());
  }

  @SuppressWarnings("ConstantConditions")
  private void callMessageProcessor(boolean discardOrDlq, RqueueMessage message) {
    MessageProcessor messageProcessor;
    if (discardOrDlq) {
      messageProcessor = container.get().getDiscardMessageProcessor();
    } else {
      messageProcessor = container.get().getDlqMessageProcessor();
    }
    String name = discardOrDlq ? "Discard Message Queue" : "Dead Letter Queue";
    try {
      getLogger().debug("Calling {} processor for {}", name, message);
      Object payload = getPayload();
      messageProcessor.process(payload);
    } catch (Exception e) {
      getLogger().error("Message processor call failed", e);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void updateCounter(boolean failOrExecution) {
    RqueueCounter rqueueCounter = container.get().rqueueCounter;
    if (rqueueCounter == null) {
      return;
    }
    if (failOrExecution) {
      rqueueCounter.updateFailureCount(queueDetail.getQueueName());
    } else {
      rqueueCounter.updateExecutionCount(queueDetail.getQueueName());
    }
  }

  private void handlePostProcessing(boolean executed, int currentFailureCount, int maxRetryCount) {
    if (!isQueueActive(queueDetail.getQueueName())) {
      return;
    }
    try {
      String processingQueueName = QueueUtils.getProcessingQueueName(queueDetail.getQueueName());
      if (!executed) {
        // move to DLQ
        if (queueDetail.isDlqSet()) {
          RqueueMessage newMessage = rqueueMessage.clone();
          newMessage.setFailureCount(currentFailureCount);
          newMessage.updateReEnqueuedAt();
          callMessageProcessor(false, newMessage);
          // No transaction??
          getRqueueMessageTemplate().add(queueDetail.getDlqName(), newMessage);
          getRqueueMessageTemplate().removeFromZset(processingQueueName, rqueueMessage);
        } else if (currentFailureCount < maxRetryCount) {
          // replace the existing message with the update message
          // this will reflect new retry count
          RqueueMessage newMessage = rqueueMessage.clone();
          newMessage.setFailureCount(currentFailureCount);
          newMessage.updateReEnqueuedAt();
          getRqueueMessageTemplate().replaceMessage(processingQueueName, rqueueMessage, newMessage);
        } else {
          // discard this message
          getLogger()
              .warn(
                  "Message {} discarded due to retry limit queue: {}",
                  getPayload(),
                  queueDetail.getQueueName());
          getRqueueMessageTemplate().removeFromZset(processingQueueName, rqueueMessage);
          callMessageProcessor(true, rqueueMessage);
        }
      } else {
        getLogger().debug("Delete Queue: {} message: {}", processingQueueName, rqueueMessage);
        // delete it from processing queue
        getRqueueMessageTemplate().removeFromZset(processingQueueName, rqueueMessage);
      }
    } catch (Exception e) {
      getLogger().error("Error occurred in post processing", e);
    }
  }

  @Override
  public void run() {
    boolean executed = false;
    int currentFailureCount = rqueueMessage.getFailureCount();
    int maxRetryCount = getMaxRetryCount();
    long maxRetryTime = getMaxProcessingTime();
    do {
      if (!isQueueActive(queueDetail.getQueueName())) {
        return;
      }
      try {
        updateCounter(false);
        getMessageHandler().handleMessage(message);
        executed = true;
      } catch (Exception e) {
        updateCounter(true);
        currentFailureCount += 1;
      }
    } while (currentFailureCount < maxRetryCount
        && !executed
        && System.currentTimeMillis() < maxRetryTime);
    handlePostProcessing(executed, currentFailureCount, maxRetryCount);
  }
}
