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
import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.Objects;

abstract class AbstractPoller extends MessageContainerBase {
  private final TaskExecutionBackOff taskBackOff;
  private final int retryPerPoll;

  AbstractPoller(
      RqueueMessageListenerContainer container,
      TaskExecutionBackOff taskExecutionBackOff,
      int retryPerPoll) {
    super(container);
    taskBackOff = taskExecutionBackOff;
    this.retryPerPoll = retryPerPoll;
  }

  RqueueMessage getMessage(QueueDetail queueDetail) {
    return getRqueueMessageTemplate()
        .pop(
            queueDetail.getQueueName(),
            queueDetail.getProcessingQueueName(),
            queueDetail.getProcessingQueueChannelName(),
            queueDetail.getVisibilityTimeout());
  }

  long getPollingInterval() {
    return Objects.requireNonNull(container.get()).getPollingInterval();
  }

  long getBackOffTime() {
    return Objects.requireNonNull(container.get()).getBackOffTime();
  }

  void enqueue(QueueThread queueThread, QueueDetail queueDetail, RqueueMessage message) {
    queueThread
        .getTaskExecutor()
        .execute(
            new MessageExecutor(
                message,
                queueDetail,
                queueThread.getSemaphore(),
                container,
                Objects.requireNonNull(container.get()).getRqueueMessageHandler(),
                retryPerPoll,
                taskBackOff));
  }
}
