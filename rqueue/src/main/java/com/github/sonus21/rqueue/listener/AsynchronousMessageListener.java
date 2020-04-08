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
import java.util.concurrent.Executor;

class AsynchronousMessageListener extends MessageContainerBase implements Runnable {
  private final String queueName;
  private final QueueDetail queueDetail;

  AsynchronousMessageListener(
      String queueName, QueueDetail value, RqueueMessageListenerContainer container) {
    super(container);
    this.queueName = queueName;
    this.queueDetail = value;
  }

  private RqueueMessage getMessage() {
    return getRqueueMessageTemplate().pop(queueName, queueDetail.getMaxJobExecutionTime());
  }

  @Override
  public void run() {
    getLogger().debug("Running Queue {}", queueName);
    while (isQueueActive(queueName)) {
      try {
        RqueueMessage message = getMessage();
        getLogger().debug("Queue: {} Fetched Msg {}", queueName, message);
        if (message != null) {
          getTaskExecutor().execute(new MessageExecutor(message, queueDetail, container));
        } else {
          try {
            Thread.sleep(getPollingInterval());
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
        }
      } catch (Exception e) {
        getLogger()
            .warn(
                "Message listener failed for the queue {}, it will be retried in {} Ms",
                queueName,
                getBackOffTime(),
                e);
        try {
          Thread.sleep(getBackOffTime());
        } catch (InterruptedException ex) {
          ex.printStackTrace();
        }
      }
    }
  }

  private long getPollingInterval() {
    return container.get().getPollingInterval();
  }

  private long getBackOffTime() {
    return container.get().getBackOffTime();
  }

  private Executor getTaskExecutor() {
    return container.get().getTaskExecutor();
  }
}
