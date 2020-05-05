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

import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.utils.SystemUtils;
import java.io.Serializable;
import java.util.LinkedHashSet;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@Builder
@EqualsAndHashCode
public class QueueDetail implements Serializable {
  private static final long serialVersionUID = -4274795210784695201L;
  private String name;
  private boolean delayedQueue;
  private int numRetry;
  private long visibilityTimeout;
  private String queueName;
  private String deadLetterQueueName;
  private String processingQueueName;
  private String processingQueueChannelName;
  private String delayedQueueName;
  private String delayedQueueChannelName;

  public boolean isDlqSet() {
    return !StringUtils.isEmpty(deadLetterQueueName);
  }

  public QueueConfig toConfig() {
    QueueConfig queueConfig =
        QueueConfig.builder()
            .id(SystemUtils.getQueueConfigKey(name))
            .name(name)
            .delayed(delayedQueue)
            .numRetry(numRetry)
            .queueName(queueName)
            .delayedQueueName(delayedQueueName)
            .processingQueueName(processingQueueName)
            .visibilityTimeout(visibilityTimeout)
            .createdOn(System.currentTimeMillis())
            .updatedOn(System.currentTimeMillis())
            .deadLetterQueues(new LinkedHashSet<>())
            .build();
    if (isDlqSet()) {
      queueConfig.addDeadLetterQueue(deadLetterQueueName);
    }
    return queueConfig;
  }
}
