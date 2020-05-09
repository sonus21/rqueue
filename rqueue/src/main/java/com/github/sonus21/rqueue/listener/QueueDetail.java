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

import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@Builder
@EqualsAndHashCode
public class QueueDetail implements Serializable {
  private static final long serialVersionUID = -4274795210784695201L;
  private String name;
  private int numRetry;
  private long visibilityTimeout;
  private String queueName;
  private String deadLetterQueueName;
  private String processingQueueName;
  private String processingQueueChannelName;
  private String delayedQueueName;
  private String delayedQueueChannelName;
  private boolean active;
  private MinMax<Integer> concurrency;
  private Map<String, Integer> priority;
  private String priorityGroup;
  private boolean systemGenerated;

  public boolean isDlqSet() {
    return !StringUtils.isEmpty(deadLetterQueueName);
  }

  public QueueConfig toConfig() {
    QueueConfig queueConfig =
        QueueConfig.builder()
            .name(name)
            .numRetry(numRetry)
            .queueName(queueName)
            .delayedQueueName(delayedQueueName)
            .processingQueueName(processingQueueName)
            .visibilityTimeout(visibilityTimeout)
            .createdOn(System.currentTimeMillis())
            .updatedOn(System.currentTimeMillis())
            .deadLetterQueues(new LinkedHashSet<>())
            .concurrency(concurrency)
            .priority(priority)
            .priorityGroup(priorityGroup)
            .systemGenerated(systemGenerated)
            .build();
    if (isDlqSet()) {
      queueConfig.addDeadLetterQueue(deadLetterQueueName);
    }
    return queueConfig;
  }

  List<QueueDetail> expandQueueDetail(boolean addDefault, int priority) {
    List<QueueDetail> queueDetails = new ArrayList<>();
    for (Entry<String, Integer> entry : getPriority().entrySet()) {
      QueueDetail cloneQueueDetail = cloneQueueDetail(entry.getKey(), entry.getValue());
      cloneQueueDetail.systemGenerated = true;
      cloneQueueDetail.priorityGroup = name;
      queueDetails.add(cloneQueueDetail);
    }
    if (addDefault) {
      int defaultPriority = priority;
      if (defaultPriority == -1) {
        List<Integer> priorities = new ArrayList<>(getPriority().values());
        priorities.sort(Comparator.comparingInt(o -> o));
        defaultPriority = priorities.get(priorities.size() / 2);
      }
      this.priority.put(Constants.DEFAULT_PRIORITY_KEY, defaultPriority);
      this.priorityGroup = name;
      queueDetails.add(this);
    }
    return queueDetails;
  }

  private QueueDetail cloneQueueDetail(String priorityName, Integer priority) {
    if (priority == null || priorityName == null) {
      throw new IllegalStateException("priority name is null");
    }
    String suffix = "_" + priorityName;
    return QueueDetail.builder()
        .numRetry(numRetry)
        .visibilityTimeout(visibilityTimeout)
        .deadLetterQueueName(deadLetterQueueName)
        .priority(Collections.singletonMap(Constants.DEFAULT_PRIORITY_KEY, priority))
        .name(name + suffix)
        .queueName(queueName + suffix)
        .processingQueueName(processingQueueName + suffix)
        .processingQueueChannelName(processingQueueChannelName + suffix)
        .delayedQueueName(delayedQueueName + suffix)
        .delayedQueueChannelName(delayedQueueChannelName + suffix)
        .build();
  }
}
