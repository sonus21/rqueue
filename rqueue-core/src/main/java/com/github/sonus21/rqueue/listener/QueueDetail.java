/*
 * Copyright (c) 2019-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.listener;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@EqualsAndHashCode(callSuper = true)
@ToString
public class QueueDetail extends SerializableBase {

  private static final long serialVersionUID = 9153752084449974622L;
  // visibility timeout in milliseconds
  private final long visibilityTimeout;
  private final String name;
  private final int numRetry;
  @Builder.Default
  private final QueueType type = QueueType.QUEUE;
  private final String queueName;
  private final String deadLetterQueueName;
  private final boolean deadLetterConsumerEnabled;
  private final String completedQueueName;
  private final String processingQueueName;
  private final String processingQueueChannelName;
  private final String scheduledQueueName;
  private final String scheduledQueueChannelName;
  private final boolean active;
  private final Concurrency concurrency;
  private final boolean systemGenerated;
  private final int batchSize;
  private Map<String, Integer> priority;
  private String priorityGroup;

  public boolean isDlqSet() {
    return !StringUtils.isEmpty(deadLetterQueueName);
  }

  @JsonIgnore
  public DeadLetterQueue getDeadLetterQueue() {
    return new DeadLetterQueue(deadLetterQueueName, deadLetterConsumerEnabled);
  }

  public QueueConfig toConfig() {
    QueueConfig queueConfig =
        QueueConfig.builder()
            .name(name)
            .numRetry(numRetry)
            .queueName(queueName)
            .scheduledQueueName(scheduledQueueName)
            .processingQueueName(processingQueueName)
            .completedQueueName(completedQueueName)
            .visibilityTimeout(visibilityTimeout)
            .createdOn(System.currentTimeMillis())
            .updatedOn(System.currentTimeMillis())
            .deadLetterQueues(new LinkedList<>())
            .concurrency(concurrency.toMinMax())
            .priority(Collections.unmodifiableMap(priority))
            .priorityGroup(priorityGroup)
            .systemGenerated(systemGenerated)
            .build();
    if (isDlqSet()) {
      queueConfig.addDeadLetterQueue(getDeadLetterQueue());
    }
    return queueConfig;
  }

  List<QueueDetail> expandQueueDetail(boolean addDefault, int priority) {
    List<QueueDetail> queueDetails = new ArrayList<>();
    for (Entry<String, Integer> entry : getPriority().entrySet()) {
      QueueDetail cloneQueueDetail = cloneQueueDetail(entry.getKey(), entry.getValue(), name);
      queueDetails.add(cloneQueueDetail);
    }
    if (addDefault) {
      int defaultPriority = priority;
      if (defaultPriority == -1) {
        List<Integer> priorities = new ArrayList<>(getPriority().values());
        priorities.sort(Comparator.comparingInt(o -> o));
        defaultPriority = priorities.get(priorities.size() / 2);
      }
      Map<String, Integer> priorityMap = new HashMap<>(this.priority);
      priorityMap.put(Constants.DEFAULT_PRIORITY_KEY, defaultPriority);
      this.priority = Collections.unmodifiableMap(priorityMap);
      this.priorityGroup = name;
      queueDetails.add(this);
    }
    return queueDetails;
  }

  private QueueDetail cloneQueueDetail(
      String priorityName, Integer priority, String priorityGroup) {
    if (priority == null || priorityName == null) {
      throw new IllegalStateException("priority name is null");
    }
    String suffix = PriorityUtils.getSuffix(priorityName);
    return QueueDetail.builder()
        .numRetry(numRetry)
        .visibilityTimeout(visibilityTimeout)
        .deadLetterQueueName(deadLetterQueueName)
        .deadLetterConsumerEnabled(deadLetterConsumerEnabled)
        .name(name + suffix)
        .queueName(queueName + suffix)
        .processingQueueName(processingQueueName + suffix)
        .processingQueueChannelName(processingQueueChannelName + suffix)
        .scheduledQueueName(scheduledQueueName + suffix)
        .scheduledQueueChannelName(scheduledQueueChannelName + suffix)
        .completedQueueName(completedQueueName + suffix)
        .active(active)
        .batchSize(batchSize)
        .systemGenerated(true)
        .priorityGroup(priorityGroup)
        .concurrency(concurrency)
        .priority(Collections.singletonMap(Constants.DEFAULT_PRIORITY_KEY, priority))
        .build();
  }

  public Duration visibilityDuration() {
    return Duration.ofMillis(visibilityTimeout);
  }

  public enum QueueType {
    QUEUE,
    STREAM
  }
}
