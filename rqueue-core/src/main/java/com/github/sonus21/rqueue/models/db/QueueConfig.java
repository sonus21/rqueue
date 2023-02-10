/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.models.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.models.SerializableBase;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.CollectionUtils;

@Getter
@Setter
@ToString
@Builder(toBuilder = true)
@EqualsAndHashCode(
    callSuper = false,
    exclude = {"createdOn", "updatedOn"})
@AllArgsConstructor
@NoArgsConstructor
public class QueueConfig extends SerializableBase {

  private static final long serialVersionUID = 2644813429709395582L;
  private String id;
  private String name;
  private String queueName;
  private String completedQueueName;
  private String processingQueueName;
  private String scheduledQueueName;
  private int numRetry;
  private long visibilityTimeout;
  private boolean paused;
  private MinMax<Integer> concurrency;

  @JsonProperty("deadLetterQueuesV2")
  private List<DeadLetterQueue> deadLetterQueues;

  private boolean systemGenerated;
  private String priorityGroup;
  private Map<String, Integer> priority;
  private boolean deleted;
  private Long createdOn;
  private Long updatedOn;
  private Long deletedOn;

  public void updateTime() {
    this.updatedOn = System.currentTimeMillis();
  }

  public boolean updateRetryCount(int newRetryCount) {
    if (this.numRetry != newRetryCount) {
      this.numRetry = newRetryCount;
      return true;
    }
    return false;
  }

  public boolean addDeadLetterQueue(DeadLetterQueue deadLetterQueue) {
    if (deadLetterQueues == null) {
      deadLetterQueues = new LinkedList<>();
    }
    DeadLetterQueue existing = null;
    for (DeadLetterQueue dlq : deadLetterQueues) {
      if (dlq.getName().equals(deadLetterQueue.getName())) {
        if (dlq.isConsumerEnabled() == deadLetterQueue.isConsumerEnabled()) {
          return false;
        }
        existing = dlq;
        break;
      }
    }
    if (existing != null) {
      deadLetterQueues.remove(existing);
    }
    deadLetterQueues.add(deadLetterQueue);
    return true;
  }

  public boolean updateVisibilityTimeout(long newTimeOut) {
    if (visibilityTimeout != newTimeOut) {
      this.visibilityTimeout = newTimeOut;
      return true;
    }
    return false;
  }

  public boolean updateConcurrency(MinMax<Integer> concurrency) {
    if (this.concurrency == null || !this.concurrency.equals(concurrency)) {
      this.concurrency = concurrency;
      return true;
    }
    return false;
  }

  public boolean updatePriorityGroup(String priorityGroup) {
    if (this.priorityGroup == null || !this.priorityGroup.equals(priorityGroup)) {
      this.priorityGroup = priorityGroup;
      return true;
    }
    return false;
  }

  public boolean updatePriority(Map<String, Integer> newPriority) {
    if (CollectionUtils.isEmpty(newPriority) && !CollectionUtils.isEmpty(priority)) {
      this.priority = new HashMap<>();
      return true;
    }
    // when both are empty
    if (CollectionUtils.isEmpty(newPriority)) {
      return false;
    }
    boolean updated = false;
    Map<String, Integer> updatedPriority = new HashMap<>(priority);
    for (Entry<String, Integer> entry : newPriority.entrySet()) {
      Integer val = priority.get(entry.getKey());
      if (val == null || !val.equals(entry.getValue())) {
        updated = true;
        updatedPriority.put(entry.getKey(), entry.getValue());
      }
    }
    for (String key : priority.keySet()) {
      Integer val = newPriority.get(key);
      if (val == null) {
        updated = true;
        updatedPriority.remove(key);
      }
    }
    if (updated) {
      this.priority = updatedPriority;
    }
    return updated;
  }

  @JsonIgnore
  public boolean isDeadLetterQueue(String name) {
    if (!hasDeadLetterQueue()) {
      return false;
    }
    for (DeadLetterQueue deadLetterQueue : deadLetterQueues) {
      if (deadLetterQueue.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  @JsonIgnore
  public boolean hasDeadLetterQueue() {
    return !CollectionUtils.isEmpty(deadLetterQueues);
  }
}
