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

package com.github.sonus21.rqueue.models.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.models.SerializableBase;
import java.util.LinkedHashSet;
import java.util.Set;
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
@Builder
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@NoArgsConstructor
public class QueueConfig extends SerializableBase {
  private static final long serialVersionUID = 2644813429709395582L;
  private String id;
  private String name;
  private String queueName;
  private String processingQueueName;
  private String delayedQueueName;
  private int numRetry;
  private long visibilityTimeout;
  private boolean deleted;
  private Long createdOn;
  private Long updatedOn;
  private Long deletedOn;
  private MinMax<Integer> concurrency;
  private Set<String> deadLetterQueues;

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

  public boolean addDeadLetterQueue(String queueName) {
    if (deadLetterQueues == null) {
      deadLetterQueues = new LinkedHashSet<>();
    }
    if (!deadLetterQueues.contains(queueName)) {
      deadLetterQueues.add(queueName);
      return true;
    }
    return false;
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

  @JsonIgnore
  public boolean isDeadLetterQueue(String name) {
    return deadLetterQueues.contains(name);
  }

  @JsonIgnore
  public boolean hasDeadLetterQueue() {
    return !CollectionUtils.isEmpty(deadLetterQueues);
  }
}
