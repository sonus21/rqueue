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
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.util.CollectionUtils;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class QueueConfig extends SerializableBase {
  private static final long serialVersionUID = 2644813429709395582L;
  private String id;
  private String name;
  private boolean delayed;
  private int numRetry;
  private long visibilityTimeout;
  private boolean deleted;
  private Long createdOn;
  private Long updatedOn;
  private Long deletedOn;
  private Set<String> deadLetterQueues;

  public QueueConfig(
      String id, String name, int numRetry, boolean delayed, long visibilityTimeout) {
    this(id, name, numRetry, delayed, visibilityTimeout, null);
  }

  public QueueConfig(
      String id,
      String name,
      int numRetry,
      boolean delayed,
      long visibilityTimeout,
      String deadLetterQueue) {
    this.id = id;
    this.name = name;
    this.numRetry = numRetry;
    this.delayed = delayed;
    this.visibilityTimeout = visibilityTimeout;
    this.createdOn = System.currentTimeMillis();
    this.deadLetterQueues = new LinkedHashSet<>();
    updateTime();
    if (!StringUtils.isEmpty(deadLetterQueue)) {
      addDeadLetterQueue(deadLetterQueue);
    }
  }

  public void updateTime() {
    this.updatedOn = System.currentTimeMillis();
  }

  public boolean updateIsDelay(boolean delayed) {
    if (this.delayed != delayed) {
      this.delayed = delayed;
      return true;
    }
    return false;
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

  @JsonIgnore
  public boolean isDelayedQueue(String name) {
    return deadLetterQueues.contains(name);
  }

  @JsonIgnore
  public boolean hasDeadLetterQueue() {
    return !CollectionUtils.isEmpty(deadLetterQueues);
  }
}
