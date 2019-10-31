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

package com.github.sonus21.rqueue.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings({"UnusedDeclaration"})
public class RqueueMessage implements Serializable, Cloneable {

  private static final long serialVersionUID = -3488860960637488519L;
  private String id;
  private String queueName;
  private String message;
  private Integer retryCount;
  private long queuedTime;
  private long processAt;
  private Long reEnqueuedAt;
  private int failureCount;

  public RqueueMessage() {}

  public RqueueMessage(String queueName, String message, Integer retryCount, Long delay) {
    this.queueName = queueName;
    this.message = message;
    this.retryCount = retryCount;
    this.queuedTime = System.currentTimeMillis();
    this.id = queueName + UUID.randomUUID().toString();
    if (delay != null) {
      this.processAt = queuedTime + delay;
    }
  }

  public void updateReEnqueuedAt() {
    this.reEnqueuedAt = System.currentTimeMillis();
  }

  public String getQueueName() {
    return this.queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public String getMessage() {
    return this.message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public Integer getRetryCount() {
    return this.retryCount;
  }

  public void setRetryCount(Integer retryCount) {
    this.retryCount = retryCount;
  }

  public long getQueuedTime() {
    return this.queuedTime;
  }

  public void setQueuedTime(long queuedTime) {
    this.queuedTime = queuedTime;
  }

  public long getProcessAt() {
    return this.processAt;
  }

  public void setProcessAt(Long processAt) {
    this.processAt = processAt;
  }

  public Long getReEnqueuedAt() {
    return this.reEnqueuedAt;
  }

  public void setReEnqueuedAt(Long reEnqueuedAt) {
    this.reEnqueuedAt = reEnqueuedAt;
  }

  @Override
  public String toString() {
    return "RqueueMessage(id="
        + this.getId()
        + ", queueName="
        + this.getQueueName()
        + ", message="
        + this.getMessage()
        + ", retryCount="
        + this.getRetryCount()
        + ", queuedTime="
        + this.getQueuedTime()
        + ", processAt="
        + this.getProcessAt()
        + ", reEnqueuedAt="
        + this.getReEnqueuedAt()
        + ", failureCount="
        + this.getFailureCount()
        + ")";
  }

  @Override
  public RqueueMessage clone() throws CloneNotSupportedException {
    return (RqueueMessage) super.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RqueueMessage) {
      RqueueMessage otherMessage = (RqueueMessage) other;
      if (otherMessage.getId() != null && this.getId() != null) {
        return this.getId().equals(otherMessage.getId());
      }
    }
    return false;
  }

  public int getFailureCount() {
    return failureCount;
  }

  public void setFailureCount(int failureCount) {
    this.failureCount = failureCount;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
