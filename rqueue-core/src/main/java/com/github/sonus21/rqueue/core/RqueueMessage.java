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

package com.github.sonus21.rqueue.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.github.sonus21.rqueue.models.SerializableBase;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@JsonPropertyOrder({"failureCount"})
@Builder
public class RqueueMessage extends SerializableBase implements Cloneable {

  private static final long serialVersionUID = -3488860960637488519L;
  // The message id, each message has a unique id
  private String id;
  // Queue name on which message was enqueued
  private String queueName;
  // JSON encoded message, this message can be deserialize to actual object with
  // the help of MessageUtils#convertMessageToObject method.
  private String message;
  // Any retry count used while enqueueing
  private Integer retryCount;
  // when this message was enqueued, this is in nano second
  private long queuedTime;
  // when this message was supposed to be processed
  private long processAt;
  // The time when it was re-enqueue due to failure.
  private Long reEnqueuedAt;
  // Number of times this message has failed.
  private int failureCount;
  // period of this task, if this is a periodic task.
  private long period;

  public void updateReEnqueuedAt() {
    reEnqueuedAt = System.currentTimeMillis();
  }

  @Override
  @SuppressWarnings("squid:S2975")
  public RqueueMessage clone() throws CloneNotSupportedException {
    return (RqueueMessage) super.clone();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RqueueMessage) {
      RqueueMessage otherMessage = (RqueueMessage) other;
      if (otherMessage.getId() != null && getId() != null) {
        return getId().equals(otherMessage.getId());
      }
    }
    return false;
  }

  @JsonIgnore
  public boolean isPeriodicTask() {
    return period > 0;
  }
}
