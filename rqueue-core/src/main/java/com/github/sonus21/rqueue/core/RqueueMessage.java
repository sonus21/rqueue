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

import com.github.sonus21.rqueue.models.SerializableBase;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class RqueueMessage extends SerializableBase implements Cloneable {

  private static final long serialVersionUID = -3488860960637488519L;
  private String id;
  private String queueName;
  private String message;
  private Integer retryCount;
  private long queuedTime;
  private long processAt;
  private Long reEnqueuedAt;
  private int failureCount;

  public RqueueMessage(String queueName, String message, Integer retryCount, Long delay) {
    this.queueName = queueName;
    this.message = message;
    this.retryCount = retryCount;
    this.queuedTime = System.currentTimeMillis();
    this.id = UUID.randomUUID().toString();
    this.processAt = this.queuedTime;
    if (delay != null) {
      this.processAt += delay;
    }
  }

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
}
