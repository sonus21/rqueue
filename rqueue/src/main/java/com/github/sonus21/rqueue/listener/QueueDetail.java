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

import com.github.sonus21.rqueue.utils.StringUtils;
import java.io.Serializable;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class QueueDetail implements Serializable {

  private static final long serialVersionUID = -4274795210784695201L;
  private final String queueName;
  private final boolean delayedQueue;
  private final String deadLetterQueueName;
  private final int numRetries;
  private final long visibilityTimeout;

  public QueueDetail(
      @NonNull String queueName,
      int numRetries,
      String deadLetterQueueName,
      boolean delayedQueue,
      long visibilityTimeout) {
    if (numRetries == -1) {
      throw new IllegalArgumentException("numRetries must be greater than one");
    }
    this.queueName = queueName;
    this.numRetries = numRetries;
    this.delayedQueue = delayedQueue;
    this.deadLetterQueueName = deadLetterQueueName;
    this.visibilityTimeout = visibilityTimeout;
  }

  public boolean isDlqSet() {
    return !StringUtils.isEmpty(deadLetterQueueName);
  }
}
