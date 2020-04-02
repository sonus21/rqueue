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

public class QueueDetail {
  private final String queueName;
  private final boolean delayedQueue;
  private final String dlqName;
  private final int numRetries;

  public QueueDetail(
      String queueName, int numRetries, String deadLetterQueueName, boolean delayedQueue) {
    this.queueName = queueName;
    this.numRetries = numRetries;
    this.delayedQueue = delayedQueue;
    dlqName = deadLetterQueueName;
  }

  public String getQueueName() {
    return queueName;
  }

  public boolean isDelayedQueue() {
    return delayedQueue;
  }

  public String getDlqName() {
    return dlqName;
  }

  public int getNumRetries() {
    return numRetries;
  }

  public boolean isDlqSet() {
    return dlqName != null && !dlqName.isEmpty();
  }
}
