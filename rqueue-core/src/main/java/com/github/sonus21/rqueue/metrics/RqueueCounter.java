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

package com.github.sonus21.rqueue.metrics;

/**
 * Rqueue counter counts the different types of events related to a queue. It's used to count how
 * many messages have been processed and how many of them have been failed. In the case of failure
 * count increases.
 */
public class RqueueCounter implements RqueueMetricsCounter {

  private final QueueCounter queueCounter;

  public RqueueCounter(QueueCounter queueCounter) {
    this.queueCounter = queueCounter;
  }

  @Override
  public void updateFailureCount(String queueName) {
    queueCounter.updateFailureCount(queueName);
  }

  @Override
  public void updateExecutionCount(String queueName) {
    queueCounter.updateExecutionCount(queueName);
  }
}
