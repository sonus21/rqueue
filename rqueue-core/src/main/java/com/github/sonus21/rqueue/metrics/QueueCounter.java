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

import static com.github.sonus21.rqueue.metrics.RqueueMetrics.QUEUE_KEY;

import com.github.sonus21.rqueue.config.MetricsProperties;
import com.github.sonus21.rqueue.listener.QueueDetail;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Map;

/**
 * Queue counter counts the different types of events related to a queue. Failure and execution
 * count, it supports queue registrations.
 */
public class QueueCounter {

  static final String FAILURE_COUNT = "failure.count";
  static final String EXECUTION_COUNT = "execution.count";
  private final Map<String, Counter> queueNameToFailureCounter = new HashMap<>();
  private final Map<String, Counter> queueNameToExecutionCounter = new HashMap<>();

  private void updateCounter(Map<String, Counter> map, String queueName) {
    Counter counter = map.get(queueName);
    if (counter == null) {
      return;
    }
    counter.increment();
  }

  void updateFailureCount(String queueName) {
    updateCounter(queueNameToFailureCounter, queueName);
  }

  void updateExecutionCount(String queueName) {
    updateCounter(queueNameToExecutionCounter, queueName);
  }

  void registerQueue(
      MetricsProperties metricsProperties,
      Tags queueTags,
      MeterRegistry registry,
      QueueDetail queueDetail) {
    if (metricsProperties.countFailure()) {
      Counter.Builder builder =
          Counter.builder(FAILURE_COUNT)
              .tags(queueTags.and(QUEUE_KEY, queueDetail.getQueueName()))
              .description("Failure count");
      Counter counter = builder.register(registry);
      queueNameToFailureCounter.put(queueDetail.getName(), counter);
    }
    if (metricsProperties.countExecution()) {
      Counter.Builder builder =
          Counter.builder(EXECUTION_COUNT)
              .tags(queueTags.and(QUEUE_KEY, queueDetail.getQueueName()))
              .description("Task execution count");
      Counter counter = builder.register(registry);
      queueNameToExecutionCounter.put(queueDetail.getName(), counter);
    }
  }
}
