/*
 * Copyright 2019 Sonu Kumar
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

package com.github.sonus21.rqueue.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Map;

/**
 * Rqueue counter counts the different types of events related to a queue. It's used to count how
 * many messages have been processed and how many of them have been failed. In the case of failure
 * count increases.
 */
public class RqueueCounter {
  private static final String FAILURE_COUNT = "failure.count";
  private static final String EXECUTION_COUNT = "execution.count";
  private Map<String, Counter> queueNameToFailureCounter = new HashMap<>();
  private Map<String, Counter> queueNameToExecutionCounter = new HashMap<>();

  private void updateCounter(Map<String, Counter> map, String queueName) {
    Counter counter = map.get(queueName);
    if (counter == null) {
      return;
    }
    counter.increment();
  }

  public void updateFailureCount(String queueName) {
    updateCounter(queueNameToFailureCounter, queueName);
  }

  public void updateExecutionCount(String queueName) {
    updateCounter(queueNameToExecutionCounter, queueName);
  }

  void registerQueue(
      RqueueMetricsProperties metricsProperties,
      Tags queueTags,
      MeterRegistry registry,
      String queueName) {
    if (metricsProperties.countFailure()) {
      Counter.Builder builder =
          Counter.builder(FAILURE_COUNT).tags(queueTags).description("Failure count");
      Counter counter = builder.register(registry);
      queueNameToFailureCounter.put(queueName, counter);
    }
    if (metricsProperties.countExecution()) {
      Counter.Builder builder =
          Counter.builder(EXECUTION_COUNT).tags(queueTags).description("Task execution count");
      Counter counter = builder.register(registry);
      queueNameToExecutionCounter.put(queueName, counter);
    }
  }
}
