/*
 * Copyright (c) 2019-2026 Sonu Kumar
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
 *
 * <p><b>Multi-consumer keying.</b> Two {@code @RqueueListener} methods on the same queue with
 * different {@code consumerName} overrides each produce a distinct {@link QueueDetail} and need
 * their own counters. The maps are keyed by {@code queueName##consumerName} (or just
 * {@code queueName} when no override is set) so the second registration does not silently
 * overwrite the first. Increment lookups follow the same composite key — see
 * {@link #updateFailureCount(String, String)}.
 */
public class QueueCounter {

  static final String FAILURE_COUNT = "failure.count";
  static final String EXECUTION_COUNT = "execution.count";
  private final Map<String, Counter> queueNameToFailureCounter = new HashMap<>();
  private final Map<String, Counter> queueNameToExecutionCounter = new HashMap<>();

  private static String key(String queueName, String consumerName) {
    return (consumerName == null || consumerName.isEmpty())
        ? queueName
        : queueName + "##" + consumerName;
  }

  private void updateCounter(Map<String, Counter> map, String queueName, String consumerName) {
    // Try the consumer-specific entry first; fall back to the bare queue-name entry so callers
    // that don't yet pass a consumer name (older paths, single-consumer queues) still work.
    Counter counter = map.get(key(queueName, consumerName));
    if (counter == null && consumerName != null && !consumerName.isEmpty()) {
      counter = map.get(queueName);
    }
    if (counter == null) {
      return;
    }
    counter.increment();
  }

  /** Backward-compatible single-arg increment; route to the bare-queue counter only. */
  void updateFailureCount(String queueName) {
    updateCounter(queueNameToFailureCounter, queueName, null);
  }

  /** Backward-compatible single-arg increment; route to the bare-queue counter only. */
  void updateExecutionCount(String queueName) {
    updateCounter(queueNameToExecutionCounter, queueName, null);
  }

  /**
   * Consumer-aware increment: increments the counter registered for
   * {@code (queueName, consumerName)}, falling back to the bare {@code queueName} counter when no
   * consumer-scoped entry exists. Use this from
   * {@link com.github.sonus21.rqueue.listener.RqueueExecutor} (which has the {@link QueueDetail})
   * so multi-consumer queues keep accurate per-consumer counts.
   */
  void updateFailureCount(String queueName, String consumerName) {
    updateCounter(queueNameToFailureCounter, queueName, consumerName);
  }

  /** Consumer-aware execution-count increment. See {@link #updateFailureCount(String, String)}. */
  void updateExecutionCount(String queueName, String consumerName) {
    updateCounter(queueNameToExecutionCounter, queueName, consumerName);
  }

  void registerQueue(
      MetricsProperties metricsProperties,
      Tags queueTags,
      MeterRegistry registry,
      QueueDetail queueDetail) {
    String mapKey = key(queueDetail.getName(), queueDetail.getConsumerName());
    if (metricsProperties.countFailure()) {
      Counter.Builder builder = Counter.builder(metricsProperties.getMetricName(FAILURE_COUNT))
          .tags(queueTags.and(QUEUE_KEY, queueDetail.getQueueName()))
          .description("Failure count");
      Counter counter = builder.register(registry);
      queueNameToFailureCounter.put(mapKey, counter);
    }
    if (metricsProperties.countExecution()) {
      Counter.Builder builder = Counter.builder(metricsProperties.getMetricName(EXECUTION_COUNT))
          .tags(queueTags.and(QUEUE_KEY, queueDetail.getQueueName()))
          .description("Task execution count");
      Counter counter = builder.register(registry);
      queueNameToExecutionCounter.put(mapKey, counter);
    }
  }
}
