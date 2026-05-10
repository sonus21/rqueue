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

public interface RqueueMetricsCounter {

  void updateFailureCount(String queueName);

  void updateExecutionCount(String queueName);

  /**
   * Consumer-aware failure increment. When a queue carries multiple {@code @RqueueListener}
   * methods with distinct {@code consumerName} overrides, each consumer has its own counter
   * registered under {@code (queueName, consumerName)}; calling the bare-queue overload would
   * route every increment to the same (last-registered) counter and silently lose per-consumer
   * counts. Defaults to the queue-level path so callers that don't have a consumer name keep
   * working unchanged.
   */
  default void updateFailureCount(String queueName, String consumerName) {
    updateFailureCount(queueName);
  }

  /** Consumer-aware execution increment. See {@link #updateFailureCount(String, String)}. */
  default void updateExecutionCount(String queueName, String consumerName) {
    updateExecutionCount(queueName);
  }
}
