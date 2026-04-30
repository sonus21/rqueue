/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
 * Backend-agnostic provider of queue-depth metrics. Each backend (Redis, NATS, ...) supplies its
 * own implementation; consumers like {@link RqueueMetrics} read sizes through this interface
 * instead of reaching into a Redis-shaped DAO.
 *
 * <p>The {@code queueName} argument is the user-facing queue name (the value bound on
 * {@code @RqueueListener(value="...")}), not an internal storage key. Implementations are
 * responsible for mapping it to the appropriate backend-specific key(s).
 *
 * <p>Implementations must return {@code 0} when a queue has no messages of the requested kind
 * (rather than throwing) so callers can use the values directly as gauge readings.
 */
public interface RqueueQueueMetricsProvider {

  /**
   * Number of messages waiting to be consumed from {@code queueName} — i.e. enqueued and ready for
   * a worker to pick up, excluding messages already in-flight (processing) or scheduled for a
   * future delivery time.
   */
  long getPendingMessageCount(String queueName);

  /**
   * Number of messages enqueued to {@code queueName} with a future delivery time that has not yet
   * elapsed. Backends that don't support delayed delivery return {@code 0}.
   */
  long getScheduledMessageCount(String queueName);

  /**
   * Number of messages currently in-flight for {@code queueName} — handed to a worker but not yet
   * acked or nacked. Backends without an explicit in-flight set return {@code 0}.
   */
  long getProcessingMessageCount(String queueName);

  /**
   * Number of messages in the dead-letter queue associated with {@code queueName}. Returns
   * {@code 0} when no DLQ is configured for the queue or the backend does not surface DLQ depth.
   */
  long getDeadLetterMessageCount(String queueName);
}
