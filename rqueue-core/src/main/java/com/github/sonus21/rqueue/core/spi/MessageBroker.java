/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.core.spi;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

/**
 * Internal SPI. Subject to change. Application code must not depend on this directly.
 */
public interface MessageBroker {
  void enqueue(QueueDetail q, RqueueMessage m);

  /**
   * Priority-aware enqueue overload. Implementations that route to a per-priority destination
   * (e.g. a NATS subject suffixed with the priority name) override this. The default delegates
   * to {@link #enqueue(QueueDetail, RqueueMessage)} so backends without per-priority routing
   * (Redis already encodes priority in the queue name) keep their existing behavior.
   *
   * @param q queue detail (already priority-suffixed for backends that key off queue name)
   * @param priority priority name as declared on {@code @RqueueListener.priority}; may be
   *     {@code null} or empty for the default priority bucket
   * @param m message to publish
   */
  default void enqueue(QueueDetail q, String priority, RqueueMessage m) {
    enqueue(q, m);
  }

  void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs);

  /**
   * Called by {@code RqueueEndpointManager.registerQueue} after a queue is added to the registry.
   * Backends that need to provision resources (e.g. JetStream streams) at registration time should
   * override this. The default is a no-op so Redis and other backends are unaffected.
   */
  default void onQueueRegistered(QueueDetail q) {}

  /**
   * Validate the queue name against backend-specific rules. Called from every queue-registration
   * path ({@code RqueueEndpointManager.registerQueue} and the {@code @RqueueListener} bootstrap)
   * before the queue is added to the registry, so an illegal name fails fast with a clear error
   * instead of surfacing later as an opaque NATS / driver-side rejection.
   *
   * <p>Default is a no-op — backends like Redis accept any non-empty name.
   *
   * @throws IllegalArgumentException if {@code queueName} is not legal for this backend
   */
  default void validateQueueName(String queueName) {}

  /**
   * Reactive variant of {@link #enqueue(QueueDetail, RqueueMessage)}. The default falls back to the
   * blocking implementation wrapped in {@code Mono.fromRunnable}; backends with native async
   * publish APIs (e.g. JetStream) should override this to avoid blocking the calling thread.
   */
  default Mono<Void> enqueueReactive(QueueDetail q, RqueueMessage m) {
    return Mono.fromRunnable(() -> enqueue(q, m));
  }

  /**
   * Reactive variant of {@link #enqueueWithDelay(QueueDetail, RqueueMessage, long)}. The default
   * falls back to the blocking implementation. Backends that do not support delayed enqueue should
   * override this to return {@code Mono.error(new UnsupportedOperationException(...))}.
   */
  default Mono<Void> enqueueWithDelayReactive(QueueDetail q, RqueueMessage m, long delayMs) {
    return Mono.fromRunnable(() -> enqueueWithDelay(q, m, delayMs));
  }

  List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait);

  /**
   * Priority-aware pop overload. Implementations that route to a per-priority stream/consumer
   * override this; the default delegates to
   * {@link #pop(QueueDetail, String, int, Duration)}.
   *
   * @param q queue detail
   * @param priority priority name; {@code null} or empty for the default bucket
   * @param consumerName durable consumer name (already priority-suffixed by the caller for
   *     backends that key off the consumer name)
   * @param batch maximum messages to fetch
   * @param wait fetch wait duration
   */
  default List<RqueueMessage> pop(
      QueueDetail q, String priority, String consumerName, int batch, Duration wait) {
    return pop(q, consumerName, batch, wait);
  }

  boolean ack(QueueDetail q, RqueueMessage m);

  boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs);

  long moveExpired(QueueDetail q, long now, int batch);

  List<RqueueMessage> peek(QueueDetail q, long offset, long count);

  /**
   * Remove {@code old} from the processing store and re-enqueue {@code updated} for retry.
   * {@code delayMs <= 0} means immediate; {@code delayMs > 0} means schedule after that delay.
   * Backends without a processing store (e.g. NATS) default to a plain nack.
   */
  default void parkForRetry(QueueDetail q, RqueueMessage old, RqueueMessage updated, long delayMs) {
    nack(q, updated, delayMs);
  }

  /**
   * Remove {@code old} from the processing store and enqueue {@code updated} to {@code targetQueue}.
   * {@code delayMs <= 0} means immediate (list push); {@code delayMs > 0} means schedule (sorted-set).
   * Backends without a processing store default to a plain enqueue to the DLQ.
   */
  default void moveToDlq(
      QueueDetail source,
      String targetQueue,
      RqueueMessage old,
      RqueueMessage updated,
      long delayMs) {
    if (delayMs > 0) {
      enqueueWithDelay(source, updated, delayMs);
    } else {
      enqueue(source, updated);
    }
  }

  /**
   * Schedule the next execution of a periodic message.
   * {@code messageKey} is the deduplication key; {@code expirySeconds} is the TTL for that key.
   * Backends that don't support server-side scheduling default to a delayed enqueue.
   */
  default void scheduleNext(
      QueueDetail q, String messageKey, RqueueMessage message, long expirySeconds) {
    long delayMs = Math.max(0, message.getProcessAt() - System.currentTimeMillis());
    enqueueWithDelay(q, message, delayMs);
  }

  /**
   * Returns the score (epoch-ms deadline) of {@code m} in the processing store, or {@code null}
   * if the backend does not track per-message visibility (e.g. NATS uses consumer-level AckWait).
   */
  default Long getVisibilityTimeoutScore(QueueDetail q, RqueueMessage m) {
    return null;
  }

  /**
   * Adds {@code deltaMs} to the visibility timeout of {@code m} in the processing store.
   * Returns {@code false} if the backend does not support per-message lease extension.
   */
  default boolean extendVisibilityTimeout(QueueDetail q, RqueueMessage m, long deltaMs) {
    return false;
  }

  long size(QueueDetail q);

  /**
   * Short label for the storage backend shown in the dashboard "Queue Storage Footprint" section
   * header (e.g. "Redis", "NATS"). Defaults to "Redis".
   */
  default String storageKicker() {
    return "Redis";
  }

  /**
   * One-line description for the storage backend shown below the footprint section heading.
   * Defaults to the Redis description.
   */
  default String storageDescription() {
    return "Underlying Redis structures for the queues visible on this page.";
  }

  /**
   * Display name for the primary storage unit backing the given queue's messages (pending,
   * in-flight, and completed). Returns {@code null} to fall back to the Redis key name.
   */
  default String storageDisplayName(QueueDetail q) {
    return null;
  }

  /**
   * Display name for the dead-letter storage unit of the given queue. Returns {@code null} to
   * fall back to the DLQ key name stored in {@code DeadLetterQueue}.
   */
  default String dlqStorageDisplayName(QueueDetail q) {
    return null;
  }

  /**
   * Backend-aware human-readable label for the given Redis-shaped {@code DataType} on the given
   * dashboard tab. Surfaces in the queue-detail page's "Data Type" column so NATS deployments
   * can show "Queue (Stream)" instead of "LIST".
   *
   * <p>The default returns {@code null}, which the dashboard interprets as "fall back to
   * {@code DataType.name()}" (the historical Redis behavior).
   *
   * @param tab the dashboard nav tab the row corresponds to (PENDING, RUNNING, SCHEDULED, DEAD,
   *     COMPLETED, etc.). May be {@code null} when called in a context without a tab.
   * @param type Redis-shaped data type used by the dashboard's table rendering.
   * @return display label, or {@code null} to fall through to the default rendering.
   */
  default String dataTypeLabel(
      com.github.sonus21.rqueue.models.enums.NavTab tab,
      com.github.sonus21.rqueue.models.enums.DataType type) {
    return null;
  }

  AutoCloseable subscribe(String channel, Consumer<String> handler);

  void publish(String channel, String payload);

  Capabilities capabilities();
}
