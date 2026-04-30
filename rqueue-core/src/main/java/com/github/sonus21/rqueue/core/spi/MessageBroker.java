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

  long size(QueueDetail q);

  AutoCloseable subscribe(String channel, Consumer<String> handler);

  void publish(String channel, String payload);

  Capabilities capabilities();
}
