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

/**
 * Internal SPI. Subject to change. Application code must not depend on this directly.
 */
public interface MessageBroker {
  void enqueue(QueueDetail q, RqueueMessage m);

  void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs);

  List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait);

  boolean ack(QueueDetail q, RqueueMessage m);

  boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs);

  long moveExpired(QueueDetail q, long now, int batch);

  List<RqueueMessage> peek(QueueDetail q, long offset, long count);

  long size(QueueDetail q);

  AutoCloseable subscribe(String channel, Consumer<String> handler);

  void publish(String channel, String payload);

  Capabilities capabilities();
}
