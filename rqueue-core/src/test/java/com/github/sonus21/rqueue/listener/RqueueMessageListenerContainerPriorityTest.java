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
package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueMessageListenerContainerPriorityTest {

  @Test
  void messageBrokerDefaultEnqueueDelegatesToUnsuffixedOverload() {
    final AtomicInteger plain = new AtomicInteger();
    MessageBroker broker = new MessageBroker() {
      @Override
      public void enqueue(QueueDetail q, RqueueMessage m) {
        plain.incrementAndGet();
      }

      @Override
      public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {}

      @Override
      public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
        return Collections.emptyList();
      }

      @Override
      public boolean ack(QueueDetail q, RqueueMessage m) {
        return true;
      }

      @Override
      public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
        return true;
      }

      @Override
      public long moveExpired(QueueDetail q, long now, int batch) {
        return 0;
      }

      @Override
      public List<RqueueMessage> peek(QueueDetail q, long offset, long count) {
        return Collections.emptyList();
      }

      @Override
      public long size(QueueDetail q) {
        return 0;
      }

      @Override
      public AutoCloseable subscribe(String channel, Consumer<String> handler) {
        return () -> {};
      }

      @Override
      public void publish(String channel, String payload) {}

      @Override
      public Capabilities capabilities() {
        return Capabilities.REDIS_DEFAULTS;
      }
    };
    QueueDetail qd = QueueDetail.builder()
        .name("q1")
        .queueName("__rq::queue::q1")
        .processingQueueName("__rq::pq::q1")
        .completedQueueName("__rq::cq::q1")
        .scheduledQueueName("__rq::sq::q1")
        .processingQueueChannelName("__rq::ch::q1")
        .scheduledQueueChannelName("__rq::sch::q1")
        .visibilityTimeout(30000)
        .numRetry(3)
        .priority(Collections.emptyMap())
        .build();
    RqueueMessage msg =
        RqueueMessage.builder().id("x").queueName("q1").message("p").build();
    broker.enqueue(qd, "high", msg);
    broker.enqueue(qd, msg);
    assertEquals(2, plain.get(), "default priority overload should delegate to enqueue(q, m)");
  }
}
