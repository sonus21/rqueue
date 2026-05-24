/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.core.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

@CoreUnitTest
class MessageBrokerDefaultMethodsTest {

  private final QueueDetail queue = TestUtils.createQueueDetail("queue", 1, 30_000L, null);
  private final RqueueMessage oldMessage =
      RqueueMessage.builder().id("old").message("old").build();
  private final RqueueMessage updatedMessage =
      RqueueMessage.builder().id("updated").message("updated").processAt(1L).build();

  @Test
  void priorityAndReactiveDefaultsDelegateToBlockingOperations() {
    RecordingBroker broker = new RecordingBroker();

    broker.enqueue(queue, "high", updatedMessage);
    StepVerifier.create(broker.enqueueReactive(queue, oldMessage)).verifyComplete();
    StepVerifier.create(broker.enqueueWithDelayReactive(queue, updatedMessage, 17L))
        .verifyComplete();
    broker.pop(queue, "high", "consumer", 3, Duration.ofMillis(25L));

    assertEquals(2, broker.enqueueCalls);
    assertEquals(oldMessage, broker.lastEnqueued);
    assertEquals(1, broker.delayCalls);
    assertEquals(17L, broker.lastDelayMs);
    assertEquals(1, broker.popCalls);
    assertEquals("consumer", broker.lastConsumerName);
    assertEquals(Duration.ofMillis(25L), broker.lastWait);
  }

  @Test
  void retryDlqAndScheduleDefaultsUseBackendPrimitives() {
    RecordingBroker broker = new RecordingBroker();

    broker.parkForRetry(queue, oldMessage, updatedMessage, 123L);
    broker.moveToDlq(queue, "dlq", oldMessage, updatedMessage, 0L);
    broker.moveToDlq(queue, "dlq", oldMessage, updatedMessage, 99L);
    broker.scheduleNext(queue, "period-key", updatedMessage, 60L);

    assertEquals(1, broker.nackCalls);
    assertEquals(updatedMessage, broker.lastNacked);
    assertEquals(123L, broker.lastRetryDelayMs);
    assertEquals(1, broker.enqueueCalls);
    assertEquals(2, broker.delayCalls);
    assertEquals(updatedMessage, broker.lastDelayed);
  }

  @Test
  void dashboardDefaultsExposeRedisLabelsAndSingleSubscriberFallback() {
    RecordingBroker broker = new RecordingBroker();

    assertEquals("Redis", broker.storageKicker());
    assertEquals(
        "Underlying Redis structures for the queues visible on this page.",
        broker.storageDescription());
    assertNull(broker.storageDisplayName(queue));
    assertNull(broker.dlqStorageDisplayName(queue));
    assertNull(broker.consumerPendingSizes(queue));
    assertNull(broker.dataTypeLabel(null, null));
    assertFalse(broker.isSizeApproximate(queue));
    assertNull(broker.getVisibilityTimeoutScore(queue, oldMessage));
    assertFalse(broker.extendVisibilityTimeout(queue, oldMessage, 1L));

    List<SubscriberView> subscribers = broker.subscribers(queue);

    assertEquals(1, subscribers.size());
    assertEquals(queue.resolvedConsumerName(), subscribers.get(0).consumerName());
    assertEquals(42L, subscribers.get(0).pending());
    assertEquals(0L, subscribers.get(0).inFlight());
    assertTrue(subscribers.get(0).pendingShared());
  }

  @Test
  void subscribersDefaultFallsBackToZeroWhenSizeFails() {
    RecordingBroker broker = new RecordingBroker();
    broker.failSize = true;

    List<SubscriberView> subscribers = broker.subscribers(queue);

    assertEquals(1, subscribers.size());
    assertEquals(0L, subscribers.get(0).pending());
  }

  private static final class RecordingBroker implements MessageBroker {

    int enqueueCalls;
    int delayCalls;
    int popCalls;
    int nackCalls;
    boolean failSize;
    String lastConsumerName;
    Duration lastWait;
    long lastDelayMs;
    long lastRetryDelayMs;
    RqueueMessage lastEnqueued;
    RqueueMessage lastDelayed;
    RqueueMessage lastNacked;

    @Override
    public void enqueue(QueueDetail q, RqueueMessage m) {
      enqueueCalls++;
      lastEnqueued = m;
    }

    @Override
    public void enqueueWithDelay(QueueDetail q, RqueueMessage m, long delayMs) {
      delayCalls++;
      lastDelayed = m;
      lastDelayMs = delayMs;
    }

    @Override
    public List<RqueueMessage> pop(QueueDetail q, String consumerName, int batch, Duration wait) {
      popCalls++;
      lastConsumerName = consumerName;
      lastWait = wait;
      return Collections.emptyList();
    }

    @Override
    public boolean ack(QueueDetail q, RqueueMessage m) {
      return true;
    }

    @Override
    public boolean nack(QueueDetail q, RqueueMessage m, long retryDelayMs) {
      nackCalls++;
      lastNacked = m;
      lastRetryDelayMs = retryDelayMs;
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
      if (failSize) {
        throw new IllegalStateException("backend down");
      }
      return 42L;
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
  }
}
