/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.js;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Pure-Java coverage for the static resolvers that translate rqueue's per-queue settings
 * (visibilityTimeout, numRetry) into JetStream consumer config. These are the only place
 * where backend semantics are mapped, so a regression here silently changes redelivery
 * behaviour for every NATS-backed queue.
 */
@NatsUnitTest
class JetStreamMessageBrokerResolveTest {

  private static QueueDetail queue(long visibilityTimeoutMs, int numRetry) {
    QueueDetail q = mock(QueueDetail.class);
    when(q.getVisibilityTimeout()).thenReturn(visibilityTimeoutMs);
    when(q.getNumRetry()).thenReturn(numRetry);
    return q;
  }

  // ---- resolveAckWait ----------------------------------------------------

  @Test
  void resolveAckWait_usesVisibilityTimeoutWhenPositive() {
    QueueDetail q = queue(45_000L, 3);
    assertEquals(
        Duration.ofMillis(45_000L),
        JetStreamMessageBroker.resolveAckWait(q, RqueueNatsConfig.defaults()));
  }

  @Test
  void resolveAckWait_fallsBackToConfigDefaultWhenZero() {
    QueueDetail q = queue(0L, 3);
    assertEquals(
        Duration.ofSeconds(30),
        JetStreamMessageBroker.resolveAckWait(q, RqueueNatsConfig.defaults()));
  }

  @Test
  void resolveAckWait_fallsBackToConfigDefaultWhenNegative() {
    QueueDetail q = queue(-1L, 3);
    assertEquals(
        Duration.ofSeconds(30),
        JetStreamMessageBroker.resolveAckWait(q, RqueueNatsConfig.defaults()));
  }

  // ---- resolveMaxDeliver -------------------------------------------------

  @Test
  void resolveMaxDeliver_isNumRetryPlusOne() {
    QueueDetail q = queue(30_000L, 3);
    // numRetry=3 means 1 initial attempt + 3 retries = 4 deliveries total
    assertEquals(4L, JetStreamMessageBroker.resolveMaxDeliver(q, RqueueNatsConfig.defaults()));
  }

  @Test
  void resolveMaxDeliver_fallsBackToConfigDefaultWhenZero() {
    QueueDetail q = queue(30_000L, 0);
    // RqueueNatsConfig.defaults().consumerDefaults.maxDeliver = 3
    assertEquals(3L, JetStreamMessageBroker.resolveMaxDeliver(q, RqueueNatsConfig.defaults()));
  }

  @Test
  void resolveMaxDeliver_fallsBackToConfigDefaultWhenNegative() {
    QueueDetail q = queue(30_000L, -5);
    assertEquals(3L, JetStreamMessageBroker.resolveMaxDeliver(q, RqueueNatsConfig.defaults()));
  }

  /**
   * rqueue uses {@link Integer#MAX_VALUE} as the "retry forever" sentinel. JetStream's Java
   * client treats any non-positive {@code maxDeliver} as unset (omits the field from the
   * wire payload) which lets the server default to unlimited — so {@code -1} is the right
   * value to hand to the builder for that case.
   */
  @Test
  void resolveMaxDeliver_retryForeverSentinelMapsToUnlimited() {
    QueueDetail q = queue(30_000L, Integer.MAX_VALUE);
    assertEquals(-1L, JetStreamMessageBroker.resolveMaxDeliver(q, RqueueNatsConfig.defaults()));
  }
}
