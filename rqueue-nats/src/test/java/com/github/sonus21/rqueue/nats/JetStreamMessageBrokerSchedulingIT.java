/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Integration tests for NATS message scheduling via the {@code Nats-Next-Deliver-Time} header
 * (ADR-51, NATS >= 2.12). Requires the container image declared in {@link AbstractJetStreamIT}
 * to be a 2.12+ server; tests self-skip via {@link org.junit.jupiter.api.Assumptions#assumeTrue}
 * when run against an older externally-managed NATS.
 */
@NatsIntegrationTest
class JetStreamMessageBrokerSchedulingIT extends AbstractJetStreamIT {

  private static final long DELAY_MS = 3_000L;
  private static final long BUFFER_MS = 2_000L;

  @Test
  void enqueueWithDelay_messageIsHeldThenDelivered() throws Exception {
    QueueDetail q = mockQueue("sched-" + System.nanoTime(), QueueType.QUEUE, "sched-worker");
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      assumeTrue(broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION + ")");

      RqueueMessage m = RqueueMessage.builder()
          .id("sched-1")
          .message("scheduled-hello")
          .processAt(System.currentTimeMillis() + DELAY_MS)
          .build();
      broker.enqueueWithDelay(q, m, DELAY_MS);

      // Message must not arrive before the scheduled time
      List<RqueueMessage> before = broker.pop(q, "sched-worker", 5, Duration.ofMillis(500));
      assertTrue(before.isEmpty(),
          "Message must be held by JetStream until the Nats-Next-Deliver-Time");

      Thread.sleep(DELAY_MS + BUFFER_MS);

      List<RqueueMessage> after = broker.pop(q, "sched-worker", 5, Duration.ofSeconds(3));
      assertEquals(1, after.size(), "Message must be delivered after the scheduled time");
      assertEquals("scheduled-hello", after.get(0).getMessage());
      broker.ack(q, after.get(0));
    }
  }

  @Test
  void enqueueWithDelayReactive_messageIsHeldThenDelivered() throws Exception {
    QueueDetail q = mockQueue("rsched-" + System.nanoTime(), QueueType.QUEUE, "rsched-worker");
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      assumeTrue(broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION + ")");

      RqueueMessage m = RqueueMessage.builder()
          .id("rsched-1")
          .message("reactive-scheduled")
          .processAt(System.currentTimeMillis() + DELAY_MS)
          .build();

      Mono<Void> publish = broker.enqueueWithDelayReactive(q, m, DELAY_MS);
      StepVerifier.create(publish).verifyComplete();

      List<RqueueMessage> before = broker.pop(q, "rsched-worker", 5, Duration.ofMillis(500));
      assertTrue(before.isEmpty(),
          "Message must be held by JetStream until the Nats-Next-Deliver-Time");

      Thread.sleep(DELAY_MS + BUFFER_MS);

      List<RqueueMessage> after = broker.pop(q, "rsched-worker", 5, Duration.ofSeconds(3));
      assertEquals(1, after.size(), "Message must be delivered after the scheduled time");
      assertEquals("reactive-scheduled", after.get(0).getMessage());
      broker.ack(q, after.get(0));
    }
  }

  @Test
  void enqueueWithDelay_multipleMessages_allDeliveredAfterTheirTimes() throws Exception {
    QueueDetail q = mockQueue("msched-" + System.nanoTime(), QueueType.QUEUE, "msched-worker");
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      assumeTrue(broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION + ")");

      // Three messages with the same delay — all arrive in the same window
      long now = System.currentTimeMillis();
      for (int i = 0; i < 3; i++) {
        RqueueMessage m = RqueueMessage.builder()
            .id("msched-" + i)
            .message("batch-" + i)
            .processAt(now + DELAY_MS)
            .build();
        broker.enqueueWithDelay(q, m, DELAY_MS);
      }

      // Nothing visible before delay
      List<RqueueMessage> before = broker.pop(q, "msched-worker", 10, Duration.ofMillis(500));
      assertTrue(before.isEmpty(), "No messages should arrive before the scheduled time");

      Thread.sleep(DELAY_MS + BUFFER_MS);

      // All three visible after delay
      List<RqueueMessage> after = new java.util.ArrayList<>();
      for (int round = 0; round < 5 && after.size() < 3; round++) {
        after.addAll(broker.pop(q, "msched-worker", 3, Duration.ofSeconds(2)));
      }
      assertEquals(3, after.size(), "All 3 scheduled messages must be delivered after the delay");
      for (RqueueMessage msg : after) {
        broker.ack(q, msg);
      }
    }
  }
}
