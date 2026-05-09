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
      assumeTrue(
          broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION
              + ")");

      RqueueMessage m = RqueueMessage.builder()
          .id("sched-1")
          .message("scheduled-hello")
          .processAt(System.currentTimeMillis() + DELAY_MS)
          .build();
      broker.enqueueWithDelay(q, m, DELAY_MS);

      // Message must not arrive before the scheduled time
      List<RqueueMessage> before = broker.pop(q, "sched-worker", 5, Duration.ofMillis(500));
      assertTrue(
          before.isEmpty(), "Message must be held by JetStream until the Nats-Next-Deliver-Time");

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
      assumeTrue(
          broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION
              + ")");

      RqueueMessage m = RqueueMessage.builder()
          .id("rsched-1")
          .message("reactive-scheduled")
          .processAt(System.currentTimeMillis() + DELAY_MS)
          .build();

      Mono<Void> publish = broker.enqueueWithDelayReactive(q, m, DELAY_MS);
      StepVerifier.create(publish).verifyComplete();

      List<RqueueMessage> before = broker.pop(q, "rsched-worker", 5, Duration.ofMillis(500));
      assertTrue(
          before.isEmpty(), "Message must be held by JetStream until the Nats-Next-Deliver-Time");

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
      assumeTrue(
          broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION
              + ")");

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

  /**
   * Regression test for the "enqueue-first then enqueueWithDelay" ordering bug.
   *
   * <p>If a plain {@code enqueue()} call happens before the first {@code enqueueWithDelay()}, the
   * stream is created with only the work subject and no {@code allow_msg_schedules} flag. The
   * provisioner must detect this on the delayed call and upgrade the stream in-place: add the
   * sched-wildcard subject AND set the flag in a single {@code updateStream()}, otherwise NATS
   * rejects the sched-subject publish with "no stream matches subject".
   */
  /**
   * Regression test for the "enqueue-first then enqueueWithDelay" ordering bug.
   *
   * <p>Uses a longer delay (10 s) than the other tests to absorb the stream-upgrade overhead
   * (updateStream round-trip + consumer creation) that happens in this path but not when the
   * stream is provisioned with scheduling from the start. Without the extra headroom the
   * "beforeDelay" check can race with the scheduler firing.
   */
  @Test
  void enqueueWithDelay_afterPlainEnqueue_streamUpgradedAndMessageHeld() throws Exception {
    final long UPGRADE_DELAY_MS = 10_000L;
    final long UPGRADE_BUFFER_MS = 3_000L;
    QueueDetail q = mockQueue("mixed-" + System.nanoTime(), QueueType.QUEUE, "mixed-worker");
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      assumeTrue(
          broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< "
              + com.github.sonus21.rqueue.nats.internal.NatsProvisioner.SCHEDULING_MIN_VERSION
              + ")");

      // Step 1 — plain enqueue: creates stream with only the work subject, no sched flag
      RqueueMessage immediate =
          RqueueMessage.builder().id("imm-1").message("immediate").build();
      broker.enqueue(q, immediate);

      // Step 2 — delayed enqueue: must upgrade the stream (add sched wildcard + flag)
      RqueueMessage delayed = RqueueMessage.builder()
          .id("del-1")
          .message("delayed")
          .processAt(System.currentTimeMillis() + UPGRADE_DELAY_MS)
          .build();
      broker.enqueueWithDelay(q, delayed, UPGRADE_DELAY_MS);

      // The immediate message must be available right away
      List<RqueueMessage> nowMsgs = broker.pop(q, "mixed-worker", 5, Duration.ofSeconds(2));
      assertEquals(1, nowMsgs.size(), "Immediate message must be delivered right away");
      assertEquals("immediate", nowMsgs.get(0).getMessage());
      broker.ack(q, nowMsgs.get(0));

      // The delayed message must NOT be visible yet (10 s away — stream upgrade takes < 1 s)
      List<RqueueMessage> beforeDelay = broker.pop(q, "mixed-worker", 5, Duration.ofMillis(500));
      assertTrue(beforeDelay.isEmpty(), "Delayed message must not arrive before scheduled time");

      Thread.sleep(UPGRADE_DELAY_MS + UPGRADE_BUFFER_MS);

      // After the delay the scheduled message must arrive
      List<RqueueMessage> afterDelay = broker.pop(q, "mixed-worker", 5, Duration.ofSeconds(3));
      assertEquals(1, afterDelay.size(), "Delayed message must be delivered after scheduled time");
      assertEquals("delayed", afterDelay.get(0).getMessage());
      broker.ack(q, afterDelay.get(0));
    }
  }
}
