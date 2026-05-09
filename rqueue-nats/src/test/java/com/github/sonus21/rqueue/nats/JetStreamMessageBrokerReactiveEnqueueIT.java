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

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@NatsIntegrationTest
class JetStreamMessageBrokerReactiveEnqueueIT extends AbstractJetStreamIT {

  @Test
  void enqueueReactive_publishesAllMessages() {
    QueueDetail q = mockQueue("re-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {

      Flux<Void> publishes = Flux.range(0, 5).flatMap(i -> {
        RqueueMessage m =
            RqueueMessage.builder().id("rm-" + i).message("payload-" + i).build();
        return broker.enqueueReactive(q, m);
      });

      StepVerifier.create(publishes).verifyComplete();

      assertEquals(5L, broker.size(q));
    }
  }

  @Test
  void enqueueWithDelayReactive_publishesAndMessageIsDeliveredAfterDelay() throws Exception {
    QueueDetail q = mockQueue(
        "rd-" + System.nanoTime(), com.github.sonus21.rqueue.enums.QueueType.QUEUE, "rd-worker");
    long delayMs = 3_000L;
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      org.junit.jupiter.api.Assumptions.assumeTrue(
          broker.capabilities().supportsDelayedEnqueue(),
          "Skipping: connected NATS server does not support message scheduling (< 2.12)");

      RqueueMessage m = RqueueMessage.builder()
          .id("rm-delay")
          .message("delayed-p")
          .processAt(System.currentTimeMillis() + delayMs)
          .build();
      Mono<Void> mono = broker.enqueueWithDelayReactive(q, m, delayMs);
      StepVerifier.create(mono).verifyComplete();

      // Message must not arrive before the delay expires
      java.util.List<RqueueMessage> before =
          broker.pop(q, "rd-worker", 5, java.time.Duration.ofMillis(500));
      assertTrue(before.isEmpty(), "Message must be held until delivery time");

      Thread.sleep(delayMs + 1_500L);

      java.util.List<RqueueMessage> after =
          broker.pop(q, "rd-worker", 5, java.time.Duration.ofSeconds(3));
      assertEquals(1, after.size(), "Message must arrive after delay");
      assertEquals("delayed-p", after.get(0).getMessage());
      broker.ack(q, after.get(0));
    }
  }
}
