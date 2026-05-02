/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * End-to-end producer-only smoke test: the broker enqueues messages but never pops or acks them.
 * Covers plain enqueue, priority enqueue, and reactive enqueue — verifying that all variants land
 * in JetStream and are reflected by {@link JetStreamMessageBroker#size}.
 */
@NatsIntegrationTest
class JetStreamMessageBrokerProducerOnlyIT extends AbstractJetStreamIT {

  @Test
  void enqueue_messagesAccumulateInStream() throws Exception {
    QueueDetail q = mockQueue("po-plain-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      int count = 10;
      for (int i = 0; i < count; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("m-" + i).message("payload-" + i).build());
      }
      assertEquals(count, broker.size(q), "all enqueued messages should be visible in the stream");
    }
  }

  @Test
  void enqueueWithPriority_messagesAccumulateInPriorityStreams() throws Exception {
    QueueDetail q = mockQueue("po-prio-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      String[] priorities = {"high", "low", "critical"};
      int perPriority = 5;
      for (String priority : priorities) {
        for (int i = 0; i < perPriority; i++) {
          broker.enqueue(
              q,
              priority,
              RqueueMessage.builder()
                  .id(priority + "-m-" + i)
                  .message("payload-" + i)
                  .build());
        }
      }
      // Each priority maps to its own JetStream stream; verify each independently.
      // subjectFor(q, priority) = prefix + q.getName() + "_" + priority, so size(pq) where
      // pq.getName() = q.getName() + "_" + priority resolves to the same stream.
      for (String priority : priorities) {
        QueueDetail pq = mockQueue(q.getName() + "_" + priority);
        assertEquals(
            perPriority,
            broker.size(pq),
            "priority=" + priority + " stream should hold " + perPriority + " messages");
      }
    }
  }

  @Test
  void enqueueReactive_messagesAccumulateInStream() {
    QueueDetail q = mockQueue("po-reactive-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      int count = 8;
      Flux<Void> publishes = Flux.range(0, count).flatMap(i ->
          broker.enqueueReactive(
              q, RqueueMessage.builder().id("rm-" + i).message("reactive-payload-" + i).build()));

      StepVerifier.create(publishes).verifyComplete();

      assertEquals(count, broker.size(q), "all reactively enqueued messages should be in the stream");
    }
  }

  @Test
  void mixedEnqueue_allVariantsLandInCorrectStreams() {
    String base = "po-mixed-" + System.nanoTime();
    QueueDetail mainQ = mockQueue(base);
    QueueDetail highQ = mockQueue(base + "_high");

    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {

      // 3 plain messages on the main queue
      for (int i = 0; i < 3; i++) {
        broker.enqueue(mainQ, RqueueMessage.builder().id("plain-" + i).message("p" + i).build());
      }
      // 2 priority messages on the "high" sub-queue
      for (int i = 0; i < 2; i++) {
        broker.enqueue(
            mainQ,
            "high",
            RqueueMessage.builder().id("high-" + i).message("h" + i).build());
      }
      // 1 reactive message on the main queue
      StepVerifier.create(
              broker.enqueueReactive(
                  mainQ, RqueueMessage.builder().id("react-0").message("r0").build()))
          .verifyComplete();

      assertEquals(4L, broker.size(mainQ), "main stream: 3 plain + 1 reactive");
      assertEquals(2L, broker.size(highQ), "high-priority stream: 2 messages");
    }
  }
}
