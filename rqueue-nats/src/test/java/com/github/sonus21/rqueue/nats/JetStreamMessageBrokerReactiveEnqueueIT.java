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
  void enqueueWithDelayReactive_returnsUnsupportedOperationException() {
    QueueDetail q = mockQueue("rd-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      RqueueMessage m = RqueueMessage.builder().id("rm-delay").message("p").build();

      Mono<Void> mono = broker.enqueueWithDelayReactive(q, m, 1_000L);

      StepVerifier.create(mono).verifyError(UnsupportedOperationException.class);
    }
  }
}
