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
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

@NatsIntegrationTest
class JetStreamMessageBrokerIndependentConsumersIT extends AbstractJetStreamIT {

  @Test
  void twoDurables_eachReceiveAllMessages() throws Exception {
    QueueDetail q = mockQueue("icq-" + System.nanoTime(), QueueType.STREAM);
    int total = 5;
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      for (int i = 0; i < total; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("m-" + i).message("p" + i).build());
      }

      Set<String> aSeen = new HashSet<>();
      Set<String> bSeen = new HashSet<>();
      drainInto(broker, q, "consumer-a", aSeen);
      drainInto(broker, q, "consumer-b", bSeen);

      assertEquals(total, aSeen.size());
      assertEquals(total, bSeen.size());
    }
  }

  private void drainInto(
      JetStreamMessageBroker broker, QueueDetail q, String consumer, Set<String> sink)
      throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline && sink.size() < 5) {
      List<RqueueMessage> popped = broker.pop(q, consumer, 5, Duration.ofMillis(500));
      for (RqueueMessage m : popped) {
        sink.add(m.getId());
        broker.ack(q, m);
      }
    }
  }
}
