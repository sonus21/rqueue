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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JetStreamMessageBrokerEnqueueAckIT extends AbstractJetStreamIT {

  @Test
  void enqueuePopAck_drainsStream() throws Exception {
    QueueDetail q = mockQueue("eaq-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      List<RqueueMessage> sent = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        RqueueMessage m =
            RqueueMessage.builder().id("m-" + i).message("payload-" + i).build();
        broker.enqueue(q, m);
        sent.add(m);
      }
      assertEquals(10L, broker.size(q));

      int received = 0;
      for (int round = 0; round < 5 && received < 10; round++) {
        List<RqueueMessage> popped =
            broker.pop(q, "worker", 4, Duration.ofSeconds(2));
        for (RqueueMessage m : popped) {
          assertTrue(broker.ack(q, m), "ack should succeed for " + m.getId());
          received++;
        }
      }
      assertEquals(10, received);
      // WorkQueue retention removes acked msgs from stream
      assertEquals(0L, broker.size(q));
    }
  }
}
