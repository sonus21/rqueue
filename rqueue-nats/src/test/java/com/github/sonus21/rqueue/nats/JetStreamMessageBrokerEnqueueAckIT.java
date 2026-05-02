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
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

@NatsIntegrationTest
class JetStreamMessageBrokerEnqueueAckIT extends AbstractJetStreamIT {

  @Test
  void enqueuePopAck_drainsStream() throws Exception {
    QueueDetail q = mockQueue("eaq-" + System.nanoTime());
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults();
    cfg.getStreamDefaults().setRetention(io.nats.client.api.RetentionPolicy.WorkQueue);
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).config(cfg).build()) {
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
        List<RqueueMessage> popped = broker.pop(q, "worker", 4, Duration.ofSeconds(2));
        for (RqueueMessage m : popped) {
          assertTrue(broker.ack(q, m), "ack should succeed for " + m.getId());
          received++;
        }
      }
      assertEquals(10, received);
      // WorkQueue retention removes acked msgs from the stream, but JetStream processes ACKs
      // asynchronously: nm.ack() is fire-and-forget, so size() can briefly observe non-zero
      // until the server applies the deletion. Poll for drain instead of asserting strictly.
      long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      long size = broker.size(q);
      while (size != 0L && System.nanoTime() < deadlineNanos) {
        Thread.sleep(50L);
        size = broker.size(q);
      }
      assertEquals(0L, size);
    }
  }
}
