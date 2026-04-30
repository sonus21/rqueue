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
import io.nats.client.api.ConsumerInfo;
import java.util.List;
import org.junit.jupiter.api.Test;

@NatsIntegrationTest
class JetStreamMessageBrokerPeekIT extends AbstractJetStreamIT {

  @Test
  void peek_doesNotPerturbDurableConsumerAckPending() throws Exception {
    QueueDetail q = mockQueue("pkq-" + System.nanoTime());
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults();
    cfg.getStreamDefaults().setRetention(io.nats.client.api.RetentionPolicy.Limits);
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).config(cfg).build()) {
      for (int i = 1; i <= 5; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("m-" + i).message("p" + i).build());
      }
      // create durable consumer first so we can compare ack-pending before/after peek
      broker.pop(q, "worker", 0, java.time.Duration.ofMillis(50)); // ensures consumer exists
      String stream = cfg.getStreamPrefix() + q.getName();
      ConsumerInfo before = connection.jetStreamManagement().getConsumerInfo(stream, "worker");

      List<RqueueMessage> peeked = broker.peek(q, 2, 3);
      assertEquals(3, peeked.size(), "expected 3 messages starting at offset 2");

      ConsumerInfo after = connection.jetStreamManagement().getConsumerInfo(stream, "worker");
      assertEquals(
          before.getNumAckPending(),
          after.getNumAckPending(),
          "peek must not affect durable consumer's ack-pending count");
    }
  }
}
