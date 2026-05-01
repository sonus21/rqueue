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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

@NatsIntegrationTest
class JetStreamMessageBrokerRetryDlqIT extends AbstractJetStreamIT {

  @Test
  void exhaustedMessage_landsOnDlqStream() throws Exception {
    String name = "rdq-" + System.nanoTime();
    QueueDetail q = mockQueue(name);
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults();
    cfg.getConsumerDefaults().setMaxDeliver(2);
    cfg.getConsumerDefaults().setAckWait(Duration.ofMillis(500));

    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).config(cfg).build()) {
      // Provision DLQ + advisory bridge
      broker.provisionDlq(q);
      try (AutoCloseable bridge = broker.installDeadLetterBridge(q, "worker")) {
        broker.enqueue(q, RqueueMessage.builder().id("retry-1").message("hi").build());

        // Pop and nak twice (maxDeliver=2 means after 2 deliveries it's exhausted)
        for (int i = 0; i < 3; i++) {
          List<RqueueMessage> popped = broker.pop(q, "worker", 1, Duration.ofSeconds(2));
          for (RqueueMessage m : popped) {
            broker.nack(q, m, 0L);
          }
          Thread.sleep(600);
        }

        // give the advisory bridge a moment to react
        long deadline = System.currentTimeMillis() + 5000;
        long dlqSize = 0;
        QueueDetail dlqProbe = mockQueue(name); // size of original stream
        // We don't expose dlq stream directly; verify via JSM stream lookup via a fresh probe
        while (System.currentTimeMillis() < deadline) {
          long s = connection
              .jetStreamManagement()
              .getStreamInfo(cfg.getStreamPrefix() + name + cfg.getDlqStreamSuffix())
              .getStreamState()
              .getMsgCount();
          if (s > 0) {
            dlqSize = s;
            break;
          }
          Thread.sleep(200);
        }
        assertTrue(dlqSize >= 1, "Expected at least 1 message on DLQ stream, got " + dlqSize);
      }
    }
  }
}
