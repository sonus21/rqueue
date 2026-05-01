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

import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@NatsIntegrationTest
class JetStreamMessageBrokerPubSubIT extends AbstractJetStreamIT {

  @Test
  void publishSubscribe_handlerReceivesPayload() throws Exception {
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      ArrayBlockingQueue<String> received = new ArrayBlockingQueue<>(4);
      String channel = "test.channel." + System.nanoTime();
      try (AutoCloseable sub = broker.subscribe(channel, received::offer)) {
        // small wait so the dispatcher subscription is registered server-side
        Thread.sleep(100);
        broker.publish(channel, "hello");
        String msg = received.poll(2, TimeUnit.SECONDS);
        assertTrue(msg != null, "handler should receive a message");
        assertEquals("hello", msg);
      }
    }
  }
}
