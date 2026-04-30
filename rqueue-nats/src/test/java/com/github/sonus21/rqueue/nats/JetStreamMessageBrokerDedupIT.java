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
import org.junit.jupiter.api.Test;

class JetStreamMessageBrokerDedupIT extends AbstractJetStreamIT {

  @Test
  void duplicateMsgIdInsideWindow_isDeduped() throws Exception {
    QueueDetail q = mockQueue("ddq-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      RqueueMessage m1 = RqueueMessage.builder().id("dup-1").message("a").build();
      RqueueMessage m2 = RqueueMessage.builder().id("dup-1").message("b").build();
      broker.enqueue(q, m1);
      broker.enqueue(q, m2);
      assertEquals(1L, broker.size(q));
    }
  }
}
