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
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

@NatsIntegrationTest
class JetStreamMessageBrokerCompetingConsumersIT extends AbstractJetStreamIT {

  @Test
  void twoWorkersSharingDurable_eachMessageDeliveredOnce() throws Exception {
    QueueDetail q = mockQueue(
        "ccq-" + System.nanoTime(), com.github.sonus21.rqueue.enums.QueueType.QUEUE, "shared");
    int total = 20;
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      for (int i = 0; i < total; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("m-" + i).message("p" + i).build());
      }
      Set<String> seen = ConcurrentHashMap.newKeySet();
      CountDownLatch done = new CountDownLatch(total);
      var pool = Executors.newFixedThreadPool(2);
      for (int t = 0; t < 2; t++) {
        pool.submit(() -> {
          for (int round = 0; round < 50 && done.getCount() > 0; round++) {
            List<RqueueMessage> popped = broker.pop(q, "shared", 5, Duration.ofMillis(500));
            for (RqueueMessage m : popped) {
              if (seen.add(m.getId())) {
                done.countDown();
              }
              broker.ack(q, m);
            }
          }
        });
      }
      done.await(20, java.util.concurrent.TimeUnit.SECONDS);
      pool.shutdownNow();
      assertEquals(total, seen.size(), "every message should be seen exactly once across workers");
    }
  }
}
