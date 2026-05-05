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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Covers {@link com.github.sonus21.rqueue.core.spi.MessageBroker#peek(QueueDetail, String, long,
 * long)} on a Limits-retention stream with two durable consumers at different progress levels.
 *
 * <p>The dashboard explorer wires the {@code consumerName} so each row's "browse" action shows
 * messages still outstanding for that specific subscriber instead of the entire retained
 * window. This test asserts that contract:
 *
 * <ul>
 *   <li>consumer-fast pops + acks the first half of the stream, advancing its {@code ackFloor}.
 *   <li>consumer-slow does nothing, so its {@code ackFloor} stays at 0.
 *   <li>{@code peek(q, "consumer-fast", 0, total)} returns only the second half (skips acked).
 *   <li>{@code peek(q, "consumer-slow", 0, total)} returns the whole stream.
 *   <li>{@code peek(q, null, 0, total)} also returns the whole stream — the no-consumer
 *       overload bases on the stream's first sequence and is unchanged by per-consumer state.
 * </ul>
 */
@NatsIntegrationTest
class JetStreamMessageBrokerConsumerAwarePeekIT extends AbstractJetStreamIT {

  @Test
  void peek_skipsAlreadyAckedRangeForSpecificConsumer() throws Exception {
    String name = "cap-" + System.nanoTime();
    QueueDetail enqueueFacet = mockQueue(name, QueueType.STREAM);
    QueueDetail qFast = mockQueue(name, QueueType.STREAM, "consumer-fast");
    QueueDetail qSlow = mockQueue(name, QueueType.STREAM, "consumer-slow");
    int total = 8;
    int firstHalf = total / 2;

    RqueueNatsConfig cfg = RqueueNatsConfig.defaults();
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).config(cfg).build()) {
      // Enqueue first to create the stream — pop's ensureConsumer needs the stream to exist.
      // DeliverPolicy.All on the consumer means it starts at the stream's first sequence, so
      // both durables created after the publish still see every message.
      for (int i = 0; i < total; i++) {
        broker.enqueue(
            enqueueFacet, RqueueMessage.builder().id("m-" + i).message("p" + i).build());
      }

      // Drain the first half on consumer-fast — pop + ack so its ackFloor advances.
      Set<String> fastSeen = new HashSet<>();
      long deadline = System.currentTimeMillis() + 5000;
      while (fastSeen.size() < firstHalf && System.currentTimeMillis() < deadline) {
        List<RqueueMessage> batch =
            broker.pop(qFast, "consumer-fast", firstHalf - fastSeen.size(), Duration.ofMillis(500));
        for (RqueueMessage m : batch) {
          if (fastSeen.add(m.getId())) {
            assertTrue(broker.ack(qFast, m), "ack must succeed for " + m.getId());
          }
        }
      }
      assertEquals(firstHalf, fastSeen.size(), "consumer-fast should have drained first half");
      // Wait for the server to apply the acks before peeking — getConsumerInfo's ackFloor is
      // observed asynchronously after nm.ack().
      waitForAckFloorAtLeast(cfg.getStreamPrefix() + name, "consumer-fast", firstHalf);

      // Per-consumer peek for consumer-fast must SKIP the acked range and return only the
      // second half (msgs whose stream seq > ackFloor).
      List<RqueueMessage> fastPeek = broker.peek(qFast, "consumer-fast", 0, total);
      Set<String> fastIds = fastPeek.stream().map(RqueueMessage::getId).collect(Collectors.toSet());
      assertEquals(
          total - firstHalf,
          fastPeek.size(),
          "consumer-fast peek should return only the un-acked tail; got " + fastIds);
      for (String acked : fastSeen) {
        assertFalse(
            fastIds.contains(acked),
            "consumer-fast peek must not include already-acked id=" + acked);
      }

      // Per-consumer peek for consumer-slow should still see every message — its ackFloor is 0.
      List<RqueueMessage> slowPeek = broker.peek(qSlow, "consumer-slow", 0, total);
      assertEquals(
          total,
          slowPeek.size(),
          "consumer-slow peek should return the full stream — its ackFloor hasn't advanced");

      // No-consumer peek behaves identically regardless of per-consumer progress — bases on
      // the stream's first sequence (this is the legacy 2-arg overload's contract).
      List<RqueueMessage> globalPeek = broker.peek(qFast, 0, total);
      assertEquals(
          total,
          globalPeek.size(),
          "global (no-consumer) peek should ignore per-consumer ackFloor");
    }
  }

  private void waitForAckFloorAtLeast(String stream, String consumer, long minStreamSeq)
      throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      io.nats.client.api.ConsumerInfo ci =
          connection.jetStreamManagement().getConsumerInfo(stream, consumer);
      if (ci != null
          && ci.getAckFloor() != null
          && ci.getAckFloor().getStreamSequence() >= minStreamSeq) {
        return;
      }
      Thread.sleep(50L);
    }
    throw new AssertionError(
        "Timed out waiting for " + consumer + " ackFloor to reach streamSeq " + minStreamSeq);
  }
}
