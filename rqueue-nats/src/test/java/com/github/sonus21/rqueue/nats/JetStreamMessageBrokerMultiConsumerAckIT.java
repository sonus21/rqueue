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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import io.nats.client.api.ConsumerInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Regression test for the inFlight key-collision bug.
 *
 * <p>On a Limits-retention stream with two durable consumers (multi-listener fan-out), each
 * consumer receives its own NATS Message handle for every published message. The broker's
 * {@code inFlight} map was previously keyed only on {@code RqueueMessage.id}, so the second
 * consumer's pop overwrote the first's handle, and the first's later {@code ack} would target
 * the wrong NATS Message — leaving the original delivery stuck in {@code numAckPending} until
 * AckWait expired.
 *
 * <p>The test pops on both consumers <em>before</em> acking either, which is the only timing
 * that triggers the collision: a sequential drain-then-drain hides the bug because the inFlight
 * key is removed before the second pop populates it. After acking each consumer's deliveries,
 * the test asserts that <em>both</em> consumers reach {@code numAckPending == 0} and their ack
 * floors advance to the stream's last sequence.
 */
@NatsIntegrationTest
class JetStreamMessageBrokerMultiConsumerAckIT extends AbstractJetStreamIT {

  @Test
  void twoDurables_bothAckTheirOwnDeliveries() throws Exception {
    String name = "mca-" + System.nanoTime();
    QueueDetail enqueueFacet = mockQueue(name, QueueType.STREAM);
    QueueDetail qa = mockQueue(name, QueueType.STREAM, "consumer-a");
    QueueDetail qb = mockQueue(name, QueueType.STREAM, "consumer-b");
    int total = 6;

    RqueueNatsConfig cfg = RqueueNatsConfig.defaults();
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).config(cfg).build()) {
      for (int i = 0; i < total; i++) {
        broker.enqueue(
            enqueueFacet, RqueueMessage.builder().id("m-" + i).message("p" + i).build());
      }

      // Pop on BOTH consumers BEFORE acking either — this is the key to triggering the
      // collision: consumer-b's pop overwrites consumer-a's inFlight entry, so when
      // consumer-a later calls ack, the buggy implementation reaches for consumer-b's
      // NATS Message handle. Acking sequentially (drain-then-drain) hides the bug
      // because the inFlight key is removed before the second pop populates it.
      List<RqueueMessage> aPopped = pop(broker, qa, "consumer-a", total);
      List<RqueueMessage> bPopped = pop(broker, qb, "consumer-b", total);

      assertEquals(total, aPopped.size(), "consumer-a should see every published message");
      assertEquals(total, bPopped.size(), "consumer-b should see every published message");

      // Ack both — under the buggy implementation, consumer-a's ack resolves to consumer-b's
      // NATS Message and acks that one; consumer-a's original delivery stays stuck in
      // numAckPending and consumer-b's ack returns false (entry already removed by a).
      for (RqueueMessage m : aPopped) {
        assertTrue(broker.ack(qa, m), "ack(consumer-a, " + m.getId() + ") must succeed");
      }
      for (RqueueMessage m : bPopped) {
        assertTrue(broker.ack(qb, m), "ack(consumer-b, " + m.getId() + ") must succeed");
      }

      String stream = cfg.getStreamPrefix() + name;
      // Acks are async — the server applies them after the broker returns. Poll for drain.
      ConsumerInfo aInfo = waitForAckPendingZero(stream, "consumer-a");
      ConsumerInfo bInfo = waitForAckPendingZero(stream, "consumer-b");

      assertEquals(
          0L,
          aInfo.getNumAckPending(),
          "consumer-a numAckPending must drain to 0; was " + aInfo.getNumAckPending()
              + " — indicates ack went to the wrong NATS handle");
      assertEquals(
          0L,
          bInfo.getNumAckPending(),
          "consumer-b numAckPending must drain to 0; was " + bInfo.getNumAckPending());

      long lastSeq = connection
          .jetStreamManagement()
          .getStreamInfo(stream)
          .getStreamState()
          .getLastSequence();
      assertTrue(
          aInfo.getAckFloor().getStreamSequence() >= lastSeq,
          "consumer-a ackFloor should reach lastSeq=" + lastSeq + " but was "
              + aInfo.getAckFloor().getStreamSequence());
      assertTrue(
          bInfo.getAckFloor().getStreamSequence() >= lastSeq,
          "consumer-b ackFloor should reach lastSeq=" + lastSeq + " but was "
              + bInfo.getAckFloor().getStreamSequence());
    }
  }

  private List<RqueueMessage> pop(
      JetStreamMessageBroker broker, QueueDetail q, String consumer, int expected)
      throws Exception {
    List<RqueueMessage> all = new ArrayList<>(expected);
    long deadline = System.currentTimeMillis() + 5000;
    while (all.size() < expected && System.currentTimeMillis() < deadline) {
      List<RqueueMessage> batch = broker.pop(q, consumer, expected, Duration.ofMillis(500));
      all.addAll(batch);
    }
    return all;
  }

  private ConsumerInfo waitForAckPendingZero(String stream, String consumer) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    ConsumerInfo last = connection.jetStreamManagement().getConsumerInfo(stream, consumer);
    while (last.getNumAckPending() > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(50L);
      last = connection.jetStreamManagement().getConsumerInfo(stream, consumer);
    }
    return last;
  }
}
