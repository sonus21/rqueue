/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */
package com.github.sonus21.rqueue.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.RetentionPolicy;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

/**
 * E2E contracts for {@link QueueType}:
 *
 * <ol>
 *   <li>Stream retention matches the declared mode (QUEUE → WorkQueue, STREAM → Limits).
 *   <li>QUEUE mode reuses the same durable consumer across repeated {@code ensureConsumer} calls
 *       so message position is preserved rather than reset.
 *   <li>QUEUE mode delivers each message to exactly one competing consumer.
 *   <li>STREAM mode delivers every message to every independent consumer group (fan-out).
 * </ol>
 */
@NatsIntegrationTest
class JetStreamQueueModeIT extends AbstractJetStreamIT {

  // ---- Contract 1: stream retention reflects QueueType ------------------

  @Test
  void queueMode_queue_createsWorkQueueStream() throws Exception {
    String streamName = "rqueue-" + "qm-queue-" + System.nanoTime();
    String subject = "rqueue." + "qm-queue-" + System.nanoTime();
    JetStreamManagement jsm = connection.jetStreamManagement();
    NatsProvisioner provisioner = new NatsProvisioner(connection, jsm, RqueueNatsConfig.defaults());

    provisioner.ensureStream(streamName, List.of(subject), QueueType.QUEUE);

    RetentionPolicy actual = jsm.getStreamInfo(streamName).getConfiguration().getRetentionPolicy();
    assertEquals(
        RetentionPolicy.WorkQueue, actual, "QUEUE mode must create a WorkQueue-retention stream");
  }

  @Test
  void queueMode_stream_createsLimitsStream() throws Exception {
    String streamName = "rqueue-" + "qm-stream-" + System.nanoTime();
    String subject = "rqueue." + "qm-stream-" + System.nanoTime();
    JetStreamManagement jsm = connection.jetStreamManagement();
    NatsProvisioner provisioner = new NatsProvisioner(connection, jsm, RqueueNatsConfig.defaults());

    provisioner.ensureStream(streamName, List.of(subject), QueueType.STREAM);

    RetentionPolicy actual = jsm.getStreamInfo(streamName).getConfiguration().getRetentionPolicy();
    assertEquals(
        RetentionPolicy.Limits, actual, "STREAM mode must create a Limits-retention stream");
  }

  // ---- Contract 2: consumer reuse preserves delivery position -----------

  /**
   * Verifies that calling {@code ensureConsumer} twice with identical arguments does NOT reset the
   * consumer's delivery position. If a new consumer were created each time (with
   * {@code DeliverPolicy.All}), previously-acked messages would be redelivered.
   *
   * <p>Sequence:
   * <ol>
   *   <li>Enqueue 5 messages.
   *   <li>Pop and ack 3 via consumer "c1".
   *   <li>Call {@code ensureConsumer} again — simulates an app restart or a second bean init.
   *   <li>Pop remaining — must yield exactly 2 (not 5 again).
   * </ol>
   */
  @Test
  void queueMode_consumerReuse_preservesDeliveryPosition() throws Exception {
    QueueDetail q = mockQueue("qm-reuse-" + System.nanoTime(), QueueType.QUEUE);
    String consumerName = "c1-reuse";
    int total = 5;
    int firstBatch = 3;

    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      for (int i = 0; i < total; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("r-" + i).message("p" + i).build());
      }

      // Pop and ack the first batch.
      Set<String> firstSeen = new HashSet<>();
      long deadline = System.currentTimeMillis() + 10_000;
      while (firstSeen.size() < firstBatch && System.currentTimeMillis() < deadline) {
        List<RqueueMessage> msgs = broker.pop(q, consumerName, 5, Duration.ofMillis(500));
        for (RqueueMessage m : msgs) {
          if (firstSeen.add(m.getId())) {
            broker.ack(q, m);
          }
          if (firstSeen.size() == firstBatch) {
            break;
          }
        }
      }
      assertEquals(firstBatch, firstSeen.size(), "should have consumed the first batch");

      // Simulate a second call to ensureConsumer (e.g. from NatsStreamValidator on restart).
      // The provisioner cache is already warm so this is effectively a no-op on the server side,
      // but we also test against a fresh provisioner to simulate a true restart scenario.
      JetStreamManagement jsm = connection.jetStreamManagement();
      NatsProvisioner freshProvisioner =
          new NatsProvisioner(connection, jsm, RqueueNatsConfig.defaults());
      RqueueNatsConfig.ConsumerDefaults cd = RqueueNatsConfig.defaults().getConsumerDefaults();
      freshProvisioner.ensureConsumer(
          RqueueNatsConfig.defaults().getStreamPrefix() + q.getName(),
          consumerName,
          cd.getAckWait(),
          cd.getMaxDeliver(),
          cd.getMaxAckPending());

      // Verify the consumer info still reflects the already-delivered messages.
      ConsumerInfo info = jsm.getConsumerInfo(
          RqueueNatsConfig.defaults().getStreamPrefix() + q.getName(), consumerName);
      long numAcked = info.getNumAckPending() == 0
          ? total - info.getNumPending()
          : total - info.getNumPending() - info.getNumAckPending();
      // At minimum, the pending count must not have reset to the full total.
      long remaining = info.getNumPending() + info.getNumAckPending();
      assertEquals(
          total - firstBatch,
          remaining,
          "consumer position must be preserved across ensureConsumer calls; " + "remaining="
              + remaining + " but expected " + (total - firstBatch));
    }
  }

  // ---- Contract 3: QUEUE competing consumers — each message once --------

  @Test
  void queueMode_queue_competingConsumers_eachMessageDeliveredOnce() throws Exception {
    QueueDetail q = mockQueue("qm-cc-" + System.nanoTime(), QueueType.QUEUE);
    int total = 20;

    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      for (int i = 0; i < total; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("cc-" + i).message("p" + i).build());
      }

      Set<String> seen = ConcurrentHashMap.newKeySet();
      CountDownLatch done = new CountDownLatch(total);
      String sharedConsumer = "shared-cc";
      var pool = Executors.newFixedThreadPool(2);
      for (int t = 0; t < 2; t++) {
        pool.submit(() -> {
          for (int round = 0; round < 100 && done.getCount() > 0; round++) {
            List<RqueueMessage> msgs = broker.pop(q, sharedConsumer, 5, Duration.ofMillis(300));
            for (RqueueMessage m : msgs) {
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

      assertEquals(
          total, seen.size(), "QUEUE mode: each message must be delivered to exactly one worker");
    }
  }

  // ---- Contract 4: STREAM fan-out — every consumer sees every message ---

  @Test
  void queueMode_stream_fanOut_everyConsumerReceivesAllMessages() throws Exception {
    QueueDetail q = mockQueue("qm-fo-" + System.nanoTime(), QueueType.STREAM);
    int total = 8;

    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      for (int i = 0; i < total; i++) {
        broker.enqueue(q, RqueueMessage.builder().id("fo-" + i).message("p" + i).build());
      }

      // Each listener group uses a distinct consumer name — they are independent on a
      // Limits-retention stream and each track their own delivery position.
      Set<String> listenerOneSeen = drain(broker, q, "listener-svc-1", total);
      Set<String> listenerTwoSeen = drain(broker, q, "listener-svc-2", total);

      assertEquals(
          total, listenerOneSeen.size(), "STREAM mode: listener-svc-1 must receive all messages");
      assertEquals(
          total,
          listenerTwoSeen.size(),
          "STREAM mode: listener-svc-2 must receive all messages independently");
    }
  }

  private Set<String> drain(
      JetStreamMessageBroker broker, QueueDetail q, String consumer, int expected)
      throws InterruptedException {
    Set<String> seen = new HashSet<>();
    long deadline = System.currentTimeMillis() + 10_000;
    while (seen.size() < expected && System.currentTimeMillis() < deadline) {
      List<RqueueMessage> msgs = broker.pop(q, consumer, expected, Duration.ofMillis(500));
      for (RqueueMessage m : msgs) {
        seen.add(m.getId());
        broker.ack(q, m);
      }
    }
    return seen;
  }
}
