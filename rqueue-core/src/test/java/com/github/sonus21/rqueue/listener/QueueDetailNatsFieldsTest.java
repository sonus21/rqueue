/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.models.Concurrency;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class QueueDetailNatsFieldsTest extends TestBase {

  private static QueueDetail.QueueDetailBuilder baseBuilder() {
    return QueueDetail.builder()
        .name("orders")
        .queueName("__rq::queue::orders")
        .processingQueueName("__rq::p-queue::orders")
        .processingQueueChannelName("__rq::p-channel::orders")
        .scheduledQueueName("__rq::d-queue::orders")
        .scheduledQueueChannelName("__rq::d-channel::orders")
        .completedQueueName("__rq::c-queue::orders")
        .numRetry(3)
        .visibilityTimeout(900_000L)
        .active(true)
        .concurrency(new Concurrency(1, 1))
        .priority(Collections.emptyMap());
  }

  @Test
  void natsFieldsDefaultToNullAndDeriveSensibly() {
    QueueDetail q = baseBuilder().build();

    assertNull(q.getNatsStream());
    assertNull(q.getNatsSubject());
    assertNull(q.getNatsDlqStream());
    assertNull(q.getNatsDlqSubject());
    assertNull(q.getNatsAckWaitOverride());
    assertNull(q.getNatsMaxDeliverOverride());
    assertNull(q.getNatsDedupWindow());

    assertEquals("rqueue-__rq::queue::orders", q.resolvedNatsStream());
    assertEquals("rqueue.__rq::queue::orders", q.resolvedNatsSubject());
    assertEquals("rqueue-__rq::queue::orders-dlq", q.resolvedNatsDlqStream());
    assertEquals("rqueue.__rq::queue::orders.dlq", q.resolvedNatsDlqSubject());

    Duration fallback = Duration.ofSeconds(30);
    assertEquals(fallback, q.resolvedAckWait(fallback));
    assertEquals(4, q.resolvedMaxDeliver(4));
  }

  @Test
  void natsFieldsPassThroughWhenSet() {
    Duration ack = Duration.ofSeconds(60);
    Duration dedup = Duration.ofMinutes(2);
    QueueDetail q = baseBuilder()
        .natsStream("STREAM_X")
        .natsSubject("subj.x")
        .natsDlqStream("STREAM_X_DLQ")
        .natsDlqSubject("subj.x.dead")
        .natsAckWaitOverride(ack)
        .natsMaxDeliverOverride(7)
        .natsDedupWindow(dedup)
        .build();

    assertEquals("STREAM_X", q.getNatsStream());
    assertEquals("subj.x", q.getNatsSubject());
    assertEquals("STREAM_X_DLQ", q.getNatsDlqStream());
    assertEquals("subj.x.dead", q.getNatsDlqSubject());
    assertEquals(ack, q.getNatsAckWaitOverride());
    assertEquals(Integer.valueOf(7), q.getNatsMaxDeliverOverride());
    assertEquals(dedup, q.getNatsDedupWindow());

    assertEquals("STREAM_X", q.resolvedNatsStream());
    assertEquals("subj.x", q.resolvedNatsSubject());
    assertEquals("STREAM_X_DLQ", q.resolvedNatsDlqStream());
    assertEquals("subj.x.dead", q.resolvedNatsDlqSubject());
    assertEquals(ack, q.resolvedAckWait(Duration.ofSeconds(1)));
    assertEquals(7, q.resolvedMaxDeliver(99));
  }

  @Test
  void javaSerializationRoundTripPreservesNatsFields() throws Exception {
    QueueDetail q = baseBuilder()
        .natsStream("S1")
        .natsSubject("subj")
        .natsAckWaitOverride(Duration.ofSeconds(45))
        .natsMaxDeliverOverride(5)
        .natsDedupWindow(Duration.ofMinutes(1))
        .build();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(q);
    }
    QueueDetail back;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
      back = (QueueDetail) ois.readObject();
    }

    assertNotNull(back);
    assertEquals(q.getNatsStream(), back.getNatsStream());
    assertEquals(q.getNatsSubject(), back.getNatsSubject());
    assertEquals(q.getNatsAckWaitOverride(), back.getNatsAckWaitOverride());
    assertEquals(q.getNatsMaxDeliverOverride(), back.getNatsMaxDeliverOverride());
    assertEquals(q.getNatsDedupWindow(), back.getNatsDedupWindow());
    assertEquals(q.getQueueName(), back.getQueueName());
    // equals() on the whole object should still hold round-trip
    assertEquals(q, back);
  }
}
