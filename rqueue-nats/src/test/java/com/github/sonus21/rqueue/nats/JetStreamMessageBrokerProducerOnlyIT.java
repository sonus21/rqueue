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
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;

/**
 * End-to-end producer-only smoke test: the broker enqueues typed domain events but never pops or
 * acks them, mirroring the producer-only application mode where the process only publishes work.
 *
 * <p>Covers plain enqueue, priority enqueue, and reactive enqueue — verifying that all variants
 * land in JetStream and are reflected by {@link JetStreamMessageBroker#size}.
 */
@NatsIntegrationTest
class JetStreamMessageBrokerProducerOnlyIT extends AbstractJetStreamIT {

  private static final ObjectMapper MAPPER = SerializationUtils.objectMapper;

  // ---- minimal domain events used as message payloads --------------------

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class EmailEvent {
    private String id;
    private String to;
    private String subject;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class JobEvent {
    private String id;
    private String type;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  static class NotificationEvent {
    private String id;
    private String message;
  }

  // ---- helpers -----------------------------------------------------------

  private static String serialize(Object event) throws Exception {
    return MAPPER.writeValueAsString(event);
  }

  private static RqueueMessage rqueueMessage(String id, Object event) throws Exception {
    return RqueueMessage.builder().id(id).message(serialize(event)).build();
  }

  // ---- tests -------------------------------------------------------------

  @Test
  void enqueueEmailEvents_accumulateInStream() throws Exception {
    QueueDetail emailQueue = mockQueue("email-queue-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      int count = 5;
      for (int i = 0; i < count; i++) {
        EmailEvent event = new EmailEvent(
            UUID.randomUUID().toString(), "user" + i + "@example.com", "Subject " + i);
        broker.enqueue(emailQueue, rqueueMessage("email-" + i, event));
      }
      assertEquals(
          count, broker.size(emailQueue), "all email events should be visible in the stream");
    }
  }

  @Test
  void enqueueJobEvents_accumulateInStream() throws Exception {
    QueueDetail jobQueue = mockQueue("job-queue-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      String[] types = {"FULL_TIME", "PART_TIME", "CONTRACT"};
      for (int i = 0; i < types.length; i++) {
        JobEvent event = new JobEvent(UUID.randomUUID().toString(), types[i]);
        broker.enqueue(jobQueue, rqueueMessage("job-" + i, event));
      }
      assertEquals(
          types.length, broker.size(jobQueue), "all job events should be visible in the stream");
    }
  }

  @Test
  void enqueueWithPriority_notificationEvents_accumulateInPriorityStreams() throws Exception {
    QueueDetail notifQueue = mockQueue("notif-queue-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      String[] priorities = {"high", "low"};
      int perPriority = 4;
      for (String priority : priorities) {
        for (int i = 0; i < perPriority; i++) {
          NotificationEvent event =
              new NotificationEvent(UUID.randomUUID().toString(), priority + "-notification-" + i);
          broker.enqueue(notifQueue, priority, rqueueMessage(priority + "-notif-" + i, event));
        }
      }
      for (String priority : priorities) {
        QueueDetail pq = mockQueue(notifQueue.getName() + "_" + priority);
        assertEquals(
            perPriority,
            broker.size(pq),
            "priority=" + priority + " stream should hold " + perPriority + " notification events");
      }
    }
  }

  @Test
  void enqueueReactive_emailEvents_accumulateInStream() throws Exception {
    QueueDetail emailQueue = mockQueue("email-reactive-" + System.nanoTime());
    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {
      int count = 6;
      List<RqueueMessage> messages = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        EmailEvent event = new EmailEvent(
            UUID.randomUUID().toString(), "user" + i + "@example.com", "RE: item " + i);
        messages.add(rqueueMessage("re-email-" + i, event));
      }
      Flux<Void> publishes =
          Flux.fromIterable(messages).flatMap(m -> broker.enqueueReactive(emailQueue, m));
      StepVerifier.create(publishes).verifyComplete();
      assertEquals(
          count,
          broker.size(emailQueue),
          "all reactively enqueued email events should be in the stream");
    }
  }

  @Test
  void mixedEvents_allVariantsLandInCorrectStreams() throws Exception {
    String base = "mixed-events-" + System.nanoTime();
    QueueDetail mainQueue = mockQueue(base);
    QueueDetail highQueue = mockQueue(base + "_high");

    try (JetStreamMessageBroker broker =
        JetStreamMessageBroker.builder().connection(connection).build()) {

      // 3 email events on the main queue
      for (int i = 0; i < 3; i++) {
        EmailEvent email =
            new EmailEvent(UUID.randomUUID().toString(), "to" + i + "@example.com", "Hello " + i);
        broker.enqueue(mainQueue, rqueueMessage("email-" + i, email));
      }
      // 2 job events on the "high" priority sub-queue
      for (int i = 0; i < 2; i++) {
        JobEvent job = new JobEvent(UUID.randomUUID().toString(), "CONTRACT");
        broker.enqueue(mainQueue, "high", rqueueMessage("job-high-" + i, job));
      }
      // 1 notification reactively on the main queue
      NotificationEvent notif =
          new NotificationEvent(UUID.randomUUID().toString(), "reactive notif");
      StepVerifier.create(broker.enqueueReactive(mainQueue, rqueueMessage("notif-0", notif)))
          .verifyComplete();

      assertEquals(4L, broker.size(mainQueue), "main stream: 3 email + 1 reactive notification");
      assertEquals(2L, broker.size(highQueue), "high-priority stream: 2 job events");
    }
  }
}
