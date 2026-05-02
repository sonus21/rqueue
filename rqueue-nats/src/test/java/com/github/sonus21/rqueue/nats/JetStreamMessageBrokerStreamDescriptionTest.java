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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Pins the stream {@code description} that JetStreamMessageBroker forwards to {@link
 * NatsProvisioner#ensureStream(String, java.util.List, QueueType, String)} so {@code nats stream
 * info} shows operators which rqueue queue created the stream.
 */
@NatsUnitTest
class JetStreamMessageBrokerStreamDescriptionTest {

  private static QueueDetail queueNamed(String name) {
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn(name);
    when(q.getType()).thenReturn(QueueType.QUEUE);
    return q;
  }

  private static class Fixture {
    final JetStream js;
    final NatsProvisioner provisioner;
    final JetStreamMessageBroker broker;

    Fixture() {
      Connection conn = mock(Connection.class);
      this.js = mock(JetStream.class);
      JetStreamManagement jsm = mock(JetStreamManagement.class);
      this.provisioner = mock(NatsProvisioner.class);
      this.broker = new JetStreamMessageBroker(
          conn,
          js,
          jsm,
          RqueueNatsConfig.defaults(),
          new RqJacksonSerDes(SerializationUtils.getObjectMapper()),
          provisioner);
    }
  }

  @Test
  void onQueueRegistered_passesQueueNameDescription() {
    Fixture f = new Fixture();
    f.broker.onQueueRegistered(queueNamed("orders"));
    verify(f.provisioner)
        .ensureStream(
            eq("rqueue-js-orders"), anyList(), eq(QueueType.QUEUE), eq("rqueue queue: orders"));
  }

  @Test
  void enqueue_passesQueueNameDescription() throws Exception {
    Fixture f = new Fixture();
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.provisioner)
        .ensureStream(
            eq("rqueue-js-orders"), anyList(), any(QueueType.class), eq("rqueue queue: orders"));
  }

  @Test
  void enqueueWithPriority_includesPriorityInDescription() throws Exception {
    Fixture f = new Fixture();
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"),
        "high",
        RqueueMessage.builder().id("m1").message("hi").build());
    ArgumentCaptor<String> desc = ArgumentCaptor.forClass(String.class);
    verify(f.provisioner, atLeastOnce())
        .ensureStream(any(String.class), anyList(), any(QueueType.class), desc.capture());
    String d = desc.getValue();
    assertTrue(d.contains("orders"), "description should include queue name, was: " + d);
    assertTrue(d.contains("high"), "description should include priority, was: " + d);
  }
}
