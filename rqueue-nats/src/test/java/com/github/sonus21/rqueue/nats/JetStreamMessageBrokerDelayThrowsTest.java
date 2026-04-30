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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.listener.QueueDetail;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

/** Unit tests that exercise pure-Java code paths (no docker container needed). */
class JetStreamMessageBrokerDelayThrowsTest {

  private JetStreamMessageBroker newBroker() {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);
    return new JetStreamMessageBroker(
        conn, js, jsm, RqueueNatsConfig.defaults(), new ObjectMapper());
  }

  @Test
  void enqueueWithDelay_throwsUOE() {
    JetStreamMessageBroker broker = newBroker();
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn("orders");
    RqueueMessage m = RqueueMessage.builder().id("id-1").message("hi").build();
    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> broker.enqueueWithDelay(q, m, 100));
    // message must mention NATS so users grep'ing for UOE find a useful pointer
    org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().toLowerCase().contains("nats"));
  }

  @Test
  void capabilities_areAllFalse() {
    Capabilities caps = newBroker().capabilities();
    assertEquals(false, caps.supportsDelayedEnqueue());
    assertEquals(false, caps.supportsScheduledIntrospection());
    assertEquals(false, caps.supportsCronJobs());
    assertEquals(false, caps.usesPrimaryHandlerDispatch());
  }

  @Test
  void moveExpired_isNoOpReturningZero() {
    JetStreamMessageBroker broker = newBroker();
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn("orders");
    assertEquals(0L, broker.moveExpired(q, System.currentTimeMillis(), 100));
  }

  @Test
  void ack_withoutInFlight_returnsFalse() {
    JetStreamMessageBroker broker = newBroker();
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn("orders");
    assertFalse(broker.ack(q, RqueueMessage.builder().id("never-popped").build()));
    assertFalse(broker.nack(q, RqueueMessage.builder().id("never-popped").build(), 100));
  }
}
