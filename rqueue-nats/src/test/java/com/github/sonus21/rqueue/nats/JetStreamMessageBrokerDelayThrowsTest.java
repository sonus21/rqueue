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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import org.junit.jupiter.api.Test;

/** Unit tests that exercise pure-Java code paths (no docker container needed). */
@NatsUnitTest
class JetStreamMessageBrokerDelayThrowsTest {

  private JetStreamMessageBroker newBroker() {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);
    return new JetStreamMessageBroker(
        conn,
        js,
        jsm,
        RqueueNatsConfig.defaults(),
        new RqJacksonSerDes(SerializationUtils.getObjectMapper()),
        null);
  }

  @Test
  void enqueueWithDelay_throwsWhenSchedulingUnsupported() {
    // null provisioner → schedulingSupported=false
    JetStreamMessageBroker broker = newBroker();
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn("orders");
    RqueueMessage m = RqueueMessage.builder().id("id-1").message("hi").build();
    RqueueNatsException ex =
        assertThrows(RqueueNatsException.class, () -> broker.enqueueWithDelay(q, m, 100));
    assertTrue(ex.getMessage().contains(NatsProvisioner.SCHEDULING_MIN_VERSION));
  }

  @Test
  void capabilities_schedulingFalse_whenProvisionerNull() {
    // null provisioner → schedulingSupported=false
    Capabilities caps = newBroker().capabilities();
    assertEquals(false, caps.supportsDelayedEnqueue());
    assertEquals(false, caps.supportsScheduledIntrospection());
    assertEquals(false, caps.supportsCronJobs());
    // NATS uses its own JetStream subscription dispatch, not the Redis primary handler path.
    assertEquals(false, caps.usesPrimaryHandlerDispatch());
  }

  @Test
  void capabilities_schedulingTrue_whenProvisionerSupportsIt() {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);
    NatsProvisioner provisioner = mock(NatsProvisioner.class);
    when(provisioner.isMessageSchedulingSupported()).thenReturn(true);
    JetStreamMessageBroker broker = new JetStreamMessageBroker(
        conn,
        js,
        jsm,
        RqueueNatsConfig.defaults(),
        new RqJacksonSerDes(SerializationUtils.getObjectMapper()),
        provisioner);
    assertTrue(broker.capabilities().supportsDelayedEnqueue());
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
