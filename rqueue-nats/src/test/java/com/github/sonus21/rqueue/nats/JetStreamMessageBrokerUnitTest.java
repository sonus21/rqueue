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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.MessageHandler;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * Non-container unit tests for {@link JetStreamMessageBroker} that mock the underlying NATS
 * primitives. These tests target subject naming, pub/sub plumbing, and exception wrapping —
 * end-to-end JetStream behavior is covered by the Docker-gated ITs.
 */
@NatsUnitTest
class JetStreamMessageBrokerUnitTest {

  private static QueueDetail queueNamed(String name) {
    QueueDetail q = mock(QueueDetail.class);
    when(q.getName()).thenReturn(name);
    return q;
  }

  /** Build a broker with all NATS primitives mocked and stream provisioning short-circuited. */
  private static Fixture newFixture(RqueueNatsConfig config) {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);
    // Mock the provisioner so ensureStream() is a no-op — these tests verify subject
    // naming and exception wrapping, not stream creation.
    NatsProvisioner provisioner = mock(NatsProvisioner.class);
    JetStreamMessageBroker broker = new JetStreamMessageBroker(
        conn, js, jsm, config, new RqJacksonSerDes(SerializationUtils.getObjectMapper()),
        provisioner);
    return new Fixture(conn, js, jsm, broker);
  }

  @Test
  void enqueue_publishesToPrefixedSubject() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1)).publish(eq("rqueue.js.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueWithPriority_appendsPrioritySuffixToSubject() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"),
        "high",
        RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1))
        .publish(eq("rqueue.js.orders.high"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueWithEmptyPriority_fallsBackToUnsuffixedSubject() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), "", RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1)).publish(eq("rqueue.js.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueue_honorsCustomSubjectPrefix() throws Exception {
    RqueueNatsConfig cfg = RqueueNatsConfig.defaults().setSubjectPrefix("custom.");
    Fixture f = newFixture(cfg);
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(mock(PublishAck.class));
    f.broker.enqueue(
        queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build());
    verify(f.js, times(1)).publish(eq("custom.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueue_wrapsIoExceptionInRqueueNatsException() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenThrow(new IOException("boom"));
    RqueueNatsException ex = assertThrows(
        RqueueNatsException.class,
        () -> f.broker.enqueue(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()));
    assertNotNull(ex.getCause());
  }

  @Test
  void enqueue_wrapsJetStreamApiExceptionInRqueueNatsException() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    when(f.js.publish(any(String.class), any(Headers.class), any(byte[].class)))
        .thenThrow(mock(JetStreamApiException.class));
    assertThrows(
        RqueueNatsException.class,
        () -> f.broker.enqueue(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()));
  }

  @Test
  void publish_writesUtf8BytesToConnection() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    f.broker.publish("chan-1", "hello");
    verify(f.conn, times(1)).publish("chan-1", "hello".getBytes(UTF_8));
  }

  @Test
  void subscribe_createsDispatcherAndSubscribesChannel_closeReleasesIt() throws Exception {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    Dispatcher d = mock(Dispatcher.class);
    when(f.conn.createDispatcher(any(MessageHandler.class))).thenReturn(d);
    when(d.subscribe(any(String.class))).thenReturn(d);

    AutoCloseable closer = f.broker.subscribe("chan-1", payload -> {});
    verify(f.conn, times(1)).createDispatcher(any(MessageHandler.class));
    verify(d, times(1)).subscribe("chan-1");
    verify(f.conn, never()).closeDispatcher(any());

    closer.close();
    verify(f.conn, times(1)).closeDispatcher(d);
  }

  @Test
  void enqueueReactive_completesWhenPublishFutureCompletes() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    PublishAck ack = mock(PublishAck.class);
    CompletableFuture<PublishAck> done = CompletableFuture.completedFuture(ack);
    when(f.js.publishAsync(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(done);

    StepVerifier.create(f.broker.enqueueReactive(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()))
        .verifyComplete();
    verify(f.js, times(1))
        .publishAsync(eq("rqueue.js.orders"), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueReactive_wrapsAsyncFailureInRqueueNatsException() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    CompletableFuture<PublishAck> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IOException("network down"));
    when(f.js.publishAsync(any(String.class), any(Headers.class), any(byte[].class)))
        .thenReturn(failed);

    StepVerifier.create(f.broker.enqueueReactive(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build()))
        .expectError(RqueueNatsException.class)
        .verify();
  }

  @Test
  void enqueueWithDelayReactive_returnsErrorMonoOfUOE() {
    Fixture f = newFixture(RqueueNatsConfig.defaults());
    StepVerifier.create(f.broker.enqueueWithDelayReactive(
            queueNamed("orders"), RqueueMessage.builder().id("m1").message("hi").build(), 100))
        .expectError(UnsupportedOperationException.class)
        .verify();
  }

  // ---- helper -----------------------------------------------------------

  private static final class Fixture {
    final Connection conn;
    final JetStream js;
    final JetStreamManagement jsm;
    final JetStreamMessageBroker broker;

    Fixture(Connection conn, JetStream js, JetStreamManagement jsm, JetStreamMessageBroker broker) {
      this.conn = conn;
      this.js = js;
      this.jsm = jsm;
      this.broker = broker;
    }
  }
}
