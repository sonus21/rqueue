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
package com.github.sonus21.rqueue.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.nats.js.NatsStreamValidator;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.core.env.Environment;

/**
 * Unit tests for {@link RqueueNatsListenerConfig}: verifies every {@code @Bean} method
 * including all branches inside {@link RqueueNatsListenerConfig#natsConnection()}.
 *
 * <p>Uses {@code mockito-inline}'s {@link MockedStatic} to stub the {@link Nats#connect(Options)}
 * static call so no real NATS server is required.
 */
@Tag("unit")
@Tag("nats")
class RqueueNatsListenerConfigTest {

  private RqueueNatsListenerConfig config;
  private Environment env;

  @BeforeEach
  void setUp() {
    env = mock(Environment.class);
    config = new RqueueNatsListenerConfig();
    config.environment = env;
  }

  // ---- natsConnection – URL only (no auth, no connection name) ----

  @Test
  void natsConnection_urlOnly_connectsCalled() throws IOException, InterruptedException {
    when(env.getProperty("rqueue.nats.url", Options.DEFAULT_URL)).thenReturn("nats://localhost:4222");
    when(env.getProperty("rqueue.nats.username")).thenReturn(null);
    when(env.getProperty("rqueue.nats.password")).thenReturn(null);
    when(env.getProperty("rqueue.nats.token")).thenReturn(null);
    when(env.getProperty("rqueue.nats.connection-name")).thenReturn(null);

    Connection mockConn = mock(Connection.class);
    try (MockedStatic<Nats> natsStatic = mockStatic(Nats.class)) {
      natsStatic.when(() -> Nats.connect(any(Options.class))).thenReturn(mockConn);
      Connection result = config.natsConnection();
      assertNotNull(result);
    }
  }

  // ---- natsConnection – with connection name ----

  @Test
  void natsConnection_withConnectionName_connectsCalled() throws IOException, InterruptedException {
    when(env.getProperty("rqueue.nats.url", Options.DEFAULT_URL)).thenReturn("nats://localhost:4222");
    when(env.getProperty("rqueue.nats.username")).thenReturn(null);
    when(env.getProperty("rqueue.nats.password")).thenReturn(null);
    when(env.getProperty("rqueue.nats.token")).thenReturn(null);
    when(env.getProperty("rqueue.nats.connection-name")).thenReturn("my-conn");

    Connection mockConn = mock(Connection.class);
    try (MockedStatic<Nats> natsStatic = mockStatic(Nats.class)) {
      natsStatic.when(() -> Nats.connect(any(Options.class))).thenReturn(mockConn);
      Connection result = config.natsConnection();
      assertNotNull(result);
    }
  }

  // ---- natsConnection – with token auth ----

  @Test
  void natsConnection_withToken_connectsCalled() throws IOException, InterruptedException {
    when(env.getProperty("rqueue.nats.url", Options.DEFAULT_URL)).thenReturn("nats://localhost:4222");
    when(env.getProperty("rqueue.nats.username")).thenReturn(null);
    when(env.getProperty("rqueue.nats.password")).thenReturn(null);
    when(env.getProperty("rqueue.nats.token")).thenReturn("secret-token");
    when(env.getProperty("rqueue.nats.connection-name")).thenReturn(null);

    Connection mockConn = mock(Connection.class);
    try (MockedStatic<Nats> natsStatic = mockStatic(Nats.class)) {
      natsStatic.when(() -> Nats.connect(any(Options.class))).thenReturn(mockConn);
      Connection result = config.natsConnection();
      assertNotNull(result);
    }
  }

  // ---- natsConnection – with username/password auth (token is empty) ----

  @Test
  void natsConnection_withUsernamePassword_connectsCalled() throws IOException, InterruptedException {
    when(env.getProperty("rqueue.nats.url", Options.DEFAULT_URL)).thenReturn("nats://localhost:4222");
    when(env.getProperty("rqueue.nats.username")).thenReturn("user");
    when(env.getProperty("rqueue.nats.password")).thenReturn("pass");
    when(env.getProperty("rqueue.nats.token")).thenReturn("");   // empty → fall through to user/pass
    when(env.getProperty("rqueue.nats.connection-name")).thenReturn(null);

    Connection mockConn = mock(Connection.class);
    try (MockedStatic<Nats> natsStatic = mockStatic(Nats.class)) {
      natsStatic.when(() -> Nats.connect(any(Options.class))).thenReturn(mockConn);
      Connection result = config.natsConnection();
      assertNotNull(result);
    }
  }

  // ---- natsConnection – InterruptedException is wrapped in IOException ----

  @Test
  void natsConnection_interrupted_throwsIOException() throws InterruptedException {
    when(env.getProperty("rqueue.nats.url", Options.DEFAULT_URL)).thenReturn("nats://localhost:4222");
    when(env.getProperty("rqueue.nats.username")).thenReturn(null);
    when(env.getProperty("rqueue.nats.password")).thenReturn(null);
    when(env.getProperty("rqueue.nats.token")).thenReturn(null);
    when(env.getProperty("rqueue.nats.connection-name")).thenReturn(null);

    try (MockedStatic<Nats> natsStatic = mockStatic(Nats.class)) {
      natsStatic.when(() -> Nats.connect(any(Options.class)))
          .thenThrow(new InterruptedException("interrupted"));

      assertThrows(IOException.class, () -> config.natsConnection());
    }
    // Also verify the interrupt flag was restored
    // (Thread.currentThread().interrupt() is called inside the catch block)
    assertTrue(Thread.currentThread().isInterrupted());
    Thread.interrupted(); // clear for subsequent tests
  }

  // ---- jetStream ----

  @Test
  void jetStream_delegatesToConnection() throws IOException {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    when(conn.jetStream()).thenReturn(js);

    JetStream result = config.jetStream(conn);
    assertNotNull(result);
  }

  // ---- jetStreamManagement ----

  @Test
  void jetStreamManagement_delegatesToConnection() throws IOException {
    Connection conn = mock(Connection.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);
    when(conn.jetStreamManagement()).thenReturn(jsm);

    JetStreamManagement result = config.jetStreamManagement(conn);
    assertNotNull(result);
  }

  // ---- jetStreamMessageBroker ----

  @Test
  void jetStreamMessageBroker_returnsJetStreamMessageBroker() {
    Connection conn = mock(Connection.class);
    JetStream js = mock(JetStream.class);
    JetStreamManagement jsm = mock(JetStreamManagement.class);

    MessageBroker broker = config.jetStreamMessageBroker(conn, js, jsm);

    assertNotNull(broker);
    assertTrue(broker instanceof JetStreamMessageBroker);
  }

  // ---- natsStreamValidator ----

  @Test
  void natsStreamValidator_returnsNatsStreamValidator() {
    NatsProvisioner provisioner = mock(NatsProvisioner.class);

    NatsStreamValidator validator = config.natsStreamValidator(provisioner);

    assertNotNull(validator);
  }
}
