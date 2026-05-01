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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.spi.MessageBrokerFactory;
import com.github.sonus21.rqueue.core.spi.MessageBrokerLoader;
import com.github.sonus21.rqueue.nats.js.JetStreamMessageBrokerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/** ServiceLoader and configuration-parsing tests for {@link JetStreamMessageBrokerFactory}. */
@NatsUnitTest
class JetStreamMessageBrokerFactoryTest {

  /** Use a port that isn't bound; jnats should fail fast trying to connect. */
  private static Map<String, String> unreachableConfig() {
    Map<String, String> cfg = new HashMap<>();
    cfg.put("rqueue.nats.url", "nats://127.0.0.1:1");
    return cfg;
  }

  @Test
  void name_isLowercaseNats() {
    assertEquals("nats", new JetStreamMessageBrokerFactory().name());
  }

  @Test
  void serviceLoader_findsJetStreamFactory() {
    JetStreamMessageBrokerFactory found = null;
    for (MessageBrokerFactory f : ServiceLoader.load(MessageBrokerFactory.class)) {
      if ("nats".equals(f.name()) && f instanceof JetStreamMessageBrokerFactory) {
        found = (JetStreamMessageBrokerFactory) f;
        break;
      }
    }
    assertNotNull(found, "ServiceLoader did not discover JetStreamMessageBrokerFactory");
  }

  @Test
  void create_withUnreachableUrl_throwsRqueueNatsException() {
    JetStreamMessageBrokerFactory factory = new JetStreamMessageBrokerFactory();
    RqueueNatsException ex =
        assertThrows(RqueueNatsException.class, () -> factory.create(unreachableConfig()));
    // message should reference NATS and the URL so failures are debuggable
    String msg = ex.getMessage();
    assertNotNull(msg);
    assertTrue(msg.toLowerCase().contains("nats"), "message should mention NATS: " + msg);
    assertTrue(msg.contains("127.0.0.1:1"), "message should include URL: " + msg);
  }

  @Test
  void create_withNullConfig_throwsNpe() {
    JetStreamMessageBrokerFactory factory = new JetStreamMessageBrokerFactory();
    assertThrows(NullPointerException.class, () -> factory.create(null));
  }

  @Test
  void messageBrokerLoader_load_natsBackend_routesToJetStreamFactory() {
    // Same exception expected as direct factory.create() since URL is unreachable.
    RqueueNatsException ex = assertThrows(
        RqueueNatsException.class, () -> MessageBrokerLoader.load("nats", unreachableConfig()));
    assertTrue(ex.getMessage().toLowerCase().contains("nats"));
  }

  @Test
  void messageBrokerLoader_load_unknownBackend_throwsIAE() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class, () -> MessageBrokerLoader.load("nope", new HashMap<>()));
    assertTrue(
        ex.getMessage().contains("No MessageBrokerFactory found"),
        "expected 'No MessageBrokerFactory found' in message but was: " + ex.getMessage());
  }
}
