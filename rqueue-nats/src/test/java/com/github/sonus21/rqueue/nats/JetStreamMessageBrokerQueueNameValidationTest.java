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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.nats.js.JetStreamMessageBroker;
import com.github.sonus21.rqueue.serdes.RqJacksonSerDes;
import com.github.sonus21.rqueue.serdes.SerializationUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import org.junit.jupiter.api.Test;

/**
 * Pins the dot/wildcard/whitespace rejection on queue names for the NATS backend. NATS subjects
 * use {@code .} as a hierarchy separator and stream / consumer names disallow it outright, so
 * silently accepting an illegal name leads to an opaque driver-side rejection at first publish.
 */
@NatsUnitTest
class JetStreamMessageBrokerQueueNameValidationTest {

  private JetStreamMessageBroker newBroker() {
    return new JetStreamMessageBroker(
        mock(Connection.class),
        mock(JetStream.class),
        mock(JetStreamManagement.class),
        RqueueNatsConfig.defaults(),
        new RqJacksonSerDes(SerializationUtils.getObjectMapper()),
        null);
  }

  @Test
  void rejects_queueName_with_dot() {
    JetStreamMessageBroker broker = newBroker();
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> broker.validateQueueName("orders.us"));
    assertTrue(ex.getMessage().contains("orders.us"));
    assertTrue(ex.getMessage().contains("'.'"));
  }

  @Test
  void rejects_queueName_with_star_wildcard() {
    JetStreamMessageBroker broker = newBroker();
    assertThrows(IllegalArgumentException.class, () -> broker.validateQueueName("foo*bar"));
  }

  @Test
  void rejects_queueName_with_gt_wildcard() {
    JetStreamMessageBroker broker = newBroker();
    assertThrows(IllegalArgumentException.class, () -> broker.validateQueueName("foo>bar"));
  }

  @Test
  void rejects_queueName_with_whitespace() {
    JetStreamMessageBroker broker = newBroker();
    assertThrows(IllegalArgumentException.class, () -> broker.validateQueueName("foo bar"));
    assertThrows(IllegalArgumentException.class, () -> broker.validateQueueName("foo\tbar"));
  }

  @Test
  void accepts_legal_queue_names() {
    JetStreamMessageBroker broker = newBroker();
    assertDoesNotThrow(() -> broker.validateQueueName("orders"));
    assertDoesNotThrow(() -> broker.validateQueueName("orders-us"));
    assertDoesNotThrow(() -> broker.validateQueueName("orders_us"));
    assertDoesNotThrow(() -> broker.validateQueueName("orders123"));
    assertDoesNotThrow(() -> broker.validateQueueName("a"));
  }

  @Test
  void accepts_null_or_empty_so_that_callers_higher_up_the_stack_handle_those_errors() {
    JetStreamMessageBroker broker = newBroker();
    assertDoesNotThrow(() -> broker.validateQueueName(null));
    assertDoesNotThrow(() -> broker.validateQueueName(""));
  }
}
