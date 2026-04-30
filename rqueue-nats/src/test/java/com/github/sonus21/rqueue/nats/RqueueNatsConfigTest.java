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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.nats.RqueueNatsConfig.ConsumerDefaults;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig.StreamDefaults;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** POJO-only tests for {@link RqueueNatsConfig}; no broker / no NATS. */
@NatsUnitTest
class RqueueNatsConfigTest {

  @Test
  void defaults_returnsSensibleValues() {
    RqueueNatsConfig c = RqueueNatsConfig.defaults();
    assertNotNull(c.getStreamPrefix());
    assertNotNull(c.getSubjectPrefix());
    assertNotNull(c.getDlqStreamSuffix());
    assertNotNull(c.getDlqSubjectSuffix());
    assertTrue(c.isAutoCreateStreams());
    assertTrue(c.isAutoCreateConsumers());
    assertTrue(c.isAutoCreateDlqStream());
    assertNotNull(c.getDefaultFetchWait());
    assertTrue(c.getDefaultFetchWait().toMillis() > 0);
    assertNotNull(c.getStreamDefaults());
    assertNotNull(c.getConsumerDefaults());
    // sanity-check sane consumer defaults
    assertTrue(c.getConsumerDefaults().getMaxAckPending() > 0);
    assertTrue(c.getConsumerDefaults().getMaxDeliver() > 0);
    assertNotNull(c.getConsumerDefaults().getAckWait());
  }

  @Test
  void fluentSetters_returnSameInstanceAndUpdateFields() {
    RqueueNatsConfig c = RqueueNatsConfig.defaults();
    assertSame(c, c.setStreamPrefix("s-"));
    assertSame(c, c.setSubjectPrefix("sub."));
    assertSame(c, c.setDlqStreamSuffix("-x"));
    assertSame(c, c.setDlqSubjectSuffix(".x"));
    assertSame(c, c.setAutoCreateStreams(false));
    assertSame(c, c.setAutoCreateConsumers(false));
    assertSame(c, c.setAutoCreateDlqStream(false));
    assertSame(c, c.setDefaultFetchWait(Duration.ofSeconds(7)));

    assertEquals("s-", c.getStreamPrefix());
    assertEquals("sub.", c.getSubjectPrefix());
    assertEquals("-x", c.getDlqStreamSuffix());
    assertEquals(".x", c.getDlqSubjectSuffix());
    assertEquals(false, c.isAutoCreateStreams());
    assertEquals(false, c.isAutoCreateConsumers());
    assertEquals(false, c.isAutoCreateDlqStream());
    assertEquals(Duration.ofSeconds(7), c.getDefaultFetchWait());
  }

  @Test
  void streamDefaults_setNestedReference() {
    RqueueNatsConfig c = RqueueNatsConfig.defaults();
    StreamDefaults sd = new StreamDefaults();
    assertSame(c, c.setStreamDefaults(sd));
    assertSame(sd, c.getStreamDefaults());
  }

  @Test
  void consumerDefaults_setNestedReference() {
    RqueueNatsConfig c = RqueueNatsConfig.defaults();
    ConsumerDefaults cd = new ConsumerDefaults();
    assertSame(c, c.setConsumerDefaults(cd));
    assertSame(cd, c.getConsumerDefaults());
  }

  @Test
  void streamDefaults_fluentSettersRoundtripEachProperty() {
    StreamDefaults sd = new StreamDefaults();
    assertSame(sd, sd.setReplicas(3));
    assertSame(sd, sd.setStorage(StorageType.Memory));
    assertSame(sd, sd.setRetention(RetentionPolicy.Limits));
    assertSame(sd, sd.setDuplicateWindow(Duration.ofMinutes(10)));
    assertSame(sd, sd.setMaxMsgs(1234L));
    assertSame(sd, sd.setMaxBytes(99999L));

    assertEquals(3, sd.getReplicas());
    assertEquals(StorageType.Memory, sd.getStorage());
    assertEquals(RetentionPolicy.Limits, sd.getRetention());
    assertEquals(Duration.ofMinutes(10), sd.getDuplicateWindow());
    assertEquals(1234L, sd.getMaxMsgs());
    assertEquals(99999L, sd.getMaxBytes());
  }

  @Test
  void consumerDefaults_fluentSettersRoundtripEachProperty() {
    ConsumerDefaults cd = new ConsumerDefaults();
    assertSame(cd, cd.setAckWait(Duration.ofSeconds(45)));
    assertSame(cd, cd.setMaxDeliver(11));
    assertSame(cd, cd.setMaxAckPending(2048));

    assertEquals(Duration.ofSeconds(45), cd.getAckWait());
    assertEquals(11L, cd.getMaxDeliver());
    assertEquals(2048L, cd.getMaxAckPending());
  }

  @Test
  void emptyAndNullPrefixesAreAccepted() {
    RqueueNatsConfig c = RqueueNatsConfig.defaults().setSubjectPrefix("").setStreamPrefix(null);
    assertEquals("", c.getSubjectPrefix());
    assertEquals(null, c.getStreamPrefix());
  }

  @Test
  void toString_doesNotNpeOnDefaults() {
    // RqueueNatsConfig is plain Java (no Lombok) — but still verify no surprise NPEs and equals to
    // self holds.
    RqueueNatsConfig c = RqueueNatsConfig.defaults();
    assertDoesNotThrow(c::toString);
    assertEquals(c, c);
  }
}
