/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.spring.boot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootUnitTest;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RqueueNatsAutoConfig#toBrokerConfig(RqueueNatsProperties)}: verifies that
 * every property in {@link RqueueNatsProperties} is correctly mapped to {@link RqueueNatsConfig}.
 */
@SpringBootUnitTest
class RqueueNatsAutoConfigToBrokerConfigTest {

  private RqueueNatsProperties defaultProps() {
    return new RqueueNatsProperties();
  }

  // ---- naming ----

  @Test
  void toBrokerConfig_streamPrefixMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getNaming().setStreamPrefix("myapp-");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals("myapp-", cfg.getStreamPrefix());
  }

  @Test
  void toBrokerConfig_subjectPrefixMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getNaming().setSubjectPrefix("myapp.");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals("myapp.", cfg.getSubjectPrefix());
  }

  @Test
  void toBrokerConfig_dlqSuffixMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getNaming().setDlqSuffix("-dead");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals("-dead", cfg.getDlqStreamSuffix());
  }

  // ---- auto-create flags ----

  @Test
  void toBrokerConfig_autoCreateStreamsMapped() {
    RqueueNatsProperties p = defaultProps();
    p.setAutoCreateStreams(false);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertFalse(cfg.isAutoCreateStreams());
  }

  @Test
  void toBrokerConfig_autoCreateConsumersMapped() {
    RqueueNatsProperties p = defaultProps();
    p.setAutoCreateConsumers(false);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertFalse(cfg.isAutoCreateConsumers());
  }

  @Test
  void toBrokerConfig_autoCreateDlqStreamMapped() {
    RqueueNatsProperties p = defaultProps();
    p.setAutoCreateDlqStream(true);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertTrue(cfg.isAutoCreateDlqStream());
  }

  // ---- consumer ----

  @Test
  void toBrokerConfig_fetchWaitMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getConsumer().setFetchWait(Duration.ofSeconds(10));
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(Duration.ofSeconds(10), cfg.getDefaultFetchWait());
  }

  @Test
  void toBrokerConfig_ackWaitMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getConsumer().setAckWait(Duration.ofSeconds(60));
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(Duration.ofSeconds(60), cfg.getConsumerDefaults().getAckWait());
  }

  @Test
  void toBrokerConfig_maxDeliverMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getConsumer().setMaxDeliver(5L);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(5L, cfg.getConsumerDefaults().getMaxDeliver());
  }

  @Test
  void toBrokerConfig_maxAckPendingMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getConsumer().setMaxAckPending(200L);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(200L, cfg.getConsumerDefaults().getMaxAckPending());
  }

  // ---- stream defaults ----

  @Test
  void toBrokerConfig_replicasMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setReplicas(3);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(3, cfg.getStreamDefaults().getReplicas());
  }

  @Test
  void toBrokerConfig_storageMemory_mapsToMemoryType() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setStorage("MEMORY");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(StorageType.Memory, cfg.getStreamDefaults().getStorage());
  }

  @Test
  void toBrokerConfig_storageFile_mapsToFileType() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setStorage("FILE");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(StorageType.File, cfg.getStreamDefaults().getStorage());
  }

  @Test
  void toBrokerConfig_retentionWorkQueue_mapsToWorkQueuePolicy() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setRetention("WORKQUEUE");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(RetentionPolicy.WorkQueue, cfg.getStreamDefaults().getRetention());
  }

  @Test
  void toBrokerConfig_retentionInterest_mapsToInterestPolicy() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setRetention("INTEREST");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(RetentionPolicy.Interest, cfg.getStreamDefaults().getRetention());
  }

  @Test
  void toBrokerConfig_retentionLimits_mapsToLimitsPolicy() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setRetention("LIMITS");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(RetentionPolicy.Limits, cfg.getStreamDefaults().getRetention());
  }

  @Test
  void toBrokerConfig_retentionUnknown_defaultsToLimits() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setRetention("BOGUS");
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(RetentionPolicy.Limits, cfg.getStreamDefaults().getRetention());
  }

  @Test
  void toBrokerConfig_maxBytesMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setMaxBytes(512L);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(512L, cfg.getStreamDefaults().getMaxBytes());
  }

  @Test
  void toBrokerConfig_maxMessagesMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setMaxMessages(1000L);
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(1000L, cfg.getStreamDefaults().getMaxMsgs());
  }

  @Test
  void toBrokerConfig_maxAgeMapped() {
    RqueueNatsProperties p = defaultProps();
    p.getStream().setMaxAge(Duration.ofDays(7));
    RqueueNatsConfig cfg = RqueueNatsAutoConfig.toBrokerConfig(p);
    assertEquals(Duration.ofDays(7), cfg.getStreamDefaults().getMaxAge());
  }

  // ---- returned object ----

  @Test
  void toBrokerConfig_returnsNonNull() {
    assertNotNull(RqueueNatsAutoConfig.toBrokerConfig(defaultProps()));
  }

  @Test
  void toBrokerConfig_consumerDefaultsNotNull() {
    assertNotNull(RqueueNatsAutoConfig.toBrokerConfig(defaultProps()).getConsumerDefaults());
  }

  @Test
  void toBrokerConfig_streamDefaultsNotNull() {
    assertNotNull(RqueueNatsAutoConfig.toBrokerConfig(defaultProps()).getStreamDefaults());
  }
}
