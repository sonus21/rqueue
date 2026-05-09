/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsRqueueSystemConfigDao}: cache behaviour, KV CRUD, scan-by-id, and
 * exception swallowing.
 */
@NatsUnitTest
class NatsRqueueSystemConfigDaoTest {

  private NatsProvisioner provisioner;
  private RqueueSerDes serdes;
  private KeyValue kv;
  private NatsRqueueSystemConfigDao dao;

  @BeforeEach
  void setUp() throws IOException, JetStreamApiException {
    provisioner = mock(NatsProvisioner.class);
    serdes = mock(RqueueSerDes.class);
    kv = mock(KeyValue.class);
    when(provisioner.ensureKv(anyString(), nullable(Duration.class))).thenReturn(kv);
    dao = new NatsRqueueSystemConfigDao(provisioner, serdes);
  }

  private QueueConfig config(String id, String name) {
    return QueueConfig.builder().id(id).name(name).queueName(name).build();
  }

  private static KeyValueEntry entry(byte[] value) {
    return mock(
        KeyValueEntry.class, inv -> "getValue".equals(inv.getMethod().getName()) ? value : null);
  }

  // ---- getConfigByName (cached) ------------------------------------

  @Test
  void getConfigByName_cacheMiss_loadsFromKv() throws IOException, JetStreamApiException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    QueueConfig result = dao.getConfigByName("orders");

    assertNotNull(result);
    assertEquals("orders", result.getName());
  }

  @Test
  void getConfigByName_cacheHit_skipsKv() throws IOException, JetStreamApiException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    dao.getConfigByName("orders"); // primes cache
    dao.getConfigByName("orders"); // should hit cache

    verify(kv, atMostOnce()).get("orders");
  }

  @Test
  void getConfigByName_cached_false_bypassesCache() throws IOException, JetStreamApiException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    dao.getConfigByName("orders"); // primes cache
    dao.getConfigByName("orders", false); // bypasses cache, hits kv again

    verify(kv, times(2)).get("orders");
  }

  @Test
  void getConfigByName_miss_returnsNull() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);
    assertNull(dao.getConfigByName("no-such"));
  }

  @Test
  void getConfigByName_ioException_returnsNull() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenThrow(new IOException("network error"));
    assertNull(dao.getConfigByName("orders"));
  }

  // ---- getQConfig --------------------------------------------------

  @Test
  void getQConfig_cacheHit_returnsFromCache()
      throws IOException, JetStreamApiException, InterruptedException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    dao.getConfigByName("orders"); // primes cache with name="orders", id="id-1"
    QueueConfig result = dao.getQConfig("id-1", true);

    assertNotNull(result);
    assertEquals("id-1", result.getId());
    // should not scan kv since cache hit
    verify(kv, atMostOnce()).get(anyString());
  }

  @Test
  void getQConfig_cacheMiss_scansKv()
      throws IOException, JetStreamApiException, InterruptedException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.keys()).thenReturn(Collections.singletonList("orders"));
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    QueueConfig result = dao.getQConfig("id-1", false);

    assertNotNull(result);
    assertEquals("id-1", result.getId());
  }

  @Test
  void getQConfig_nullId_returnsNull() {
    assertNull(dao.getQConfig(null, false));
  }

  @Test
  void getQConfig_scanInterrupted_setsInterruptFlag()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new InterruptedException("interrupted"));

    dao.getQConfig("id-1", false);

    assertTrue(Thread.currentThread().isInterrupted());
    Thread.interrupted(); // clear
  }

  @Test
  void getQConfig_ioException_returnsNull()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new IOException("kv down"));
    assertNull(dao.getQConfig("id-x", false));
  }

  // ---- getConfigByNames / findAllQConfig ---------------------------

  @Test
  void getConfigByNames_returnsAllFound() throws IOException, JetStreamApiException {
    QueueConfig c1 = config("id-1", "q1");
    QueueConfig c2 = config("id-2", "q2");
    byte[] bytes = "{}".getBytes();
    when(kv.get("q1")).thenReturn(entry(bytes));
    when(kv.get("q2")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(c1).thenReturn(c2);

    List<QueueConfig> result = dao.getConfigByNames(Arrays.asList("q1", "q2"));

    assertEquals(2, result.size());
  }

  @Test
  void getConfigByNames_emptyList_returnsEmpty() {
    assertTrue(dao.getConfigByNames(Collections.emptyList()).isEmpty());
  }

  @Test
  void findAllQConfig_returnsMatchingById()
      throws IOException, JetStreamApiException, InterruptedException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.keys()).thenReturn(Collections.singletonList("orders"));
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    List<QueueConfig> result = dao.findAllQConfig(Collections.singletonList("id-1"));

    assertEquals(1, result.size());
  }

  // ---- saveQConfig -------------------------------------------------

  @Test
  void saveQConfig_putsToKvAndCaches() throws IOException, JetStreamApiException {
    QueueConfig cfg = config("id-1", "orders");
    when(serdes.serialize(cfg)).thenReturn("{}".getBytes());

    dao.saveQConfig(cfg);

    verify(kv, times(1)).put(eq("orders"), any(byte[].class));
    // now it should be in cache
    when(kv.get(anyString())).thenReturn(null); // prove kv is NOT consulted
    QueueConfig cached = dao.getConfigByName("orders");
    assertNotNull(cached);
    verify(kv, never()).get(anyString());
  }

  @Test
  void saveQConfig_ioException_isSwallowed() throws IOException, JetStreamApiException {
    QueueConfig cfg = config("id-1", "orders");
    when(serdes.serialize(cfg)).thenReturn("{}".getBytes());
    doThrow(new IOException("kv down")).when(kv).put(anyString(), any(byte[].class));

    dao.saveQConfig(cfg); // must not throw
  }

  @Test
  void saveAllQConfig_savesEach() throws IOException, JetStreamApiException {
    QueueConfig c1 = config("id-1", "q1");
    QueueConfig c2 = config("id-2", "q2");
    when(serdes.serialize(c1)).thenReturn("{}".getBytes());
    when(serdes.serialize(c2)).thenReturn("{}".getBytes());

    dao.saveAllQConfig(Arrays.asList(c1, c2));

    verify(kv).put(eq("q1"), any(byte[].class));
    verify(kv).put(eq("q2"), any(byte[].class));
  }

  // ---- clearCacheByName -------------------------------------------

  @Test
  void clearCacheByName_evictsEntry() throws IOException, JetStreamApiException {
    QueueConfig cfg = config("id-1", "orders");
    byte[] bytes = "{}".getBytes();
    when(kv.get("orders")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueConfig.class)).thenReturn(cfg);

    dao.getConfigByName("orders"); // prime cache
    dao.clearCacheByName("orders");

    // now getConfigByName must re-fetch from kv
    dao.getConfigByName("orders");
    verify(kv, times(2)).get("orders");
  }

  @Test
  void clearCacheByName_nonExistentKey_isNoOp() {
    dao.clearCacheByName("ghost"); // must not throw
  }
}
