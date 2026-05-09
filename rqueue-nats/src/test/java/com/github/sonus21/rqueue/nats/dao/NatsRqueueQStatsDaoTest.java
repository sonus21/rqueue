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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.models.db.QueueStatistics;
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
 * Unit tests for {@link NatsRqueueQStatsDao}: findById, findAll, and save paths including
 * null-id validation and exception swallowing.
 */
@NatsUnitTest
class NatsRqueueQStatsDaoTest {

  private NatsProvisioner provisioner;
  private RqueueSerDes serdes;
  private KeyValue kv;
  private NatsRqueueQStatsDao dao;

  @BeforeEach
  void setUp() throws IOException, JetStreamApiException {
    provisioner = mock(NatsProvisioner.class);
    serdes = mock(RqueueSerDes.class);
    kv = mock(KeyValue.class);
    when(provisioner.ensureKv(anyString(), nullable(Duration.class))).thenReturn(kv);
    dao = new NatsRqueueQStatsDao(provisioner, serdes);
  }

  private static KeyValueEntry entry(byte[] value) {
    return mock(
        KeyValueEntry.class, inv -> "getValue".equals(inv.getMethod().getName()) ? value : null);
  }

  // ---- findById ----------------------------------------------------

  @Test
  void findById_null_returnsNull() {
    assertNull(dao.findById(null));
  }

  @Test
  void findById_returnsDeserialisedStat() throws IOException, JetStreamApiException {
    QueueStatistics stat = new QueueStatistics("s1");
    byte[] bytes = "{}".getBytes();
    when(kv.get("s1")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueStatistics.class)).thenReturn(stat);

    QueueStatistics result = dao.findById("s1");

    assertEquals("s1", result.getId());
  }

  @Test
  void findById_entryNotFound_returnsNull() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);
    assertNull(dao.findById("missing"));
  }

  @Test
  void findById_entryNullValue_returnsNull() throws IOException, JetStreamApiException {
    KeyValueEntry e = mock(KeyValueEntry.class);
    when(e.getValue()).thenReturn(null);
    when(kv.get(anyString())).thenReturn(e);
    assertNull(dao.findById("s1"));
  }

  @Test
  void findById_ioException_returnsNull() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenThrow(new IOException("network error"));
    assertNull(dao.findById("s1"));
  }

  @Test
  void findById_deserializeException_returnsNull() throws IOException, JetStreamApiException {
    byte[] bytes = "bad".getBytes();
    when(kv.get(anyString())).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueStatistics.class))
        .thenThrow(new RuntimeException("bad json"));

    assertNull(dao.findById("s1"));
  }

  @Test
  void findById_sanitizesKey() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);
    dao.findById("stat$key#1");
    verify(kv).get("stat_key_1");
  }

  // ---- findAll -----------------------------------------------------

  @Test
  void findAll_returnsAllPresent() throws IOException, JetStreamApiException {
    QueueStatistics s1 = new QueueStatistics("s1");
    QueueStatistics s2 = new QueueStatistics("s2");
    byte[] bytes = "{}".getBytes();
    when(kv.get("s1")).thenReturn(entry(bytes));
    when(kv.get("s2")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, QueueStatistics.class)).thenReturn(s1).thenReturn(s2);

    List<QueueStatistics> result = dao.findAll(Arrays.asList("s1", "s2"));

    assertEquals(2, result.size());
  }

  @Test
  void findAll_emptyCollection_returnsEmpty() {
    assertTrue(dao.findAll(Collections.emptyList()).isEmpty());
  }

  @Test
  void findAll_someNull_onlyNonNullReturned() throws IOException, JetStreamApiException {
    QueueStatistics s1 = new QueueStatistics("s1");
    byte[] bytes = "{}".getBytes();
    when(kv.get("s1")).thenReturn(entry(bytes));
    when(kv.get("s2")).thenReturn(null);
    when(serdes.deserialize(bytes, QueueStatistics.class)).thenReturn(s1);

    List<QueueStatistics> result = dao.findAll(Arrays.asList("s1", "s2"));

    assertEquals(1, result.size());
    assertEquals("s1", result.get(0).getId());
  }

  // ---- save --------------------------------------------------------

  @Test
  void save_nullStat_throwsIllegalArgument() {
    assertThrows(IllegalArgumentException.class, () -> dao.save(null));
  }

  @Test
  void save_nullId_throwsIllegalArgument() {
    QueueStatistics stat = new QueueStatistics(null);
    assertThrows(IllegalArgumentException.class, () -> dao.save(stat));
  }

  @Test
  void save_serialisesAndPutsToKv() throws IOException, JetStreamApiException {
    QueueStatistics stat = new QueueStatistics("s1");
    when(serdes.serialize(stat)).thenReturn("{}".getBytes());

    dao.save(stat);

    verify(kv, times(1)).put(eq("s1"), any(byte[].class));
  }

  @Test
  void save_ioException_isSwallowed() throws IOException, JetStreamApiException {
    QueueStatistics stat = new QueueStatistics("s1");
    when(serdes.serialize(stat)).thenReturn("{}".getBytes());
    doThrow(new IOException("kv down")).when(kv).put(anyString(), any(byte[].class));

    dao.save(stat); // must not throw
  }

  @Test
  void save_sanitizesSpecialCharsInId() throws IOException, JetStreamApiException {
    QueueStatistics stat = new QueueStatistics("__rq::q-stat::orders");
    when(serdes.serialize(stat)).thenReturn("{}".getBytes());

    dao.save(stat);

    // "::" contains non-allowed chars; should be sanitized
    verify(kv).put(eq("__rq__q-stat__orders"), any(byte[].class));
  }
}
