/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.models.registry.RqueueWorkerInfo;
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
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsWorkerRegistryStore}: CRUD operations across the worker-info and
 * heartbeat buckets, key sanitisation, and swallowed-exception resilience.
 */
@NatsUnitTest
class NatsWorkerRegistryStoreTest {

  private NatsProvisioner provisioner;
  private RqueueSerDes serdes;
  private KeyValue workerKv;
  private KeyValue heartbeatKv;
  private NatsWorkerRegistryStore store;

  private static final Duration TTL = Duration.ofMinutes(5);

  @BeforeEach
  void setUp() throws IOException, JetStreamApiException {
    provisioner = mock(NatsProvisioner.class);
    serdes = mock(RqueueSerDes.class);
    workerKv = mock(KeyValue.class);
    heartbeatKv = mock(KeyValue.class);

    // Route bucket names to respective mocks
    when(provisioner.ensureKv(eq("rqueue-workers"), any(Duration.class))).thenReturn(workerKv);
    when(provisioner.ensureKv(eq("rqueue-worker-heartbeats"), nullable(Duration.class)))
        .thenReturn(heartbeatKv);

    store = new NatsWorkerRegistryStore(provisioner, serdes);
  }

  // ---- putWorkerInfo -------------------------------------------------------

  @Test
  void putWorkerInfo_serialisesAndPutsToWorkerBucket() throws IOException, JetStreamApiException {
    RqueueWorkerInfo info = new RqueueWorkerInfo();
    info.setWorkerId("w1");
    when(serdes.serialize(info)).thenReturn("{}".getBytes());

    store.putWorkerInfo("w1", info, TTL);

    verify(workerKv, times(1)).put(eq("w1"), any(byte[].class));
  }

  @Test
  void putWorkerInfo_ioException_swallowed() throws IOException, JetStreamApiException {
    RqueueWorkerInfo info = new RqueueWorkerInfo();
    when(serdes.serialize(info)).thenReturn("{}".getBytes());
    doThrow(new IOException("kv down")).when(workerKv).put(anyString(), any(byte[].class));

    // must not propagate
    store.putWorkerInfo("w1", info, TTL);
  }

  // ---- deleteWorkerInfo ----------------------------------------------------

  @Test
  void deleteWorkerInfo_deletesFromWorkerBucket() throws IOException, JetStreamApiException {
    // Ensure bucket TTL is primed first
    RqueueWorkerInfo info = new RqueueWorkerInfo();
    when(serdes.serialize(info)).thenReturn("{}".getBytes());
    store.putWorkerInfo("w1", info, TTL);

    store.deleteWorkerInfo("w1");
    verify(workerKv).delete("w1");
  }

  @Test
  void deleteWorkerInfo_ioException_swallowed() throws IOException, JetStreamApiException {
    RqueueWorkerInfo info = new RqueueWorkerInfo();
    when(serdes.serialize(info)).thenReturn("{}".getBytes());
    store.putWorkerInfo("w1", info, TTL);

    doThrow(new IOException("gone")).when(workerKv).delete(anyString());
    store.deleteWorkerInfo("w1"); // should not throw
  }

  // ---- getWorkerInfos ------------------------------------------------------

  @Test
  void getWorkerInfos_returnsDeserialisedEntries() throws IOException, JetStreamApiException {
    RqueueWorkerInfo info = new RqueueWorkerInfo();
    info.setWorkerId("w1");
    byte[] bytes = "{}".getBytes();
    KeyValueEntry entry = mock(KeyValueEntry.class);
    when(entry.getValue()).thenReturn(bytes);
    when(workerKv.get("w1")).thenReturn(entry);
    when(serdes.deserialize(bytes, RqueueWorkerInfo.class)).thenReturn(info);

    // prime bucket TTL
    when(serdes.serialize(info)).thenReturn(bytes);
    store.putWorkerInfo("w1", info, TTL);

    Map<String, RqueueWorkerInfo> result = store.getWorkerInfos(Collections.singletonList("w1"));

    assertEquals(1, result.size());
    assertNotNull(result.get("w1"));
  }

  @Test
  void getWorkerInfos_emptyKeys_returnsEmptyMap() {
    Map<String, RqueueWorkerInfo> result = store.getWorkerInfos(Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test
  void getWorkerInfos_nullKeys_returnsEmptyMap() {
    Map<String, RqueueWorkerInfo> result = store.getWorkerInfos(null);
    assertTrue(result.isEmpty());
  }

  @Test
  void getWorkerInfos_missingEntry_skipped() throws IOException, JetStreamApiException {
    RqueueWorkerInfo info = new RqueueWorkerInfo();
    when(serdes.serialize(info)).thenReturn("{}".getBytes());
    store.putWorkerInfo("w1", info, TTL);

    when(workerKv.get(anyString())).thenReturn(null);

    Map<String, RqueueWorkerInfo> result = store.getWorkerInfos(Arrays.asList("w1", "w2"));
    assertTrue(result.isEmpty());
  }

  // ---- putQueueHeartbeat ---------------------------------------------------

  @Test
  void putQueueHeartbeat_putsCompositeKey() throws IOException, JetStreamApiException {
    store.putQueueHeartbeat("orders", "w1", "{\"alive\":true}");
    verify(heartbeatKv).put(eq("orders__w1"), any(byte[].class));
  }

  @Test
  void putQueueHeartbeat_sanitisesSpecialCharsInQueueKey()
      throws IOException, JetStreamApiException {
    store.putQueueHeartbeat("orders$2", "w#1", "{}");
    verify(heartbeatKv).put(eq("orders_2__w_1"), any(byte[].class));
  }

  @Test
  void putQueueHeartbeat_ioException_swallowed() throws IOException, JetStreamApiException {
    doThrow(new IOException("bucket gone")).when(heartbeatKv).put(anyString(), any(byte[].class));
    store.putQueueHeartbeat("q", "w", "{}"); // must not throw
  }

  // ---- getQueueHeartbeats --------------------------------------------------

  @Test
  void getQueueHeartbeats_returnsMatchingEntries() throws Exception {
    String prefix = "orders__";
    List<String> keys = Arrays.asList("orders__w1", "orders__w2", "other__w3");
    when(heartbeatKv.keys()).thenReturn(keys);

    KeyValueEntry e1 = mock(KeyValueEntry.class);
    when(e1.getValue()).thenReturn("{\"alive\":true}".getBytes());
    KeyValueEntry e2 = mock(KeyValueEntry.class);
    when(e2.getValue()).thenReturn("{\"alive\":false}".getBytes());
    when(heartbeatKv.get("orders__w1")).thenReturn(e1);
    when(heartbeatKv.get("orders__w2")).thenReturn(e2);

    Map<String, String> result = store.getQueueHeartbeats("orders");

    assertEquals(2, result.size());
    assertTrue(result.containsKey("w1"));
    assertTrue(result.containsKey("w2"));
  }

  @Test
  void getQueueHeartbeats_ioException_returnsEmptyMap() throws Exception {
    when(heartbeatKv.keys()).thenThrow(new IOException("kv down"));
    Map<String, String> result = store.getQueueHeartbeats("orders");
    assertTrue(result.isEmpty());
  }

  @Test
  void getQueueHeartbeats_interruptedException_setsInterruptFlag() throws Exception {
    when(heartbeatKv.keys()).thenThrow(new InterruptedException("interrupted"));
    store.getQueueHeartbeats("orders"); // must not propagate, just set interrupt flag
    assertTrue(Thread.currentThread().isInterrupted());
    // clear for subsequent tests
    Thread.interrupted();
  }

  // ---- deleteQueueHeartbeats -----------------------------------------------

  @Test
  void deleteQueueHeartbeats_deletesCompositeKeys() throws IOException, JetStreamApiException {
    store.deleteQueueHeartbeats("orders", "w1", "w2");
    verify(heartbeatKv).delete("orders__w1");
    verify(heartbeatKv).delete("orders__w2");
  }

  @Test
  void deleteQueueHeartbeats_emptyWorkerIds_isNoOp() throws IOException, JetStreamApiException {
    store.deleteQueueHeartbeats("orders");
    verify(heartbeatKv, never()).delete(anyString());
  }

  @Test
  void deleteQueueHeartbeats_nullWorkerIds_isNoOp() throws IOException, JetStreamApiException {
    store.deleteQueueHeartbeats("orders", (String[]) null);
    verify(heartbeatKv, never()).delete(anyString());
  }

  // ---- refreshQueueTtl -----------------------------------------------------

  @Test
  void refreshQueueTtl_capturesFirstObservedTtl() throws IOException, JetStreamApiException {
    store.refreshQueueTtl("orders", Duration.ofMinutes(10));
    store.refreshQueueTtl("orders", Duration.ofMinutes(99)); // should be ignored

    // trigger a heartbeat write to verify the bucket TTL is used
    store.putQueueHeartbeat("orders", "w1", "{}");
    // ensureKv called with the first-observed ttl (10 min), not the later one
    verify(provisioner).ensureKv(eq("rqueue-worker-heartbeats"), eq(Duration.ofMinutes(10)));
  }
}
