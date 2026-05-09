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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.models.db.RqueueJob;
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
 * Unit tests for {@link NatsRqueueJobDao}: CRUD operations, key sanitisation, exception
 * swallowing, and scan-by-message-id logic.
 */
@NatsUnitTest
class NatsRqueueJobDaoTest {

  private NatsProvisioner provisioner;
  private RqueueSerDes serdes;
  private KeyValue kv;
  private NatsRqueueJobDao dao;

  @BeforeEach
  void setUp() throws IOException, JetStreamApiException {
    provisioner = mock(NatsProvisioner.class);
    serdes = mock(RqueueSerDes.class);
    kv = mock(KeyValue.class);
    when(provisioner.ensureKv(anyString(), nullable(Duration.class))).thenReturn(kv);
    dao = new NatsRqueueJobDao(provisioner, serdes);
  }

  // ---- helpers -------------------------------------------------------

  private RqueueJob job(String id, String messageId) {
    RqueueJob j = new RqueueJob();
    j.setId(id);
    j.setMessageId(messageId);
    return j;
  }

  private static KeyValueEntry entry(byte[] value) {
    return mock(KeyValueEntry.class,
        inv -> "getValue".equals(inv.getMethod().getName()) ? value : null);
  }

  // ---- save / createJob ----------------------------------------------

  @Test
  void save_serialisesAndPutsToKv() throws IOException, JetStreamApiException {
    RqueueJob j = job("job-1", "msg-1");
    when(serdes.serialize(j)).thenReturn("{}".getBytes());

    dao.save(j, Duration.ofMinutes(10));

    verify(kv, times(1)).put(eq("job-1"), any(byte[].class));
  }

  @Test
  void createJob_delegatesToSave() throws IOException, JetStreamApiException {
    RqueueJob j = job("job-2", "msg-2");
    when(serdes.serialize(j)).thenReturn("{}".getBytes());

    dao.createJob(j, Duration.ofMinutes(5));

    verify(kv, times(1)).put(eq("job-2"), any(byte[].class));
  }

  @Test
  void save_ioException_isSwallowed() throws IOException, JetStreamApiException {
    RqueueJob j = job("job-3", "msg-3");
    when(serdes.serialize(j)).thenReturn("{}".getBytes());
    doThrow(new IOException("kv down")).when(kv).put(anyString(), any(byte[].class));

    dao.save(j, Duration.ofMinutes(1)); // must not throw
  }

  @Test
  void save_jetStreamApiException_isSwallowed() throws IOException, JetStreamApiException {
    RqueueJob j = job("job-4", "msg-4");
    when(serdes.serialize(j)).thenReturn("{}".getBytes());
    doThrow(mock(JetStreamApiException.class)).when(kv).put(anyString(), any(byte[].class));

    dao.save(j, Duration.ofMinutes(1)); // must not throw
  }

  @Test
  void save_sanitizesSpecialCharsInId() throws IOException, JetStreamApiException {
    RqueueJob j = job("job$class#1", "msg-1");
    when(serdes.serialize(j)).thenReturn("{}".getBytes());

    dao.save(j, Duration.ofMinutes(1));

    verify(kv).put(eq("job_class_1"), any(byte[].class));
  }

  // ---- findById ------------------------------------------------------

  @Test
  void findById_returnsDeserialisedJob() throws IOException, JetStreamApiException {
    byte[] bytes = "{}".getBytes();
    RqueueJob j = job("job-1", "msg-1");
    when(kv.get("job-1")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, RqueueJob.class)).thenReturn(j);

    RqueueJob result = dao.findById("job-1");

    assertNotNull(result);
    assertEquals("msg-1", result.getMessageId());
  }

  @Test
  void findById_entryNotFound_returnsNull() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);

    assertNull(dao.findById("ghost"));
  }

  @Test
  void findById_entryNullValue_returnsNull() throws IOException, JetStreamApiException {
    KeyValueEntry e = mock(KeyValueEntry.class);
    when(e.getValue()).thenReturn(null);
    when(kv.get(anyString())).thenReturn(e);

    assertNull(dao.findById("job-1"));
  }

  @Test
  void findById_ioException_returnsNull() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenThrow(new IOException("network error"));

    assertNull(dao.findById("job-1"));
  }

  @Test
  void findById_sanitizesKey() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);

    dao.findById("a$b#c");

    verify(kv).get("a_b_c");
  }

  // ---- findJobsByIdIn -----------------------------------------------

  @Test
  void findJobsByIdIn_returnsMatchingJobs() throws IOException, JetStreamApiException {
    byte[] bytes = "{}".getBytes();
    RqueueJob j1 = job("j1", "m1");
    RqueueJob j2 = job("j2", "m2");
    when(kv.get("j1")).thenReturn(entry(bytes));
    when(kv.get("j2")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, RqueueJob.class)).thenReturn(j1).thenReturn(j2);

    List<RqueueJob> result = dao.findJobsByIdIn(Arrays.asList("j1", "j2"));

    assertEquals(2, result.size());
  }

  @Test
  void findJobsByIdIn_emptyCollection_returnsEmpty() {
    assertTrue(dao.findJobsByIdIn(Collections.emptyList()).isEmpty());
  }

  @Test
  void findJobsByIdIn_missingEntry_skipped() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);

    List<RqueueJob> result = dao.findJobsByIdIn(Arrays.asList("missing-1", "missing-2"));

    assertTrue(result.isEmpty());
  }

  // ---- finByMessageId -----------------------------------------------

  @Test
  void finByMessageId_null_returnsEmpty() {
    assertTrue(dao.finByMessageId(null).isEmpty());
  }

  @Test
  void finByMessageId_matchingJob_returnsIt() throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    RqueueJob j = job("j1", "target-msg");
    when(kv.keys()).thenReturn(Collections.singletonList("j1"));
    when(kv.get("j1")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, RqueueJob.class)).thenReturn(j);

    List<RqueueJob> result = dao.finByMessageId("target-msg");

    assertEquals(1, result.size());
    assertEquals("j1", result.get(0).getId());
  }

  @Test
  void finByMessageId_noMatch_returnsEmpty() throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    RqueueJob j = job("j1", "other-msg");
    when(kv.keys()).thenReturn(Collections.singletonList("j1"));
    when(kv.get("j1")).thenReturn(entry(bytes));
    when(serdes.deserialize(bytes, RqueueJob.class)).thenReturn(j);

    assertTrue(dao.finByMessageId("not-here").isEmpty());
  }

  @Test
  void finByMessageId_ioException_returnsEmpty() throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new IOException("kv down"));
    assertTrue(dao.finByMessageId("x").isEmpty());
  }

  // ---- finByMessageIdIn --------------------------------------------

  @Test
  void finByMessageIdIn_null_returnsEmpty() {
    assertTrue(dao.finByMessageIdIn(null).isEmpty());
  }

  @Test
  void finByMessageIdIn_empty_returnsEmpty() {
    assertTrue(dao.finByMessageIdIn(Collections.emptyList()).isEmpty());
  }

  @Test
  void finByMessageIdIn_interruptedException_setsInterruptFlag()
      throws IOException, JetStreamApiException, InterruptedException {
    when(kv.keys()).thenThrow(new InterruptedException("interrupted"));

    dao.finByMessageIdIn(Collections.singletonList("msg-1"));

    assertTrue(Thread.currentThread().isInterrupted());
    Thread.interrupted(); // clear for subsequent tests
  }

  @Test
  void finByMessageIdIn_scansAndFilters() throws IOException, JetStreamApiException, InterruptedException {
    byte[] bytes = "{}".getBytes();
    RqueueJob j1 = job("j1", "m1");
    RqueueJob j2 = job("j2", "m2");
    when(kv.keys()).thenReturn(Arrays.asList("j1", "j2", "j3"));
    when(kv.get("j1")).thenReturn(entry(bytes));
    when(kv.get("j2")).thenReturn(entry(bytes));
    when(kv.get("j3")).thenReturn(null);
    when(serdes.deserialize(bytes, RqueueJob.class)).thenReturn(j1).thenReturn(j2);

    List<RqueueJob> result = dao.finByMessageIdIn(Collections.singletonList("m1"));

    assertEquals(1, result.size());
    assertEquals("j1", result.get(0).getId());
  }

  // ---- delete -------------------------------------------------------

  @Test
  void delete_callsKvDelete() throws IOException, JetStreamApiException {
    dao.delete("job-1");
    verify(kv).delete("job-1");
  }

  @Test
  void delete_ioException_isSwallowed() throws IOException, JetStreamApiException {
    doThrow(new IOException("gone")).when(kv).delete(anyString());
    dao.delete("job-1"); // must not throw
  }

  @Test
  void delete_sanitizesKey() throws IOException, JetStreamApiException {
    dao.delete("job$1");
    verify(kv).delete("job_1");
    verify(kv, never()).delete("job$1");
  }
}
