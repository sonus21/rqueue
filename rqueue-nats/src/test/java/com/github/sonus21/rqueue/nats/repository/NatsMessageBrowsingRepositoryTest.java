/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.exception.BackendCapabilityException;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamState;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsMessageBrowsingRepository}: getDataSize routing (main queue, DLQ,
 * processing, scheduled, edge cases) and viewData capability guard.
 */
@NatsUnitTest
class NatsMessageBrowsingRepositoryTest {

  private JetStreamManagement jsm;
  private RqueueNatsConfig config;
  private NatsMessageBrowsingRepository repo;

  @BeforeEach
  void setUp() {
    jsm = mock(JetStreamManagement.class);
    config = RqueueNatsConfig.defaults();
    repo = new NatsMessageBrowsingRepository(jsm, config);
  }

  private static StreamInfo streamInfo(long msgCount) {
    StreamState state = mock(StreamState.class,
        inv -> "getMsgCount".equals(inv.getMethod().getName()) ? msgCount : null);
    return mock(StreamInfo.class,
        inv -> "getStreamState".equals(inv.getMethod().getName()) ? state : null);
  }

  // ---- getDataSize -- null / empty ---------------------------------

  @Test
  void getDataSize_null_returnsZero() {
    assertEquals(0L, repo.getDataSize(null, DataType.LIST));
  }

  @Test
  void getDataSize_empty_returnsZero() {
    assertEquals(0L, repo.getDataSize("", DataType.LIST));
  }

  // ---- getDataSize -- __rq:: main queue pattern -------------------

  @Test
  void getDataSize_mainQueuePattern_returnsStreamMsgCount()
      throws IOException, JetStreamApiException {
    String stream = config.getStreamPrefix() + "orders";
    when(jsm.getStreamInfo(stream)).thenReturn(streamInfo(17L));

    long count = repo.getDataSize("__rq::queue::orders", DataType.LIST);

    assertEquals(17L, count);
  }

  @Test
  void getDataSize_processingQueuePattern_returnsZero() {
    assertEquals(0L, repo.getDataSize("__rq::p-queue::orders", null));
  }

  @Test
  void getDataSize_scheduledQueuePattern_returnsZero() {
    assertEquals(0L, repo.getDataSize("__rq::d-queue::orders", null));
  }

  // ---- getDataSize -- DLQ -----------------------------------------

  @Test
  void getDataSize_dlqName_returnsDlqStreamCount() throws IOException, JetStreamApiException {
    // Any name not starting with "__rq::" is treated as a DLQ candidate
    String dlqStream = config.getStreamPrefix() + "orders" + config.getDlqStreamSuffix();
    when(jsm.getStreamInfo(dlqStream)).thenReturn(streamInfo(5L));

    long count = repo.getDataSize("orders", null);

    assertEquals(5L, count);
  }

  // ---- getDataSize -- stream-not-found (error code 10059) ----------

  @Test
  void getDataSize_streamNotFound10059_returnsZero() throws IOException, JetStreamApiException {
    JetStreamApiException notFound = mock(JetStreamApiException.class);
    when(notFound.getApiErrorCode()).thenReturn(10059);
    when(jsm.getStreamInfo(anyString())).thenThrow(notFound);

    assertEquals(0L, repo.getDataSize("__rq::queue::ghost", null));
  }

  // ---- getDataSize -- other JsApiException -------------------------

  @Test
  void getDataSize_otherJsApiException_returnsZero() throws IOException, JetStreamApiException {
    JetStreamApiException other = mock(JetStreamApiException.class);
    when(other.getApiErrorCode()).thenReturn(500);
    when(jsm.getStreamInfo(anyString())).thenThrow(other);

    // getDataSize catches all JsApiException, so should return 0 (not re-throw)
    assertEquals(0L, repo.getDataSize("__rq::queue::orders", null));
  }

  // ---- getDataSize -- IOException ----------------------------------

  @Test
  void getDataSize_ioException_returnsZero() throws IOException, JetStreamApiException {
    when(jsm.getStreamInfo(anyString())).thenThrow(new IOException("timeout"));

    assertEquals(0L, repo.getDataSize("__rq::queue::orders", null));
  }

  // ---- getDataSizes ------------------------------------------------

  @Test
  void getDataSizes_null_returnsEmpty() {
    assertTrue(repo.getDataSizes(null, null).isEmpty());
  }

  @Test
  void getDataSizes_empty_returnsEmpty() {
    assertTrue(repo.getDataSizes(Collections.emptyList(), Collections.emptyList()).isEmpty());
  }

  @Test
  void getDataSizes_delegatesToGetDataSize() throws IOException, JetStreamApiException {
    String stream = config.getStreamPrefix() + "q1";
    when(jsm.getStreamInfo(stream)).thenReturn(streamInfo(3L));

    List<Long> result = repo.getDataSizes(
        Arrays.asList("__rq::queue::q1", ""),
        Arrays.asList(DataType.LIST, DataType.LIST));

    assertEquals(2, result.size());
    assertEquals(3L, result.get(0));
    assertEquals(0L, result.get(1));
  }

  // ---- viewData ----------------------------------------------------

  @Test
  void viewData_throwsBackendCapabilityException() {
    assertThrows(
        BackendCapabilityException.class,
        () -> repo.viewData("orders", DataType.LIST, null, 0, 10));
  }

  @Test
  void viewData_exceptionMessageContainsNats() {
    BackendCapabilityException ex = assertThrows(
        BackendCapabilityException.class,
        () -> repo.viewData("orders", DataType.LIST, null, 0, 10));
    assertTrue(ex.getMessage().toLowerCase().contains("nats")
        || ex.getMessage().toLowerCase().contains("jetstream"));
  }
}
