/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.ServerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsProvisioner}: KV provisioning, stream provisioning, consumer
 * provisioning, and scheduling support detection.
 */
@NatsUnitTest
class NatsProvisionerTest {

  private Connection connection;
  private KeyValueManagement kvm;
  private JetStreamManagement jsm;
  private RqueueNatsConfig config;
  private NatsProvisioner provisioner;

  @BeforeEach
  void setUp() throws IOException {
    connection = mock(Connection.class);
    kvm = mock(KeyValueManagement.class);
    jsm = mock(JetStreamManagement.class);
    config = RqueueNatsConfig.defaults();

    // Required by constructor
    when(connection.keyValueManagement()).thenReturn(kvm);
    ServerInfo serverInfo = mock(ServerInfo.class);
    when(serverInfo.isSameOrNewerThanVersion(NatsProvisioner.SCHEDULING_MIN_VERSION))
        .thenReturn(true);
    when(serverInfo.getVersion()).thenReturn("2.12.0");
    when(connection.getServerInfo()).thenReturn(serverInfo);

    provisioner = new NatsProvisioner(connection, jsm, config);
  }

  // ---- isMessageSchedulingSupported ----

  @Test
  void isMessageSchedulingSupported_serverSupports_returnsTrue() {
    assertTrue(provisioner.isMessageSchedulingSupported());
  }

  @Test
  void isMessageSchedulingSupported_serverTooOld_returnsFalse() throws IOException {
    Connection oldConn = mock(Connection.class);
    KeyValueManagement oldKvm = mock(KeyValueManagement.class);
    when(oldConn.keyValueManagement()).thenReturn(oldKvm);
    ServerInfo oldInfo = mock(ServerInfo.class);
    when(oldInfo.isSameOrNewerThanVersion(anyString())).thenReturn(false);
    when(oldInfo.getVersion()).thenReturn("2.10.0");
    when(oldConn.getServerInfo()).thenReturn(oldInfo);

    NatsProvisioner oldProvisioner = new NatsProvisioner(oldConn, jsm, config);
    assertFalse(oldProvisioner.isMessageSchedulingSupported());
  }

  @Test
  void isMessageSchedulingSupported_nullServerInfo_returnsFalse() throws IOException {
    Connection nullInfoConn = mock(Connection.class);
    KeyValueManagement nullKvm = mock(KeyValueManagement.class);
    when(nullInfoConn.keyValueManagement()).thenReturn(nullKvm);
    when(nullInfoConn.getServerInfo()).thenReturn(null);

    NatsProvisioner p = new NatsProvisioner(nullInfoConn, jsm, config);
    assertFalse(p.isMessageSchedulingSupported());
  }

  // ---- ensureKv ----

  @Test
  void ensureKv_bucketExists_returnsExistingHandle() throws IOException, JetStreamApiException {
    KeyValueStatus status = mock(KeyValueStatus.class);
    when(kvm.getStatus("rqueue-jobs")).thenReturn(status);
    KeyValue kv = mock(KeyValue.class);
    when(connection.keyValue("rqueue-jobs")).thenReturn(kv);

    KeyValue result = provisioner.ensureKv("rqueue-jobs", null);

    assertSame(kv, result);
    verify(kvm, never()).create(any());
  }

  @Test
  void ensureKv_bucketAbsent_createsAndReturnsBucket() throws IOException, JetStreamApiException {
    // Simulate bucket-not-found by throwing JetStreamApiException
    JetStreamApiException notFound = mock(JetStreamApiException.class);
    when(kvm.getStatus("rqueue-jobs")).thenThrow(notFound);
    KeyValue kv = mock(KeyValue.class);
    when(connection.keyValue("rqueue-jobs")).thenReturn(kv);

    KeyValue result = provisioner.ensureKv("rqueue-jobs", null);

    assertNotNull(result);
    verify(kvm, times(1)).create(any());
  }

  @Test
  void ensureKv_secondCall_usesCacheSkipsKvm() throws IOException, JetStreamApiException {
    KeyValueStatus status = mock(KeyValueStatus.class);
    when(kvm.getStatus("rqueue-jobs")).thenReturn(status);
    KeyValue kv = mock(KeyValue.class);
    when(connection.keyValue("rqueue-jobs")).thenReturn(kv);

    provisioner.ensureKv("rqueue-jobs", null);
    provisioner.ensureKv("rqueue-jobs", null);

    // kvm.getStatus called once (cache hit on second call)
    verify(kvm, times(1)).getStatus("rqueue-jobs");
  }

  @Test
  void ensureKv_withTtl_appliesTtlOnCreation() throws IOException, JetStreamApiException {
    JetStreamApiException notFound = mock(JetStreamApiException.class);
    when(kvm.getStatus("ttl-bucket")).thenThrow(notFound);
    KeyValue kv = mock(KeyValue.class);
    when(connection.keyValue("ttl-bucket")).thenReturn(kv);

    provisioner.ensureKv("ttl-bucket", Duration.ofHours(1));

    verify(kvm, times(1)).create(any());
  }

  // ---- ensureStream ----

  @Test
  void ensureStream_streamAlreadyExists_skipsCreation() throws IOException, JetStreamApiException {
    StreamInfo existing = mock(StreamInfo.class);
    StreamConfiguration existingCfg = mock(StreamConfiguration.class);
    when(existingCfg.getRetentionPolicy()).thenReturn(io.nats.client.api.RetentionPolicy.WorkQueue);
    when(existing.getConfiguration()).thenReturn(existingCfg);
    when(jsm.getStreamInfo("rqueue-js-orders")).thenReturn(existing);

    provisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"));

    verify(jsm, never()).addStream(any());
  }

  /**
   * Creates a JetStreamApiException mock BEFORE calling when()/thenThrow() to avoid
   * Mockito's UnfinishedStubbingException caused by nested mock() + when() calls.
   * Usage: create the exception first, then use it in thenThrow().
   */
  private static JetStreamApiException makeStreamNotFoundEx() {
    return mock(
        JetStreamApiException.class,
        inv -> "getApiErrorCode".equals(inv.getMethod().getName()) ? 10059 : null);
  }

  @Test
  void ensureStream_streamNotExist_createsStream() throws IOException, JetStreamApiException {
    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders")).thenThrow(notFound);

    provisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"));

    verify(jsm, times(1)).addStream(any(StreamConfiguration.class));
  }

  /**
   * When the NATS server supports scheduling (≥ 2.12), streams must be created with
   * {@code allowMessageSchedules=true} so the server accepts {@code Nats-Schedule}
   * publish headers (ADR-51).  Equivalent to: {@code nats stream add --allow-schedules}.
   */
  @Test
  void ensureStream_schedulingSupported_setsAllowMessageSchedules()
      throws IOException, JetStreamApiException {
    // setUp() already wires serverInfo.isSameOrNewerThanVersion() → true (scheduling supported)
    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders")).thenThrow(notFound);

    provisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"),
        QueueType.QUEUE, null, true);

    verify(jsm, times(1)).addStream(
        argThat(cfg -> cfg.getAllowMsgSchedules()));
  }

  @Test
  void ensureStream_schedulingNotSupported_doesNotSetAllowMessageSchedules()
      throws IOException, JetStreamApiException {
    // Build a provisioner backed by a server that does NOT support scheduling
    Connection oldConn = mock(Connection.class);
    KeyValueManagement oldKvm = mock(KeyValueManagement.class);
    when(oldConn.keyValueManagement()).thenReturn(oldKvm);
    io.nats.client.api.ServerInfo oldInfo = mock(io.nats.client.api.ServerInfo.class);
    when(oldInfo.isSameOrNewerThanVersion(anyString())).thenReturn(false);
    when(oldInfo.getVersion()).thenReturn("2.10.0");
    when(oldConn.getServerInfo()).thenReturn(oldInfo);
    NatsProvisioner oldProvisioner = new NatsProvisioner(oldConn, jsm, config);

    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders")).thenThrow(notFound);

    // allowSchedules=true is requested but server doesn't support it → flag must NOT be set
    oldProvisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"),
        QueueType.QUEUE, null, true);

    verify(jsm, times(1)).addStream(
        argThat(cfg -> !cfg.getAllowMsgSchedules()));
  }

  /**
   * If a stream was initially created without scheduling (allowSchedules=false) and a later call
   * requests scheduling (allowSchedules=true), the provisioner must call updateStream() to add the
   * flag rather than silently skipping because the stream is already in cache.
   */
  @Test
  void ensureStream_upgradeScheduling_updatesExistingStream() throws IOException, JetStreamApiException {
    // First call: stream doesn't exist, created without scheduling
    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders"))
        .thenThrow(notFound)             // first call: not found → create
        .thenReturn(mock(StreamInfo.class, inv -> {  // second call: exists, no scheduling
          String m = inv.getMethod().getName();
          if ("getConfiguration".equals(m)) {
            StreamConfiguration cfg = StreamConfiguration.builder()
                .name("rqueue-js-orders")
                .subjects(Collections.singletonList("rqueue.js.orders"))
                .build();
            return cfg;
          }
          return null;
        }));

    // First call: no scheduling
    provisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"),
        QueueType.QUEUE, null, false);
    // Second call (different provisioner instance to bypass cache): scheduling requested
    NatsProvisioner p2 = new NatsProvisioner(connection, jsm, config);
    p2.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"),
        QueueType.QUEUE, null, true);

    verify(jsm, times(1)).addStream(any(StreamConfiguration.class));
    verify(jsm, times(1)).updateStream(argThat(cfg -> cfg.getAllowMsgSchedules()));
  }

  @Test
  void ensureStream_calledTwice_onlyCallsNatsOnce() throws IOException, JetStreamApiException {
    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders")).thenThrow(notFound);

    provisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"));
    provisioner.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders"));

    verify(jsm, times(1)).addStream(any(StreamConfiguration.class));
  }

  @Test
  void ensureStream_autoCreateDisabled_throwsWhenStreamAbsent()
      throws IOException, JetStreamApiException {
    RqueueNatsConfig noAutoCreate = RqueueNatsConfig.defaults().setAutoCreateStreams(false);
    NatsProvisioner p = new NatsProvisioner(connection, jsm, noAutoCreate);

    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders")).thenThrow(notFound);

    assertThrows(
        RqueueNatsException.class,
        () -> p.ensureStream("rqueue-js-orders", Collections.singletonList("rqueue.js.orders")));
  }

  @Test
  void ensureStream_ioException_wrapsInRqueueNatsException()
      throws IOException, JetStreamApiException {
    when(jsm.getStreamInfo(anyString())).thenThrow(new IOException("connection refused"));

    assertThrows(
        RqueueNatsException.class,
        () -> provisioner.ensureStream(
            "rqueue-js-orders", Collections.singletonList("rqueue.js.orders")));
  }

  @Test
  void ensureStream_queueType_streamRetention_createsLimitsPolicy()
      throws IOException, JetStreamApiException {
    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-events")).thenThrow(notFound);

    provisioner.ensureStream(
        "rqueue-js-events", Arrays.asList("rqueue.js.events"), QueueType.STREAM);

    verify(jsm, times(1)).addStream(any(StreamConfiguration.class));
  }

  // ---- ensureDlqStream ----

  @Test
  void ensureDlqStream_autoCreateDlqFalse_doesNotCreateStream()
      throws IOException, JetStreamApiException {
    // Default config has autoCreateDlqStream=false
    provisioner.ensureDlqStream(
        "rqueue-js-orders-dlq", Collections.singletonList("rqueue.js.orders.dlq"));
    verify(jsm, never()).addStream(any());
  }

  @Test
  void ensureDlqStream_autoCreateDlqTrue_createsStream() throws IOException, JetStreamApiException {
    RqueueNatsConfig dlqConfig = RqueueNatsConfig.defaults().setAutoCreateDlqStream(true);
    NatsProvisioner p = new NatsProvisioner(connection, jsm, dlqConfig);

    JetStreamApiException notFound = makeStreamNotFoundEx();
    when(jsm.getStreamInfo("rqueue-js-orders-dlq")).thenThrow(notFound);

    p.ensureDlqStream("rqueue-js-orders-dlq", Collections.singletonList("rqueue.js.orders.dlq"));

    verify(jsm, times(1)).addStream(any(StreamConfiguration.class));
  }
}
