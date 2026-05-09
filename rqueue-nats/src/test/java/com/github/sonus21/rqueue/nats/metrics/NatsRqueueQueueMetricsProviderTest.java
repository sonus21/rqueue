/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamState;
import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsRqueueQueueMetricsProvider}: pending, scheduled, processing,
 * and DLQ counts, including unknown-queue and stream-not-provisioned edge cases.
 */
@NatsUnitTest
class NatsRqueueQueueMetricsProviderTest {

  private JetStreamManagement jsm;
  private NatsRqueueQueueMetricsProvider provider;

  private static final String QUEUE = "orders";
  private static final RqueueNatsConfig CONFIG = RqueueNatsConfig.defaults();

  @BeforeEach
  void setUp() {
    EndpointRegistry.delete();
    jsm = mock(JetStreamManagement.class);
    provider = new NatsRqueueQueueMetricsProvider(jsm, CONFIG);
    EndpointRegistry.register(queue(QUEUE));
  }

  @AfterEach
  void tearDown() {
    EndpointRegistry.delete();
  }

  private static QueueDetail queue(String name) {
    return QueueDetail.builder()
        .name(name)
        .queueName(name)
        .processingQueueName(name + "-pq")
        .completedQueueName(name + "-cq")
        .scheduledQueueName(name + "-sq")
        .processingQueueChannelName(name + "-pch")
        .scheduledQueueChannelName(name + "-sch")
        .visibilityTimeout(30_000)
        .numRetry(3)
        .priority(Collections.emptyMap())
        .active(true)
        .type(QueueType.QUEUE)
        .build();
  }

  // ---- getPendingMessageCount ---------------------------------------------

  @Test
  void getPendingMessageCount_returnsStreamMsgCount() throws IOException, JetStreamApiException {
    StreamState state = mock(StreamState.class);
    when(state.getMsgCount()).thenReturn(42L);
    StreamInfo info = mock(StreamInfo.class);
    when(info.getStreamState()).thenReturn(state);
    when(jsm.getStreamInfo(CONFIG.getStreamPrefix() + QUEUE)).thenReturn(info);

    assertEquals(42L, provider.getPendingMessageCount(QUEUE));
  }

  @Test
  void getPendingMessageCount_unknownQueue_returnsZero() {
    assertEquals(0L, provider.getPendingMessageCount("no-such-queue"));
  }

  @Test
  void getPendingMessageCount_streamNotYetProvisioned_returnsZero()
      throws IOException, JetStreamApiException {
    JetStreamApiException notFound = mock(JetStreamApiException.class);
    when(jsm.getStreamInfo(anyString())).thenThrow(notFound);

    assertEquals(0L, provider.getPendingMessageCount(QUEUE));
  }

  @Test
  void getPendingMessageCount_ioException_throwsRqueueNatsException()
      throws IOException, JetStreamApiException {
    when(jsm.getStreamInfo(anyString())).thenThrow(new IOException("network error"));

    assertThrows(RqueueNatsException.class, () -> provider.getPendingMessageCount(QUEUE));
  }

  // ---- getScheduledMessageCount -------------------------------------------

  @Test
  void getScheduledMessageCount_alwaysReturnsZero() {
    assertEquals(0L, provider.getScheduledMessageCount(QUEUE));
    assertEquals(0L, provider.getScheduledMessageCount("no-such-queue"));
  }

  // ---- getProcessingMessageCount ------------------------------------------

  @Test
  void getProcessingMessageCount_alwaysReturnsZero() {
    assertEquals(0L, provider.getProcessingMessageCount(QUEUE));
  }

  // ---- getDeadLetterMessageCount ------------------------------------------

  @Test
  void getDeadLetterMessageCount_returnsStreamMsgCount() throws IOException, JetStreamApiException {
    StreamState state = mock(StreamState.class);
    when(state.getMsgCount()).thenReturn(5L);
    StreamInfo info = mock(StreamInfo.class);
    when(info.getStreamState()).thenReturn(state);
    String dlqStream = CONFIG.getStreamPrefix() + QUEUE + CONFIG.getDlqStreamSuffix();
    when(jsm.getStreamInfo(dlqStream)).thenReturn(info);

    assertEquals(5L, provider.getDeadLetterMessageCount(QUEUE));
  }

  @Test
  void getDeadLetterMessageCount_unknownQueue_returnsZero() {
    assertEquals(0L, provider.getDeadLetterMessageCount("ghost"));
  }

  @Test
  void getDeadLetterMessageCount_dlqStreamNotProvisioned_returnsZero()
      throws IOException, JetStreamApiException {
    JetStreamApiException notFound = mock(JetStreamApiException.class);
    when(jsm.getStreamInfo(anyString())).thenThrow(notFound);

    assertEquals(0L, provider.getDeadLetterMessageCount(QUEUE));
  }

  @Test
  void getDeadLetterMessageCount_ioException_throwsRqueueNatsException()
      throws IOException, JetStreamApiException {
    when(jsm.getStreamInfo(anyString())).thenThrow(new IOException("timeout"));

    assertThrows(RqueueNatsException.class, () -> provider.getDeadLetterMessageCount(QUEUE));
  }
}
