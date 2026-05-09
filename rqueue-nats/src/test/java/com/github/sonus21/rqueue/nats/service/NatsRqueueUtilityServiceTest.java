/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.nats.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamState;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link NatsRqueueUtilityService}. Covers soft-delete, pause/unpause,
 * enqueueMessage, and moveMessage behavior.
 */
@NatsUnitTest
class NatsRqueueUtilityServiceTest {

  private RqueueWebConfig webConfig;
  private RqueueSystemConfigDao systemConfigDao;
  private RqueueMessageMetadataService metadataService;
  private RqueueMessageListenerContainer listenerContainer;
  private JetStreamManagement jsm;
  private JetStream js;
  private RqueueSerDes serdes;
  private NatsRqueueUtilityService service;

  @BeforeEach
  void setup() {
    webConfig = Mockito.mock(RqueueWebConfig.class);
    when(webConfig.getHistoryDay()).thenReturn(7);
    when(webConfig.getMaxMessageMoveCount()).thenReturn(100);
    systemConfigDao = Mockito.mock(RqueueSystemConfigDao.class);
    metadataService = Mockito.mock(RqueueMessageMetadataService.class);
    listenerContainer = Mockito.mock(RqueueMessageListenerContainer.class);
    jsm = Mockito.mock(JetStreamManagement.class);
    js = Mockito.mock(JetStream.class);
    serdes = Mockito.mock(RqueueSerDes.class);
    service = new NatsRqueueUtilityService(
        webConfig,
        systemConfigDao,
        metadataService,
        listenerContainer,
        jsm,
        js,
        serdes,
        RqueueNatsConfig.defaults());
  }

  // --- deleteMessage --------------------------------------------------------

  @Test
  void deleteMessage_softDeletesMetadata_returnsValueTrue() {
    when(metadataService.deleteMessage(eq("q"), eq("m1"), any(Duration.class))).thenReturn(true);
    BooleanResponse response = service.deleteMessage("q", "m1");
    assertEquals(0, response.getCode());
    assertTrue(response.isValue());
    verify(metadataService).deleteMessage(eq("q"), eq("m1"), any(Duration.class));
  }

  @Test
  void deleteMessage_metadataMissing_returnsErrorCode() {
    when(metadataService.deleteMessage(anyString(), anyString(), any(Duration.class)))
        .thenReturn(false);
    BooleanResponse response = service.deleteMessage("q", "missing");
    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
    assertFalse(response.isValue());
  }

  @Test
  void deleteMessage_emptyQueueName_returnsValidationError() {
    BooleanResponse response = service.deleteMessage("", "m1");
    assertEquals(1, response.getCode());
    verify(metadataService, never()).deleteMessage(anyString(), anyString(), any(Duration.class));
  }

  @Test
  void deleteMessage_metadataServiceThrows_returnsErrorCode() {
    when(metadataService.deleteMessage(anyString(), anyString(), any(Duration.class)))
        .thenThrow(new RuntimeException("kv unavailable"));
    BooleanResponse response = service.deleteMessage("q", "m1");
    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
    assertTrue(response.getMessage().contains("kv unavailable"));
  }

  // --- pauseUnpauseQueue ----------------------------------------------------

  @Test
  void pauseUnpauseQueue_persistsFlagAndNotifiesListener() {
    QueueConfig config =
        QueueConfig.builder().name("q").queueName("q").paused(false).build();
    when(systemConfigDao.getConfigByName("q", true)).thenReturn(config);
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("q");
    request.setPause(true);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(0, response.getCode());
    ArgumentCaptor<QueueConfig> captor = ArgumentCaptor.forClass(QueueConfig.class);
    verify(systemConfigDao).saveQConfig(captor.capture());
    assertTrue(captor.getValue().isPaused(), "QueueConfig should be persisted with paused=true");
    verify(listenerContainer).pauseUnpauseQueue("q", true);
  }

  @Test
  void pauseUnpauseQueue_unpause_propagatesFalse() {
    QueueConfig config =
        QueueConfig.builder().name("q").queueName("q").paused(true).build();
    when(systemConfigDao.getConfigByName("q", true)).thenReturn(config);
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("q");
    request.setPause(false);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(0, response.getCode());
    verify(listenerContainer).pauseUnpauseQueue("q", false);
  }

  @Test
  void pauseUnpauseQueue_unknownQueue_returns404() {
    when(systemConfigDao.getConfigByName(anyString(), anyBoolean())).thenReturn(null);
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("missing");
    request.setPause(true);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(404, response.getCode());
    verify(systemConfigDao, never()).saveQConfig(any(QueueConfig.class));
    verify(listenerContainer, never()).pauseUnpauseQueue(anyString(), anyBoolean());
  }

  @Test
  void pauseUnpauseQueue_alreadyInTargetState_isNoOp() {
    QueueConfig config =
        QueueConfig.builder().name("q").queueName("q").paused(true).build();
    when(systemConfigDao.getConfigByName("q", true)).thenReturn(config);
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("q");
    request.setPause(true);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(0, response.getCode());
    verify(systemConfigDao, never()).saveQConfig(any(QueueConfig.class));
    verify(listenerContainer, never()).pauseUnpauseQueue(anyString(), anyBoolean());
  }

  @Test
  void pauseUnpauseQueue_emptyName_returns400() {
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("");
    request.setPause(true);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(400, response.getCode());
  }

  @Test
  void pauseUnpauseQueue_listenerThrows_persistsButReports500() {
    QueueConfig config =
        QueueConfig.builder().name("q").queueName("q").paused(false).build();
    when(systemConfigDao.getConfigByName("q", true)).thenReturn(config);
    Mockito.doThrow(new RuntimeException("listener offline"))
        .when(listenerContainer)
        .pauseUnpauseQueue(anyString(), anyBoolean());
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("q");
    request.setPause(true);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(500, response.getCode());
    verify(systemConfigDao, times(1)).saveQConfig(any(QueueConfig.class));
  }

  // --- enqueueMessage -------------------------------------------------------

  @Test
  void enqueueMessage_happyPath_publishesToSubjectAndReturnsTrue() throws Exception {
    RqueueMessage msg =
        RqueueMessage.builder().id("m1").message("hello").queueName("q").build();
    MessageMetadata meta = new MessageMetadata(msg, MessageStatus.ENQUEUED);
    when(metadataService.getByMessageId("q", "m1")).thenReturn(meta);
    when(serdes.serialize(msg)).thenReturn("hello".getBytes());

    BooleanResponse response = service.enqueueMessage("q", "m1", "FRONT");

    assertEquals(0, response.getCode());
    assertTrue(response.isValue());
    ArgumentCaptor<String> subjectCaptor = ArgumentCaptor.forClass(String.class);
    verify(js, times(1)).publish(subjectCaptor.capture(), any(Headers.class), any(byte[].class));
    assertEquals("rqueue.js.q", subjectCaptor.getValue());
  }

  @Test
  void enqueueMessage_dedupKeyContainsOriginalId() throws Exception {
    RqueueMessage msg =
        RqueueMessage.builder().id("m1").message("hi").queueName("q").build();
    MessageMetadata meta = new MessageMetadata(msg, MessageStatus.ENQUEUED);
    when(metadataService.getByMessageId("q", "m1")).thenReturn(meta);
    when(serdes.serialize(msg)).thenReturn("hi".getBytes());

    service.enqueueMessage("q", "m1", "BACK");

    ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
    verify(js).publish(anyString(), headersCaptor.capture(), any(byte[].class));
    String dedupKey = headersCaptor.getValue().getFirst("Nats-Msg-Id");
    assertNotNull(dedupKey);
    assertTrue(dedupKey.startsWith("m1-requeue-"), "Dedup key must start with original id");
  }

  @Test
  void enqueueMessage_emptyId_returnsValidationError() throws Exception {
    BooleanResponse response = service.enqueueMessage("q", "", "FRONT");
    assertEquals(1, response.getCode());
    verify(js, never()).publish(anyString(), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueMessage_emptyQueueName_returnsValidationError() throws Exception {
    BooleanResponse response = service.enqueueMessage("", "m1", "FRONT");
    assertEquals(1, response.getCode());
    verify(js, never()).publish(anyString(), any(Headers.class), any(byte[].class));
  }

  @Test
  void enqueueMessage_metadataNotFound_returnsError() {
    when(metadataService.getByMessageId("q", "missing")).thenReturn(null);
    BooleanResponse response = service.enqueueMessage("q", "missing", "FRONT");
    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
  }

  @Test
  void enqueueMessage_publishThrows_returnsError() throws Exception {
    RqueueMessage msg =
        RqueueMessage.builder().id("m1").message("hi").queueName("q").build();
    MessageMetadata meta = new MessageMetadata(msg, MessageStatus.ENQUEUED);
    when(metadataService.getByMessageId("q", "m1")).thenReturn(meta);
    when(serdes.serialize(msg)).thenReturn("hi".getBytes());
    when(js.publish(anyString(), any(Headers.class), any(byte[].class)))
        .thenThrow(new IOException("network"));

    BooleanResponse response = service.enqueueMessage("q", "m1", "FRONT");

    assertEquals(1, response.getCode());
    assertFalse(response.isValue());
  }

  // --- moveMessage ----------------------------------------------------------

  @Test
  void moveMessage_happyPath_movesMessagesAndDeletesSources() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest("orders", DataType.LIST, "orders-dlq", DataType.LIST);
    StreamState state = Mockito.mock(StreamState.class);
    when(state.getFirstSequence()).thenReturn(1L);
    when(state.getLastSequence()).thenReturn(2L);
    StreamInfo streamInfo = Mockito.mock(StreamInfo.class);
    when(streamInfo.getStreamState()).thenReturn(state);
    when(jsm.getStreamInfo("rqueue-js-orders")).thenReturn(streamInfo);

    MessageInfo mi1 = Mockito.mock(MessageInfo.class);
    when(mi1.getData()).thenReturn("msg1".getBytes());
    when(mi1.getHeaders()).thenReturn(null);
    MessageInfo mi2 = Mockito.mock(MessageInfo.class);
    when(mi2.getData()).thenReturn("msg2".getBytes());
    when(mi2.getHeaders()).thenReturn(null);
    when(jsm.getMessage("rqueue-js-orders", 1L)).thenReturn(mi1);
    when(jsm.getMessage("rqueue-js-orders", 2L)).thenReturn(mi2);

    MessageMoveResponse response = service.moveMessage(request);

    assertEquals(0, response.getCode());
    assertTrue(response.isValue());
    assertEquals(2, response.getNumberOfMessageTransferred());
    verify(js, times(2)).publish(eq("rqueue.js.orders-dlq"), any(Headers.class), any(byte[].class));
    verify(jsm).deleteMessage("rqueue-js-orders", 1L, false);
    verify(jsm).deleteMessage("rqueue-js-orders", 2L, false);
  }

  @Test
  void moveMessage_skipsAlreadyConsumedSequences() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest("orders", DataType.LIST, "orders-dlq", DataType.LIST);
    StreamState state = Mockito.mock(StreamState.class);
    when(state.getFirstSequence()).thenReturn(1L);
    when(state.getLastSequence()).thenReturn(2L);
    StreamInfo streamInfo = Mockito.mock(StreamInfo.class);
    when(streamInfo.getStreamState()).thenReturn(state);
    when(jsm.getStreamInfo("rqueue-js-orders")).thenReturn(streamInfo);

    JetStreamApiException notFound = Mockito.mock(JetStreamApiException.class);
    when(notFound.getApiErrorCode()).thenReturn(10037);
    when(jsm.getMessage("rqueue-js-orders", 1L)).thenThrow(notFound);
    MessageInfo mi2 = Mockito.mock(MessageInfo.class);
    when(mi2.getData()).thenReturn("msg2".getBytes());
    when(mi2.getHeaders()).thenReturn(null);
    when(jsm.getMessage("rqueue-js-orders", 2L)).thenReturn(mi2);

    MessageMoveResponse response = service.moveMessage(request);

    assertEquals(0, response.getCode());
    assertEquals(1, response.getNumberOfMessageTransferred());
    verify(js, times(1)).publish(eq("rqueue.js.orders-dlq"), any(Headers.class), any(byte[].class));
    verify(jsm, never()).deleteMessage(eq("rqueue-js-orders"), eq(1L), anyBoolean());
  }

  @Test
  void moveMessage_stripsNatsMsgIdHeaderFromCopiedMessages() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest("orders", DataType.LIST, "orders-dlq", DataType.LIST);
    StreamState state = Mockito.mock(StreamState.class);
    when(state.getFirstSequence()).thenReturn(1L);
    when(state.getLastSequence()).thenReturn(1L);
    StreamInfo streamInfo = Mockito.mock(StreamInfo.class);
    when(streamInfo.getStreamState()).thenReturn(state);
    when(jsm.getStreamInfo("rqueue-js-orders")).thenReturn(streamInfo);

    Headers srcHeaders = new Headers();
    srcHeaders.add("Nats-Msg-Id", "original-dedup-key");
    srcHeaders.add("Rqueue-Process-At", "12345");
    MessageInfo mi = Mockito.mock(MessageInfo.class);
    when(mi.getData()).thenReturn("body".getBytes());
    when(mi.getHeaders()).thenReturn(srcHeaders);
    when(jsm.getMessage("rqueue-js-orders", 1L)).thenReturn(mi);

    service.moveMessage(request);

    ArgumentCaptor<Headers> headersCaptor = ArgumentCaptor.forClass(Headers.class);
    verify(js).publish(anyString(), headersCaptor.capture(), any(byte[].class));
    Headers dst = headersCaptor.getValue();
    // Nats-Msg-Id must be stripped; other headers forwarded
    assertEquals(
        null, dst.getFirst("Nats-Msg-Id"), "Nats-Msg-Id must not be copied to destination");
    assertEquals("12345", dst.getFirst("Rqueue-Process-At"), "Other headers must be forwarded");
  }

  @Test
  void moveMessage_validationFails_returnsErrorWithoutTouchingStreams() throws Exception {
    MessageMoveRequest request = new MessageMoveRequest();
    // src and dst are both null → validationMessage returns non-null

    MessageMoveResponse response = service.moveMessage(request);

    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
    verify(jsm, never()).getStreamInfo(anyString());
    verify(js, never()).publish(anyString(), any(Headers.class), any(byte[].class));
  }

  @Test
  void moveMessage_streamNotFound_returnsError() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest("orders", DataType.LIST, "orders-dlq", DataType.LIST);
    when(jsm.getStreamInfo("rqueue-js-orders")).thenThrow(new IOException("stream not found"));

    MessageMoveResponse response = service.moveMessage(request);

    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
  }

  // --- unsupported operations -----------------------------------------------

  @Test
  void makeEmpty_returnsNotSupported() {
    BooleanResponse response = service.makeEmpty("q", "queue:q");
    assertEquals(1, response.getCode());
    assertTrue(response.getMessage().contains("not supported"));
  }

  // --- backend-agnostic operations ------------------------------------------

  @Test
  void getDataType_alwaysReportsStream() {
    StringResponse response = service.getDataType("anything");
    assertEquals(0, response.getCode());
    assertEquals("STREAM", response.getVal());
  }

  @Test
  void getLatestVersion_returnsEmptyPair() {
    assertNotNull(service.getLatestVersion());
  }

  @Test
  void aggregateDataCounter_dailyHasSelectionEntry() {
    DataSelectorResponse response = service.aggregateDataCounter(AggregationType.DAILY);
    assertNotNull(response);
    assertEquals("Select Number of Days", response.getTitle());
    assertNotEquals(0, response.getData().size());
  }

  @Test
  void aggregateDataCounter_weeklyHasSelectionEntry() {
    DataSelectorResponse response = service.aggregateDataCounter(AggregationType.WEEKLY);
    assertEquals("Select Number of Weeks", response.getTitle());
    assertNotEquals(0, response.getData().size());
  }

  @Test
  void aggregateDataCounter_monthlyHasSelectionEntry() {
    DataSelectorResponse response = service.aggregateDataCounter(AggregationType.MONTHLY);
    assertEquals("Select Number of Months", response.getTitle());
    assertNotEquals(0, response.getData().size());
  }

  // --- reactive wrappers ----------------------------------------------------

  @Test
  void reactivePauseUnpauseQueue_delegatesToSync() {
    QueueConfig config =
        QueueConfig.builder().name("q").queueName("q").paused(false).build();
    when(systemConfigDao.getConfigByName("q", true)).thenReturn(config);
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("q");
    request.setPause(true);

    BaseResponse response = service.reactivePauseUnpauseQueue(request).block();

    assertNotNull(response);
    assertEquals(0, response.getCode());
    verify(listenerContainer).pauseUnpauseQueue("q", true);
  }

  @Test
  void deleteReactiveMessage_delegatesToSync() {
    when(metadataService.deleteMessage(eq("q"), eq("m1"), any(Duration.class))).thenReturn(true);

    BooleanResponse response = service.deleteReactiveMessage("q", "m1").block();

    assertNotNull(response);
    assertTrue(response.isValue());
  }
}
