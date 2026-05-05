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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link NatsRqueueUtilityService}. Covers the soft-delete, pause/unpause, and
 * "not supported" stubs to lock in v1 behavior.
 */
@NatsUnitTest
class NatsRqueueUtilityServiceTest {

  private RqueueWebConfig webConfig;
  private RqueueSystemConfigDao systemConfigDao;
  private RqueueMessageMetadataService metadataService;
  private RqueueMessageListenerContainer listenerContainer;
  private NatsRqueueUtilityService service;

  @BeforeEach
  void setup() {
    webConfig = Mockito.mock(RqueueWebConfig.class);
    when(webConfig.getHistoryDay()).thenReturn(7);
    systemConfigDao = Mockito.mock(RqueueSystemConfigDao.class);
    metadataService = Mockito.mock(RqueueMessageMetadataService.class);
    listenerContainer = Mockito.mock(RqueueMessageListenerContainer.class);
    service =
        new NatsRqueueUtilityService(webConfig, systemConfigDao, metadataService, listenerContainer);
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
    QueueConfig config = QueueConfig.builder().name("q").queueName("q").paused(false).build();
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
    QueueConfig config = QueueConfig.builder().name("q").queueName("q").paused(true).build();
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
    QueueConfig config = QueueConfig.builder().name("q").queueName("q").paused(true).build();
    when(systemConfigDao.getConfigByName("q", true)).thenReturn(config);
    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest();
    request.setName("q");
    request.setPause(true);

    BaseResponse response = service.pauseUnpauseQueue(request);

    assertEquals(0, response.getCode());
    // No save and no listener notification when state is already correct.
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
    QueueConfig config = QueueConfig.builder().name("q").queueName("q").paused(false).build();
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

  // --- unsupported operations ----------------------------------------------

  @Test
  void enqueueMessage_returnsNotSupported() {
    BooleanResponse response = service.enqueueMessage("q", "m1", "FRONT");
    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
    assertTrue(response.getMessage().contains("not supported"));
  }

  @Test
  void moveMessage_returnsNotSupported() {
    MessageMoveResponse response = service.moveMessage(new MessageMoveRequest());
    assertEquals(1, response.getCode());
    assertTrue(response.getMessage().contains("not supported"));
  }

  @Test
  void makeEmpty_returnsNotSupported() {
    BooleanResponse response = service.makeEmpty("q", "queue:q");
    assertEquals(1, response.getCode());
    assertTrue(response.getMessage().contains("not supported"));
  }

  // --- backend-agnostic operations -----------------------------------------

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
    QueueConfig config = QueueConfig.builder().name("q").queueName("q").paused(false).build();
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
