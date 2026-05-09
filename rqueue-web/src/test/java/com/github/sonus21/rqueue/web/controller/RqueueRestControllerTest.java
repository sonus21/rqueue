/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.web.controller;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.DataDeleteRequest;
import com.github.sonus21.rqueue.models.request.DataTypeRequest;
import com.github.sonus21.rqueue.models.request.DateViewRequest;
import com.github.sonus21.rqueue.models.request.MessageDeleteRequest;
import com.github.sonus21.rqueue.models.request.MessageEnqueueRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.request.QueueExploreRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.service.RqueueUtilityService;
import com.github.sonus21.rqueue.web.RqueueDashboardChartService;
import com.github.sonus21.rqueue.web.RqueueJobService;
import com.github.sonus21.rqueue.web.RqueueQDetailService;
import com.github.sonus21.rqueue.web.RqueueSystemManagerService;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Unit tests for {@link RqueueRestController}: enabled/disabled path guarding across all endpoints.
 */
@CoreUnitTest
class RqueueRestControllerTest {

  @Mock private RqueueDashboardChartService chartService;
  @Mock private RqueueQDetailService qDetailService;
  @Mock private RqueueUtilityService utilityService;
  @Mock private RqueueSystemManagerService systemManagerService;
  @Mock private RqueueWebConfig webConfig;
  @Mock private RqueueJobService jobService;
  @Mock private ObjectProvider<MessageBroker> brokerProvider;
  @Mock private HttpServletResponse response;

  private RqueueRestController controller;

  @BeforeEach
  void setUp() {
    controller = new RqueueRestController(
        chartService, qDetailService, utilityService, systemManagerService,
        webConfig, jobService, brokerProvider);
  }

  private void enableWeb() {
    when(webConfig.isEnable()).thenReturn(true);
  }

  private void disableWeb() {
    when(webConfig.isEnable()).thenReturn(false);
  }

  // ---- chart ----

  @Test
  void getDashboardData_enabled_delegatesToChartService() {
    enableWeb();
    ChartDataRequest req = new ChartDataRequest();
    ChartDataResponse expected = new ChartDataResponse();
    when(chartService.getDashboardChartData(req)).thenReturn(expected);
    assertNotNull(controller.getDashboardData(req, response));
  }

  @Test
  void getDashboardData_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.getDashboardData(new ChartDataRequest(), response));
    verify(chartService, never()).getDashboardChartData(any());
  }

  // ---- jobs ----

  @Test
  void getJobs_enabled_delegatesToJobService() throws ProcessingException {
    enableWeb();
    DataViewResponse expected = new DataViewResponse();
    when(jobService.getJobs("msg-1")).thenReturn(expected);
    assertNotNull(controller.getJobs("msg-1", response));
  }

  @Test
  void getJobs_disabled_returnsNull() throws ProcessingException {
    disableWeb();
    assertNull(controller.getJobs("msg-1", response));
    verify(jobService, never()).getJobs(anyString());
  }

  // ---- queue-data ----

  @Test
  void exploreQueue_enabled_delegatesToQDetailService() {
    enableWeb();
    QueueExploreRequest req = new QueueExploreRequest();
    req.setName("orders");
    DataViewResponse expected = new DataViewResponse();
    when(qDetailService.getExplorePageData(any(), anyString(), any(), any(), anyInt(), anyInt()))
        .thenReturn(expected);
    assertNotNull(controller.exploreQueue(req, response));
  }

  @Test
  void exploreQueue_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.exploreQueue(new QueueExploreRequest(), response));
  }

  // ---- view-data ----

  @Test
  void viewData_enabled_delegatesToQDetailService() {
    enableWeb();
    DateViewRequest req = new DateViewRequest();
    req.setName("orders");
    DataViewResponse expected = new DataViewResponse();
    when(qDetailService.viewData(any(), any(), any(), anyInt(), anyInt())).thenReturn(expected);
    assertNotNull(controller.viewData(req, response));
  }

  @Test
  void viewData_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.viewData(new DateViewRequest(), response));
  }

  // ---- delete-message ----

  @Test
  void deleteMessage_enabled_delegatesToUtilityService() {
    enableWeb();
    MessageDeleteRequest req = new MessageDeleteRequest();
    req.setQueueName("orders");
    req.setMessageId("msg-1");
    BooleanResponse expected = new BooleanResponse();
    when(utilityService.deleteMessage("orders", "msg-1")).thenReturn(expected);
    assertNotNull(controller.deleteMessage(req, response));
  }

  @Test
  void deleteMessage_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.deleteMessage(new MessageDeleteRequest(), response));
  }

  // ---- enqueue-message ----

  @Test
  void enqueueMessage_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.enqueueMessage(new MessageEnqueueRequest(), response));
  }

  @Test
  void enqueueMessage_enabled_delegatesToUtilityService() {
    enableWeb();
    MessageEnqueueRequest req = new MessageEnqueueRequest();
    req.setQueueName("orders");
    req.setMessageId("msg-1");
    when(utilityService.enqueueMessage(anyString(), anyString(), any())).thenReturn(new BooleanResponse());
    assertNotNull(controller.enqueueMessage(req, response));
  }

  // ---- delete-queue ----

  @Test
  void deleteQueue_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.deleteQueue(new DataTypeRequest(), response));
  }

  @Test
  void deleteQueue_enabled_delegatesToSystemManagerService() {
    enableWeb();
    DataTypeRequest req = new DataTypeRequest();
    req.setName("orders");
    when(systemManagerService.deleteQueue("orders")).thenReturn(new BaseResponse());
    assertNotNull(controller.deleteQueue(req, response));
  }

  // ---- delete-queue-part ----

  @Test
  void deleteAll_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.deleteAll(new DataDeleteRequest(), response));
  }

  @Test
  void deleteAll_enabled_delegatesToUtilityService() {
    enableWeb();
    DataDeleteRequest req = new DataDeleteRequest();
    req.setQueueName("orders");
    req.setDatasetName("dlq");
    when(utilityService.makeEmpty("orders", "dlq")).thenReturn(new BooleanResponse());
    assertNotNull(controller.deleteAll(req, response));
  }

  // ---- data-type ----

  @Test
  void dataType_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.dataType(new DataTypeRequest(), response));
  }

  @Test
  void dataType_enabled_delegatesToUtilityService() {
    enableWeb();
    DataTypeRequest req = new DataTypeRequest();
    req.setName("orders");
    when(utilityService.getDataType("orders")).thenReturn(new StringResponse());
    assertNotNull(controller.dataType(req, response));
  }

  // ---- move-data ----

  @Test
  void moveData_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.dataType(new MessageMoveRequest(), response));
  }

  @Test
  void moveData_enabled_delegatesToUtilityService() {
    enableWeb();
    MessageMoveRequest req = new MessageMoveRequest();
    when(utilityService.moveMessage(req)).thenReturn(new MessageMoveResponse());
    assertNotNull(controller.dataType(req, response));
  }

  // ---- pause-unpause-queue ----

  @Test
  void pauseUnpauseQueue_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.pauseUnpauseQueue(new PauseUnpauseQueueRequest(), response));
  }

  @Test
  void pauseUnpauseQueue_enabled_delegatesToUtilityService() {
    enableWeb();
    PauseUnpauseQueueRequest req = new PauseUnpauseQueueRequest();
    when(utilityService.pauseUnpauseQueue(req)).thenReturn(new BaseResponse());
    assertNotNull(controller.pauseUnpauseQueue(req, response));
  }

  // ---- aggregate-data-selector ----

  @Test
  void aggregateDataCounter_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.aggregateDataCounter(AggregationType.DAILY, response));
  }

  @Test
  void aggregateDataCounter_enabled_delegatesToUtilityService() {
    enableWeb();
    when(utilityService.aggregateDataCounter(AggregationType.DAILY)).thenReturn(new DataSelectorResponse());
    assertNotNull(controller.aggregateDataCounter(AggregationType.DAILY, response));
  }

  // ---- capabilities ----

  @Test
  void capabilities_disabled_returnsNull() {
    disableWeb();
    assertNull(controller.capabilities(response));
  }

  @Test
  void capabilities_enabled_noBroker_returnsRedisDefaults() {
    enableWeb();
    when(brokerProvider.getIfAvailable()).thenReturn(null);
    Capabilities caps = controller.capabilities(response);
    assertNotNull(caps);
  }

  @Test
  void capabilities_enabled_withBroker_returnsBrokerCapabilities() {
    enableWeb();
    MessageBroker broker = mock(MessageBroker.class);
    Capabilities expected = Capabilities.REDIS_DEFAULTS;
    when(broker.capabilities()).thenReturn(expected);
    when(brokerProvider.getIfAvailable()).thenReturn(broker);
    assertNotNull(controller.capabilities(response));
  }
}
