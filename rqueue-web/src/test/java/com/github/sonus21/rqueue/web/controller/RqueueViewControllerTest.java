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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.web.RqueueViewControllerService;
import com.github.sonus21.rqueue.web.view.RqueueHtmlRenderer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.ui.Model;

/**
 * Unit tests for {@link RqueueViewController}: disabled guard and HTML rendering delegation.
 */
@CoreUnitTest
class RqueueViewControllerTest {

  @Mock private RqueueHtmlRenderer renderer;
  @Mock private RqueueWebConfig webConfig;
  @Mock private RqueueViewControllerService viewControllerService;
  @Mock private HttpServletRequest request;
  @Mock private HttpServletResponse response;
  @Mock private Model model;
  @Mock private PrintWriter writer;

  private RqueueViewController controller;

  @BeforeEach
  void setUp() throws IOException {
    controller = new RqueueViewController(renderer, webConfig, viewControllerService);
    // Use lenient stubs because disabled-path tests don't call getWriter() or model.asMap()
    org.mockito.Mockito.lenient().when(response.getWriter()).thenReturn(writer);
    org.mockito.Mockito.lenient().when(model.asMap()).thenReturn(Collections.emptyMap());
  }

  private void enableWeb() {
    when(webConfig.isEnable()).thenReturn(true);
  }

  private void disableWeb() {
    when(webConfig.isEnable()).thenReturn(false);
  }

  // ---- index ----

  @Test
  void index_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.index(model, request, response);
    verify(viewControllerService, never()).index(any(), any());
  }

  @Test
  void index_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderIndex(any())).thenReturn("<html/>");
    controller.index(model, request, response);
    verify(viewControllerService).index(eq(model), any());
  }

  @Test
  void index_enabled_writesHtmlToResponse() throws IOException {
    enableWeb();
    when(renderer.renderIndex(any())).thenReturn("<html>index</html>");
    controller.index(model, request, response);
    verify(writer).write("<html>index</html>");
  }

  // ---- queues ----

  @Test
  void queues_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.queues(model, 1, request, response);
    verify(viewControllerService, never()).queues(any(), any(), anyInt());
  }

  @Test
  void queues_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderQueues(any())).thenReturn("<html/>");
    controller.queues(model, 2, request, response);
    verify(viewControllerService).queues(eq(model), any(), eq(2));
  }

  // ---- workers ----

  @Test
  void workers_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.workers(model, 1, request, response);
    verify(viewControllerService, never()).workers(any(), any(), anyInt());
  }

  @Test
  void workers_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderWorkers(any())).thenReturn("<html/>");
    controller.workers(model, 1, request, response);
    verify(viewControllerService).workers(eq(model), any(), eq(1));
  }

  // ---- queueDetail ----

  @Test
  void queueDetail_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.queueDetail("orders", model, request, response);
    verify(viewControllerService, never()).queueDetail(any(), any(), anyString());
  }

  @Test
  void queueDetail_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderQueueDetail(any())).thenReturn("<html/>");
    controller.queueDetail("orders", model, request, response);
    verify(viewControllerService).queueDetail(eq(model), any(), eq("orders"));
  }

  // ---- running ----

  @Test
  void running_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.running(model, request, response);
    verify(viewControllerService, never()).running(any(), any());
  }

  @Test
  void running_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderRunning(any())).thenReturn("<html/>");
    controller.running(model, request, response);
    verify(viewControllerService).running(eq(model), any());
  }

  // ---- scheduled ----

  @Test
  void scheduled_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.scheduled(model, request, response);
    verify(viewControllerService, never()).scheduled(any(), any());
  }

  @Test
  void scheduled_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderRunning(any())).thenReturn("<html/>");
    controller.scheduled(model, request, response);
    verify(viewControllerService).scheduled(eq(model), any());
  }

  // ---- dead ----

  @Test
  void dead_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.dead(model, request, response);
    verify(viewControllerService, never()).dead(any(), any());
  }

  @Test
  void dead_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderRunning(any())).thenReturn("<html/>");
    controller.dead(model, request, response);
    verify(viewControllerService).dead(eq(model), any());
  }

  // ---- pending ----

  @Test
  void pending_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.pending(model, request, response);
    verify(viewControllerService, never()).pending(any(), any());
  }

  @Test
  void pending_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderRunning(any())).thenReturn("<html/>");
    controller.pending(model, request, response);
    verify(viewControllerService).pending(eq(model), any());
  }

  // ---- utility ----

  @Test
  void utility_disabled_doesNotCallService() throws IOException {
    disableWeb();
    controller.utility(model, request, response);
    verify(viewControllerService, never()).utility(any(), any());
  }

  @Test
  void utility_enabled_delegatesToService() throws IOException {
    enableWeb();
    when(renderer.renderUtility(any())).thenReturn("<html/>");
    controller.utility(model, request, response);
    verify(viewControllerService).utility(eq(model), any());
  }

  // ---- x-forwarded-prefix header ----

  @Test
  void index_xForwardedPrefix_passedToService() throws IOException {
    enableWeb();
    when(request.getHeader("x-forwarded-prefix")).thenReturn("/app");
    when(renderer.renderIndex(any())).thenReturn("<html/>");
    controller.index(model, request, response);
    verify(viewControllerService).index(eq(model), eq("/app"));
  }

  @Test
  void index_noXForwardedPrefix_nullPassedToService() throws IOException {
    enableWeb();
    when(request.getHeader("x-forwarded-prefix")).thenReturn(null);
    when(renderer.renderIndex(any())).thenReturn("<html/>");
    controller.index(model, request, response);
    verify(viewControllerService).index(eq(model), eq(null));
  }
}
