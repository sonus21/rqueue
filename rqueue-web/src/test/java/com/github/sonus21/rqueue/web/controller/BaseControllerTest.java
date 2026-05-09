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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

/**
 * Unit tests for {@link BaseController#isEnable(HttpServletResponse)}.
 */
@CoreUnitTest
class BaseControllerTest {

  @Mock
  private RqueueWebConfig rqueueWebConfig;
  @Mock
  private HttpServletResponse response;

  // Concrete subclass to expose isEnable
  private BaseController controller;

  @BeforeEach
  void setUp() {
    controller = new BaseController(rqueueWebConfig) {};
  }

  @Test
  void isEnable_webEnabled_returnsTrue() {
    when(rqueueWebConfig.isEnable()).thenReturn(true);
    assertTrue(controller.isEnable(response));
  }

  @Test
  void isEnable_webEnabled_doesNotSetStatus() {
    when(rqueueWebConfig.isEnable()).thenReturn(true);
    controller.isEnable(response);
    // When enabled, the 503 status is NOT set
    verify(response, org.mockito.Mockito.never()).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
  }

  @Test
  void isEnable_webDisabled_returnsFalse() {
    when(rqueueWebConfig.isEnable()).thenReturn(false);
    assertFalse(controller.isEnable(response));
  }

  @Test
  void isEnable_webDisabled_sets503Status() {
    when(rqueueWebConfig.isEnable()).thenReturn(false);
    controller.isEnable(response);
    verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
  }
}
