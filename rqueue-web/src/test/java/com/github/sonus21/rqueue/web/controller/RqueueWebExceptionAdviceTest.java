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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.exception.BackendCapabilityException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Unit tests for {@link RqueueWebExceptionAdvice}: HTTP 501 mapping for
 * {@link BackendCapabilityException}.
 */
@CoreUnitTest
class RqueueWebExceptionAdviceTest {

  private final RqueueWebExceptionAdvice advice = new RqueueWebExceptionAdvice();

  @Test
  void handleBackendCapability_returnsHttp501() {
    BackendCapabilityException ex =
        new BackendCapabilityException("nats", "viewData", "not supported");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    assertEquals(HttpStatus.NOT_IMPLEMENTED, resp.getStatusCode());
  }

  @Test
  void handleBackendCapability_bodyContainsCode501() {
    BackendCapabilityException ex =
        new BackendCapabilityException("nats", "viewData", "not supported");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    assertNotNull(resp.getBody());
    assertEquals(501, resp.getBody().get("code"));
  }

  @Test
  void handleBackendCapability_bodyContainsBackend() {
    BackendCapabilityException ex =
        new BackendCapabilityException("nats", "viewData", "not supported");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    assertEquals("nats", resp.getBody().get("backend"));
  }

  @Test
  void handleBackendCapability_bodyContainsOperation() {
    BackendCapabilityException ex =
        new BackendCapabilityException("nats", "viewData", "not supported");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    assertEquals("viewData", resp.getBody().get("operation"));
  }

  @Test
  void handleBackendCapability_bodyContainsMessage() {
    BackendCapabilityException ex =
        new BackendCapabilityException("nats", "viewData", "not supported");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    assertEquals("not supported", resp.getBody().get("message"));
  }

  @Test
  void handleBackendCapability_bodyHasAllFourKeys() {
    BackendCapabilityException ex =
        new BackendCapabilityException("redis", "moveData", "atomic move unavailable");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    Map<String, Object> body = resp.getBody();
    assertNotNull(body);
    assertEquals(4, body.size());
  }

  @Test
  void handleBackendCapability_differentBackendAndOperation() {
    BackendCapabilityException ex = new BackendCapabilityException("redis", "scan", "cannot scan");
    ResponseEntity<Map<String, Object>> resp = advice.handleBackendCapability(ex);
    assertEquals("redis", resp.getBody().get("backend"));
    assertEquals("scan", resp.getBody().get("operation"));
  }
}
