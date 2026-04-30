/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.exception.BackendCapabilityException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps rqueue web exceptions to HTTP responses. Scoped to rqueue's controller package so it does
 * not interfere with the host application's exception handling.
 */
@RestControllerAdvice(basePackageClasses = RqueueWebExceptionAdvice.class)
public class RqueueWebExceptionAdvice {

  @ExceptionHandler(BackendCapabilityException.class)
  public ResponseEntity<Map<String, Object>> handleBackendCapability(
      BackendCapabilityException ex) {
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("code", HttpStatus.NOT_IMPLEMENTED.value());
    body.put("backend", ex.getBackend());
    body.put("operation", ex.getOperation());
    body.put("message", ex.getMessage());
    return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(body);
  }
}
