/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.exception;

/**
 * Raised when a web/dashboard operation is invoked against a backend that does not implement the
 * primitive it requires (e.g. atomic positional message moves on JetStream). Mapped to HTTP 501 by
 * {@code RqueueWebExceptionAdvice}; carries the backend identifier and operation name so callers
 * can distinguish "not supported here" from generic failures.
 */
public class BackendCapabilityException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final String backend;
  private final String operation;

  public BackendCapabilityException(String backend, String operation, String reason) {
    super(reason);
    this.backend = backend;
    this.operation = operation;
  }

  public String getBackend() {
    return backend;
  }

  public String getOperation() {
    return operation;
  }
}
