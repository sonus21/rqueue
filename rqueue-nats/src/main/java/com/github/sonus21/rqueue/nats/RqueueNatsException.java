/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

/**
 * Runtime wrapper for IOException, JetStreamApiException and other NATS errors thrown from the
 * JetStream broker. The message always includes the queue or subject context that the call was
 * targeting so operators can grep server-side logs.
 */
public class RqueueNatsException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RqueueNatsException(String message) {
    super(message);
  }

  public RqueueNatsException(String message, Throwable cause) {
    super(message, cause);
  }
}
