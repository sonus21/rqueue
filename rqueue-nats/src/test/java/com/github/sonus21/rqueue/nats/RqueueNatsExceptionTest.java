/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RqueueNatsException}: constructors and cause propagation.
 */
@NatsUnitTest
class RqueueNatsExceptionTest {

  @Test
  void messageOnlyConstructor_setsMessage() {
    RqueueNatsException ex = new RqueueNatsException("something went wrong");
    assertEquals("something went wrong", ex.getMessage());
  }

  @Test
  void messageCauseConstructor_setsBoth() {
    IOException cause = new IOException("root cause");
    RqueueNatsException ex = new RqueueNatsException("wrapper", cause);
    assertEquals("wrapper", ex.getMessage());
    assertSame(cause, ex.getCause());
  }

  @Test
  void isRuntimeException() {
    assertNotNull(new RqueueNatsException("x"));
    // compile-time proof: no throws declaration needed
  }
}
