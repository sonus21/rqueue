/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.nats.NatsUnitTest;
import org.junit.jupiter.api.Test;

@NatsUnitTest
class NatsKvKeysTest {

  @Test
  void sanitizeStripsRedisHashTagsBeforeReplacingUnsupportedCharacters() {
    assertEquals(
        "__rq__q-pollers__email-notification-dispatch-queue",
        NatsKvKeys.sanitize("__rq::q-pollers::{email-notification-dispatch-queue}"));
  }

  @Test
  void sanitizeReplacesUnsupportedCharacters() {
    assertEquals("orders_2__w_1", NatsKvKeys.sanitize("orders$2::w#1"));
  }

  @Test
  void sanitizeReturnsFallbackForNullOrEmptyResult() {
    assertEquals("_", NatsKvKeys.sanitize(null));
    assertEquals("_", NatsKvKeys.sanitize("{}"));
  }
}
