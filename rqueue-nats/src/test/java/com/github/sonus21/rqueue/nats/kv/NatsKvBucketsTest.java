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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.nats.NatsUnitTest;
import org.junit.jupiter.api.Test;

/**
 * Sanity tests for {@link NatsKvBuckets}: ensures the ALL_BUCKETS catalogue is complete and
 * immutable so any accidental bucket addition or removal is caught immediately.
 */
@NatsUnitTest
class NatsKvBucketsTest {

  @Test
  void allBucketsContainsAllNamedConstants() {
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.QUEUE_CONFIG));
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.JOBS));
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.LOCKS));
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.MESSAGE_METADATA));
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.WORKERS));
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.WORKER_HEARTBEATS));
    assertTrue(NatsKvBuckets.ALL_BUCKETS.contains(NatsKvBuckets.QUEUE_STATS));
  }

  @Test
  void allBucketsHasExactlySevenEntries() {
    assertEquals(7, NatsKvBuckets.ALL_BUCKETS.size());
  }

  @Test
  void allBucketsIsImmutable() {
    assertFalse(NatsKvBuckets.ALL_BUCKETS.isEmpty());
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> NatsKvBuckets.ALL_BUCKETS.add("rogue-bucket"));
  }

  @Test
  void bucketNamesAreDistinct() {
    long distinct = NatsKvBuckets.ALL_BUCKETS.stream().distinct().count();
    assertEquals(NatsKvBuckets.ALL_BUCKETS.size(), distinct);
  }
}
