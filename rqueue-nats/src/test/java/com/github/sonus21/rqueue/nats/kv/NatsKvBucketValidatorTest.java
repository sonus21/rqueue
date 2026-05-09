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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.nats.NatsUnitTest;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueStatus;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsKvBucketValidator}. Covers autoCreate skip, all-present pass,
 * missing-bucket failure, kvm IOException, and the InitializingBean path.
 */
@NatsUnitTest
class NatsKvBucketValidatorTest {

  private Connection connection;
  private KeyValueManagement kvm;

  @BeforeEach
  void setUp() throws IOException {
    connection = mock(Connection.class);
    kvm = mock(KeyValueManagement.class);
    when(connection.keyValueManagement()).thenReturn(kvm);
  }

  @Test
  void validate_autoCreateTrue_skipsCheckEntirely() throws IOException {
    // autoCreate=true → kvm should never be touched
    assertDoesNotThrow(() -> NatsKvBucketValidator.validate(connection, true));
    verify(connection, never()).keyValueManagement();
  }

  @Test
  void validate_allBucketsPresent_doesNotThrow() throws IOException, JetStreamApiException {
    KeyValueStatus status = mock(KeyValueStatus.class);
    when(kvm.getStatus(anyString())).thenReturn(status);

    assertDoesNotThrow(() -> NatsKvBucketValidator.validate(connection, false));
  }

  @Test
  void validate_oneBucketMissing_throwsIllegalStateListingBucket()
      throws IOException, JetStreamApiException {
    KeyValueStatus status = mock(KeyValueStatus.class);
    // All buckets present except LOCKS
    for (String bucket : NatsKvBuckets.ALL_BUCKETS) {
      if (bucket.equals(NatsKvBuckets.LOCKS)) {
        when(kvm.getStatus(bucket)).thenThrow(mock(JetStreamApiException.class));
      } else {
        when(kvm.getStatus(bucket)).thenReturn(status);
      }
    }

    IllegalStateException ex = assertThrows(
        IllegalStateException.class,
        () -> NatsKvBucketValidator.validate(connection, false));
    assertTrue(ex.getMessage().contains(NatsKvBuckets.LOCKS));
  }

  @Test
  void validate_multipleBucketsMissing_listsBothInException()
      throws IOException, JetStreamApiException {
    KeyValueStatus status = mock(KeyValueStatus.class);
    for (String bucket : NatsKvBuckets.ALL_BUCKETS) {
      if (bucket.equals(NatsKvBuckets.JOBS) || bucket.equals(NatsKvBuckets.MESSAGE_METADATA)) {
        when(kvm.getStatus(bucket)).thenThrow(mock(JetStreamApiException.class));
      } else {
        when(kvm.getStatus(bucket)).thenReturn(status);
      }
    }

    IllegalStateException ex = assertThrows(
        IllegalStateException.class,
        () -> NatsKvBucketValidator.validate(connection, false));
    assertTrue(ex.getMessage().contains(NatsKvBuckets.JOBS));
    assertTrue(ex.getMessage().contains(NatsKvBuckets.MESSAGE_METADATA));
  }

  @Test
  void validate_kvmIoException_throwsIllegalState() throws IOException {
    when(connection.keyValueManagement()).thenThrow(new IOException("connection lost"));

    IllegalStateException ex = assertThrows(
        IllegalStateException.class,
        () -> NatsKvBucketValidator.validate(connection, false));
    assertTrue(ex.getMessage().contains("KeyValueManagement"));
  }

  @Test
  void afterPropertiesSet_autoCreateFalse_allPresent_doesNotThrow()
      throws IOException, JetStreamApiException {
    KeyValueStatus status = mock(KeyValueStatus.class);
    when(kvm.getStatus(anyString())).thenReturn(status);

    NatsKvBucketValidator validator = new NatsKvBucketValidator(connection, false);
    assertDoesNotThrow(validator::afterPropertiesSet);
  }

  @Test
  void afterPropertiesSet_autoCreateTrue_doesNotTouchKvm() throws IOException {
    NatsKvBucketValidator validator = new NatsKvBucketValidator(connection, true);
    assertDoesNotThrow(validator::afterPropertiesSet);
    verify(connection, never()).keyValueManagement();
  }
}
