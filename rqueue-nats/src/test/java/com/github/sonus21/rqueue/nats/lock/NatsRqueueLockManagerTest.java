/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.rqueue.nats.NatsUnitTest;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NatsRqueueLockManager}: acquire/release happy paths and all failure
 * branches, plus the sanitize helper exercised through public API.
 */
@NatsUnitTest
class NatsRqueueLockManagerTest {

  private NatsProvisioner provisioner;
  private KeyValue kv;
  private NatsRqueueLockManager lockManager;

  @BeforeEach
  void setUp() throws IOException, JetStreamApiException {
    provisioner = mock(NatsProvisioner.class);
    kv = mock(KeyValue.class);
    when(provisioner.ensureKv(anyString(), any(Duration.class))).thenReturn(kv);
    lockManager = new NatsRqueueLockManager(provisioner);
  }

  // ---- acquireLock --------------------------------------------------------

  @Test
  void acquireLock_keyAbsent_returnsTrue() throws IOException, JetStreamApiException {
    when(kv.create(anyString(), any(byte[].class))).thenReturn(1L);
    assertTrue(lockManager.acquireLock("myLock", "owner-1", Duration.ofSeconds(10)));
  }

  @Test
  void acquireLock_keyAlreadyExists_returnsFalse() throws IOException, JetStreamApiException {
    JetStreamApiException conflict = mock(JetStreamApiException.class);
    when(kv.create(anyString(), any(byte[].class))).thenThrow(conflict);
    assertFalse(lockManager.acquireLock("myLock", "owner-1", Duration.ofSeconds(10)));
  }

  @Test
  void acquireLock_ioException_returnsFalse() throws IOException, JetStreamApiException {
    when(kv.create(anyString(), any(byte[].class))).thenThrow(new IOException("network error"));
    assertFalse(lockManager.acquireLock("myLock", "owner-1", Duration.ofSeconds(10)));
  }

  @Test
  void acquireLock_runtimeException_returnsFalse() throws IOException, JetStreamApiException {
    when(kv.create(anyString(), any(byte[].class))).thenThrow(new RuntimeException("unexpected"));
    assertFalse(lockManager.acquireLock("myLock", "owner-1", Duration.ofSeconds(10)));
  }

  @Test
  void acquireLock_sanitizesSpecialCharsInKey() throws IOException, JetStreamApiException {
    when(kv.create(anyString(), any(byte[].class))).thenReturn(1L);
    // keys with '$' and '#' (common in inner-class/legacy names) must be sanitized
    assertTrue(lockManager.acquireLock("queue$class#key", "v", Duration.ofSeconds(5)));
    verify(kv).create(eq("queue_class_key"), any(byte[].class));
  }

  @Test
  void acquireLock_passesLockValueAsUtf8Bytes() throws IOException, JetStreamApiException {
    when(kv.create(anyString(), any(byte[].class))).thenReturn(1L);
    lockManager.acquireLock("k", "my-value", Duration.ofSeconds(5));
    verify(kv).create(eq("k"), eq("my-value".getBytes(StandardCharsets.UTF_8)));
  }

  // ---- releaseLock --------------------------------------------------------

  @Test
  void releaseLock_matchingValue_deletesAndReturnsTrue()
      throws IOException, JetStreamApiException {
    KeyValueEntry entry = mock(KeyValueEntry.class);
    when(entry.getValue()).thenReturn("owner-1".getBytes(StandardCharsets.UTF_8));
    when(entry.getRevision()).thenReturn(3L);
    when(kv.get(anyString())).thenReturn(entry);

    assertTrue(lockManager.releaseLock("myLock", "owner-1"));
    verify(kv).delete("myLock", 3L);
  }

  @Test
  void releaseLock_valueMismatch_returnsFalseWithoutDelete()
      throws IOException, JetStreamApiException {
    KeyValueEntry entry = mock(KeyValueEntry.class);
    when(entry.getValue()).thenReturn("owner-2".getBytes(StandardCharsets.UTF_8));
    when(kv.get(anyString())).thenReturn(entry);

    assertFalse(lockManager.releaseLock("myLock", "owner-1"));
    verify(kv, never()).delete(anyString(), anyLong());
  }

  @Test
  void releaseLock_entryNotFound_returnsFalse() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenReturn(null);
    assertFalse(lockManager.releaseLock("myLock", "owner-1"));
    verify(kv, never()).delete(anyString(), anyLong());
  }

  @Test
  void releaseLock_ioException_returnsFalse() throws IOException, JetStreamApiException {
    when(kv.get(anyString())).thenThrow(new IOException("kv unavailable"));
    assertFalse(lockManager.releaseLock("myLock", "owner-1"));
  }

  @Test
  void releaseLock_deleteThrowsJsApiException_returnsFalse()
      throws IOException, JetStreamApiException {
    KeyValueEntry entry = mock(KeyValueEntry.class);
    when(entry.getValue()).thenReturn("owner-1".getBytes(StandardCharsets.UTF_8));
    when(entry.getRevision()).thenReturn(1L);
    when(kv.get(anyString())).thenReturn(entry);
    JetStreamApiException ex = mock(JetStreamApiException.class);
    doThrow(ex).when(kv).delete(anyString(), anyLong());

    assertFalse(lockManager.releaseLock("myLock", "owner-1"));
  }

  @Test
  void releaseLock_sanitizesKey() throws IOException, JetStreamApiException {
    KeyValueEntry entry = mock(KeyValueEntry.class);
    when(entry.getValue()).thenReturn("v".getBytes(StandardCharsets.UTF_8));
    when(entry.getRevision()).thenReturn(1L);
    when(kv.get(anyString())).thenReturn(entry);

    lockManager.releaseLock("a$b#c", "v");
    verify(kv).get("a_b_c");
  }
}
