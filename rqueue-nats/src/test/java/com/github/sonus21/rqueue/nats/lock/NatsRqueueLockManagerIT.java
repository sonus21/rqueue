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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.nats.AbstractJetStreamIT;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link NatsRqueueLockManager}'s NATS KV-backed acquire / release semantics.
 *
 * <ul>
 *   <li>First acquire wins; concurrent acquire of the same key by a different holder fails.
 *   <li>Release with a matching value succeeds; release with a different value is a no-op.
 *   <li>After a successful release the key is again acquirable by anyone.
 * </ul>
 */
class NatsRqueueLockManagerIT extends AbstractJetStreamIT {

  private NatsRqueueLockManager lockManager;

  @BeforeEach
  void freshBucket() throws IOException, JetStreamApiException {
    KeyValueManagement kvm = connection.keyValueManagement();
    try {
      kvm.delete("rqueue-locks");
    } catch (JetStreamApiException notFound) {
      // bucket didn't exist; first run
    }
    NatsProvisioner provisioner = new NatsProvisioner(
        connection, connection.jetStreamManagement(), RqueueNatsConfig.defaults());
    lockManager = new NatsRqueueLockManager(provisioner);
  }

  @Test
  void acquireWhenKeyAbsent() {
    assertTrue(lockManager.acquireLock("k1", "holder-A", Duration.ofSeconds(10)));
  }

  @Test
  void acquireRejectsConcurrentHolder() {
    assertTrue(lockManager.acquireLock("k2", "holder-A", Duration.ofSeconds(10)));
    assertFalse(lockManager.acquireLock("k2", "holder-B", Duration.ofSeconds(10)));
  }

  @Test
  void releaseSucceedsForMatchingHolder() {
    lockManager.acquireLock("k3", "holder-A", Duration.ofSeconds(10));
    assertTrue(lockManager.releaseLock("k3", "holder-A"));
    // After release the key is acquirable again.
    assertTrue(lockManager.acquireLock("k3", "holder-B", Duration.ofSeconds(10)));
  }

  @Test
  void releaseRejectsForeignHolder() throws IOException, JetStreamApiException {
    lockManager.acquireLock("k4", "holder-A", Duration.ofSeconds(10));
    assertFalse(lockManager.releaseLock("k4", "holder-B"));
    // Lock still held: holder-A's value remains in the bucket.
    KeyValue kv = connection.keyValue("rqueue-locks");
    assertEquals("holder-A", new String(kv.get("k4").getValue()));
  }

  @Test
  void releaseOnAbsentKeyReturnsFalse() {
    assertFalse(lockManager.releaseLock("never-acquired", "holder-A"));
  }

  @Test
  void sanitizationCoercesIllegalKeyCharacters() {
    // The KV layer rejects '$', '#', etc.; lock manager sanitizes them transparently.
    assertTrue(
        lockManager.acquireLock("queue#$/with-illegal:chars", "holder-A", Duration.ofSeconds(10)));
  }
}
