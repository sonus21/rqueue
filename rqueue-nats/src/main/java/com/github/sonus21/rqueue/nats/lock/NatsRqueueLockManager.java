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

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.kv.NatsKvBuckets;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

/**
 * NATS-backed {@link RqueueLockManager} using a JetStream KV bucket as the lock store.
 *
 * <p>Acquire is implemented via {@link KeyValue#create} which writes only when the key doesn't
 * exist (revision == 0); on existing-key conflict the JetStream server rejects the write with
 * {@code wrong last sequence} and the lock is reported as not acquired. The bucket is created
 * lazily with the {@code duration} of the first acquire used as the TTL — long-running keys
 * past the TTL get garbage-collected by the server, so an orphaned lock from a crashed holder
 * eventually self-releases.
 *
 * <p>Release verifies that the stored value matches the caller's {@code lockValue} before
 * deleting, so a holder cannot release a lock another process re-acquired after expiry.
 */
@Component
@Conditional(NatsBackendCondition.class)
@DependsOn("natsKvBucketValidator")
public class NatsRqueueLockManager implements RqueueLockManager {

  private static final Logger log = Logger.getLogger(NatsRqueueLockManager.class.getName());
  private static final String BUCKET_NAME = NatsKvBuckets.LOCKS;

  private final NatsProvisioner provisioner;

  public NatsRqueueLockManager(NatsProvisioner provisioner) {
    this.provisioner = provisioner;
  }

  @Override
  public boolean acquireLock(String lockKey, String lockValue, Duration duration) {
    try {
      KeyValue kv = provisioner.ensureKv(BUCKET_NAME, duration);
      kv.create(sanitize(lockKey), lockValue.getBytes(StandardCharsets.UTF_8));
      return true;
    } catch (JetStreamApiException existing) {
      // Most common path: key already exists; another holder owns the lock.
      return false;
    } catch (IOException io) {
      log.log(Level.WARNING, "acquireLock " + lockKey + " I/O failure", io);
      return false;
    } catch (RuntimeException rt) {
      log.log(Level.WARNING, "acquireLock " + lockKey + " failed", rt);
      return false;
    }
  }

  @Override
  public boolean releaseLock(String lockKey, String lockValue) {
    try {
      KeyValue kv = provisioner.ensureKv(BUCKET_NAME, Duration.ofSeconds(60));
      String key = sanitize(lockKey);
      KeyValueEntry entry = kv.get(key);
      if (entry == null) {
        return false;
      }
      String stored = new String(entry.getValue(), StandardCharsets.UTF_8);
      if (!stored.equals(lockValue)) {
        return false;
      }
      kv.delete(key, entry.getRevision());
      return true;
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "releaseLock " + lockKey + " failed", e);
      return false;
    }
  }

  /**
   * KV keys allow {@code [A-Za-z0-9_=.-]} only; coerce other characters to {@code _} so the
   * caller can pass arbitrary lock keys. {@code $} and {@code #} surface in queue/listener
   * names from inner classes and the legacy separator respectively.
   */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
