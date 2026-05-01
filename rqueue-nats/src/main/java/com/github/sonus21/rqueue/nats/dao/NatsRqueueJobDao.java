/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.nats.dao;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.nats.kv.NatsKvBuckets;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

/**
 * NATS-backed {@link RqueueJobDao} using a JetStream KV bucket as the job store. Entries are
 * keyed by job id and serialized via Java serialization. Look-ups by message id walk the bucket
 * keys; for the volumes rqueue typically tracks (current in-flight + recent retry history) this
 * is acceptable for v1 — the Redis impl uses an explicit reverse index, that's a follow-up here.
 *
 * <p>The {@code expiry} on save / create is currently best-effort: the bucket is created on the
 * first call with the expiry as its TTL. Subsequent saves reuse the same bucket regardless of
 * the requested per-key expiry.
 */
@Repository
@Conditional(NatsBackendCondition.class)
@DependsOn("natsKvBucketValidator")
public class NatsRqueueJobDao implements RqueueJobDao {

  private static final Logger log = Logger.getLogger(NatsRqueueJobDao.class.getName());
  private static final String BUCKET_NAME = NatsKvBuckets.JOBS;

  private final Connection connection;
  private final KeyValueManagement kvm;
  private final AtomicReference<KeyValue> kvRef = new AtomicReference<>();

  public NatsRqueueJobDao(Connection connection) throws IOException {
    this.connection = connection;
    this.kvm = connection.keyValueManagement();
  }

  private KeyValue ensureBucket(Duration ttl) throws IOException, JetStreamApiException {
    KeyValue cached = kvRef.get();
    if (cached != null) {
      return cached;
    }
    synchronized (this) {
      cached = kvRef.get();
      if (cached != null) {
        return cached;
      }
      try {
        KeyValueStatus status = kvm.getStatus(BUCKET_NAME);
        if (status != null) {
          KeyValue kv = connection.keyValue(BUCKET_NAME);
          kvRef.set(kv);
          return kv;
        }
      } catch (JetStreamApiException missing) {
        // fall through
      }
      KeyValueConfiguration.Builder cfg = KeyValueConfiguration.builder().name(BUCKET_NAME);
      if (ttl != null && !ttl.isZero() && !ttl.isNegative()) {
        cfg.ttl(ttl);
      }
      kvm.create(cfg.build());
      KeyValue kv = connection.keyValue(BUCKET_NAME);
      kvRef.set(kv);
      return kv;
    }
  }

  @Override
  public void createJob(RqueueJob rqueueJob, Duration expiry) {
    save(rqueueJob, expiry);
  }

  @Override
  public void save(RqueueJob rqueueJob, Duration expiry) {
    try {
      KeyValue kv = ensureBucket(expiry);
      kv.put(sanitize(rqueueJob.getId()), serialize(rqueueJob));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "save job " + rqueueJob.getId() + " failed", e);
    }
  }

  @Override
  public RqueueJob findById(String jobId) {
    return loadByKey(sanitize(jobId));
  }

  @Override
  public List<RqueueJob> findJobsByIdIn(Collection<String> jobIds) {
    List<RqueueJob> out = new ArrayList<>(jobIds.size());
    for (String id : jobIds) {
      RqueueJob j = findById(id);
      if (j != null) {
        out.add(j);
      }
    }
    return out;
  }

  @Override
  public List<RqueueJob> finByMessageId(String messageId) {
    if (messageId == null) {
      return Collections.emptyList();
    }
    return scanForMessageIds(Collections.singletonList(messageId));
  }

  @Override
  public List<RqueueJob> finByMessageIdIn(List<String> messageIds) {
    if (messageIds == null || messageIds.isEmpty()) {
      return Collections.emptyList();
    }
    return scanForMessageIds(messageIds);
  }

  @Override
  public void delete(String jobId) {
    try {
      KeyValue kv = ensureBucket(null);
      kv.delete(sanitize(jobId));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "delete job " + jobId + " failed", e);
    }
  }

  // ---- helpers ----------------------------------------------------------

  private RqueueJob loadByKey(String key) {
    try {
      KeyValue kv = ensureBucket(null);
      KeyValueEntry entry = kv.get(key);
      if (entry == null || entry.getValue() == null) {
        return null;
      }
      return deserialize(entry.getValue());
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "loadByKey " + key + " failed", e);
      return null;
    }
  }

  private List<RqueueJob> scanForMessageIds(Collection<String> messageIds) {
    try {
      KeyValue kv = ensureBucket(null);
      List<String> keys = new ArrayList<>(kv.keys());
      List<RqueueJob> out = new ArrayList<>();
      for (String k : keys) {
        RqueueJob j = loadByKey(k);
        if (j != null && messageIds.contains(j.getMessageId())) {
          out.add(j);
        }
      }
      return out;
    } catch (IOException | JetStreamApiException | InterruptedException e) {
      log.log(Level.WARNING, "scanForMessageIds failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return Collections.emptyList();
    }
  }

  private static byte[] serialize(RqueueJob job) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(job);
    }
    return baos.toByteArray();
  }

  private static RqueueJob deserialize(byte[] bytes) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      Object o = ois.readObject();
      return o instanceof RqueueJob ? (RqueueJob) o : null;
    } catch (IOException | ClassNotFoundException e) {
      log.log(Level.WARNING, "deserialize RqueueJob failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
