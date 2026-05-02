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
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.kv.NatsKvBuckets;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

/**
 * NATS-backed {@link RqueueJobDao} using a JetStream KV bucket as the job store. Entries are
 * keyed by job id and serialized as JSON. Look-ups by message id walk the bucket
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

  private final NatsProvisioner provisioner;
  private final com.github.sonus21.rqueue.serdes.RqueueSerDes serdes;

  public NatsRqueueJobDao(NatsProvisioner provisioner, com.github.sonus21.rqueue.serdes.RqueueSerDes serdes) {
    this.provisioner = provisioner;
    this.serdes = serdes;
  }

  private KeyValue kv(Duration ttl) throws IOException, JetStreamApiException {
    return provisioner.ensureKv(BUCKET_NAME, ttl);
  }

  @Override
  public void createJob(RqueueJob rqueueJob, Duration expiry) {
    save(rqueueJob, expiry);
  }

  @Override
  public void save(RqueueJob rqueueJob, Duration expiry) {
    try {
      kv(expiry).put(sanitize(rqueueJob.getId()), serialize(rqueueJob));
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
      kv(null).delete(sanitize(jobId));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "delete job " + jobId + " failed", e);
    }
  }

  // ---- helpers ----------------------------------------------------------

  private RqueueJob loadByKey(String key) {
    try {
      KeyValueEntry entry = kv(null).get(key);
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
      List<String> keys = new ArrayList<>(kv(null).keys());
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

  private byte[] serialize(RqueueJob job) throws IOException {
    return serdes.serialize(job);
  }

  private RqueueJob deserialize(byte[] bytes) {
    try {
      return serdes.deserialize(bytes, RqueueJob.class);
    } catch (Exception e) {
      log.log(Level.WARNING, "deserialize RqueueJob failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
