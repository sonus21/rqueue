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
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.kv.NatsKvBuckets;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

/**
 * NATS-backed {@link RqueueSystemConfigDao} using a JetStream KV bucket as the queue-config
 * store. Entries are keyed by {@link QueueConfig#getName()} and serialized via standard Java
 * serialization, matching the Redis impl which also relies on
 * {@link com.github.sonus21.rqueue.models.SerializableBase}.
 *
 * <p>An in-process cache mirrors the Redis impl's {@code byCachedXxx} methods for parity.
 * {@link #clearCacheByName(String)} evicts; {@link #saveQConfig(QueueConfig)} keeps the cache
 * in sync.
 */
@Repository
@Conditional(NatsBackendCondition.class)
@DependsOn("natsKvBucketValidator")
public class NatsRqueueSystemConfigDao implements RqueueSystemConfigDao {

  private static final Logger log = Logger.getLogger(NatsRqueueSystemConfigDao.class.getName());
  private static final String BUCKET_NAME = NatsKvBuckets.QUEUE_CONFIG;

  private final NatsProvisioner provisioner;
  private final com.github.sonus21.rqueue.serdes.RqueueSerDes serdes;
  private final ConcurrentHashMap<String, QueueConfig> cache = new ConcurrentHashMap<>();

  public NatsRqueueSystemConfigDao(NatsProvisioner provisioner, com.github.sonus21.rqueue.serdes.RqueueSerDes serdes) {
    this.provisioner = provisioner;
    this.serdes = serdes;
  }

  private KeyValue kv() throws IOException, JetStreamApiException {
    return provisioner.ensureKv(BUCKET_NAME, null);
  }

  @Override
  public QueueConfig getConfigByName(String name) {
    return getConfigByName(name, true);
  }

  @Override
  public QueueConfig getConfigByName(String name, boolean cached) {
    if (cached) {
      QueueConfig hit = cache.get(name);
      if (hit != null) {
        return hit;
      }
    }
    QueueConfig loaded = loadByKey(sanitize(name));
    if (loaded != null) {
      cache.put(name, loaded);
    }
    return loaded;
  }

  @Override
  public QueueConfig getQConfig(String id, boolean cached) {
    if (cached) {
      for (QueueConfig hit : cache.values()) {
        if (id != null && id.equals(hit.getId())) {
          return hit;
        }
      }
    }
    return scanForId(id);
  }

  @Override
  public List<QueueConfig> getConfigByNames(Collection<String> names) {
    List<QueueConfig> out = new ArrayList<>(names.size());
    for (String n : names) {
      QueueConfig c = getConfigByName(n);
      if (c != null) {
        out.add(c);
      }
    }
    return out;
  }

  @Override
  public List<QueueConfig> findAllQConfig(Collection<String> ids) {
    List<QueueConfig> out = new ArrayList<>(ids.size());
    for (String id : ids) {
      QueueConfig c = getQConfig(id, true);
      if (c != null) {
        out.add(c);
      }
    }
    return out;
  }

  @Override
  public void saveQConfig(QueueConfig queueConfig) {
    try {
      kv().put(sanitize(queueConfig.getName()), serialize(queueConfig));
      cache.put(queueConfig.getName(), queueConfig);
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "saveQConfig " + queueConfig.getName() + " failed", e);
    }
  }

  @Override
  public void saveAllQConfig(List<QueueConfig> newConfigs) {
    for (QueueConfig c : newConfigs) {
      saveQConfig(c);
    }
  }

  @Override
  public void clearCacheByName(String name) {
    cache.remove(name);
  }

  // ---- helpers ----------------------------------------------------------

  private QueueConfig loadByKey(String key) {
    try {
      KeyValueEntry entry = kv().get(key);
      if (entry == null || entry.getValue() == null) {
        return null;
      }
      return deserialize(entry.getValue());
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "loadByKey " + key + " failed", e);
      return null;
    }
  }

  private QueueConfig scanForId(String id) {
    if (id == null) {
      return null;
    }
    try {
      List<String> keys = new ArrayList<>(kv().keys());
      for (String k : keys) {
        QueueConfig c = loadByKey(k);
        if (c != null && id.equals(c.getId())) {
          return c;
        }
      }
      return null;
    } catch (IOException | JetStreamApiException | InterruptedException e) {
      log.log(Level.WARNING, "scanForId " + id + " failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

  private byte[] serialize(QueueConfig c) throws IOException {
    return serdes.serialize(c);
  }

  private QueueConfig deserialize(byte[] bytes) {
    try {
      return serdes.deserialize(bytes, QueueConfig.class);
    } catch (Exception e) {
      log.log(Level.WARNING, "deserialize QueueConfig failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
