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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.annotation.Conditional;
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
public class NatsRqueueSystemConfigDao implements RqueueSystemConfigDao {

  private static final Logger log = Logger.getLogger(NatsRqueueSystemConfigDao.class.getName());
  private static final String BUCKET_NAME = "rqueue-queue-config";

  private final Connection connection;
  private final KeyValueManagement kvm;
  private final AtomicReference<KeyValue> kvRef = new AtomicReference<>();
  private final ConcurrentHashMap<String, QueueConfig> cache = new ConcurrentHashMap<>();

  public NatsRqueueSystemConfigDao(Connection connection) throws IOException {
    this.connection = connection;
    this.kvm = connection.keyValueManagement();
  }

  private KeyValue ensureBucket() throws IOException, JetStreamApiException {
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
        // fall through to create
      }
      kvm.create(KeyValueConfiguration.builder().name(BUCKET_NAME).build());
      KeyValue kv = connection.keyValue(BUCKET_NAME);
      kvRef.set(kv);
      return kv;
    }
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
      KeyValue kv = ensureBucket();
      kv.put(sanitize(queueConfig.getName()), serialize(queueConfig));
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
      KeyValue kv = ensureBucket();
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

  private QueueConfig scanForId(String id) {
    if (id == null) {
      return null;
    }
    try {
      KeyValue kv = ensureBucket();
      List<String> keys = new ArrayList<>(kv.keys());
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

  private static byte[] serialize(QueueConfig c) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(c);
    }
    return baos.toByteArray();
  }

  private static QueueConfig deserialize(byte[] bytes) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      Object o = ois.readObject();
      return o instanceof QueueConfig ? (QueueConfig) o : null;
    } catch (IOException | ClassNotFoundException e) {
      log.log(Level.WARNING, "deserialize QueueConfig failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
