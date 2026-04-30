/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.nats.service;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
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
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * NATS-backed {@link RqueueMessageMetadataService} using a JetStream KV bucket as the metadata
 * store. Entries are keyed by metadata id (which the Redis impl computes via
 * {@link RqueueMessageUtils#getMessageMetaId}) and serialized via Java serialization.
 *
 * <p>Per-queue read methods ({@link #readMessageMetadataForQueue}) walk the bucket; rqueue
 * normally tracks recent metadata only so the volume is acceptable for v1. The Redis impl keeps
 * a per-queue ZSET as an explicit reverse index — that's a follow-up here.
 *
 * <p>{@link #saveReactive} wraps the synchronous {@code save} in a {@code Mono}; rqueue's
 * reactive enqueue path is the only caller and short-circuits storeMessageMetadata when the
 * broker has {@code !usesPrimaryHandlerDispatch}, so this method is rarely hit in practice.
 */
@Service
@Conditional(NatsBackendCondition.class)
public class NatsRqueueMessageMetadataService implements RqueueMessageMetadataService {

  private static final Logger log =
      Logger.getLogger(NatsRqueueMessageMetadataService.class.getName());
  private static final String BUCKET_NAME = "rqueue-message-metadata";

  private final Connection connection;
  private final KeyValueManagement kvm;
  private final AtomicReference<KeyValue> kvRef = new AtomicReference<>();

  public NatsRqueueMessageMetadataService(Connection connection) throws IOException {
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
        // fall through
      }
      kvm.create(KeyValueConfiguration.builder().name(BUCKET_NAME).build());
      KeyValue kv = connection.keyValue(BUCKET_NAME);
      kvRef.set(kv);
      return kv;
    }
  }

  @Override
  public MessageMetadata get(String id) {
    return loadByKey(sanitize(id));
  }

  @Override
  public void delete(String id) {
    try {
      KeyValue kv = ensureBucket();
      kv.delete(sanitize(id));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "delete metadata " + id + " failed", e);
    }
  }

  @Override
  public void deleteAll(Collection<String> ids) {
    for (String id : ids) {
      delete(id);
    }
  }

  @Override
  public List<MessageMetadata> findAll(Collection<String> ids) {
    List<MessageMetadata> out = new ArrayList<>(ids.size());
    for (String id : ids) {
      MessageMetadata m = get(id);
      if (m != null) {
        out.add(m);
      }
    }
    return out;
  }

  @Override
  public void save(MessageMetadata messageMetadata, Duration ttl, boolean checkUnique) {
    try {
      KeyValue kv = ensureBucket();
      kv.put(sanitize(messageMetadata.getId()), serialize(messageMetadata));
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "save metadata " + messageMetadata.getId() + " failed", e);
    }
  }

  @Override
  public MessageMetadata getByMessageId(String queueName, String messageId) {
    return get(RqueueMessageUtils.getMessageMetaId(queueName, messageId));
  }

  @Override
  public boolean deleteMessage(String queueName, String messageId, Duration ttl) {
    String metaId = RqueueMessageUtils.getMessageMetaId(queueName, messageId);
    MessageMetadata m = get(metaId);
    if (m == null) {
      return false;
    }
    m.setDeleted(true);
    m.setDeletedOn(System.currentTimeMillis());
    save(m, ttl, false);
    return true;
  }

  @Override
  public MessageMetadata getOrCreateMessageMetadata(RqueueMessage rqueueMessage) {
    String metaId =
        RqueueMessageUtils.getMessageMetaId(rqueueMessage.getQueueName(), rqueueMessage.getId());
    MessageMetadata existing = get(metaId);
    return existing != null ? existing : new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
  }

  @Override
  public Mono<Boolean> saveReactive(MessageMetadata m, Duration ttl, boolean checkUnique) {
    return Mono.fromCallable(() -> {
      save(m, ttl, checkUnique);
      return Boolean.TRUE;
    });
  }

  @Override
  public List<TypedTuple<MessageMetadata>> readMessageMetadataForQueue(
      String queueName, long start, long end) {
    // The Redis impl uses a ZSET sorted by createdAt for pagination. We don't have a sorted
    // index here, so this returns all metadata for the queue and the caller paginates.
    try {
      KeyValue kv = ensureBucket();
      List<String> keys = new ArrayList<>(kv.keys());
      List<TypedTuple<MessageMetadata>> out = new ArrayList<>();
      String prefix = sanitize(queueName);
      for (String k : keys) {
        if (!k.startsWith(prefix)) {
          continue;
        }
        MessageMetadata m = loadByKey(k);
        if (m != null) {
          out.add(TypedTuple.of(m, (double) m.getUpdatedOn()));
        }
      }
      return out;
    } catch (IOException | JetStreamApiException | InterruptedException e) {
      log.log(Level.WARNING, "readMessageMetadataForQueue " + queueName + " failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return Collections.emptyList();
    }
  }

  @Override
  public void saveMessageMetadataForQueue(
      String queueName, MessageMetadata messageMetadata, Long ttlInMillisecond) {
    save(
        messageMetadata,
        ttlInMillisecond == null ? null : Duration.ofMillis(ttlInMillisecond),
        false);
  }

  @Override
  public void deleteQueueMessages(String queueName, long before) {
    try {
      KeyValue kv = ensureBucket();
      List<String> keys = new ArrayList<>(kv.keys());
      String prefix = sanitize(queueName);
      for (String k : keys) {
        if (!k.startsWith(prefix)) {
          continue;
        }
        MessageMetadata m = loadByKey(k);
        if (m != null && m.getUpdatedOn() < before) {
          kv.delete(k);
        }
      }
    } catch (IOException | JetStreamApiException | InterruptedException e) {
      log.log(Level.WARNING, "deleteQueueMessages " + queueName + " failed", e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  // ---- helpers ----------------------------------------------------------

  private MessageMetadata loadByKey(String key) {
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

  private static byte[] serialize(MessageMetadata m) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(m);
    }
    return baos.toByteArray();
  }

  private static MessageMetadata deserialize(byte[] bytes) {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      Object o = ois.readObject();
      return o instanceof MessageMetadata ? (MessageMetadata) o : null;
    } catch (IOException | ClassNotFoundException e) {
      log.log(Level.WARNING, "deserialize MessageMetadata failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
