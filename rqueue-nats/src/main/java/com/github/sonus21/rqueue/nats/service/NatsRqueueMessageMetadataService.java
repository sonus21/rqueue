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
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.nats.kv.NatsKvBuckets;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
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
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * NATS-backed {@link RqueueMessageMetadataService} using a JetStream KV bucket as the metadata
 * store. Entries are keyed by metadata id (which the Redis impl computes via
 * {@link RqueueMessageUtils#getMessageMetaId}) and serialized as JSON.
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
@DependsOn("natsKvBucketValidator")
public class NatsRqueueMessageMetadataService implements RqueueMessageMetadataService {

  private static final Logger log =
      Logger.getLogger(NatsRqueueMessageMetadataService.class.getName());
  private static final String BUCKET_NAME = NatsKvBuckets.MESSAGE_METADATA;

  private final NatsProvisioner provisioner;
  private final com.github.sonus21.rqueue.serdes.RqueueSerDes serdes;

  public NatsRqueueMessageMetadataService(NatsProvisioner provisioner, com.github.sonus21.rqueue.serdes.RqueueSerDes serdes) {
    this.provisioner = provisioner;
    this.serdes = serdes;
  }

  private KeyValue kv() throws IOException, JetStreamApiException {
    return provisioner.ensureKv(BUCKET_NAME, null);
  }

  @Override
  public MessageMetadata get(String id) {
    return loadByKey(sanitize(id));
  }

  @Override
  public void delete(String id) {
    try {
      kv().delete(sanitize(id));
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
      kv().put(sanitize(messageMetadata.getId()), serialize(messageMetadata));
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
      List<String> keys = new ArrayList<>(kv().keys());
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
      List<String> keys = new ArrayList<>(kv().keys());
      String prefix = sanitize(queueName);
      for (String k : keys) {
        if (!k.startsWith(prefix)) {
          continue;
        }
        MessageMetadata m = loadByKey(k);
        if (m != null && m.getUpdatedOn() < before) {
          kv().delete(k);
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

  private byte[] serialize(MessageMetadata m) throws IOException {
    return serdes.serialize(m);
  }

  private MessageMetadata deserialize(byte[] bytes) {
    try {
      return serdes.deserialize(bytes, MessageMetadata.class);
    } catch (Exception e) {
      log.log(Level.WARNING, "deserialize MessageMetadata failed", e);
      return null;
    }
  }

  /** KV keys allow {@code [A-Za-z0-9_=.-]} only. */
  private static String sanitize(String key) {
    return key == null ? "_" : key.replaceAll("[^A-Za-z0-9_=.-]", "_");
  }
}
