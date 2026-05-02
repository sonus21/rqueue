/*
 * Copyright (c) 2024-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.sonus21.rqueue.nats.internal;

import com.github.sonus21.rqueue.enums.QueueType;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.CompressionOption;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Idempotent stream/consumer/KV provisioning helpers. All methods are safe to call repeatedly;
 * if the target object already exists with a compatible config, this class leaves it alone. If it
 * exists but with diverging config (e.g. ackWait, maxDeliver), this class logs a WARN and does
 * NOT auto-mutate so user customizations are preserved.
 */
public class NatsProvisioner {

  private static final Logger log = Logger.getLogger(NatsProvisioner.class.getName());

  private final Connection connection;
  private final KeyValueManagement kvm;
  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;

  private final ConcurrentHashMap<String, KeyValue> kvCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Object> kvLocks = new ConcurrentHashMap<>();

  // stream name → provisioned (set membership acts as the boolean flag)
  private final Set<String> streamsDone = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<String, Object> streamLocks = new ConcurrentHashMap<>();

  // "streamName/requestedConsumerName" → actual consumer name (may differ for stale-rebind)
  private final ConcurrentHashMap<String, String> consumerCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Object> consumerLocks = new ConcurrentHashMap<>();

  public NatsProvisioner(Connection connection, JetStreamManagement jsm, RqueueNatsConfig config)
      throws IOException {
    this.connection = connection;
    this.kvm = connection.keyValueManagement();
    this.jsm = jsm;
    this.config = config;
  }

  // ---- KV provisioning --------------------------------------------------

  /**
   * Returns a {@link KeyValue} handle for {@code bucketName}, creating the bucket on first call.
   * All buckets are created with S2 compression. A bucket-level TTL is applied at creation time
   * only; existing buckets are reused as-is.
   *
   * @param bucketName NATS KV bucket name (e.g. {@code "rqueue-jobs"})
   * @param ttl        bucket-level max-age; {@code null} or non-positive = no TTL
   */
  public KeyValue ensureKv(String bucketName, Duration ttl)
      throws IOException, JetStreamApiException {
    KeyValue cached = kvCache.get(bucketName);
    if (cached != null) {
      return cached;
    }
    Object lock = kvLocks.computeIfAbsent(bucketName, k -> new Object());
    synchronized (lock) {
      cached = kvCache.get(bucketName);
      if (cached != null) {
        return cached;
      }
      try {
        KeyValueStatus status = kvm.getStatus(bucketName);
        if (status != null) {
          KeyValue kv = connection.keyValue(bucketName);
          kvCache.put(bucketName, kv);
          return kv;
        }
      } catch (JetStreamApiException missing) {
        // bucket absent — fall through to create
      }
      RqueueNatsConfig.StreamDefaults sd = config.getStreamDefaults();
      KeyValueConfiguration.Builder cfg = KeyValueConfiguration.builder()
          .name(bucketName)
          .compression(true)
          .replicas(sd.getReplicas())
          .storageType(sd.getStorage());
      if (ttl != null && !ttl.isZero() && !ttl.isNegative()) {
        cfg.ttl(ttl);
      }
      kvm.create(cfg.build());
      KeyValue kv = connection.keyValue(bucketName);
      kvCache.put(bucketName, kv);
      return kv;
    }
  }

  // ---- Stream provisioning ----------------------------------------------

  /**
   * Ensure a JetStream stream exists with the given subjects, using {@link QueueType#QUEUE}
   * (WorkQueue retention) as the default. Callers that have a {@link QueueType} available should
   * use {@link #ensureStream(String, List, QueueType)} instead.
   */
  public void ensureStream(String streamName, List<String> subjects) {
    ensureStream(streamName, subjects, QueueType.QUEUE);
  }

  /**
   * Ensure a JetStream stream exists with the given subjects and retention policy derived from
   * {@code queueType}:
   * <ul>
   *   <li>{@link QueueType#QUEUE} — {@link io.nats.client.api.RetentionPolicy#WorkQueue}: each
   *       message is delivered to exactly one consumer; competing-consumer semantics.
   *   <li>{@link QueueType#STREAM} — {@link io.nats.client.api.RetentionPolicy#Limits}: every
   *       independent durable consumer group receives all messages; stream/fan-out semantics.
   * </ul>
   *
   * <p>Hits the NATS backend at most once per stream name per process lifetime; subsequent calls
   * return immediately from the in-process cache. If the stream already exists with a different
   * retention policy, a WARNING is logged and the existing config is left untouched.
   */
  public void ensureStream(String streamName, List<String> subjects, QueueType queueType) {
    if (streamsDone.contains(streamName)) {
      return;
    }
    Object lock = streamLocks.computeIfAbsent(streamName, k -> new Object());
    synchronized (lock) {
      if (streamsDone.contains(streamName)) {
        return;
      }
      try {
        StreamInfo existing = safeGetStreamInfo(streamName);
        RetentionPolicy desired = queueType == QueueType.STREAM
            ? RetentionPolicy.Limits
            : RetentionPolicy.WorkQueue;
        if (existing == null) {
          if (!config.isAutoCreateStreams()) {
            throw new RqueueNatsException(
                "Stream '" + streamName + "' does not exist and autoCreateStreams=false");
          }
          RqueueNatsConfig.StreamDefaults sd = config.getStreamDefaults();
          StreamConfiguration.Builder b = StreamConfiguration.builder()
              .name(streamName)
              .subjects(subjects)
              .replicas(sd.getReplicas())
              .storageType(sd.getStorage())
              .retentionPolicy(desired)
              .duplicateWindow(sd.getDuplicateWindow())
              .compressionOption(CompressionOption.S2);
          if (sd.getMaxMsgs() > 0) {
            b.maxMessages(sd.getMaxMsgs());
          }
          if (sd.getMaxBytes() > 0) {
            b.maxBytes(sd.getMaxBytes());
          }
          if (sd.getMaxAge() != null
              && !sd.getMaxAge().isZero()
              && !sd.getMaxAge().isNegative()) {
            b.maxAge(sd.getMaxAge());
          }
          jsm.addStream(b.build());
        } else {
          RetentionPolicy actual = existing.getConfiguration().getRetentionPolicy();
          if (actual != desired) {
            log.log(
                Level.WARNING,
                "Stream ''{0}'' exists with retention={1} but queueMode requires retention={2}"
                    + " — leaving existing config in place.",
                new Object[] {streamName, actual, desired});
          }
        }
      } catch (IOException | JetStreamApiException e) {
        throw new RqueueNatsException(
            "Failed to ensure stream '" + streamName + "' for subjects " + subjects, e);
      }
      streamsDone.add(streamName);
    }
  }

  /**
   * Ensure a durable pull consumer exists, returning the consumer name.
   * Hits the NATS backend at most once per (stream, consumer) pair per process lifetime.
   *
   * <p>No filter subject is set on the consumer: each queue already has its own dedicated stream
   * with a single subject, so a filter would be redundant. More importantly, omitting the filter
   * allows multiple independent consumer groups (fan-out) to coexist on the same stream — NATS
   * rejects two consumers with the same filter subject (error 10100) regardless of retention type.
   */
  public String ensureConsumer(
      String streamName,
      String consumerName,
      Duration ackWait,
      long maxDeliver,
      long maxAckPending) {
    String cacheKey = streamName + "/" + consumerName;
    String cached = consumerCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }
    Object lock = consumerLocks.computeIfAbsent(cacheKey, k -> new Object());
    synchronized (lock) {
      cached = consumerCache.get(cacheKey);
      if (cached != null) {
        return cached;
      }
      String actual =
          doEnsureConsumer(streamName, consumerName, ackWait, maxDeliver, maxAckPending);
      consumerCache.put(cacheKey, actual);
      return actual;
    }
  }

  private String doEnsureConsumer(
      String streamName,
      String consumerName,
      Duration ackWait,
      long maxDeliver,
      long maxAckPending) {
    try {
      ConsumerInfo info = safeGetConsumerInfo(streamName, consumerName);
      if (info != null) {
        ConsumerConfiguration cc = info.getConsumerConfiguration();
        if (cc.getAckWait() != null && !cc.getAckWait().equals(ackWait)) {
          log.log(
              Level.WARNING,
              "Consumer " + streamName + "/" + consumerName
                  + " ackWait differs (existing=" + cc.getAckWait()
                  + ", desired=" + ackWait + ") - leaving existing config in place.");
        }
        if (cc.getMaxDeliver() != maxDeliver) {
          log.log(
              Level.WARNING,
              "Consumer " + streamName + "/" + consumerName
                  + " maxDeliver differs (existing=" + cc.getMaxDeliver()
                  + ", desired=" + maxDeliver + ") - leaving existing config in place.");
        }
        return consumerName;
      }
      if (!config.isAutoCreateConsumers()) {
        throw new RqueueNatsException("Consumer '" + consumerName + "' on stream '" + streamName
            + "' does not exist and autoCreateConsumers=false");
      }
      jsm.addOrUpdateConsumer(
          streamName,
          ConsumerConfiguration.builder()
              .durable(consumerName)
              .ackPolicy(AckPolicy.Explicit)
              .deliverPolicy(DeliverPolicy.All)
              .ackWait(ackWait)
              .maxDeliver(maxDeliver)
              .maxAckPending(maxAckPending)
              .build());
      return consumerName;
    } catch (JetStreamApiException e) {
      throw new RqueueNatsException(
          "Failed to ensure consumer '" + consumerName + "' on stream '" + streamName + "'", e);
    } catch (IOException e) {
      throw new RqueueNatsException(
          "Failed to ensure consumer '" + consumerName + "' on stream '" + streamName + "'", e);
    }
  }

  /** Ensure a DLQ stream exists capturing dead-letter subjects (e.g. "rqueue.js.*.dlq"). */
  public void ensureDlqStream(String dlqStreamName, List<String> dlqSubjects) {
    if (!config.isAutoCreateDlqStream()) {
      return;
    }
    ensureStream(dlqStreamName, dlqSubjects);
  }

  // ---- private helpers --------------------------------------------------

  private StreamInfo safeGetStreamInfo(String streamName)
      throws IOException, JetStreamApiException {
    try {
      return jsm.getStreamInfo(streamName);
    } catch (JetStreamApiException e) {
      // 10059 = stream not found
      if (e.getApiErrorCode() == 10059 || e.getErrorCode() == 404) {
        return null;
      }
      throw e;
    }
  }

  private ConsumerInfo safeGetConsumerInfo(String streamName, String consumerName)
      throws IOException, JetStreamApiException {
    try {
      return jsm.getConsumerInfo(streamName, consumerName);
    } catch (JetStreamApiException e) {
      // 10014 = consumer not found, 10059 = stream not found
      if (e.getApiErrorCode() == 10014 || e.getApiErrorCode() == 10059 || e.getErrorCode() == 404) {
        return null;
      }
      throw e;
    }
  }
}
